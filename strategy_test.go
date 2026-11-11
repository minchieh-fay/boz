package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// TestRollingWindow 验证当前 K 线计入窗口、过期极值移除、同区间乱序回报不缩小极值。
func TestRollingWindow(t *testing.T) {
	// step.1 构造包含当前区间的连续窗口并检查严格振幅门槛。
	m := newMarket(2)
	cfg := Config{KlineCount: 2, MinMoveRate: .3, StalePriceSeconds: 30}
	now := time.UnixMilli(1800000000000)
	current := now.UnixMilli() / 300000 * 300000
	m.update("TESTUSDT", Candle{current - 300000, decimal.NewFromInt(130), decimal.NewFromInt(100)}, 0)
	m.update("TESTUSDT", Candle{current, decimal.NewFromInt(120), decimal.NewFromInt(100)}, now.UnixMilli())
	if len(m.candidates(now, cfg)) != 0 {
		t.Fatal("等于阈值不应入选")
	}
	m.update("TESTUSDT", Candle{current, decimal.NewFromInt(131), decimal.NewFromInt(100)}, now.UnixMilli())
	m.update("TESTUSDT", Candle{current, decimal.NewFromInt(110), decimal.NewFromInt(105)}, now.UnixMilli()-1000)
	if len(m.candidates(now, cfg)) != 1 {
		t.Fatal("乱序累计回报不应覆盖更高极值")
	}
	// step.2 滚动新 K 线后，历史最高价退出；缺口及过期行情不允许开仓。
	next := now.Add(10 * time.Minute)
	m.update("TESTUSDT", Candle{current + 600000, decimal.NewFromInt(105), decimal.NewFromInt(100)}, next.UnixMilli())
	if len(m.candidates(next, cfg)) != 0 {
		t.Fatal("缺口窗口不应入选")
	}
	m.update("TESTUSDT", Candle{current + 300000, decimal.NewFromInt(110), decimal.NewFromInt(100)}, 0)
	if len(m.candidates(next, cfg)) != 0 {
		t.Fatal("被移出的旧极值不应保留")
	}
	m.update("TESTUSDT", Candle{current + 600000, decimal.NewFromInt(150), decimal.NewFromInt(100)}, next.UnixMilli())
	if len(m.candidates(next, cfg)) != 1 {
		t.Fatal("完整新窗口应该入选")
	}
	if len(m.candidates(next.Add(time.Minute), cfg)) != 0 {
		t.Fatal("过期数据不应入选")
	}
}

// TestCancelRaceAndRemaining 验证撤单期间新增成交只记一次，重挂仅补最终剩余量。
func TestCancelRaceAndRemaining(t *testing.T) {
	// step.1 模拟查询部分成交后，撤单竞态使最终成交从 0.2 增加到 0.5。
	reads, posts := 0, 0
	// 调用本目录辅助函数，构造只响应本地请求的交易所。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/ticker/bookTicker" {
			// 调用本目录辅助函数，返回稳定买卖盘口。
			writeTestJSON(t, w, map[string]string{"symbol": "TESTUSDT", "bidPrice": "100", "askPrice": "100.01"})
			return
		}
		switch r.Method {
		case http.MethodGet:
			reads++
			qty, status := "0.2", "PARTIALLY_FILLED"
			if reads >= 2 {
				qty, status = "0.5", "CANCELED"
			}
			quote := "20"
			if reads >= 2 {
				quote = "50"
			}
			// 调用本目录辅助函数，模拟撤单后最终成交回报。
			writeTestJSON(t, w, map[string]any{"symbol": "TESTUSDT", "clientOrderId": "boz_old", "price": "100",
				"executedQty": qty, "cumQuote": quote, "status": status})
		case http.MethodDelete:
			w.Write([]byte(`{}`))
		case http.MethodPost:
			posts++
			r.ParseForm()
			if r.Form.Get("quantity") != "0.5" || r.Form.Get("timeInForce") != "GTX" {
				t.Errorf("错误的剩余挂单: %s", r.Form.Encode())
			}
			// 调用本目录辅助函数，回报已接收剩余开仓。
			writeTestJSON(t, w, map[string]string{"status": "NEW"})
		}
	})
	// 调用本目录辅助函数，创建共同开仓目标。
	p := newTestPlan(e)
	l := p.Legs[0]
	l.Entry = &Intent{ID: "boz_old"}
	if err := e.settleEntry(context.Background(), p, l, true); err != nil {
		t.Fatal(err)
	}
	if !l.Filled.Equal(decimal.RequireFromString("0.5")) || l.Entry != nil {
		t.Fatalf("记账错误: %+v", l)
	}
	// step.2 已结清旧单再次处理不增加成交，新挂单只补最终剩余数量。
	if err := e.settleEntry(context.Background(), p, l, true); err != nil {
		t.Fatal(err)
	}
	if err := e.placeEntry(context.Background(), p, l); err != nil {
		t.Fatal(err)
	}
	if posts != 1 || reads != 2 {
		t.Fatalf("异常请求数 reads=%d posts=%d", reads, posts)
	}
	restored, err := loadState(e.Config.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Plans[p.Symbol].Legs[0].Entry.ID != l.Entry.ID {
		t.Fatal("订单身份没有持久化")
	}
}

// TestPostOnlyFallback 验证明确穿价拒单后按一个 tick 改价并保持 GTX。
func TestPostOnlyFallback(t *testing.T) {
	// step.1 模拟首个买单明确被 GTX 拒绝，次单接收。
	var prices []string
	// 调用本目录辅助函数，创建本地行情及下单回报。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// 调用本目录辅助函数，生成买一卖一报价。
			writeTestJSON(t, w, map[string]string{"symbol": "TESTUSDT", "bidPrice": "100", "askPrice": "100.01"})
			return
		}
		r.ParseForm()
		prices = append(prices, r.Form.Get("price"))
		if r.Form.Get("timeInForce") != "GTX" {
			t.Error("开仓不允许降级吃单")
		}
		if len(prices) == 1 {
			w.WriteHeader(400)
			w.Write([]byte(`{"code":-5022,"msg":"post only reject"}`))
			return
		}
		w.Write([]byte(`{"status":"NEW"}`))
	})
	// 调用本目录辅助函数，创建相同双向目标。
	p := newTestPlan(e)
	if err := e.placeEntry(context.Background(), p, p.Legs[0]); err != nil {
		t.Fatal(err)
	}
	// step.2 核对两次只相差一个被动价格档位。
	if len(prices) != 2 || prices[0] != "100" || prices[1] != "99.99" {
		t.Fatal(prices)
	}
}

// TestUnknownEntryNeverResubmits 验证执行未知和查询未找到都不会产生第二笔开仓。
func TestUnknownEntryNeverResubmits(t *testing.T) {
	// step.1 模拟交易所执行未知，随后查询返回订单未找到。
	posts := 0
	// 调用本目录辅助函数，使用本地未知回报模拟实盘网络故障。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/ticker/bookTicker" {
			w.Write([]byte(`{"symbol":"TESTUSDT","bidPrice":"100","askPrice":"100.01"}`))
			return
		}
		w.WriteHeader(503)
		if r.Method == http.MethodPost {
			posts++
			w.Write([]byte(`{"code":-1007,"msg":"unknown execution"}`))
			return
		}
		w.Write([]byte(`{"code":-2013,"msg":"not found"}`))
	})
	// 调用本目录辅助函数，建立开仓目标。
	p := newTestPlan(e)
	l := p.Legs[0]
	if err := e.placeEntry(context.Background(), p, l); err == nil {
		t.Fatal("应保留执行未知")
	}
	if l.Entry == nil {
		t.Fatal("未知身份丢失")
	}
	id := l.Entry.ID
	// step.2 多次查询及再次尝试开仓均保留原身份，不重复发送。
	for i := 0; i < 3; i++ {
		if _, err := e.readEntry(context.Background(), p, l); err == nil {
			t.Fatal("应保留查询未知")
		}
		if err := e.placeEntry(context.Background(), p, l); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 || l.Entry.ID != id {
		t.Fatal("发生重复发送或原身份丢失")
	}
}

// TestFullProtection 验证部分持仓也使用全量保护，双边按独立成本计算触发价。
func TestFullProtection(t *testing.T) {
	// step.1 收集两方向保护提交，检查全量平仓和仓位方向参数。
	prices := map[string]string{}
	// 调用本目录辅助函数，创建接收保护单的本地交易所。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Method != http.MethodPost || r.Form.Get("closePosition") != "true" ||
			r.Form.Has("quantity") || r.Form.Has("reduceOnly") {
			t.Errorf("保护参数错误 %s", r.Form.Encode())
		}
		direction, kind := r.Form.Get("positionSide"), r.Form.Get("type")
		expectedSide := "SELL"
		if direction == "SHORT" {
			expectedSide = "BUY"
		}
		if r.Form.Get("side") != expectedSide {
			t.Error("退出买卖方向错误")
		}
		prices[direction+kind] = r.Form.Get("triggerPrice")
		w.Write([]byte(`{"algoId":1,"algoStatus":"NEW"}`))
	})
	// 调用本目录辅助函数，创建双向目标但仅有部分实际持仓。
	p := newTestPlan(e)
	for _, l := range p.Legs {
		if err := e.protect(context.Background(), p, l, Position{Quantity: decimal.RequireFromString("0.2"),
			Entry: decimal.NewFromInt(100)}); err != nil {
			t.Fatal(err)
		}
	}
	// step.2 止盈止损比例按价格变化计算，不乘杠杆。
	expected := map[string]string{"LONGSTOP_MARKET": "90", "LONGTAKE_PROFIT_MARKET": "103",
		"SHORTSTOP_MARKET": "110", "SHORTTAKE_PROFIT_MARKET": "97"}
	for key, price := range expected {
		if prices[key] != price {
			t.Errorf("%s=%s want %s", key, prices[key], price)
		}
	}
}

// TestSharedRequestPacing 验证并发 REST 请求仍服从共享的最小发送间隔。
func TestSharedRequestPacing(t *testing.T) {
	// step.1 记录底层请求发出时间，多个调用者共享同一闸门。
	var mu sync.Mutex
	var times []time.Time
	transport := &PacedTransport{Base: http.DefaultTransport, Interval: 100 * time.Millisecond}
	// 调用本目录辅助函数，创建可记录请求时间的本地服务器。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		w.Write([]byte(`{}`))
	})
	client := &http.Client{Transport: transport}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(e.Client.BaseURL)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()
	// step.2 服务器侧接收时间允许少量调度误差，但不允许并发请求突发发送。
	if len(times) != 4 {
		t.Fatal(len(times))
	}
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap < 90*time.Millisecond {
			t.Errorf("间隔过短 %v", gap)
		}
	}
}

// TestCooldownPersistence 验证已完全退出的占用起点持久化，并按秒执行冷却。
func TestCooldownPersistence(t *testing.T) {
	// step.1 模拟之前占用而当前完全空闲的账户，禁止冷却中的币重新发单。
	posts := 0
	// 调用本目录辅助函数，模拟账户完全空仓且没有任何挂单。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts++
		}
		w.Write([]byte(`[]`))
	})
	e.Config.KlineCount, e.Market.Count = 1, 1
	e.Config.CooldownSeconds = 3600
	e.State.Occupied["TESTUSDT"] = true
	now := time.Now()
	e.Market.update("TESTUSDT", Candle{now.UnixMilli() / 300000 * 300000, decimal.NewFromInt(150),
		decimal.NewFromInt(100)}, now.UnixMilli())
	if err := e.scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	// step.2 重新加载状态，检查冷却不会在重启后丢失。
	restored, err := loadState(e.Config.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if posts != 0 || restored.Cooldowns["TESTUSDT"] == 0 {
		t.Fatal("未记录冷却或在冷却中开仓")
	}
	if time.Since(time.UnixMilli(restored.Cooldowns["TESTUSDT"])) > time.Second {
		t.Fatal("冷却起点不正确")
	}
}

// TestCorruptStateFailsClosed 验证损坏账本不会被当作空账户重新启动。
func TestCorruptStateFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	state, err := loadState(path)
	if err != nil {
		t.Fatal(err)
	}
	state.Version = 2
	if err := state.save(path); err != nil {
		t.Fatal(err)
	}
	if _, err := loadState(path); err == nil {
		t.Fatal("不兼容版本必须拒绝")
	}
}

// TestRejectedProtectionRequestsExit 验证保护明确拒绝后停止新增开仓并进入紧急退出状态。
func TestRejectedProtectionRequestsExit(t *testing.T) {
	// 调用本目录辅助函数，模拟条件单已经越过触发价而被拒绝。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		fmt.Fprint(w, `{"code":-2021,"msg":"would immediately trigger"}`)
	})
	// 调用本目录辅助函数，创建测试计划。
	p := newTestPlan(e)
	err := e.protect(context.Background(), p, p.Legs[0], Position{Quantity: decimal.NewFromInt(1), Entry: decimal.NewFromInt(100)})
	if err == nil || !p.Halt || !p.Emergency {
		t.Fatal("保护拒绝后必须停止新增开仓并请求退出")
	}
}

// TestTerminalStatuses 验证只有不可继续成交的普通订单状态可以释放身份。
func TestTerminalStatuses(t *testing.T) {
	for _, status := range []string{"NEW", "PARTIALLY_FILLED", "PENDING_CANCEL", ""} {
		// 调用本目录辅助函数，未知和活动状态不允许释放订单身份。
		if help_terminal(futures.OrderStatusType(status)) {
			t.Fatal(status)
		}
	}
}

// TestRestartToFullLifecycle 验证从已发送开仓身份恢复，到双向等量成交、托管退出和冷却的完整流程。
func TestRestartToFullLifecycle(t *testing.T) {
	// step.1 用本地交易所保存订单，模拟两个方向完全成交且持续持仓。
	entries := map[string]map[string]string{}
	algos := map[string]map[string]string{}
	live := true
	entryPosts, protectionPosts := 0, 0
	// 调用本目录辅助函数，构造可核对整个生命周期的离线交易所。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		switch r.URL.Path {
		case "/fapi/v1/ticker/bookTicker":
			w.Write([]byte(`{"symbol":"TESTUSDT","bidPrice":"100","askPrice":"100.01"}`))
		case "/fapi/v1/leverage":
			w.Write([]byte(`{"leverage":1}`))
		case "/fapi/v1/order":
			if r.Method == http.MethodPost {
				entryPosts++
				id, qty := r.Form.Get("newClientOrderId"), r.Form.Get("quantity")
				// 使用交易所累计成交回报，而非本地期望值驱动后续记账。
				entries[id] = map[string]string{"symbol": "TESTUSDT", "clientOrderId": id, "executedQty": qty,
					"cumQuote": decimal.RequireFromString(qty).Mul(decimal.NewFromInt(100)).String(),
					"status":   "FILLED", "price": r.Form.Get("price")}
				w.Write([]byte(`{"status":"NEW"}`))
				return
			}
			// 调用本目录辅助函数，返回原身份的最终成交。
			writeTestJSON(t, w, entries[r.Form.Get("origClientOrderId")])
		case "/fapi/v2/positionRisk":
			qty, cost := "0", "0"
			if live {
				qty, cost = "0.9", "100"
			}
			// 调用本目录辅助函数，返回双向独立仓位。
			writeTestJSON(t, w, []map[string]string{
				{"symbol": "TESTUSDT", "positionSide": "LONG", "positionAmt": qty, "entryPrice": cost},
				{"symbol": "TESTUSDT", "positionSide": "SHORT", "positionAmt": "-" + qty, "entryPrice": cost},
			})
		case "/fapi/v1/algoOrder":
			id := r.Form.Get("clientAlgoId")
			switch r.Method {
			case http.MethodPost:
				protectionPosts++
				algos[id] = map[string]string{"clientAlgoId": id, "algoStatus": "NEW"}
			case http.MethodDelete:
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				params, err := url.ParseQuery(string(body))
				if err != nil {
					t.Error(err)
					return
				}
				id = params.Get("clientAlgoId")
				if algos[id] == nil {
					t.Errorf("未知条件单 %s", id)
					w.WriteHeader(400)
					return
				}
				algos[id]["algoStatus"] = "CANCELED"
			}
			// 调用本目录辅助函数，按身份返回自有条件单状态。
			writeTestJSON(t, w, algos[id])
		case "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders":
			w.Write([]byte(`[]`))
		default:
			t.Errorf("意外路径 %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	if err := e.startPlan(context.Background(), "TESTUSDT"); err != nil {
		t.Fatal(err)
	}
	if entryPosts != 2 {
		t.Fatal("必须连续提交两个方向")
	}
	// step.2 模拟重启，只从持久化身份核对，不重复提交已成交开仓。
	restored, err := loadState(e.Config.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	e.State = restored
	p := e.State.Plans["TESTUSDT"]
	if err = e.maintainPlan(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	for _, l := range p.Legs {
		if !l.Filled.Equal(p.Target) || l.Entry != nil {
			t.Fatalf("方向未等量完成: %+v", l)
		}
	}
	if entryPosts != 2 || protectionPosts != 4 {
		t.Fatalf("重复开仓或保护不全: %d %d", entryPosts, protectionPosts)
	}
	if err = e.maintainPlan(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if entryPosts != 2 || protectionPosts != 4 {
		t.Fatal("完成后的维护不应重复开仓或替换同价保护")
	}
	// step.3 模拟市场完成两边退出，核对清理、冷却和重启后的结果。
	live = false
	if err = e.maintainPlan(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	restored, err = loadState(e.Config.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Plans) != 0 || restored.Cooldowns["TESTUSDT"] == 0 {
		t.Fatal("退出后未释放计划或记录冷却")
	}
	for _, order := range algos {
		if order["algoStatus"] != "CANCELED" {
			t.Fatal("残余保护未清理")
		}
	}
}

// TestUnconfirmedCancelBlocksReplacement 验证撤单返回未知且原单仍活跃时不会重挂第二张。
func TestUnconfirmedCancelBlocksReplacement(t *testing.T) {
	// step.1 模拟撤单超时后查询原单仍处于 NEW。
	posts := 0
	// 调用本目录辅助函数，模拟未能确定撤销的普通开仓单。
	e := newTestExchange(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(503)
			w.Write([]byte(`{"code":-1007,"msg":"execution unknown"}`))
		case http.MethodPost:
			posts++
			w.Write([]byte(`{}`))
		default:
			w.Write([]byte(`{"symbol":"TESTUSDT","clientOrderId":"boz_pending","price":"100","executedQty":"0",` +
				`"cumQuote":"0","status":"NEW"}`))
		}
	})
	// 调用本目录辅助函数，创建尚无成交的双向计划。
	p := newTestPlan(e)
	l := p.Legs[0]
	l.Entry = &Intent{ID: "boz_pending"}
	if err := e.settleEntry(context.Background(), p, l, true); err == nil {
		t.Fatal("不能把未知撤单视为成功")
	}
	// step.2 再次尝试开仓仍受原有身份约束。
	if err := e.placeEntry(context.Background(), p, l); err != nil {
		t.Fatal(err)
	}
	if posts != 0 || l.Entry == nil || l.Entry.ID != "boz_pending" {
		t.Fatal("撤单未确认就释放或重发")
	}
}

// TestProcessLock 验证同一状态目录只能由一个运行实例独占。
func TestProcessLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	// 调用本目录辅助函数，为第一实例取得独占账本锁。
	first, err := help_lockState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// 调用本目录辅助函数，第二实例必须在任何交易请求之前失败。
	second, err := help_lockState(path)
	if err == nil {
		second.Close()
		t.Fatal("第二实例不应获得同一账本锁")
	}
}
