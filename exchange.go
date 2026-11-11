package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// Position 表示交易所实际持仓，独立于本地累计开仓账本。
type Position struct {
	Quantity decimal.Decimal
	Entry    decimal.Decimal
}

// Account 汇总全账户持仓及普通、条件挂单，用于名额和冷却判断。
type Account struct {
	Positions map[string]map[string]Position
	Occupied  map[string]bool
	Orders    []*futures.Order
	Algos     []futures.GetAlgoOrderResp
}

// Exchange 封装正式 Binance 合约 SDK 及共享行情和持久化账本。
type Exchange struct {
	Client *futures.Client
	Config Config
	Market *Market
	State  *State
}

// newExchange 读取两行密钥并创建仅连接正式环境的限速客户端。
func newExchange(c Config, market *Market, state *State) (*Exchange, error) {
	data, err := os.ReadFile(c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("读取密钥: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return nil, fmt.Errorf("密钥文件必须为 API key 和 secret 各一行")
	}
	client := futures.NewClient(fields[0], fields[1])
	client.BaseURL = "https://fapi.binance.com"
	client.HTTPClient = &http.Client{Timeout: 15 * time.Second, Transport: &PacedTransport{
		Base: http.DefaultTransport, Interval: 100 * time.Millisecond,
	}}
	return &Exchange{Client: client, Config: c, Market: market, State: state}, nil
}

// initialize 校时、启用双向持仓并加载可交易 USDT 永续合约规则。
func (e *Exchange) initialize(ctx context.Context) ([]string, error) {
	// step.1 同步交易所时间，并在账户允许时切换双向持仓模式。
	if _, err := e.Client.NewSetServerTimeService().Do(ctx); err != nil {
		return nil, err
	}
	mode, err := e.Client.NewGetPositionModeService().Do(ctx)
	if err != nil {
		return nil, err
	}
	if !mode.DualSidePosition {
		if err = e.Client.NewChangePositionModeService().DualSide(true).Do(ctx); err != nil {
			return nil, fmt.Errorf("启用双向持仓失败，请检查现有持仓和挂单: %w", err)
		}
	}
	// step.2 筛选正在交易的合约并缓存限价下单过滤器。
	info, err := e.Client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return nil, err
	}
	var symbols []string
	for _, s := range info.Symbols {
		if s.QuoteAsset != "USDT" || s.ContractType != futures.ContractTypePerpetual || s.Status != "TRADING" {
			continue
		}
		// 调用本目录辅助函数，读取限价开仓所需的十进制交易规则。
		rules, err := help_rules(s)
		if err != nil {
			log.Printf("跳过合约: %v", err)
			continue
		}
		e.Market.Rules[s.Symbol] = rules
		symbols = append(symbols, s.Symbol)
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("没有有效 USDT 永续合约")
	}
	return symbols, nil
}

// history 用 REST 补齐一个合约的历史和当前 K 线，不覆盖更新的实时极值。
func (e *Exchange) history(ctx context.Context, symbol string) error {
	// step.1 获取窗口所需的交易所原生五分钟 K 线。
	bars, err := e.Client.NewKlinesService().Symbol(symbol).Interval("5m").Limit(e.Config.KlineCount).Do(ctx)
	if err != nil {
		return err
	}
	// step.2 合并已收盘和当前未收盘区间，实时新鲜度只由 WebSocket 更新。
	for _, b := range bars {
		// 调用本目录辅助函数，严格解析历史最高价。
		high, err := help_decimal(b.High)
		if err != nil {
			return err
		}
		// 调用本目录辅助函数，严格解析历史最低价。
		low, err := help_decimal(b.Low)
		if err != nil {
			return err
		}
		if !low.IsPositive() || high.LessThan(low) {
			return fmt.Errorf("%s 历史极值无效", symbol)
		}
		e.Market.update(symbol, Candle{Start: b.OpenTime, High: high, Low: low}, 0)
	}
	return nil
}

// positions 查询全账户或单币实际持仓，严格解析交易所回报。
func (e *Exchange) positions(ctx context.Context, symbol string) (map[string]map[string]Position, error) {
	// step.1 读取持仓快照，只保留非零持仓。
	service := e.Client.NewGetPositionRiskService()
	if symbol != "" {
		service.Symbol(symbol)
	}
	rows, err := service.Do(ctx)
	if err != nil {
		return nil, err
	}
	positions := map[string]map[string]Position{}
	for _, row := range rows {
		// 调用本目录辅助函数，避免损坏的数量被误判为空仓。
		quantity, err := help_decimal(row.PositionAmt)
		if err != nil {
			return nil, err
		}
		if quantity.IsZero() {
			continue
		}
		// step.2 校验双向仓位及成本，避免单向仓位误用于双向计划。
		if row.PositionSide != "LONG" && row.PositionSide != "SHORT" {
			return nil, fmt.Errorf("%s 出现非双向持仓 %s", row.Symbol, row.PositionSide)
		}
		// 调用本目录辅助函数，严格读取实际持仓成本。
		entry, err := help_decimal(row.EntryPrice)
		if err != nil || !entry.IsPositive() {
			return nil, fmt.Errorf("%s 持仓成本无效", row.Symbol)
		}
		if positions[row.Symbol] == nil {
			positions[row.Symbol] = map[string]Position{}
		}
		positions[row.Symbol][row.PositionSide] = Position{Quantity: quantity.Abs(), Entry: entry}
	}
	return positions, nil
}

// account 在每轮选币前读取所有仓位、普通挂单和条件挂单。
func (e *Exchange) account(ctx context.Context) (*Account, error) {
	// step.1 任意一次接口失败都停止本轮开仓，不用不完整快照选币。
	positions, err := e.positions(ctx, "")
	if err != nil {
		return nil, err
	}
	orders, err := e.Client.NewListOpenOrdersService().Do(ctx)
	if err != nil {
		return nil, err
	}
	algos, err := e.Client.NewListOpenAlgoOrdersService().Do(ctx)
	if err != nil {
		return nil, err
	}
	// step.2 把手工和其他策略的持仓、挂单也计入占用，避免同币冲突。
	a := &Account{Positions: positions, Orders: orders, Algos: algos, Occupied: map[string]bool{}}
	for symbol := range positions {
		a.Occupied[symbol] = true
	}
	for _, order := range orders {
		a.Occupied[order.Symbol] = true
	}
	for _, algo := range algos {
		a.Occupied[algo.Symbol] = true
	}
	return a, nil
}

// persist 在任何下单或订单身份变化前后提交账本，失败立即向上传播。
func (e *Exchange) persist() error {
	return e.State.save(e.Config.StateFile)
}

// validateOwnership 拒绝在缺少自有订单账本时继续启动，防止重启重复开仓。
func (e *Exchange) validateOwnership(a *Account) error {
	// step.1 建立账本中全部可恢复的订单身份集合。
	known := map[string]bool{}
	for _, p := range e.State.Plans {
		for _, l := range p.Legs {
			if l.Entry != nil {
				known[l.Entry.ID] = true
			}
			if l.Exit != nil {
				known[l.Exit.ID] = true
			}
			known[l.StopID], known[l.ProfitID] = true, true
		}
	}
	// step.2 交易所有未记账的 boz 订单时要求保留现场并修复账本。
	for _, order := range a.Orders {
		if strings.HasPrefix(order.ClientOrderID, "boz_") && !known[order.ClientOrderID] {
			return fmt.Errorf("发现无账本的策略挂单 %s，请恢复原状态文件", order.ClientOrderID)
		}
	}
	for _, order := range a.Algos {
		if strings.HasPrefix(order.ClientAlgoId, "boz_") && !known[order.ClientAlgoId] {
			return fmt.Errorf("发现无账本的策略条件单 %s，请恢复原状态文件", order.ClientAlgoId)
		}
	}
	return nil
}
