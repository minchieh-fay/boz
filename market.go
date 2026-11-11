package main

import (
	"sort"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// Candle 保存交易所五分钟区间的价格极值，时间戳单位为毫秒。
type Candle struct {
	Start int64
	High  decimal.Decimal
	Low   decimal.Decimal
}

// Window 保存固定长度窗口及最后一次实时更新的交易所时间。
type Window struct {
	Bars []Candle
	Tick int64
}

// Rules 保存限价开仓所用 LOT_SIZE、PRICE_FILTER 和名义金额限制。
type Rules struct {
	Step        decimal.Decimal
	MinQty      decimal.Decimal
	MaxQty      decimal.Decimal
	Tick        decimal.Decimal
	MinNotional decimal.Decimal
}

// Candidate 表示完整窗口振幅超过门槛的交易对。
type Candidate struct {
	Symbol string
	Move   decimal.Decimal
}

// Market 以锁保护历史快照和 WebSocket 累积 K 线。
type Market struct {
	mu      sync.RWMutex
	Count   int
	Windows map[string]*Window
	Rules   map[string]Rules
}

// newMarket 创建行情窗口及交易规则存储。
func newMarket(count int) *Market {
	return &Market{Count: count, Windows: map[string]*Window{}, Rules: map[string]Rules{}}
}

// update 按交易所开盘时间合并极值并滚动窗口，不依赖进程启动时间。
func (m *Market) update(symbol string, bar Candle, eventTime int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.Windows[symbol]
	if w == nil {
		w = &Window{}
		m.Windows[symbol] = w
	}
	// 调用本目录辅助函数，合并同一根 K 线并保留最新固定数量。
	w.Bars = help_mergeCandle(w.Bars, bar, m.Count)
	if eventTime > w.Tick {
		w.Tick = eventTime
	}
}

// candidates 仅从连续、完整且当前实时行情新鲜的窗口选择交易对。
func (m *Market) candidates(now time.Time, cfg Config) []Candidate {
	// step.1 检查窗口完整性及最新实时行情，计算包含当前 K 线的振幅。
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Candidate
	current := now.UnixMilli() / 300000 * 300000
	for symbol, w := range m.Windows {
		if len(w.Bars) != m.Count || now.UnixMilli()-w.Tick > int64(cfg.StalePriceSeconds*1000) ||
			w.Tick > now.Add(5*time.Second).UnixMilli() || w.Bars[len(w.Bars)-1].Start != current {
			continue
		}
		high, low := w.Bars[0].High, w.Bars[0].Low
		complete := true
		for i, b := range w.Bars {
			if i > 0 && b.Start-w.Bars[i-1].Start != 300000 {
				complete = false
			}
			high, low = decimal.Max(high, b.High), decimal.Min(low, b.Low)
		}
		if !complete || !low.IsPositive() {
			continue
		}
		move := high.Sub(low).Div(low)
		if move.GreaterThan(decimal.NewFromFloat(cfg.MinMoveRate)) {
			out = append(out, Candidate{Symbol: symbol, Move: move})
		}
	}
	// step.2 优先选择振幅最高的交易对，同幅度时按名称稳定排序。
	sort.Slice(out, func(i, j int) bool {
		if out[i].Move.Equal(out[j].Move) {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].Move.GreaterThan(out[j].Move)
	})
	return out
}

// onKline 接收交易所原生累计 K 线，避免漏算订阅前的区间极值。
func (m *Market) onKline(event *futures.WsKlineEvent) {
	high, e1 := decimal.NewFromString(event.Kline.High)
	low, e2 := decimal.NewFromString(event.Kline.Low)
	if e1 != nil || e2 != nil || !low.IsPositive() || high.LessThan(low) {
		return
	}
	m.update(event.Symbol, Candle{Start: event.Kline.StartTime, High: high, Low: low}, event.Time)
}
