package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// help_mergeCandle 合并同一区间的累计极值并按交易所时间保留最新窗口。
func help_mergeCandle(bars []Candle, bar Candle, count int) []Candle {
	for i := range bars {
		if bars[i].Start == bar.Start {
			bars[i].High = decimal.Max(bars[i].High, bar.High)
			bars[i].Low = decimal.Min(bars[i].Low, bar.Low)
			return bars
		}
	}
	bars = append(bars, bar)
	sort.Slice(bars, func(i, j int) bool { return bars[i].Start < bars[j].Start })
	if len(bars) > count {
		bars = bars[len(bars)-count:]
	}
	return bars
}

// help_decimal 将交易所数值转换为十进制定点值，拒绝损坏的接口回报。
func help_decimal(raw string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(raw)
	if err != nil {
		return d, fmt.Errorf("交易所数值无效 %q: %w", raw, err)
	}
	return d, nil
}

// help_floor 将价格或数量向下对齐交易所步进。
func help_floor(value, step decimal.Decimal) decimal.Decimal {
	return value.Div(step).Floor().Mul(step)
}

// help_ceil 将价格向上对齐交易所步进。
func help_ceil(value, step decimal.Decimal) decimal.Decimal {
	return value.Div(step).Ceil().Mul(step)
}

// help_id 生成长度小于 36 的策略自有订单编号。
func help_id(kind string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "boz_" + kind + "_" + hex.EncodeToString(b[:])
}

// help_code 判断 Binance 明确拒绝码，网络异常不视为确定拒单。
func help_code(err error, codes ...int64) bool {
	var api *common.APIError
	if !errors.As(err, &api) {
		return false
	}
	for _, code := range codes {
		if api.Code == code {
			return true
		}
	}
	return false
}

// help_definiteReject 区分明确业务拒绝和执行结果未知、限流及服务器异常。
func help_definiteReject(err error) bool {
	var api *common.APIError
	if !errors.As(err, &api) {
		return false
	}
	return api.Code <= -1100 && api.Code != -2013 && api.Code != -4111 && api.Code != -4116
}

// help_terminal 判断普通订单是否已确定停止继续成交。
func help_terminal(status futures.OrderStatusType) bool {
	switch string(status) {
	case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED":
		return true
	}
	return false
}

// help_sides 根据持仓方向确定开仓和退出的买卖方向。
func help_sides(direction string) (futures.SideType, futures.SideType) {
	if direction == "LONG" {
		return futures.SideTypeBuy, futures.SideTypeSell
	}
	return futures.SideTypeSell, futures.SideTypeBuy
}

// help_wait 使重连或退避等待可被退出信号中断。
func help_wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// help_rules 从限价订单过滤器中读取精确步进及最小名义金额。
func help_rules(symbol futures.Symbol) (Rules, error) {
	// step.1 按过滤器类型提取限价交易规则，保留所有十进制精度。
	var r Rules
	for _, f := range symbol.Filters {
		keys := map[string]*decimal.Decimal{}
		switch fmt.Sprint(f["filterType"]) {
		case "LOT_SIZE":
			keys = map[string]*decimal.Decimal{"stepSize": &r.Step, "minQty": &r.MinQty, "maxQty": &r.MaxQty}
		case "PRICE_FILTER":
			keys = map[string]*decimal.Decimal{"tickSize": &r.Tick}
		case "MIN_NOTIONAL":
			keys = map[string]*decimal.Decimal{"notional": &r.MinNotional}
		}
		for key, target := range keys {
			value, err := decimal.NewFromString(fmt.Sprint(f[key]))
			if err != nil {
				return r, err
			}
			*target = value
		}
	}
	// step.2 缺失关键过滤器时不允许对该合约开仓。
	if !r.Step.IsPositive() || !r.Tick.IsPositive() || !r.MinQty.IsPositive() || !r.MaxQty.IsPositive() {
		return r, fmt.Errorf("%s 缺少限价下单精度规则", symbol.Symbol)
	}
	return r, nil
}

// help_book 读取最佳买卖价，为两边 GTX 开仓提供各自被动价格。
func help_book(ctx context.Context, client *futures.Client, symbol string) (decimal.Decimal, decimal.Decimal, error) {
	rows, err := client.NewListBookTickersService().Symbol(symbol).Do(ctx)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	if len(rows) != 1 {
		return decimal.Zero, decimal.Zero, fmt.Errorf("%s 买卖盘口缺失", symbol)
	}
	bid, err := decimal.NewFromString(rows[0].BidPrice)
	if err != nil {
		return bid, decimal.Zero, err
	}
	ask, err := decimal.NewFromString(rows[0].AskPrice)
	if err != nil || !bid.IsPositive() || !ask.GreaterThan(bid) {
		return bid, ask, fmt.Errorf("%s 买卖盘口无效", symbol)
	}
	return bid, ask, nil
}

// help_recordFill 只记入同一订单新增的成交，撤单竞态和重复查询不会重复累计。
func help_recordFill(l *Leg, order *futures.Order) error {
	// step.1 严格校验累计成交量及成交额，拒绝倒退回报。
	qty, err := decimal.NewFromString(order.ExecutedQuantity)
	if err != nil {
		return err
	}
	quote := decimal.Zero
	if qty.IsPositive() {
		quote, err = decimal.NewFromString(order.CumQuote)
		if err != nil {
			return err
		}
		if !quote.IsPositive() {
			return fmt.Errorf("订单 %s 成交额无效", order.ClientOrderID)
		}
	}
	intent := l.Entry
	if qty.LessThan(intent.Accounted) || quote.LessThan(intent.Quote) {
		return fmt.Errorf("订单 %s 累计成交回报倒退", intent.ID)
	}
	// step.2 增量并入方向总账，并保留本单已记账水位。
	l.Filled = l.Filled.Add(qty.Sub(intent.Accounted))
	l.Quote = l.Quote.Add(quote.Sub(intent.Quote))
	intent.Accounted, intent.Quote = qty, quote
	return nil
}

// help_triggerPrices 以实际仓位成本计算方向独立的止损和止盈触发价。
func help_triggerPrices(entry decimal.Decimal, direction string, cfg Config, tick decimal.Decimal) (string, string) {
	stopRate, profitRate := 1+cfg.StopLossRate, 1-cfg.TakeProfitRate
	if direction == "LONG" {
		stopRate, profitRate = 1-cfg.StopLossRate, 1+cfg.TakeProfitRate
	}
	stop := entry.Mul(decimal.NewFromFloat(stopRate)).Div(tick).Round(0).Mul(tick)
	profit := entry.Mul(decimal.NewFromFloat(profitRate)).Div(tick).Round(0).Mul(tick)
	return stop.String(), profit.String()
}

// help_validQuantity 检查限价订单的数量和名义金额，不以额外开仓方式补齐尾差。
func help_validQuantity(qty, price decimal.Decimal, rules Rules) bool {
	return qty.GreaterThanOrEqual(rules.MinQty) && qty.LessThanOrEqual(rules.MaxQty) &&
		qty.Mul(price).GreaterThanOrEqual(rules.MinNotional) && qty.Mod(rules.Step).IsZero()
}

// help_lockState 防止两个进程同时使用同一本账本发出重复实盘订单。
func help_lockState(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("另一个进程正在使用状态文件 %s: %w", path, err)
	}
	return file, nil
}
