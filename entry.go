package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// startPlan 固定同币两边相同的目标数量，并连续提交两边被动开仓单。
func (e *Exchange) startPlan(ctx context.Context, symbol string) error {
	// step.1 以每边名义金额计算共同数量，并设置该币杠杆。
	// 调用本目录辅助函数，取得买卖两边可被动挂入的盘口。
	bid, ask, err := help_book(ctx, e.Client, symbol)
	if err != nil {
		return err
	}
	rules := e.Market.Rules[symbol]
	// 调用本目录辅助函数，按卖一价预算并向下对齐共同开仓数量。
	target := help_floor(decimal.NewFromFloat(e.Config.TradeUSDT).Div(ask), rules.Step)
	// 调用本目录辅助函数，验证同量多仓满足限价下单规则。
	longValid := help_validQuantity(target, bid, rules)
	// 调用本目录辅助函数，验证同量空仓满足限价下单规则。
	if !longValid || !help_validQuantity(target, ask, rules) {
		return fmt.Errorf("%s 资金对应的共同数量 %s 不满足交易所限价过滤器", symbol, target)
	}
	if _, err = e.Client.NewChangeLeverageService().Symbol(symbol).Leverage(e.Config.Leverage).Do(ctx); err != nil {
		return err
	}
	// step.2 下单前先持久化目标，重启后延续同一组开仓义务。
	p := &Plan{Symbol: symbol, Target: target, Legs: []*Leg{{Direction: "LONG"}, {Direction: "SHORT"}}}
	e.State.Plans[symbol] = p
	if err = e.persist(); err != nil {
		return err
	}
	log.Printf("创建双向开仓计划 symbol=%s quantity=%s", symbol, target)
	for _, l := range p.Legs {
		if err = e.placeEntry(ctx, p, l); err != nil {
			return err
		}
	}
	return nil
}

// placeEntry 只对目标尚未成交的数量发出 GTX 单，明确穿价拒绝才向下一档重试。
func (e *Exchange) placeEntry(ctx context.Context, p *Plan, l *Leg) error {
	// step.1 取得方向盘口，并校验剩余开仓数量。
	if p.Halt || l.Entry != nil || !l.Filled.LessThan(p.Target) {
		return nil
	}
	remaining := p.Target.Sub(l.Filled)
	// 调用本目录辅助函数，用最佳买卖盘口生成被动限价。
	bid, ask, err := help_book(ctx, e.Client, p.Symbol)
	if err != nil {
		return err
	}
	rules := e.Market.Rules[p.Symbol]
	// 调用本目录辅助函数，把持仓方向转换成实际买卖方向。
	side, _ := help_sides(l.Direction)
	// 调用本目录辅助函数，将买价向下对齐 tick。
	price := help_floor(bid, rules.Tick)
	if l.Direction == "SHORT" {
		// 调用本目录辅助函数，将卖价向上对齐 tick。
		price = help_ceil(ask, rules.Tick)
	}
	// 调用本目录辅助函数，不允许不满足最小金额的残余订单超额开仓。
	if !help_validQuantity(remaining, price, rules) {
		p.Halt, p.Reason = true, "剩余数量不满足限价过滤器，停止新增开仓并保留仓位保护"
		return e.persist()
	}
	// step.2 每次发送前保存唯一身份；明确 GTX 穿价拒绝才移动一档。
	for attempt := 0; attempt < 4; attempt++ {
		// 调用本目录辅助函数，预写入可查询的自有开仓身份。
		l.Entry = &Intent{ID: help_id("entry"), PlacedAt: time.Now().UnixMilli()}
		if err = e.persist(); err != nil {
			return err
		}
		_, err = e.Client.NewCreateOrderService().Symbol(p.Symbol).Side(side).
			PositionSide(futures.PositionSideType(l.Direction)).Type(futures.OrderTypeLimit).
			TimeInForce(futures.TimeInForceTypeGTX).Quantity(remaining.String()).Price(price.String()).
			NewClientOrderID(l.Entry.ID).Do(ctx)
		if err == nil {
			return nil
		}
		// 调用本目录辅助函数，网络或执行未知时保留原身份，禁止盲目重发。
		if !help_definiteReject(err) {
			return fmt.Errorf("%s 开仓回报未知，保留原单待核对: %w", p.Symbol, err)
		}
		l.Entry = nil
		if saveErr := e.persist(); saveErr != nil {
			return saveErr
		}
		// 调用本目录辅助函数，只有明确的 GTX 穿价码才授权按下一档重挂。
		if !help_code(err, -5022) {
			return err
		}
		if side == futures.SideTypeSell {
			price = price.Add(rules.Tick)
		} else {
			price = price.Sub(rules.Tick)
		}
		if !price.IsPositive() {
			return fmt.Errorf("%s 被动价格无效", p.Symbol)
		}
	}
	return fmt.Errorf("%s GTX 连续穿价拒绝，下个维护周期重试", p.Symbol)
}

// readEntry 核对原身份并增量记账，仅终态订单才释放开仓身份。
func (e *Exchange) readEntry(ctx context.Context, p *Plan, l *Leg) (*futures.Order, error) {
	// step.1 仅按已持久化身份查询原订单，未知时保留身份。
	if l.Entry == nil {
		return nil, nil
	}
	order, err := e.Client.NewGetOrderService().Symbol(p.Symbol).OrigClientOrderID(l.Entry.ID).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("查询开仓 %s，确认前不重发: %w", l.Entry.ID, err)
	}
	// step.2 增量记账并在终态释放身份，随后持久化恢复水位。
	// 调用本目录辅助函数，按订单累计量只计入新成交。
	if err = help_recordFill(l, order); err != nil {
		return nil, err
	}
	// 调用本目录辅助函数，只有确定终态才允许下一笔补量。
	if help_terminal(order.Status) {
		l.Entry = nil
	}
	if err = e.persist(); err != nil {
		return nil, err
	}
	return order, nil
}

// settleEntry 查询并撤销旧开仓单，再次查询撤单竞态期间的最终成交。
func (e *Exchange) settleEntry(ctx context.Context, p *Plan, l *Leg, force bool) error {
	// step.1 查询已成交部分；小额尾单保留原单等待，避免撤单后无法按最小金额补齐。
	order, err := e.readEntry(ctx, p, l)
	if err != nil || l.Entry == nil {
		return err
	}
	if !force && time.Since(time.UnixMilli(l.Entry.PlacedAt)) < e.Config.repriceInterval() {
		return nil
	}
	rules := e.Market.Rules[p.Symbol]
	// 调用本目录辅助函数，解析剩余挂单对应的原限价。
	price, err := help_decimal(order.Price)
	if err != nil {
		return err
	}
	remaining := p.Target.Sub(l.Filled)
	// 调用本目录辅助函数，无法重新下单的尾量只允许继续原有订单。
	if !force && !help_validQuantity(remaining, price, rules) {
		return nil
	}
	// step.2 撤单请求无论成功还是未知，均按原身份核对最终成交。
	_, cancelErr := e.Client.NewCancelOrderService().Symbol(p.Symbol).OrigClientOrderID(l.Entry.ID).Do(ctx)
	_, readErr := e.readEntry(ctx, p, l)
	if readErr != nil {
		return readErr
	}
	if l.Entry != nil {
		return fmt.Errorf("%s 撤单尚未确定完成: %v", p.Symbol, cancelErr)
	}
	return nil
}
