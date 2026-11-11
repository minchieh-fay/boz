package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/shopspring/decimal"
)

// maintainPlan 核对双边成交，保护部分持仓，并在没有退出事件时补齐共同目标。
func (e *Exchange) maintainPlan(ctx context.Context, p *Plan) error {
	// step.1 两边独立核对及撤销旧单，一边接口异常也继续尝试保护另一边。
	var cycleErrors []error
	for _, l := range p.Legs {
		if err := e.settleEntry(ctx, p, l, p.Halt); err != nil {
			cycleErrors = append(cycleErrors, err)
		}
	}
	positions, err := e.positions(ctx, p.Symbol)
	if err != nil {
		return errors.Join(append(cycleErrors, err)...)
	}
	// step.2 检测提前退出，不把已平仓历史成交重新补成持仓。
	for _, l := range p.Legs {
		position := positions[p.Symbol][l.Direction]
		if position.Quantity.LessThan(l.Filled) && !p.Halt {
			p.Halt, p.Reason = true, "入场期间出现减仓，停止本组新增开仓"
			if err = e.persist(); err != nil {
				return err
			}
		}
		if !p.Emergency {
			if err = e.protect(ctx, p, l, position); err != nil {
				cycleErrors = append(cycleErrors, err)
				p.Halt, p.Emergency, p.Reason = true, true, "保护无法确认，停止开仓并请求退出: "+err.Error()
				if saveErr := e.persist(); saveErr != nil {
					return saveErr
				}
			}
		}
	}
	// step.3 退出事件发生后撤销两边剩余开仓；保护明确失败时紧急退出。
	if p.Halt {
		for _, l := range p.Legs {
			if err = e.settleEntry(ctx, p, l, true); err != nil {
				cycleErrors = append(cycleErrors, err)
			}
		}
		if p.Emergency {
			if err = e.emergencyExit(ctx, p); err != nil {
				cycleErrors = append(cycleErrors, err)
			}
		}
	}
	if len(cycleErrors) > 0 {
		return errors.Join(cycleErrors...)
	}
	// step.4 只在原订单已结清且保护正常时重挂尚未完成的等量开仓。
	if !p.Halt {
		for _, l := range p.Legs {
			if err = e.placeEntry(ctx, p, l); err != nil {
				cycleErrors = append(cycleErrors, err)
			}
		}
	}
	if len(cycleErrors) > 0 {
		return errors.Join(cycleErrors...)
	}
	return e.finishPlan(ctx, p)
}

// finishPlan 在双边完全平仓及普通订单终结后清理自有保护并开始冷却。
func (e *Exchange) finishPlan(ctx context.Context, p *Plan) error {
	// step.1 未完成且未停止的开仓目标仍占一个名额，未知订单同样不能释放。
	for _, l := range p.Legs {
		if l.Entry != nil || l.Exit != nil || (!p.Halt && l.Filled.LessThan(p.Target)) {
			return nil
		}
	}
	positions, err := e.positions(ctx, p.Symbol)
	if err != nil {
		return err
	}
	if len(positions[p.Symbol]) != 0 {
		return nil
	}
	// step.2 清理本组残留的条件单，再次核对账户该币没有任何新仓位或挂单。
	for _, l := range p.Legs {
		if err = e.cancelProtection(ctx, l.StopID); err != nil {
			return err
		}
		if err = e.cancelProtection(ctx, l.ProfitID); err != nil {
			return err
		}
	}
	a, err := e.account(ctx)
	if err != nil {
		return err
	}
	if a.Occupied[p.Symbol] {
		return nil
	}
	// step.3 查询本组期间的成交，计算毛收益、固定手续费和累计净收益。
	if err = e.recordPlanStatistics(ctx, p); err != nil {
		return err
	}
	// step.4 完成时间持久化，重启之后也继续执行冷却。
	e.State.Cooldowns[p.Symbol] = time.Now().UnixMilli()
	delete(e.State.Plans, p.Symbol)
	delete(e.State.Occupied, p.Symbol)
	log.Printf("本组完全结束 symbol=%s cooldown_seconds=%.3f", p.Symbol, e.Config.CooldownSeconds)
	return e.persist()
}

// recordPlanStatistics 按本组开始时间读取账户成交，使用 Binance 已实现盈亏并按 0.04% 估算手续费。
func (e *Exchange) recordPlanStatistics(ctx context.Context, p *Plan) error {
	// step.1 读取本组交易对在开仓后的全部账户成交，统计成交笔数、已实现盈亏和成交额。
	if p.StartedAt <= 0 {
		return fmt.Errorf("%s 缺少计划开始时间，拒绝生成不可靠的收益统计", p.Symbol)
	}
	trades, err := e.Client.NewListAccountTradeService().Symbol(p.Symbol).
		StartTime(p.StartedAt).EndTime(time.Now().UnixMilli()).Limit(1000).Do(ctx)
	if err != nil {
		return fmt.Errorf("读取 %s 成交记录: %w", p.Symbol, err)
	}
	gross, turnover := decimal.Zero, decimal.Zero
	for _, trade := range trades {
		if trade == nil {
			continue
		}
		pnl, parseErr := help_decimal(trade.RealizedPnl)
		if parseErr != nil {
			return fmt.Errorf("解析 %s 已实现盈亏: %w", p.Symbol, parseErr)
		}
		quote, parseErr := help_decimal(trade.QuoteQuantity)
		if parseErr != nil {
			return fmt.Errorf("解析 %s 成交额: %w", p.Symbol, parseErr)
		}
		gross = gross.Add(pnl)
		turnover = turnover.Add(quote.Abs())
	}
	// step.2 按开仓和平仓全部成交额收取 0.04%，并把本组结果合并进持久化总账。
	fees := turnover.Mul(decimal.NewFromFloat(0.0004))
	net := gross.Sub(fees)
	stats := &e.State.Statistics
	stats.CompletedTrades++
	stats.TotalFills += len(trades)
	stats.GrossPnL = stats.GrossPnL.Add(gross)
	stats.Fees = stats.Fees.Add(fees)
	stats.NetPnL = stats.NetPnL.Add(net)
	stats.LastSymbol, stats.LastTradeAt = p.Symbol, time.Now().UnixMilli()
	log.Printf("交易完成 symbol=%s fills=%d gross_pnl=%s fees=%s net_pnl=%s cumulative_net_pnl=%s total_trades=%d",
		p.Symbol, len(trades), gross.StringFixed(8), fees.StringFixed(8), net.StringFixed(8),
		stats.NetPnL.StringFixed(8), stats.CompletedTrades)
	return nil
}

// scan 在完整账户快照基础上先维护冷却，再按振幅创建不冲突的新计划。
func (e *Exchange) scan(ctx context.Context) error {
	// step.1 读取全账户仓位及两类订单，记录从占用到完全空闲的冷却起点。
	a, err := e.account(ctx)
	if err != nil {
		return err
	}
	if err = e.validateOwnership(a); err != nil {
		return err
	}
	now := time.Now()
	for symbol := range e.State.Occupied {
		if !a.Occupied[symbol] && e.State.Plans[symbol] == nil {
			e.State.Cooldowns[symbol] = now.UnixMilli()
		}
	}
	e.State.Occupied = a.Occupied
	if err = e.persist(); err != nil {
		return err
	}
	// step.2 合并所有活跃计划与账户占用，同币双向仅计一个名额。
	busy := map[string]bool{}
	for symbol := range a.Occupied {
		busy[symbol] = true
	}
	for symbol := range e.State.Plans {
		busy[symbol] = true
	}
	for _, candidate := range e.Market.candidates(now, e.Config) {
		if len(busy) >= e.Config.MaxSymbols {
			break
		}
		symbol := candidate.Symbol
		if busy[symbol] {
			continue
		}
		last := e.State.Cooldowns[symbol]
		if last > 0 && now.UnixMilli()-last < int64(e.Config.CooldownSeconds*1000) {
			continue
		}
		// step.3 再次核对账户，收窄扫描期间手工交易导致的冲突窗口。
		latest, err := e.account(ctx)
		if err != nil {
			return err
		}
		for occupied := range latest.Occupied {
			busy[occupied] = true
		}
		if busy[symbol] {
			continue
		}
		if len(busy) >= e.Config.MaxSymbols {
			break
		}
		log.Printf("振幅满足 symbol=%s move=%s", symbol, candidate.Move)
		if err = e.startPlan(ctx, symbol); err != nil {
			log.Printf("创建计划失败 symbol=%s: %v", symbol, err)
		}
		if e.State.Plans[symbol] != nil {
			busy[symbol] = true
		}
	}
	return nil
}

// maintain 对所有活跃计划独立维护，一个币种的 API 错误不会阻止其他币的保护。
func (e *Exchange) maintain(ctx context.Context) {
	for _, p := range e.State.Plans {
		if err := e.maintainPlan(ctx, p); err != nil {
			log.Printf("维护失败 symbol=%s: %v", p.Symbol, err)
		}
	}
}

// shutdown 停止新增开仓、核清撤单期间成交并保留交易所托管保护。
func (e *Exchange) shutdown() {
	// step.1 使用独立时限完成退出清理；进程重启后继续未完成的同一组目标。
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	for _, p := range e.State.Plans {
		for _, l := range p.Legs {
			if err := e.settleEntry(ctx, p, l, true); err != nil {
				log.Printf("退出撤单未完成 symbol=%s: %v", p.Symbol, err)
			}
		}
		// step.2 清理期间有部分成交时，继续为实际仓位补齐保护。
		positions, err := e.positions(ctx, p.Symbol)
		if err != nil {
			log.Printf("退出持仓核对失败: %v", err)
			continue
		}
		for _, l := range p.Legs {
			if err := e.protect(ctx, p, l, positions[p.Symbol][l.Direction]); err != nil {
				log.Printf("退出保护未完成 symbol=%s: %v", p.Symbol, err)
			}
		}
	}
	if err := e.persist(); err != nil {
		log.Printf("退出账本保存失败: %v", err)
	}
}

// checkStateWritable 在使用真实交易权限前验证持久化目录可写。
func (e *Exchange) checkStateWritable() error {
	if err := e.persist(); err != nil {
		return fmt.Errorf("持久化不可用: %w", err)
	}
	return nil
}
