package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/adshao/go-binance/v2/futures"
)

// protect 为实际非零持仓维护按成本计算的全量止损和止盈。
func (e *Exchange) protect(ctx context.Context, p *Plan, l *Leg, position Position) error {
	// step.1 仅对实际存在的仓位按其方向和实际成本计算触发价。
	if !position.Quantity.IsPositive() {
		return nil
	}
	rules, ok := e.Market.Rules[p.Symbol]
	if !ok {
		return fmt.Errorf("%s 缺少保护单价格精度", p.Symbol)
	}
	// 调用本目录辅助函数，以该方向实际成本生成全量保护触发价。
	stop, profit := help_triggerPrices(position.Entry, l.Direction, e.Config, rules.Tick)
	// step.2 止损先于止盈建立，部分成交同样受保护。
	if err := e.syncProtection(ctx, p, l, &l.StopID, &l.StopPrice, stop,
		futures.AlgoOrderTypeStopMarket); err != nil {
		return err
	}
	if p.Emergency {
		return nil
	}
	return e.syncProtection(ctx, p, l, &l.ProfitID, &l.ProfitPrice, profit,
		futures.AlgoOrderTypeTakeProfitMarket)
}

// syncProtection 核对可恢复的条件单身份，在成本变化时撤销旧保护并按新成本重建。
func (e *Exchange) syncProtection(
	ctx context.Context, p *Plan, l *Leg, id, savedPrice *string, price string, kind futures.AlgoOrderType,
) error {
	// step.1 核对已保存的条件单；已触发退出时停止继续开仓。
	if *id != "" {
		order, err := e.Client.NewGetAlgoOrderService().ClientAlgoID(*id).Do(ctx)
		if err != nil {
			// step.1.1 条件单已不存在时先清除旧身份，后续按仍在的实际仓位重建保护。
			if help_code(err, -2013) {
				*id, *savedPrice = "", ""
				if saveErr := e.persist(); saveErr != nil {
					return saveErr
				}
			} else {
				return fmt.Errorf("保护单 %s 回报未确认: %w", *id, err)
			}
		}
		if order != nil {
			switch string(order.AlgoStatus) {
			case "NEW":
				if *savedPrice == price {
					return nil
				}
				if err = e.cancelProtection(ctx, *id); err != nil {
					return err
				}
				order, err = e.Client.NewGetAlgoOrderService().ClientAlgoID(*id).Do(ctx)
				if err != nil {
					return err
				}
				if string(order.AlgoStatus) != "CANCELED" && string(order.AlgoStatus) != "EXPIRED" {
					return e.observeTriggered(ctx, p, order)
				}
			case "CANCELED", "EXPIRED":
				// 人工取消或过期后，只为仍然存在的持仓重建保护。
			default:
				return e.observeTriggered(ctx, p, order)
			}
			*id = ""
			if err := e.persist(); err != nil {
				return err
			}
		}
	}
	// step.2 发送前持久化自定义身份，使用 closePosition 覆盖整个方向而非固定数量。
	// 调用本目录辅助函数，生成可用于条件单恢复查询的编号。
	*id, *savedPrice = help_id("protect"), price
	if err := e.persist(); err != nil {
		return err
	}
	// 调用本目录辅助函数，把持仓方向转换成平仓买卖方向。
	_, side := help_sides(l.Direction)
	_, err := e.Client.NewCreateAlgoOrderService().Symbol(p.Symbol).Side(side).
		PositionSide(futures.PositionSideType(l.Direction)).Type(kind).ClosePosition(true).
		TriggerPrice(price).WorkingType(futures.WorkingTypeContractPrice).PriceProtect(false).
		ClientAlgoId(*id).Do(ctx)
	if err == nil {
		return nil
	}
	// step.3 执行未知时保留身份；明确保护失败则停止新增开仓并请求紧急退出。
	// 调用本目录辅助函数，避免把网络异常当作已拒单而重复创建保护。
	if help_definiteReject(err) {
		*id = ""
		p.Halt, p.Emergency, p.Reason = true, true, fmt.Sprintf("%s 保护被拒绝: %v", kind, err)
		if saveErr := e.persist(); saveErr != nil {
			return saveErr
		}
	}
	return fmt.Errorf("%s %s %s 保护提交失败: %w", p.Symbol, l.Direction, kind, err)
}

// observeTriggered 停止退出后的补开，并检查条件单子单是否已明确执行失败。
func (e *Exchange) observeTriggered(ctx context.Context, p *Plan, order *futures.GetAlgoOrderResp) error {
	// step.1 任一保护退出开始后，整组不再新增开仓，另一边继续自己的保护。
	p.Halt, p.Reason = true, "保护单已触发或结束，停止本组新增开仓"
	if string(order.AlgoStatus) == "REJECTED" {
		p.Emergency, p.Reason = true, "交易所条件单明确拒绝"
	}
	// step.2 检查已生成的市价退出子单，明确拒绝或过期时请求紧急退出。
	if order.ActualOrderId != "" && order.ActualOrderId != "0" {
		id, err := strconv.ParseInt(order.ActualOrderId, 10, 64)
		if err != nil {
			return err
		}
		child, err := e.Client.NewGetOrderService().Symbol(p.Symbol).OrderID(id).Do(ctx)
		if err != nil {
			return err
		}
		switch string(child.Status) {
		case "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH", "CANCELED":
			p.Emergency, p.Reason = true, "条件单退出子单未能完成"
		}
	}
	return e.persist()
}

// cancelProtection 撤销指定自有条件单并确认不再处于 NEW，绝不批量撤销他人的订单。
func (e *Exchange) cancelProtection(ctx context.Context, id string) error {
	// step.1 查询指定自有条件单，已经结束的条件单无需撤销。
	if id == "" {
		return nil
	}
	order, err := e.Client.NewGetAlgoOrderService().ClientAlgoID(id).Do(ctx)
	if err != nil {
		if help_code(err, -2013) {
			return nil
		}
		return err
	}
	if string(order.AlgoStatus) != "NEW" {
		return nil
	}
	// step.2 撤销后再次查询，确认条件单不再等待触发。
	_, cancelErr := e.Client.NewCancelAlgoOrderService().ClientAlgoID(id).Do(ctx)
	order, err = e.Client.NewGetAlgoOrderService().ClientAlgoID(id).Do(ctx)
	if err != nil {
		if help_code(err, -2013) {
			return nil
		}
		return err
	}
	if string(order.AlgoStatus) == "NEW" {
		return fmt.Errorf("保护单 %s 撤销未确认: %v", id, cancelErr)
	}
	return nil
}

// emergencyExit 在开仓单均已核清后按实际剩余仓位发出可恢复的市价退出单。
func (e *Exchange) emergencyExit(ctx context.Context, p *Plan) error {
	// step.1 禁止撤单未确认时平仓，避免晚到的开仓成交重新制造裸仓。
	for _, l := range p.Legs {
		if l.Entry != nil {
			return fmt.Errorf("%s 开仓尚未结清，等待核对", p.Symbol)
		}
	}
	log.Printf("紧急退出 symbol=%s reason=%s", p.Symbol, p.Reason)
	for _, l := range p.Legs {
		// step.2 核对前次退出请求，未知状态下不重复平仓。
		if l.Exit != nil {
			order, err := e.Client.NewGetOrderService().Symbol(p.Symbol).OrigClientOrderID(l.Exit.ID).Do(ctx)
			if err != nil {
				return err
			}
			// 调用本目录辅助函数，退出单未终结时继续等待原单。
			if !help_terminal(order.Status) {
				continue
			}
			l.Exit = nil
			if err = e.persist(); err != nil {
				return err
			}
		}
		// step.3 用最新实际数量退出，不用累计历史开仓量代替剩余仓位。
		positions, err := e.positions(ctx, p.Symbol)
		if err != nil {
			return err
		}
		position := positions[p.Symbol][l.Direction]
		if !position.Quantity.IsPositive() {
			continue
		}
		// 调用本目录辅助函数，确定双向模式的正确退出方向。
		_, side := help_sides(l.Direction)
		// 调用本目录辅助函数，发送前写入唯一市价退出身份。
		l.Exit = &Intent{ID: help_id("exit")}
		if err = e.persist(); err != nil {
			return err
		}
		_, err = e.Client.NewCreateOrderService().Symbol(p.Symbol).Side(side).
			PositionSide(futures.PositionSideType(l.Direction)).Type(futures.OrderTypeMarket).
			Quantity(position.Quantity.String()).NewClientOrderID(l.Exit.ID).Do(ctx)
		if err != nil {
			// 调用本目录辅助函数，明确拒绝才允许后续重新提交退出。
			if help_definiteReject(err) {
				l.Exit = nil
				if saveErr := e.persist(); saveErr != nil {
					return saveErr
				}
			}
			return err
		}
	}
	return nil
}
