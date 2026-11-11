package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/shopspring/decimal"
)

// Intent 保存已持久化的订单身份和已记账成交量，支持未知回报恢复。
type Intent struct {
	ID        string          `json:"id"`
	PlacedAt  int64           `json:"placed_at"`
	Accounted decimal.Decimal `json:"accounted"`
	Quote     decimal.Decimal `json:"quote"`
}

// Leg 保存一个方向的累计开仓和交易所托管保护。
type Leg struct {
	Direction   string          `json:"direction"`
	Filled      decimal.Decimal `json:"filled"`
	Quote       decimal.Decimal `json:"quote"`
	Entry       *Intent         `json:"entry,omitempty"`
	StopID      string          `json:"stop_id,omitempty"`
	ProfitID    string          `json:"profit_id,omitempty"`
	Exit        *Intent         `json:"exit,omitempty"`
	StopPrice   string          `json:"stop_price,omitempty"`
	ProfitPrice string          `json:"profit_price,omitempty"`
}

// Plan 保存同币双向等量开仓目标，已退出的成交不会重新补开。
type Plan struct {
	Symbol    string          `json:"symbol"`
	StartedAt int64           `json:"started_at"`
	Target    decimal.Decimal `json:"target"`
	Legs      []*Leg          `json:"legs"`
	Halt      bool            `json:"halt"`
	Emergency bool            `json:"emergency"`
	Reason    string          `json:"reason,omitempty"`
}

// Statistics 保存策略进程内已完成交易组的粗略收益和手续费统计。
type Statistics struct {
	CompletedTrades int             `json:"completed_trades"`
	TotalFills      int             `json:"total_fills"`
	GrossPnL        decimal.Decimal `json:"gross_pnl"`
	Fees            decimal.Decimal `json:"fees"`
	NetPnL          decimal.Decimal `json:"net_pnl"`
	LastSymbol      string          `json:"last_symbol,omitempty"`
	LastTradeAt     int64           `json:"last_trade_at,omitempty"`
}

// State 保存活跃计划、全账户占用记录及已完全平仓交易对的冷却起点。
type State struct {
	Version    int              `json:"version"`
	Plans      map[string]*Plan `json:"plans"`
	Cooldowns  map[string]int64 `json:"cooldowns"`
	Occupied   map[string]bool  `json:"occupied"`
	Statistics Statistics       `json:"statistics"`
}

// loadState 恢复账本，拒绝用空账本覆盖损坏或不兼容的状态。
func loadState(path string) (*State, error) {
	// step.1 仅在文件不存在时建立新账本。
	s := &State{Version: 1, Plans: map[string]*Plan{}, Cooldowns: map[string]int64{}, Occupied: map[string]bool{}}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	// step.2 校验可恢复的状态结构，避免丢失已有订单身份。
	if s.Version != 1 || s.Plans == nil || s.Cooldowns == nil || s.Occupied == nil {
		return nil, fmt.Errorf("状态文件结构或版本无效")
	}
	for symbol, p := range s.Plans {
		if p == nil || p.Symbol != symbol || !p.Target.IsPositive() || len(p.Legs) != 2 ||
			p.Legs[0] == nil || p.Legs[1] == nil || p.Legs[0].Direction != "LONG" || p.Legs[1].Direction != "SHORT" {
			return nil, fmt.Errorf("%s 双向计划损坏", symbol)
		}
	}
	return s, nil
}

// save 以同目录临时文件、同步和原子重命名提交账本。
func (s *State) save(path string) error {
	// step.1 在状态目录写入完整新版本并确保文件落盘。
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".state-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// step.2 原子替换账本并同步目录项，保证重启能恢复订单身份。
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
