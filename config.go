package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config 保存实盘策略参数；冷却字段虽然保留旧名称，但单位为秒。
type Config struct {
	KeyFile              string  `json:"keyfile"`
	KlineCount           int     `json:"kline_count"`
	TradeUSDT            float64 `json:"trade_usdt"`
	TradeIntervalSeconds float64 `json:"trade_interval_seconds"`
	EntryRepriceSeconds  float64 `json:"entry_reprice_seconds"`
	CooldownSeconds      float64 `json:"min_trade_interval_mins"`
	MaxSymbols           int     `json:"max_symbols"`
	TakeProfitRate       float64 `json:"take_profit_rate"`
	StopLossRate         float64 `json:"stop_loss_rate"`
	MinMoveRate          float64 `json:"min_move_rate"`
	Leverage             int     `json:"leverage"`
	StateFile            string  `json:"state_file"`
	StalePriceSeconds    float64 `json:"stale_price_seconds"`
}

// loadConfig 读取参数并校验实盘交易所需的完整配置。
func loadConfig(path string) (Config, error) {
	// step.1 应用默认值并读取部署配置。
	c := Config{KeyFile: "/root/key", KlineCount: 12, TradeUSDT: 100,
		TradeIntervalSeconds: 20, EntryRepriceSeconds: 1.5, CooldownSeconds: 3600,
		MaxSymbols: 2, TakeProfitRate: .03, StopLossRate: .1, MinMoveRate: .3,
		Leverage: 1, StateFile: "data/state.json", StalePriceSeconds: 30}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(data, &c); err != nil {
		return c, err
	}
	// step.2 限定双向模式和可用交易参数，拒绝无效参数。
	if c.KlineCount < 1 || c.KlineCount > 1500 || c.TradeUSDT <= 0 || c.MaxSymbols < 1 ||
		c.TradeIntervalSeconds < .1 || c.EntryRepriceSeconds < .1 || c.CooldownSeconds < 0 ||
		c.TakeProfitRate <= 0 || c.TakeProfitRate >= 1 || c.StopLossRate <= 0 || c.StopLossRate >= 1 ||
		c.MinMoveRate < 0 || c.Leverage < 1 || c.Leverage > 125 || c.StalePriceSeconds <= 0 ||
		c.KeyFile == "" || c.StateFile == "" {
		return c, fmt.Errorf("配置参数无效，请检查资金、窗口、时间间隔、数量限制和费率")
	}
	// step.3 相对路径以配置文件目录为基准。
	if !filepath.IsAbs(c.KeyFile) {
		c.KeyFile = filepath.Join(filepath.Dir(path), c.KeyFile)
	}
	if !filepath.IsAbs(c.StateFile) {
		c.StateFile = filepath.Join(filepath.Dir(path), c.StateFile)
	}
	return c, nil
}

// repriceInterval 返回开仓维护周期。
func (c Config) repriceInterval() time.Duration {
	return time.Duration(c.EntryRepriceSeconds * float64(time.Second))
}
