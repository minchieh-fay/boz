package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// newTestExchange 创建只连接本地 HTTP 服务器的交易引擎，测试不会访问实盘。
func newTestExchange(t *testing.T, handler http.HandlerFunc) *Exchange {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c, err := loadConfig("config.sample.json")
	if err != nil {
		t.Fatal(err)
	}
	c.StateFile = filepath.Join(t.TempDir(), "state.json")
	state, err := loadState(c.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	client := futures.NewClient("local-test", "local-secret")
	client.BaseURL, client.HTTPClient = server.URL, server.Client()
	market := newMarket(c.KlineCount)
	market.Rules["TESTUSDT"] = Rules{Step: decimal.RequireFromString("0.1"), MinQty: decimal.RequireFromString("0.1"),
		MaxQty: decimal.NewFromInt(1000), Tick: decimal.RequireFromString("0.01"), MinNotional: decimal.NewFromInt(5)}
	return &Exchange{Client: client, Config: c, Market: market, State: state}
}

// writeTestJSON 输出本地交易所回报，不使用真实密钥。
func writeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}

// newTestPlan 在测试账本中建立等量双向目标。
func newTestPlan(e *Exchange) *Plan {
	p := &Plan{Symbol: "TESTUSDT", Target: decimal.NewFromInt(1), Legs: []*Leg{{Direction: "LONG"}, {Direction: "SHORT"}}}
	e.State.Plans[p.Symbol] = p
	return p
}
