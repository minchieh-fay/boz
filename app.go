package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// Application 组合串行交易引擎和并发行情订阅，不并发启动多轮交易扫描。
type Application struct {
	Exchange *Exchange
	Symbols  []string
	Lock     *os.File
}

// newApplication 在不发起交易的情况下装载配置、密钥和恢复账本。
func newApplication(path string) (*Application, error) {
	// step.1 读取配置并取得状态文件的独占使用权。
	cfg, err := loadConfig(path)
	if err != nil {
		return nil, err
	}
	// 调用本目录辅助函数，独占账本以防重复启动导致双重下单。
	lock, err := help_lockState(cfg.StateFile)
	if err != nil {
		return nil, err
	}
	keepLock := false
	defer func() {
		if !keepLock {
			lock.Close()
		}
	}()
	// step.2 恢复账本和交易客户端，并验证状态目录可写。
	state, err := loadState(cfg.StateFile)
	if err != nil {
		return nil, err
	}
	exchange, err := newExchange(cfg, newMarket(cfg.KlineCount), state)
	if err != nil {
		return nil, err
	}
	if err = exchange.checkStateWritable(); err != nil {
		return nil, err
	}
	keepLock = true
	return &Application{Exchange: exchange, Lock: lock}, nil
}

// run 先恢复账户和已有仓位保护，再准备行情并周期执行实盘策略。
func (a *Application) run(ctx context.Context) error {
	// step.1 初始化交易所、核对账本归属并尽早恢复已有保护。
	defer a.Lock.Close()
	e := a.Exchange
	symbols, err := e.initialize(ctx)
	if err != nil {
		return err
	}
	a.Symbols = symbols
	defer e.shutdown()
	account, err := e.account(ctx)
	if err != nil {
		return err
	}
	if err = e.validateOwnership(account); err != nil {
		return err
	}
	e.maintain(ctx)
	// step.2 行情订阅和历史补齐并行运行，补齐期间继续管理已有仓位。
	marketCtx, cancelMarket := context.WithCancel(ctx)
	defer cancelMarket()
	go a.runSockets(marketCtx)
	ready := make(chan struct{})
	go a.warmHistory(marketCtx, ready)
	ticker := time.NewTicker(e.Config.repriceInterval())
	scan := time.NewTicker(time.Duration(e.Config.TradeIntervalSeconds * float64(time.Second)))
	defer ticker.Stop()
	defer scan.Stop()
	log.Printf("实盘启动 symbols=%d kline_count=%d", len(symbols), e.Config.KlineCount)
	// step.3 用单个业务循环管理订单，行情未准备好时只维护已有计划。
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			e.maintain(ctx)
		case <-scan.C:
			e.maintain(ctx)
			select {
			case <-ready:
				if err := e.scan(ctx); err != nil {
					log.Printf("选币扫描失败: %v", err)
				}
			default:
				log.Printf("历史窗口仍在初始化，本轮只维护已有计划")
			}
		}
	}
}

// warmHistory 在限速闸门下逐个初始化历史，失败的币种依靠订阅补齐后才可选中。
func (a *Application) warmHistory(ctx context.Context, ready chan<- struct{}) {
	defer close(ready)
	for _, symbol := range a.Symbols {
		if ctx.Err() != nil {
			return
		}
		if err := a.Exchange.history(ctx, symbol); err != nil {
			log.Printf("历史获取失败 symbol=%s: %v", symbol, err)
		}
	}
}

// runSockets 分组订阅全部合约，避免单条连接超出流数量和 URL 长度限制。
func (a *Application) runSockets(ctx context.Context) {
	var wg sync.WaitGroup
	for start := 0; start < len(a.Symbols); start += 100 {
		end := min(start+100, len(a.Symbols))
		group := append([]string(nil), a.Symbols[start:end]...)
		wg.Add(1)
		go func() { defer wg.Done(); a.runSocketGroup(ctx, group) }()
	}
	wg.Wait()
}

// runSocketGroup 断线后重新订阅并补齐缺口，补齐前不完整窗口不参与新开仓。
func (a *Application) runSocketGroup(ctx context.Context, symbols []string) {
	// step.1 维持五分钟原生 K 线订阅，使用服务端事件时间判断行情新鲜度。
	streams := map[string]string{}
	for _, symbol := range symbols {
		streams[symbol] = "5m"
	}
	reconnect := false
	for ctx.Err() == nil {
		done, stop, err := futures.WsCombinedKlineServe(streams, a.Exchange.Market.onKline,
			func(err error) { log.Printf("行情 WebSocket: %v", err) })
		if err != nil {
			log.Printf("订阅失败: %v", err)
			// 调用本目录辅助函数，连接失败后等待再重试。
			if help_wait(ctx, 5*time.Second) != nil {
				return
			}
			continue
		}
		// step.2 重连时补齐断线区间；只由行情线程修改窗口，交易逻辑仍串行运行。
		refillCtx, cancel := context.WithCancel(ctx)
		if reconnect {
			go func() {
				for _, symbol := range symbols {
					if refillCtx.Err() != nil {
						return
					}
					if err := a.Exchange.history(refillCtx, symbol); err != nil {
						log.Printf("补齐缺口 %s: %v", symbol, err)
					}
				}
			}()
		}
		select {
		case <-ctx.Done():
			cancel()
			close(stop)
			return
		case <-done:
			cancel()
			reconnect = true
		}
		// 调用本目录辅助函数，让断线重连退避响应退出。
		if help_wait(ctx, 2*time.Second) != nil {
			return
		}
	}
}
