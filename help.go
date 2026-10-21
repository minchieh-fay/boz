package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	binance "github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// 保存实时策略使用的交易对、数量、密钥路径和风险参数。
type config struct {
	Symbol          string  `json:"symbol"`
	Quantity        string  `json:"quantity"`
	KeyFile         string  `json:"keyfile"`
	TakeProfit      float64 `json:"take_profit"`
	StopLoss        float64 `json:"stop_loss"`
	IntervalSeconds int     `json:"interval_seconds"`
}

// 保存 EMA 信号计算状态和本进程创建的开仓单标识。
type strategyState struct {
	mu            sync.Mutex
	ema           float64
	emaReady      bool
	previousDelta float64
	deltaReady    bool
	side          int
	turns         int
	longEntryID   string
	longEntryAt   time.Time
	shortEntryID  string
	shortEntryAt  time.Time
	entryOrderID  string
	entryAt       time.Time
}

// 保存单一持仓方向的开仓挂单标识和提交时间。
type positionState struct {
	entryOrderID string
	entryAt      time.Time
	entryFilled  bool
	positionSeen bool
}

// 从 JSON 配置读取并校验实时交易所需参数。
func help_loadConfig(path string) (config, error) {
	// step.1 读取 JSON 配置并填充省略的默认风险与周期值。
	data, err := os.ReadFile(path)
	if err != nil {
		return config{}, fmt.Errorf("读取配置 %s: %w", path, err)
	}
	var result config
	if err := json.Unmarshal(data, &result); err != nil {
		return config{}, fmt.Errorf("解析配置 %s: %w", path, err)
	}
	if result.TakeProfit == 0 {
		result.TakeProfit = 0.03
	}
	if result.StopLoss == 0 {
		result.StopLoss = 0.02
	}
	if result.IntervalSeconds == 0 {
		result.IntervalSeconds = 300
	}
	// step.2 校验交易对、下单数量和策略比例。
	quantity, quantityErr := strconv.ParseFloat(result.Quantity, 64)
	if result.Symbol == "" || result.KeyFile == "" || quantityErr != nil || quantity <= 0 {
		return config{}, errors.New("symbol、keyfile 和正数 quantity 必须在 config.json 中配置")
	}
	if result.TakeProfit <= 0 || result.TakeProfit >= 1 || result.StopLoss <= 0 ||
		result.StopLoss >= 1 || result.IntervalSeconds < 60 {
		return config{}, errors.New("止盈止损比例必须在 0 到 1 之间，周期不能少于 60 秒")
	}
	return result, nil
}

// 从密钥文件读取币安 API Key 和 Secret 两行内容。
func help_loadCredentials(path string) (string, string, error) {
	// step.1 读取密钥文件并校验两行凭证均非空。
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("读取密钥文件 %s: %w", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) == "" || strings.TrimSpace(lines[1]) == "" {
		return "", "", fmt.Errorf("密钥文件 %s 必须包含两行：API Key 和 Secret", path)
	}
	return strings.TrimSpace(lines[0]), strings.TrimSpace(lines[1]), nil
}

// 启动 Binance K 线 WebSocket 并在每根收盘 K 线上协调账户订单。
func help_runLive(ctx context.Context, cfg config, apiKey, secret string) error {
	// step.1 创建交易客户端并验证交易对过滤器和 K 线周期。
	// 调用周期映射辅助函数取得 Binance WebSocket 所需周期名称。
	interval, err := help_intervalName(cfg.IntervalSeconds)
	if err != nil {
		return err
	}
	client := binance.NewClient(apiKey, secret)
	mode, err := client.NewGetPositionModeService().Do(ctx)
	if err != nil {
		return fmt.Errorf("读取账户持仓模式: %w", err)
	}
	if !mode.DualSidePosition {
		return errors.New("当前策略需要币安双向持仓模式，请先将账户切换为双向持仓")
	}
	info, err := client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return fmt.Errorf("读取合约交易规则: %w", err)
	}
	var symbolInfo *binance.Symbol
	for i := range info.Symbols {
		if info.Symbols[i].Symbol == strings.ToUpper(cfg.Symbol) {
			symbolInfo = &info.Symbols[i]
			break
		}
	}
	if symbolInfo == nil || symbolInfo.Status != "TRADING" {
		return fmt.Errorf("交易对 %s 不存在或当前不可交易", cfg.Symbol)
	}
	// 调用交易步长解析辅助函数获取价格精度和数量步长。
	priceTick, quantityStep, err := help_symbolSteps(symbolInfo)
	if err != nil {
		return err
	}
	// 调用数量规范化辅助函数使配置数量符合交易所步长。
	quantity, err := help_normalizeQuantity(cfg.Quantity, quantityStep)
	if err != nil {
		return err
	}

	// step.2 订阅已收盘 K 线并按顺序维护本程序的订单状态。
	state := &strategyState{}
	var callbackMu sync.Mutex
	done, stop, err := binance.WsKlineServe(strings.ToLower(cfg.Symbol), interval,
		func(event *binance.WsKlineEvent) {
			if !event.Kline.IsFinal {
				return
			}
			callbackMu.Lock()
			defer callbackMu.Unlock()
			closePrice, parseErr := strconv.ParseFloat(event.Kline.Close, 64)
			if parseErr != nil {
				log.Printf("解析收盘价失败: %v", parseErr)
				return
			}
			// 调用策略周期处理辅助函数更新信号并维护账户订单。
			if runErr := help_processCandleDual(ctx, client, cfg, state,
				closePrice, priceTick, quantity); runErr != nil {
				log.Printf("处理策略周期失败: %v", runErr)
			}
		}, func(wsErr error) {
			log.Printf("K 线 WebSocket 错误: %v", wsErr)
		})
	if err != nil {
		return fmt.Errorf("启动 K 线 WebSocket: %w", err)
	}
	defer close(stop)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return errors.New("K 线 WebSocket 已断开")
	}
}

// 将配置的秒数映射到 Binance 支持的 K 线周期名称。
func help_intervalName(seconds int) (string, error) {
	intervals := map[int]string{60: "1m", 180: "3m", 300: "5m", 900: "15m",
		1800: "30m", 3600: "1h", 7200: "2h", 14400: "4h", 21600: "6h",
		28800: "8h", 43200: "12h", 86400: "1d"}
	name, ok := intervals[seconds]
	if !ok {
		return "", fmt.Errorf("周期 %d 秒不是 Binance 支持的 K 线周期", seconds)
	}
	return name, nil
}

// 从交易所过滤器提取价格最小变动和数量步长。
func help_symbolSteps(symbol *binance.Symbol) (float64, float64, error) {
	var priceTick, quantityStep float64
	for _, filter := range symbol.Filters {
		switch filter["filterType"] {
		case "PRICE_FILTER":
			priceTick, _ = strconv.ParseFloat(fmt.Sprint(filter["tickSize"]), 64)
		case "LOT_SIZE":
			quantityStep, _ = strconv.ParseFloat(fmt.Sprint(filter["stepSize"]), 64)
		}
	}
	if priceTick <= 0 || quantityStep <= 0 {
		return 0, 0, fmt.Errorf("交易对 %s 缺少有效价格或数量过滤器", symbol.Symbol)
	}
	return priceTick, quantityStep, nil
}

// 将数量向下调整到交易所允许的数量步长。
func help_normalizeQuantity(raw string, step float64) (string, error) {
	quantity, err := strconv.ParseFloat(raw, 64)
	if err != nil || quantity <= 0 {
		return "", errors.New("quantity 必须为正数")
	}
	adjusted := math.Floor(quantity/step+1e-9) * step
	if adjusted <= 0 {
		return "", errors.New("quantity 小于交易所最小数量步长")
	}
	return strconv.FormatFloat(adjusted, 'f', -1, 64), nil
}

// 按策略信号处理开仓、过期订单撤销及持仓止盈止损。
func help_processCandle(ctx context.Context, client *binance.Client, cfg config,
	state *strategyState, closePrice, priceTick float64, quantity string) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	// step.1 查询当前持仓和本程序挂单，先维护已存在的交易。
	positions, err := client.NewGetPositionRiskService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("查询账户持仓: %w", err)
	}
	positionAmount := 0.0
	entryPrice := 0.0
	for _, position := range positions {
		if position.Symbol == cfg.Symbol {
			positionAmount, _ = strconv.ParseFloat(position.PositionAmt, 64)
			entryPrice, _ = strconv.ParseFloat(position.EntryPrice, 64)
			break
		}
	}
	openOrders, err := client.NewListOpenOrdersService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("查询普通挂单: %w", err)
	}
	ownedEntry := false
	for _, order := range openOrders {
		// 调用订单前缀辅助函数仅识别本策略创建的开仓单。
		if !strings.HasPrefix(order.ClientOrderID, help_orderPrefix(cfg.Symbol)+"E") {
			continue
		}
		ownedEntry = true
		if state.entryOrderID != order.ClientOrderID {
			state.entryOrderID = order.ClientOrderID
			state.entryAt = time.UnixMilli(order.Time)
		}
	}
	algos, err := client.NewListOpenAlgoOrdersService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("查询止盈止损条件单: %w", err)
	}
	ownedTakeProfit := false
	ownedStopLoss := false
	for _, order := range algos {
		// 调用订单前缀辅助函数区分本策略创建的保护条件单。
		if !strings.HasPrefix(order.ClientAlgoId, help_orderPrefix(cfg.Symbol)+"X") {
			continue
		}
		if order.OrderType == binance.AlgoOrderTypeTakeProfitMarket {
			ownedTakeProfit = true
		} else if order.OrderType == binance.AlgoOrderTypeStopMarket {
			ownedStopLoss = true
		}
	}
	if positionAmount != 0 {
		if state.entryOrderID == "" {
			return fmt.Errorf("检测到 %s 有非本进程可确认的持仓，暂停自动下单以保护用户仓位", cfg.Symbol)
		}
		if ownedTakeProfit && ownedStopLoss {
			return nil
		}
		for _, order := range openOrders {
			// 调用订单前缀辅助函数避免已部分成交的开仓单继续扩大仓位。
			if strings.HasPrefix(order.ClientOrderID, help_orderPrefix(cfg.Symbol)+"E") {
				if _, err := client.NewCancelOrderService().Symbol(cfg.Symbol).
					OrderID(order.OrderID).Do(ctx); err != nil {
					return fmt.Errorf("撤销部分成交开仓单剩余数量: %w", err)
				}
			}
		}
		// 调用保护单辅助函数为本策略刚建立的持仓设置止盈和止损。
		if err := help_placeProtection(ctx, client, cfg, state.entryOrderID,
			positionAmount, entryPrice, priceTick, ownedTakeProfit, ownedStopLoss); err != nil {
			return err
		}
		state.entryOrderID = ""
		return nil
	}
	for _, order := range algos {
		// 调用订单前缀辅助函数确认只清理本策略遗留的保护单。
		if strings.HasPrefix(order.ClientAlgoId, help_orderPrefix(cfg.Symbol)+"X") {
			// 调用交易所撤销服务清理本程序持仓结束后遗留的保护条件单。
			if _, err := client.NewCancelAlgoOrderService().ClientAlgoID(order.ClientAlgoId).Do(ctx); err != nil {
				return fmt.Errorf("撤销已空仓的保护单: %w", err)
			}
		}
	}
	// step.2 持仓为空时撤销上一周期开仓单，再更新 EMA 入场信号。
	if ownedEntry {
		if state.entryAt.IsZero() || time.Since(state.entryAt) >= time.Duration(cfg.IntervalSeconds)*time.Second {
			for _, order := range openOrders {
				// 调用订单前缀辅助函数确保只撤销本策略开仓挂单。
				if strings.HasPrefix(order.ClientOrderID, help_orderPrefix(cfg.Symbol)+"E") {
					// 调用交易所撤销服务清理已到期的本程序开仓限价单。
					if _, err := client.NewCancelOrderService().Symbol(cfg.Symbol).
						OrderID(order.OrderID).Do(ctx); err != nil {
						return fmt.Errorf("撤销到期开仓单: %w", err)
					}
				}
			}
			state.entryOrderID = ""
			state.entryAt = time.Time{}
			return nil
		}
		return nil
	}
	if state.emaReady {
		previous := state.ema
		state.ema = (2.0/26.0)*closePrice + (24.0/26.0)*state.ema
		delta := state.ema - previous
		if state.deltaReady && state.side != 0 {
			if state.side > 0 && state.previousDelta > 0 && delta < state.previousDelta {
				state.turns++
			} else if state.side < 0 && state.previousDelta < 0 && delta > state.previousDelta {
				state.turns++
			}
		}
		state.previousDelta = delta
		state.deltaReady = true
	} else {
		state.ema = closePrice
		state.emaReady = true
		return nil
	}
	side := 0
	if closePrice > state.ema {
		side = 1
	} else if closePrice < state.ema {
		side = -1
	}
	if side != state.side {
		state.side = side
		state.turns = 0
	}
	if side == 0 || state.turns < 2 {
		return nil
	}
	// 调用入场订单辅助函数按信号方向创建本策略 GTC 限价单。
	return help_placeEntry(ctx, client, cfg, state, -side, priceTick, quantity)
}

// 创建本策略开仓单 ID 前缀并限制在交易所支持的字符范围内。
func help_orderPrefix(symbol string) string {
	upperSymbol := strings.ToUpper(symbol)
	symbolPart := upperSymbol[:min(10, len(upperSymbol))]
	return fmt.Sprintf("BZ%s%08X-", symbolPart, crc32.ChecksumIEEE([]byte(upperSymbol)))
}

// 按盘口外侧逐档提交 GTC 限价开仓单并记录订单状态。
func help_placeEntry(ctx context.Context, client *binance.Client, cfg config,
	state *strategyState, side int, priceTick float64, quantity string) error {
	// step.1 读取最优买卖价并计算远离对手盘的首档限价。
	books, err := client.NewListBookTickersService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil || len(books) == 0 {
		return fmt.Errorf("读取最优盘口: %w", err)
	}
	bid, _ := strconv.ParseFloat(books[0].BidPrice, 64)
	ask, _ := strconv.ParseFloat(books[0].AskPrice, 64)
	basePrice := bid
	orderSide := binance.SideTypeBuy
	if side < 0 {
		basePrice = ask
		orderSide = binance.SideTypeSell
	}
	// step.2 逐档尝试 GTC 限价单，成功后将其登记为唯一开仓单。
	prefix := help_orderPrefix(cfg.Symbol) + "E"
	for level := 0; level < 5; level++ {
		price := basePrice - float64(level)*priceTick
		if side < 0 {
			price = basePrice + float64(level)*priceTick
		}
		// 调用价格格式化辅助函数确保开仓限价严格符合交易所 tick size。
		priceText := help_formatPrice(price, priceTick)
		clientID := fmt.Sprintf("%s%d", prefix, time.Now().UnixMilli())
		response, createErr := client.NewCreateOrderService().Symbol(cfg.Symbol).
			Side(orderSide).Type(binance.OrderTypeLimit).TimeInForce(binance.TimeInForceTypeGTC).
			Quantity(quantity).Price(priceText).NewClientOrderID(clientID).Do(ctx)
		if createErr == nil {
			state.entryOrderID = response.ClientOrderID
			state.entryAt = time.Now()
			state.turns = 0
			log.Printf("开仓限价单已提交: id=%s side=%s qty=%s price=%s",
				response.ClientOrderID, orderSide, quantity, priceText)
			return nil
		}
		log.Printf("第 %d 档开仓单失败: %v", level, createErr)
		if level == 4 {
			return fmt.Errorf("连续五档 GTC 开仓单均失败: %w", createErr)
		}
	}
	return nil
}

// 根据实际持仓均价提交止盈和止损条件平仓单。
func help_placeProtection(ctx context.Context, client *binance.Client, cfg config,
	entryOrderID string, position, entryPrice, priceTick float64,
	hasTakeProfit, hasStopLoss bool) error {
	// step.1 按持仓方向计算止盈止损触发价和反向平仓方向。
	closeSide := binance.SideTypeSell
	takePrice := entryPrice * (1 + cfg.TakeProfit)
	stopPrice := entryPrice * (1 - cfg.StopLoss)
	if position < 0 {
		closeSide = binance.SideTypeBuy
		takePrice = entryPrice * (1 - cfg.TakeProfit)
		stopPrice = entryPrice * (1 + cfg.StopLoss)
	}
	prefix := help_orderPrefix(cfg.Symbol) + "X" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	// step.2 设置交易所托管的止盈止损条件单，触发后平掉该方向全部仓位。
	if !hasTakeProfit {
		// 调用价格格式化辅助函数将止盈触发价调整到合法 tick 档位。
		takePriceText := help_formatPrice(takePrice, priceTick)
		_, err := client.NewCreateAlgoOrderService().Symbol(cfg.Symbol).
			Side(closeSide).Type(binance.AlgoOrderTypeTakeProfitMarket).
			ClosePosition(true).WorkingType(binance.WorkingTypeMarkPrice).
			TriggerPrice(takePriceText).
			ClientAlgoId(prefix + "T").Do(ctx)
		if err != nil {
			return fmt.Errorf("创建止盈条件单: %w", err)
		}
	}
	if !hasStopLoss {
		// 调用价格格式化辅助函数将止损触发价调整到合法 tick 档位。
		stopPriceText := help_formatPrice(stopPrice, priceTick)
		_, err := client.NewCreateAlgoOrderService().Symbol(cfg.Symbol).
			Side(closeSide).Type(binance.AlgoOrderTypeStopMarket).
			ClosePosition(true).WorkingType(binance.WorkingTypeMarkPrice).
			TriggerPrice(stopPriceText).
			ClientAlgoId(prefix + "S").Do(ctx)
		if err != nil {
			return fmt.Errorf("创建止损条件单: %w", err)
		}
	}
	log.Printf("持仓保护已设置: 来源开仓单=%s take=%g stop=%g", entryOrderID,
		takePrice, stopPrice)
	return nil
}

// 按双向持仓模式分别协调两侧订单、持仓和 EMA 入场信号。
func help_processCandleDual(ctx context.Context, client *binance.Client, cfg config,
	state *strategyState, closePrice, priceTick float64, quantity string) error {
	// step.1 查询账户交易状态并分别维护 LONG 与 SHORT。
	positions, err := client.NewGetPositionRiskService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("查询账户持仓: %w", err)
	}
	orders, err := client.NewListOpenOrdersService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("查询普通挂单: %w", err)
	}
	algos, err := client.NewListOpenAlgoOrdersService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("查询止盈止损条件单: %w", err)
	}
	// step.1.1 阻止新策略误认升级前没有方向标签的遗留订单。
	// 调用订单前缀辅助函数按交易对确认本策略订单命名空间。
	orderRoot := help_orderPrefix(cfg.Symbol)
	for _, order := range orders {
		if strings.HasPrefix(order.ClientOrderID, orderRoot+"E") &&
			!strings.HasPrefix(order.ClientOrderID, orderRoot+"EL") &&
			!strings.HasPrefix(order.ClientOrderID, orderRoot+"ES") {
			return fmt.Errorf("发现升级前遗留开仓单 %s，请先在 Binance 撤销后重启", order.ClientOrderID)
		}
	}
	for _, order := range algos {
		if strings.HasPrefix(order.ClientAlgoId, orderRoot+"X") &&
			!strings.HasPrefix(order.ClientAlgoId, orderRoot+"XL") &&
			!strings.HasPrefix(order.ClientAlgoId, orderRoot+"XS") {
			return fmt.Errorf("发现升级前遗留条件单 %s，请先在 Binance 撤销后重启", order.ClientAlgoId)
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	long := &positionState{entryOrderID: state.longEntryID, entryAt: state.longEntryAt}
	short := &positionState{entryOrderID: state.shortEntryID, entryAt: state.shortEntryAt}
	// 调用方向管理辅助函数维护本策略 LONG 仓位和订单。
	if err := help_managePositionSide(ctx, client, cfg, positions, orders,
		algos, binance.PositionSideTypeLong, long, priceTick); err != nil {
		return err
	}
	// 调用方向管理辅助函数维护本策略 SHORT 仓位和订单。
	if err := help_managePositionSide(ctx, client, cfg, positions, orders,
		algos, binance.PositionSideTypeShort, short, priceTick); err != nil {
		return err
	}
	state.longEntryID, state.longEntryAt = long.entryOrderID, long.entryAt
	state.shortEntryID, state.shortEntryAt = short.entryOrderID, short.entryAt
	// step.2 更新 EMA 入场信号，并只在目标方向没有仓位或挂单时开仓。
	if !state.emaReady {
		state.ema = closePrice
		state.emaReady = true
		return nil
	}
	previous := state.ema
	state.ema = (2.0/26.0)*closePrice + (24.0/26.0)*state.ema
	delta := state.ema - previous
	if state.deltaReady && state.side != 0 {
		if state.side > 0 && state.previousDelta > 0 && delta < state.previousDelta {
			state.turns++
		} else if state.side < 0 && state.previousDelta < 0 && delta > state.previousDelta {
			state.turns++
		}
	}
	state.previousDelta, state.deltaReady = delta, true
	signal := 0
	if closePrice > state.ema {
		signal = 1
	} else if closePrice < state.ema {
		signal = -1
	}
	if signal != state.side {
		state.side, state.turns = signal, 0
	}
	if signal == 0 || state.turns < 2 {
		return nil
	}
	entrySide := -signal
	target := long
	if entrySide < 0 {
		target = short
	}
	if target.entryOrderID != "" {
		return nil
	}
	// 调用方向开仓辅助函数创建目标持仓侧的唯一 GTC 挂单。
	if err := help_placeEntrySide(ctx, client, cfg, state, target, entrySide,
		priceTick, quantity); err != nil {
		return err
	}
	state.longEntryID, state.longEntryAt = long.entryOrderID, long.entryAt
	state.shortEntryID, state.shortEntryAt = short.entryOrderID, short.entryAt
	return nil
}

// 仅维护指定持仓方向的仓位保护单和对应开仓挂单。
func help_managePositionSide(ctx context.Context, client *binance.Client, cfg config,
	positions []*binance.PositionRisk, orders []*binance.Order,
	algos []binance.GetAlgoOrderResp, positionSide binance.PositionSideType,
	state *positionState, priceTick float64) error {
	// step.1 按 positionSide 和本策略订单 ID 前缀确认该方向交易归属。
	// 调用订单前缀辅助函数按交易对生成本策略订单命名空间。
	orderPrefix := help_orderPrefix(cfg.Symbol)
	sideCode := "L"
	if positionSide == binance.PositionSideTypeShort {
		sideCode = "S"
	}
	entryPrefix := orderPrefix + "E" + sideCode
	algoPrefix := orderPrefix + "X" + sideCode
	var amount, entryPrice float64
	for _, position := range positions {
		if position.Symbol == cfg.Symbol && position.PositionSide == string(positionSide) {
			amount, _ = strconv.ParseFloat(position.PositionAmt, 64)
			entryPrice, _ = strconv.ParseFloat(position.EntryPrice, 64)
			break
		}
	}
	ownedEntry, hasTakeProfit, hasStopLoss := false, false, false
	for _, order := range orders {
		if order.PositionSide != positionSide || !strings.HasPrefix(order.ClientOrderID, entryPrefix) {
			continue
		}
		ownedEntry = true
		if state.entryOrderID != order.ClientOrderID {
			state.entryOrderID = order.ClientOrderID
			state.entryAt = time.UnixMilli(order.Time)
		}
		state.entryFilled = false
		state.positionSeen = false
	}
	for _, order := range algos {
		if order.PositionSide != positionSide || !strings.HasPrefix(order.ClientAlgoId, algoPrefix) {
			continue
		}
		if order.OrderType == binance.AlgoOrderTypeTakeProfitMarket {
			hasTakeProfit = true
		} else if order.OrderType == binance.AlgoOrderTypeStopMarket {
			hasStopLoss = true
		}
	}
	// step.2 给已确认持仓补保护单，或只清理该方向的空仓遗留单。
	if amount != 0 {
		state.positionSeen = true
		if state.entryOrderID == "" {
			// 调用订单历史辅助函数恢复本程序已成交开仓单的归属标识。
			entryOrderID, entryAt, err := help_findFilledEntryOrder(ctx, client,
				cfg.Symbol, positionSide)
			if err != nil {
				return err
			}
			state.entryOrderID, state.entryAt = entryOrderID, entryAt
		}
		if state.entryOrderID == "" && !hasTakeProfit && !hasStopLoss {
			return fmt.Errorf("检测到 %s %s 仓位没有本策略订单标识，暂停自动管理",
				cfg.Symbol, positionSide)
		}
		for _, order := range orders {
			if order.PositionSide == positionSide && strings.HasPrefix(order.ClientOrderID, entryPrefix) {
				if _, err := client.NewCancelOrderService().Symbol(cfg.Symbol).
					OrderID(order.OrderID).Do(ctx); err != nil {
					return fmt.Errorf("撤销部分成交开仓单剩余数量: %w", err)
				}
			}
		}
		// 调用方向保护辅助函数为当前 LONG 或 SHORT 仓位补齐止盈止损。
		if err := help_placeProtectionSide(ctx, client, cfg, state.entryOrderID,
			positionSide, amount, entryPrice, priceTick, hasTakeProfit, hasStopLoss); err != nil {
			return err
		}
		return nil
	}
	if state.entryOrderID != "" && !ownedEntry {
		// 调用交易所订单查询服务确认消失的开仓单是否已经成交。
		entryOrder, err := client.NewGetOrderService().Symbol(cfg.Symbol).
			OrigClientOrderID(state.entryOrderID).Do(ctx)
		if err != nil {
			return fmt.Errorf("查询%s开仓单最终状态: %w", positionSide, err)
		}
		if entryOrder.Status == binance.OrderStatusTypeFilled {
			if state.positionSeen {
				state.entryOrderID, state.entryAt = "", time.Time{}
				state.entryFilled, state.positionSeen = false, false
			} else if !state.entryFilled {
				state.entryFilled = true
				state.entryAt = time.Now()
			} else if time.Since(state.entryAt) < 2*time.Duration(cfg.IntervalSeconds)*time.Second {
				return nil
			} else {
				state.entryOrderID, state.entryAt = "", time.Time{}
				state.entryFilled = false
			}
		} else {
			state.entryOrderID, state.entryAt = "", time.Time{}
			state.entryFilled, state.positionSeen = false, false
		}
	}
	for _, order := range algos {
		if order.PositionSide == positionSide && strings.HasPrefix(order.ClientAlgoId, algoPrefix) {
			if _, err := client.NewCancelAlgoOrderService().
				ClientAlgoID(order.ClientAlgoId).Do(ctx); err != nil {
				return fmt.Errorf("撤销空仓后的%s保护单: %w", positionSide, err)
			}
		}
	}
	if ownedEntry {
		if state.entryAt.IsZero() || time.Since(state.entryAt) >=
			time.Duration(cfg.IntervalSeconds)*time.Second {
			for _, order := range orders {
				if order.PositionSide == positionSide && strings.HasPrefix(order.ClientOrderID, entryPrefix) {
					if _, err := client.NewCancelOrderService().Symbol(cfg.Symbol).
						OrderID(order.OrderID).Do(ctx); err != nil {
						return fmt.Errorf("撤销到期的%s开仓单: %w", positionSide, err)
					}
				}
			}
			state.entryOrderID, state.entryAt = "", time.Time{}
			state.entryFilled, state.positionSeen = false, false
		}
	} else if state.entryOrderID == "" {
		state.entryFilled, state.positionSeen = false, false
	}
	return nil
}

// 从近期成交订单中恢复指定双向仓位的本策略开仓标识。
func help_findFilledEntryOrder(ctx context.Context, client *binance.Client,
	symbol string, positionSide binance.PositionSideType) (string, time.Time, error) {
	// step.1 按交易对和持仓方向筛选本策略的已成交开仓订单。
	sideCode := "L"
	orderSide := binance.SideTypeBuy
	if positionSide == binance.PositionSideTypeShort {
		sideCode, orderSide = "S", binance.SideTypeSell
	}
	// 调用订单前缀辅助函数限定当前方向的本策略订单命名空间。
	entryPrefix := help_orderPrefix(symbol) + "E" + sideCode
	orders, err := client.NewListOrdersService().Symbol(symbol).Limit(100).Do(ctx)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("查询%s历史开仓单: %w", positionSide, err)
	}
	// step.2 选择最近一笔符合本策略命名空间的已成交开仓单。
	var latest *binance.Order
	for _, order := range orders {
		if order.PositionSide != positionSide || order.Side != orderSide ||
			order.Status != binance.OrderStatusTypeFilled ||
			!strings.HasPrefix(order.ClientOrderID, entryPrefix) {
			continue
		}
		if latest == nil || order.Time > latest.Time {
			latest = order
		}
	}
	if latest == nil {
		return "", time.Time{}, nil
	}
	return latest.ClientOrderID, time.UnixMilli(latest.Time), nil
}

// 将指定方向的 GTC 开仓单绑定到对应的双向持仓。
func help_placeEntrySide(ctx context.Context, client *binance.Client, cfg config,
	strategy *strategyState, state *positionState, side int, priceTick float64, quantity string) error {
	// step.1 读取盘口并按开仓方向计算逐档限价。
	books, err := client.NewListBookTickersService().Symbol(cfg.Symbol).Do(ctx)
	if err != nil || len(books) == 0 {
		return fmt.Errorf("读取最优盘口: %w", err)
	}
	bid, _ := strconv.ParseFloat(books[0].BidPrice, 64)
	ask, _ := strconv.ParseFloat(books[0].AskPrice, 64)
	basePrice, orderSide := bid, binance.SideTypeBuy
	positionSide := binance.PositionSideTypeLong
	sideCode := "L"
	if side < 0 {
		basePrice, orderSide = ask, binance.SideTypeSell
		positionSide, sideCode = binance.PositionSideTypeShort, "S"
	}
	// step.2 使用方向专属 ID 提交最多五档限价单并记录订单状态。
	// 调用订单前缀辅助函数按交易对生成本策略订单命名空间。
	prefix := help_orderPrefix(cfg.Symbol) + "E" + sideCode
	for level := 0; level < 5; level++ {
		price := basePrice - float64(level)*priceTick*float64(side)
		// 调用价格格式化辅助函数将限价调整到合法 tick 档位。
		priceText := help_formatPrice(price, priceTick)
		clientID := fmt.Sprintf("%s%d", prefix, time.Now().UnixMilli())
		response, createErr := client.NewCreateOrderService().Symbol(cfg.Symbol).
			Side(orderSide).PositionSide(positionSide).Type(binance.OrderTypeLimit).
			TimeInForce(binance.TimeInForceTypeGTC).Quantity(quantity).Price(priceText).
			NewClientOrderID(clientID).Do(ctx)
		if createErr == nil {
			state.entryOrderID, state.entryAt = response.ClientOrderID, time.Now()
			state.entryFilled, state.positionSeen = false, false
			strategy.turns = 0
			log.Printf("%s 开仓限价单已提交: id=%s qty=%s price=%s",
				positionSide, response.ClientOrderID, quantity, priceText)
			return nil
		}
		if level == 4 {
			return fmt.Errorf("连续五档 %s GTC 开仓单均失败: %w", positionSide, createErr)
		}
	}
	return nil
}

// 为指定双向持仓提交 positionSide 隔离的止盈和止损条件单。
func help_placeProtectionSide(ctx context.Context, client *binance.Client, cfg config,
	entryOrderID string, positionSide binance.PositionSideType,
	position, entryPrice, priceTick float64, hasTakeProfit, hasStopLoss bool) error {
	// step.1 根据持仓方向计算触发价与平仓订单方向。
	closeSide := binance.SideTypeSell
	takePrice, stopPrice := entryPrice*(1+cfg.TakeProfit), entryPrice*(1-cfg.StopLoss)
	if positionSide == binance.PositionSideTypeShort {
		closeSide = binance.SideTypeBuy
		takePrice, stopPrice = entryPrice*(1-cfg.TakeProfit), entryPrice*(1+cfg.StopLoss)
	}
	sideCode := "L"
	if positionSide == binance.PositionSideTypeShort {
		sideCode = "S"
	}
	// 调用订单前缀辅助函数按交易对生成本策略订单命名空间。
	orderPrefix := help_orderPrefix(cfg.Symbol)
	stamp := fmt.Sprintf("%012d", time.Now().UnixMilli()%1_000_000_000_000)
	prefix := orderPrefix + "X" + sideCode + stamp
	// step.2 提交绑定该持仓方向的全仓止盈止损条件单。
	if !hasTakeProfit {
		// 调用价格格式化辅助函数将止盈触发价调整到合法 tick 档位。
		trigger := help_formatPrice(takePrice, priceTick)
		_, err := client.NewCreateAlgoOrderService().Symbol(cfg.Symbol).Side(closeSide).
			PositionSide(positionSide).Type(binance.AlgoOrderTypeTakeProfitMarket).
			ClosePosition(true).WorkingType(binance.WorkingTypeMarkPrice).
			TriggerPrice(trigger).ClientAlgoId(prefix + "T").Do(ctx)
		if err != nil {
			return fmt.Errorf("创建%s止盈条件单: %w", positionSide, err)
		}
	}
	if !hasStopLoss {
		// 调用价格格式化辅助函数将止损触发价调整到合法 tick 档位。
		trigger := help_formatPrice(stopPrice, priceTick)
		_, err := client.NewCreateAlgoOrderService().Symbol(cfg.Symbol).Side(closeSide).
			PositionSide(positionSide).Type(binance.AlgoOrderTypeStopMarket).
			ClosePosition(true).WorkingType(binance.WorkingTypeMarkPrice).
			TriggerPrice(trigger).ClientAlgoId(prefix + "S").Do(ctx)
		if err != nil {
			return fmt.Errorf("创建%s止损条件单: %w", positionSide, err)
		}
	}
	log.Printf("%s 持仓保护已设置: 来源开仓单=%s take=%g stop=%g",
		positionSide, entryOrderID, takePrice, stopPrice)
	return nil
}

// 将价格四舍五入并格式化到交易所允许的最小价格步长。
func help_formatPrice(price, tick float64) string {
	tickDecimal := decimal.NewFromFloat(tick)
	rounded := decimal.NewFromFloat(price).Div(tickDecimal).Round(0).Mul(tickDecimal)
	tickText := strconv.FormatFloat(tick, 'f', -1, 64)
	decimalPlaces := 0
	if point := strings.IndexByte(tickText, '.'); point >= 0 {
		decimalPlaces = len(tickText) - point - 1
	}
	return rounded.StringFixed(int32(decimalPlaces))
}
