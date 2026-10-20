package main

import (
	"context"
	"fmt"
	"log"
	"os"
)

// 主函数加载交易配置并启动 Binance Futures 实时策略。
func main() {
	// step.1 读取并校验配置，加载账户密钥。
	configPath := "config.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	// 调用配置加载辅助函数取得实时交易参数。
	config, err := help_loadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}
	// 调用凭证加载辅助函数读取 API Key 和 Secret。
	apiKey, secret, err := help_loadCredentials(config.KeyFile)
	if err != nil {
		log.Fatal(err)
	}

	// step.2 连接币安行情并按策略信号管理真实账户订单。
	ctx := context.Background()
	log.Printf("启动策略: symbol=%s quantity=%s interval=%ds",
		config.Symbol, config.Quantity, config.IntervalSeconds)
	// 调用实时交易辅助函数订阅行情并维护账户订单。
	if err := help_runLive(ctx, config, apiKey, secret); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
