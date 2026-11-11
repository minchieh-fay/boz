package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// main 读取部署配置，运行实盘交易并在系统信号下清理未完成开仓单。
func main() {
	path := flag.String("config", "config.json", "策略配置路径")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app, err := newApplication(*path)
	if err != nil {
		log.Fatal(err)
	}
	if err = app.run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
