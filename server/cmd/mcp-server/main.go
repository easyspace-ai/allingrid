package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/service"
)

func main() {
	// 解析命令行参数
	var stdioMode = flag.Bool("stdio", false, "Run in stdio mode for MCP clients")
	flag.Parse()

	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 检查 MCP 是否启用
	if !cfg.MCP.Enabled {
		log.Println("MCP service is disabled in configuration")
		return
	}

	// 创建日志器
	var logger *log.Logger
	if *stdioMode {
		// 在 stdio 模式下，将日志输出到 stderr，避免干扰 MCP 协议
		logger = log.New(os.Stderr, "[MCP-Server] ", log.LstdFlags|log.Lshortfile)
	} else {
		logger = log.New(os.Stdout, "[MCP-Server] ", log.LstdFlags|log.Lshortfile)
	}

	// 创建 MCP 服务
	mcpService := service.NewMCPService(&cfg.MCP, logger)

	if *stdioMode {
		// stdio 模式：直接处理标准输入输出
		logger.Println("Starting MCP server in stdio mode...")

		// 创建 stdio 服务
		stdioService := service.NewStdioMCPService(&cfg.MCP, logger)

		// 启动 stdio 服务
		if err := stdioService.Start(); err != nil {
			logger.Fatalf("Failed to start MCP stdio service: %v", err)
		}
	} else {
		// HTTP 模式：正常的 HTTP 服务器
		logger.Println("Starting MCP server in HTTP mode...")

		// 启动服务
		go func() {
			if err := mcpService.Start(); err != nil {
				logger.Fatalf("Failed to start MCP service: %v", err)
			}
		}()

		// 等待中断信号
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		logger.Println("Shutting down MCP server...")

		// 创建关闭上下文
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// 停止服务
		if err := mcpService.Stop(ctx); err != nil {
			logger.Printf("Error stopping MCP service: %v", err)
		}

		logger.Println("MCP server stopped")
	}
}
