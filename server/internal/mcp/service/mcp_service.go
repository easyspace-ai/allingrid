package service

import (
	"context"
	"log"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/server"
)

// MCPService MCP 服务
type MCPService struct {
	server *server.MCPServer
	config *config.MCPConfig
	logger *log.Logger
}

// NewMCPService 创建新的 MCP 服务
func NewMCPService(cfg *config.MCPConfig, logger *log.Logger) *MCPService {
	if logger == nil {
		logger = log.New(log.Writer(), "[MCP-Service] ", log.LstdFlags|log.Lshortfile)
	}

	return &MCPService{
		config: cfg,
		logger: logger,
	}
}

// Start 启动 MCP 服务
func (s *MCPService) Start() error {
	if !s.config.Enabled {
		s.logger.Println("MCP service is disabled")
		return nil
	}

	s.logger.Println("Starting MCP service...")

	// 创建 MCP 服务器配置
	serverConfig := &server.Config{
		Host:         s.config.Server.Host,
		Port:         s.config.Server.Port,
		ReadTimeout:  s.config.Server.Timeout,
		WriteTimeout: s.config.Server.Timeout,
		IdleTimeout:  s.config.Server.Timeout * 4,
		EnableCORS:   true,
		EnableDebug:  false, // 根据主配置设置
	}

	// 创建 MCP 服务器
	s.server = server.NewMCPServer(serverConfig, s.logger)

	// 启动服务器
	return s.server.Start()
}

// Stop 停止 MCP 服务
func (s *MCPService) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	s.logger.Println("Stopping MCP service...")
	return s.server.Stop(ctx)
}

// IsEnabled 检查服务是否启用
func (s *MCPService) IsEnabled() bool {
	return s.config.Enabled
}

// GetConfig 获取配置
func (s *MCPService) GetConfig() *config.MCPConfig {
	return s.config
}

// GetServer 获取服务器实例
func (s *MCPService) GetServer() *server.MCPServer {
	return s.server
}
