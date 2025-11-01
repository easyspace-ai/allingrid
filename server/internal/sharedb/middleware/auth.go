package middleware

import (
	"context"
	"errors"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
	"go.uber.org/zap"
)

// AuthMiddleware 鉴权中间件
type AuthMiddleware struct {
	tokenService TokenService
	logger       *zap.Logger
}

// TokenService Token 服务接口
type TokenService interface {
	ValidateToken(token string) (string, error) // 返回用户ID
}

// NewAuthMiddleware 创建鉴权中间件
func NewAuthMiddleware(tokenService TokenService, logger *zap.Logger) sharedb.Middleware {
	return &AuthMiddleware{
		tokenService: tokenService,
		logger:       logger,
	}
}

// Handle 处理消息
func (m *AuthMiddleware) Handle(ctx context.Context, conn *sharedb.Connection, msg *sharedb.Message) error {
	// 从连接中获取用户ID（应该在连接建立时设置）
	if conn.UserID == "" {
		m.logger.Warn("Connection without user ID (允许匿名操作)",
			zap.String("connection_id", conn.ID))
		// 不返回错误，允许继续，因为 JWT 已在 WebSocket 建立时验证
	} else {
		// 记录操作日志
		m.logger.Debug("Authenticated operation",
			zap.String("connection_id", conn.ID),
			zap.String("user_id", conn.UserID),
			zap.String("action", msg.Action))
	}

	return nil
}

// ExtractUserIDFromToken 从 Token 中提取用户ID
func ExtractUserIDFromToken(token string) (string, error) {
	// 移除 "Bearer " 前缀
	token = strings.TrimPrefix(token, "Bearer ")

	// 这里应该实现 JWT 解析逻辑
	// 暂时返回一个模拟的用户ID
	if token == "" {
		return "", errors.New("empty token")
	}

	// 模拟解析：实际应该使用 JWT 库
	return "user_" + token[:8], nil
}
