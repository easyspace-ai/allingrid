package http

import (
	"net/http"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ShareDBHandler ShareDB HTTP 处理器
type ShareDBHandler struct {
	service *sharedb.ShareDBService
	logger  *zap.Logger
}

// NewShareDBHandler 创建 ShareDB 处理器
func NewShareDBHandler(service *sharedb.ShareDBService, logger *zap.Logger) *ShareDBHandler {
	return &ShareDBHandler{
		service: service,
		logger:  logger,
	}
}

// HandleWebSocket 处理 WebSocket 连接
func (h *ShareDBHandler) HandleWebSocket(c *gin.Context) {
	if h.service == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "ShareDB service not available",
		})
		return
	}

	// 调用 ShareDB 服务处理 WebSocket
	h.service.HandleWebSocket(c)
}

// GetStats 获取统计信息
func (h *ShareDBHandler) GetStats(c *gin.Context) {
	if h.service == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "ShareDB service not available",
		})
		return
	}

	stats := h.service.GetStats()
	c.JSON(http.StatusOK, gin.H{
		"stats": stats,
	})
}

// GetConnections 获取连接信息
func (h *ShareDBHandler) GetConnections(c *gin.Context) {
	if h.service == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "ShareDB service not available",
		})
		return
	}

	// 这里可以添加获取连接详情的逻辑
	c.JSON(http.StatusOK, gin.H{
		"connections": []interface{}{},
	})
}

// ForceCleanupConnections 强制清理所有连接（开发环境使用）
func (h *ShareDBHandler) ForceCleanupConnections(c *gin.Context) {
	if h.service == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "ShareDB service not available",
		})
		return
	}

	// 强制清理所有连接
	h.service.ForceCleanupAllConnections()

	c.JSON(http.StatusOK, gin.H{
		"message":   "All connections have been cleaned up",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
