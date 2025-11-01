package http

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// HandlerHelper 处理器辅助工具，提供通用功能
type HandlerHelper struct {
	logger *zap.Logger
}

// NewHandlerHelper 创建处理器辅助工具
func NewHandlerHelper(log *zap.Logger) *HandlerHelper {
	if log == nil {
		log = logger.Logger
	}
	return &HandlerHelper{
		logger: log,
	}
}

// Success 成功响应
func (h *HandlerHelper) Success(c *gin.Context, data interface{}) {
	response.SuccessWithMessage(c, data, "")
}

// Error 错误响应
func (h *HandlerHelper) Error(c *gin.Context, err error) {
	response.Error(c, err)
}

// BadRequest 400 错误
func (h *HandlerHelper) BadRequest(c *gin.Context, message string) {
	h.Error(c, errors.ErrInvalidRequest.WithDetails(message))
}

// Unauthorized 401 错误
func (h *HandlerHelper) Unauthorized(c *gin.Context, message string) {
	h.Error(c, errors.ErrUnauthorized.WithDetails(message))
}

// GetUserID 从上下文获取用户ID
func (h *HandlerHelper) GetUserID(c *gin.Context) (string, bool) {
	userID, exists := c.Get("user_id")
	if !exists {
		return "", false
	}
	userIDStr, ok := userID.(string)
	return userIDStr, ok
}

// BindJSON 绑定JSON并处理错误
func (h *HandlerHelper) BindJSON(c *gin.Context, obj interface{}) bool {
	if err := c.ShouldBindJSON(obj); err != nil {
		h.BadRequest(c, err.Error())
		return false
	}
	return true
}

// GetQueryInt 获取查询整数参数
func (h *HandlerHelper) GetQueryInt(c *gin.Context, key string, defaultValue int) int {
	value := c.Query(key)
	if value == "" {
		return defaultValue
	}
	var intValue int
	if _, err := fmt.Sscanf(value, "%d", &intValue); err != nil {
		return defaultValue
	}
	return intValue
}
