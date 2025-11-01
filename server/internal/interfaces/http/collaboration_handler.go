package http

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// CollaborationHandler 协作功能HTTP处理器
type CollaborationHandler struct{ logger *zap.Logger }

// NewCollaborationHandler 创建协作功能HTTP处理器
func NewCollaborationHandler(_ interface{}, logger *zap.Logger) *CollaborationHandler {
	return &CollaborationHandler{logger: logger}
}

// UpdatePresenceRequest 更新在线状态请求
type UpdatePresenceRequest struct {
	Collection string                 `json:"collection" binding:"required"`
	Data       map[string]interface{} `json:"data"`
}

// UpdatePresence 更新用户在线状态
// @Summary 更新用户在线状态
// @Description 更新用户在指定集合中的在线状态
// @Tags 协作
// @Accept json
// @Produce json
// @Param request body UpdatePresenceRequest true "更新在线状态请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/presence [post]
func (h *CollaborationHandler) UpdatePresence(c *gin.Context) {
	var req UpdatePresenceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 从JWT中获取用户ID和会话ID
	_, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	sessionID := c.GetHeader("X-Session-ID")
	if sessionID == "" {
		sessionID = "default"
	}

	// 更新在线状态
	// 状态：占位符实现
	// 原因：旧协作服务依赖 WebSocket，已移除；改走业务事件/YJS 侧
	// 计划：未来将基于业务事件系统实现真实的协作功能
	// 当前：仅返回成功响应以保持API兼容性

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// RemovePresence 移除用户在线状态
// @Summary 移除用户在线状态
// @Description 移除用户在指定集合中的在线状态
// @Tags 协作
// @Produce json
// @Param collection query string true "集合名称"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/presence [delete]
func (h *CollaborationHandler) RemovePresence(c *gin.Context) {
	collection := c.Query("collection")
	if collection == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Collection is required"))
		return
	}

	// 从JWT中获取用户ID和会话ID
	_, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	sessionID := c.GetHeader("X-Session-ID")
	if sessionID == "" {
		sessionID = "default"
	}

	// 移除在线状态（legacy 占位，无实际后端存储）
	// 状态：占位符实现
	// 原因：协作系统已重构，不再需要传统的在线状态管理
	// 计划：未来将基于业务事件系统实现真实的协作功能
	// 当前：仅返回成功响应以保持API兼容性

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// GetPresence 获取在线状态
// @Summary 获取在线状态
// @Description 获取指定集合中的在线用户状态
// @Tags 协作
// @Produce json
// @Param collection query string true "集合名称"
// @Success 200 {object} Response{data=[]websocket.PresenceInfo}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/presence [get]
func (h *CollaborationHandler) GetPresence(c *gin.Context) {
	collection := c.Query("collection")
	if collection == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Collection is required"))
		return
	}

	// 获取在线状态
	response.SuccessWithMessage(c, []map[string]interface{}{}, "")
}

// UpdateCursorRequest 更新光标位置请求
type UpdateCursorRequest struct {
	Collection string                 `json:"collection" binding:"required"`
	Document   string                 `json:"document" binding:"required"`
	Position   map[string]interface{} `json:"position" binding:"required"`
	Selection  map[string]interface{} `json:"selection,omitempty"`
}

// UpdateCursor 更新用户光标位置
// @Summary 更新用户光标位置
// @Description 更新用户在指定文档中的光标位置
// @Tags 协作
// @Accept json
// @Produce json
// @Param request body UpdateCursorRequest true "更新光标位置请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/cursor [post]
func (h *CollaborationHandler) UpdateCursor(c *gin.Context) {
	var req UpdateCursorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 从JWT中获取用户ID和会话ID
	_, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	sessionID := c.GetHeader("X-Session-ID")
	if sessionID == "" {
		sessionID = "default"
	}

	// 旧协作服务已移除，此处不再存储，返回成功
	_ = time.Now() // 保留时间引用

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// RemoveCursor 移除用户光标
// @Summary 移除用户光标
// @Description 移除用户在指定文档中的光标
// @Tags 协作
// @Produce json
// @Param collection query string true "集合名称"
// @Param document query string true "文档ID"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/cursor [delete]
func (h *CollaborationHandler) RemoveCursor(c *gin.Context) {
	collection := c.Query("collection")
	document := c.Query("document")
	if collection == "" || document == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Collection and document are required"))
		return
	}

	// 从JWT中获取用户ID和会话ID
	_, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	sessionID := c.GetHeader("X-Session-ID")
	if sessionID == "" {
		sessionID = "default"
	}

	// 移除光标（legacy 占位）

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// GetCursors 获取光标信息
// @Summary 获取光标信息
// @Description 获取指定集合中的所有用户光标信息
// @Tags 协作
// @Produce json
// @Param collection query string true "集合名称"
// @Success 200 {object} Response{data=[]websocket.CursorInfo}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/cursor [get]
func (h *CollaborationHandler) GetCursors(c *gin.Context) {
	collection := c.Query("collection")
	if collection == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Collection is required"))
		return
	}

	// 获取光标信息
	response.SuccessWithMessage(c, []map[string]interface{}{}, "")
}

// SendNotificationRequest 发送通知请求
type SendNotificationRequest struct {
	Collection string                 `json:"collection" binding:"required"`
	UserID     string                 `json:"user_id" binding:"required"`
	Type       string                 `json:"type" binding:"required"`
	Data       map[string]interface{} `json:"data"`
}

// SendNotification 发送协作通知
// @Summary 发送协作通知
// @Description 向指定用户发送协作通知
// @Tags 协作
// @Accept json
// @Produce json
// @Param request body SendNotificationRequest true "发送通知请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/notification [post]
func (h *CollaborationHandler) SendNotification(c *gin.Context) {
	var req SendNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 发送通知
	// 旧协作服务已移除，此处仅返回成功占位

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// GetCollaborationStats 获取协作统计信息
// @Summary 获取协作统计信息
// @Description 获取实时协作的统计信息
// @Tags 协作
// @Produce json
// @Success 200 {object} Response{data=object}
// @Failure 500 {object} ErrorResponse
// @Router /api/collaboration/stats [get]
func (h *CollaborationHandler) GetCollaborationStats(c *gin.Context) {
	// 获取统计信息
	// 完整实现需要：
	// 1. 从WebSocket管理器获取活跃连接数
	// 2. 从协作服务获取在线用户presence
	// 3. 从协作服务获取活跃光标数
	stats := map[string]interface{}{
		"active_connections": 0, // 需要 WebSocket 管理器支持
		"active_presence":    0, // 需要协作服务支持
		"active_cursors":     0, // 需要协作服务支持
		"timestamp":          time.Now().Format(time.RFC3339),
	}

	response.SuccessWithMessage(c, stats, "")
}

func (h *CollaborationHandler) handleError(c *gin.Context, err error) {
	response.Error(c, err)
}
