package http

import (
	"github.com/easyspace-ai/luckdb/server/internal/interfaces/middleware"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// PinHandler Pin HTTP处理器
type PinHandler struct {
	// 这里可以添加 pin 相关的服务
}

// NewPinHandler 创建 Pin 处理器
func NewPinHandler() *PinHandler {
	return &PinHandler{}
}

// PinItem Pin 项目结构
type PinItem struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Order int    `json:"order"`
}

// ListPins 获取 Pin 列表
// @Summary 获取 Pin 列表
// @Description 获取用户的 Pin 列表
// @Tags Pin
// @Accept json
// @Produce json
// @Success 200 {object} SuccessResponse{data=[]PinItem}
// @Failure 401 {object} ErrorResponse
// @Router /api/pin/list [get]
func (h *PinHandler) ListPins(c *gin.Context) {
	// 获取当前用户ID
	userID, err := middleware.GetCurrentUserID(c)
	if err != nil {
		h.handleError(c, err)
		return
	}

	logger.Info("Getting pin list for user", logger.String("user_id", userID))

	// 暂时返回空数组，避免前端错误
	// 完整实现需要：
	// 1. 从数据库获取用户的 pin 记录
	// 2. 按 order 排序
	// 3. 填充相关资源信息（space、table、view等）
	pins := []PinItem{}

	response.SuccessWithMessage(c, pins, "")
}

// handleError 统一错误处理
func (h *PinHandler) handleError(c *gin.Context, err error) {
	traceID := c.GetString("request_id")

	if appErr, ok := errors.IsAppError(err); ok {
		logger.Error("Application error",
			logger.String("error", appErr.Message),
			logger.String("code", appErr.Code),
			logger.String("trace_id", traceID),
		)
		response.Error(c, appErr)
		return
	}

	logger.Error("Internal server error",
		logger.ErrorField(err),
		logger.String("trace_id", traceID),
	)

	response.Error(c, errors.ErrInternalServer)
}
