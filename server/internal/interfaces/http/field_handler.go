package http

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// FieldHandler 字段HTTP处理器
type FieldHandler struct {
	fieldService *application.FieldService
}

// NewFieldHandler 创建字段处理器
func NewFieldHandler(fieldService *application.FieldService) *FieldHandler {
	return &FieldHandler{
		fieldService: fieldService,
	}
}

// CreateField 创建字段
func (h *FieldHandler) CreateField(c *gin.Context) {
	var req dto.CreateFieldRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

    // 优先使用路径参数中的 tableId，避免客户端重复传参
    if tableID := c.Param("tableId"); tableID != "" {
        req.TableID = tableID
    }

	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	resp, err := h.fieldService.CreateField(c.Request.Context(), req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "创建字段成功")
}

// GetField 获取字段详情
func (h *FieldHandler) GetField(c *gin.Context) {
	fieldID := c.Param("fieldId")

	resp, err := h.fieldService.GetField(c.Request.Context(), fieldID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取字段成功")
}

// UpdateField 更新字段
func (h *FieldHandler) UpdateField(c *gin.Context) {
	fieldID := c.Param("fieldId")
	
	// 添加日志：记录接收到的 fieldId
	fmt.Printf("[FieldHandler.UpdateField] 收到更新请求, fieldId: %s\n", fieldID)

	var req dto.UpdateFieldRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	resp, err := h.fieldService.UpdateField(c.Request.Context(), fieldID, req)
	if err != nil {
		fmt.Printf("[FieldHandler.UpdateField] 更新失败, fieldId: %s, error: %v\n", fieldID, err)
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "更新字段成功")
}

// DeleteField 删除字段
func (h *FieldHandler) DeleteField(c *gin.Context) {
	fieldID := c.Param("fieldId")

	if err := h.fieldService.DeleteField(c.Request.Context(), fieldID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除字段成功")
}

// ListFields 列出表格的所有字段
func (h *FieldHandler) ListFields(c *gin.Context) {
	tableID := c.Param("tableId")

	resp, err := h.fieldService.ListFields(c.Request.Context(), tableID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取字段列表成功")
}
