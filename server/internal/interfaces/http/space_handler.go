package http

import (
	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// SpaceHandler 空间HTTP处理器
type SpaceHandler struct {
	spaceService *application.SpaceService
}

// NewSpaceHandler 创建空间处理器
func NewSpaceHandler(spaceService *application.SpaceService) *SpaceHandler {
	return &SpaceHandler{
		spaceService: spaceService,
	}
}

// CreateSpace 创建空间
func (h *SpaceHandler) CreateSpace(c *gin.Context) {
	var req dto.CreateSpaceRequest
	if err := ValidateBindJSON(c, &req); err != nil {
		response.Error(c, err) // ✅ ValidateBindJSON 已返回详细的 AppError
		return
	}

	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	resp, err := h.spaceService.CreateSpace(c.Request.Context(), req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "创建空间成功")
}

// GetSpace 获取空间详情
func (h *SpaceHandler) GetSpace(c *gin.Context) {
	spaceID := c.Param("spaceId")

	resp, err := h.spaceService.GetSpace(c.Request.Context(), spaceID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取空间成功")
}

// UpdateSpace 更新空间
func (h *SpaceHandler) UpdateSpace(c *gin.Context) {
	spaceID := c.Param("spaceId")

	var req dto.UpdateSpaceRequest
	if err := ValidateBindJSON(c, &req); err != nil {
		response.Error(c, err) // ✅ ValidateBindJSON 已返回详细的 AppError
		return
	}

	resp, err := h.spaceService.UpdateSpace(c.Request.Context(), spaceID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "更新空间成功")
}

// DeleteSpace 删除空间
func (h *SpaceHandler) DeleteSpace(c *gin.Context) {
	spaceID := c.Param("spaceId")

	if err := h.spaceService.DeleteSpace(c.Request.Context(), spaceID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除空间成功")
}

// ListSpaces 列出用户的所有空间
func (h *SpaceHandler) ListSpaces(c *gin.Context) {
	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	resp, err := h.spaceService.ListSpaces(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取空间列表成功")
}
