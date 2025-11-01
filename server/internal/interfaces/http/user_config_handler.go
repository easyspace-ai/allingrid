package http

import (
	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// UserConfigHandler 用户配置处理器
type UserConfigHandler struct {
	service *application.UserConfigService
}

// NewUserConfigHandler 创建用户配置处理器
func NewUserConfigHandler(service *application.UserConfigService) *UserConfigHandler {
	return &UserConfigHandler{
		service: service,
	}
}

// GetUserConfig 获取用户配置
// @Summary 获取用户配置
// @Description 获取当前用户的个人配置（时区、语言等）
// @Tags 用户配置
// @Accept json
// @Produce json
// @Success 200 {object} response.APIResponse{data=dto.UserConfigResponse}
// @Failure 401 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/user/config [get]
// @Security BearerAuth
func (h *UserConfigHandler) GetUserConfig(c *gin.Context) {
	userID := c.GetString("user_id")

	config, err := h.service.GetUserConfig(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, config, "获取用户配置成功")
}

// UpdateUserConfig 更新用户配置
// @Summary 更新用户配置
// @Description 更新当前用户的个人配置
// @Tags 用户配置
// @Accept json
// @Produce json
// @Param request body dto.UpdateUserConfigRequest true "更新请求"
// @Success 200 {object} response.APIResponse{data=dto.UserConfigResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 401 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/user/config [put]
// @Security BearerAuth
func (h *UserConfigHandler) UpdateUserConfig(c *gin.Context) {
	userID := c.GetString("user_id")

	var req dto.UpdateUserConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	config, err := h.service.UpdateUserConfig(c.Request.Context(), userID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, config, "更新用户配置成功")
}
