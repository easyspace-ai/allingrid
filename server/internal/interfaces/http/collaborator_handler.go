package http

import (
	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// CollaboratorHandler 协作者处理器
type CollaboratorHandler struct {
	service *application.CollaboratorService
}

// NewCollaboratorHandler 创建协作者处理器
func NewCollaboratorHandler(service *application.CollaboratorService) *CollaboratorHandler {
	return &CollaboratorHandler{
		service: service,
	}
}

// ListSpaceCollaborators 列出Space协作者
// @Summary 列出Space协作者
// @Description 获取指定Space的所有协作者列表
// @Tags 协作者
// @Accept json
// @Produce json
// @Param spaceId path string true "Space ID"
// @Success 200 {object} response.APIResponse{data=dto.ListCollaboratorsResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/spaces/{spaceId}/collaborators [get]
// @Security BearerAuth
func (h *CollaboratorHandler) ListSpaceCollaborators(c *gin.Context) {
	spaceID := c.Param("spaceId")

	result, err := h.service.ListCollaborators(
		c.Request.Context(),
		spaceID,
		entity.ResourceTypeSpace,
	)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, result, "获取Space协作者列表成功")
}

// AddSpaceCollaborator 添加Space协作者
// @Summary 添加Space协作者
// @Description 为Space添加新的协作者
// @Tags 协作者
// @Accept json
// @Produce json
// @Param spaceId path string true "Space ID"
// @Param request body dto.AddCollaboratorRequest true "添加协作者请求"
// @Success 200 {object} response.APIResponse{data=dto.CollaboratorResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 409 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/spaces/{spaceId}/collaborators [post]
// @Security BearerAuth
func (h *CollaboratorHandler) AddSpaceCollaborator(c *gin.Context) {
	spaceID := c.Param("spaceId")
	userID := c.GetString("user_id")

	var req dto.AddCollaboratorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	result, err := h.service.AddCollaborator(
		c.Request.Context(),
		spaceID,
		entity.ResourceTypeSpace,
		req,
		userID,
	)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, result, "添加Space协作者成功")
}

// UpdateSpaceCollaborator 更新Space协作者
// @Summary 更新Space协作者角色
// @Description 更新Space协作者的角色
// @Tags 协作者
// @Accept json
// @Produce json
// @Param spaceId path string true "Space ID"
// @Param collaboratorId path string true "Collaborator ID"
// @Param request body dto.UpdateCollaboratorRequest true "更新协作者请求"
// @Success 200 {object} response.APIResponse{data=dto.CollaboratorResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 404 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/spaces/{spaceId}/collaborators/{collaboratorId} [patch]
// @Security BearerAuth
func (h *CollaboratorHandler) UpdateSpaceCollaborator(c *gin.Context) {
	collaboratorID := c.Param("collaboratorId")

	var req dto.UpdateCollaboratorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	result, err := h.service.UpdateCollaborator(c.Request.Context(), collaboratorID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, result, "更新Space协作者成功")
}

// RemoveSpaceCollaborator 移除Space协作者
// @Summary 移除Space协作者
// @Description 从Space移除协作者
// @Tags 协作者
// @Accept json
// @Produce json
// @Param spaceId path string true "Space ID"
// @Param collaboratorId path string true "Collaborator ID"
// @Success 200 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/spaces/{spaceId}/collaborators/{collaboratorId} [delete]
// @Security BearerAuth
func (h *CollaboratorHandler) RemoveSpaceCollaborator(c *gin.Context) {
	collaboratorID := c.Param("collaboratorId")

	if err := h.service.RemoveCollaborator(c.Request.Context(), collaboratorID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "移除Space协作者成功")
}

// ListBaseCollaborators 列出Base协作者
// @Summary 列出Base协作者
// @Description 获取指定Base的所有协作者列表
// @Tags 协作者
// @Accept json
// @Produce json
// @Param baseId path string true "Base ID"
// @Success 200 {object} response.APIResponse{data=dto.ListCollaboratorsResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/bases/{baseId}/collaborators [get]
// @Security BearerAuth
func (h *CollaboratorHandler) ListBaseCollaborators(c *gin.Context) {
	baseID := c.Param("baseId")

	result, err := h.service.ListCollaborators(
		c.Request.Context(),
		baseID,
		entity.ResourceTypeBase,
	)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, result, "获取Base协作者列表成功")
}

// AddBaseCollaborator 添加Base协作者
// @Summary 添加Base协作者
// @Description 为Base添加新的协作者
// @Tags 协作者
// @Accept json
// @Produce json
// @Param baseId path string true "Base ID"
// @Param request body dto.AddCollaboratorRequest true "添加协作者请求"
// @Success 200 {object} response.APIResponse{data=dto.CollaboratorResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 409 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/bases/{baseId}/collaborators [post]
// @Security BearerAuth
func (h *CollaboratorHandler) AddBaseCollaborator(c *gin.Context) {
	baseID := c.Param("baseId")
	userID := c.GetString("user_id")

	var req dto.AddCollaboratorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	result, err := h.service.AddCollaborator(
		c.Request.Context(),
		baseID,
		entity.ResourceTypeBase,
		req,
		userID,
	)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, result, "添加Base协作者成功")
}

// UpdateBaseCollaborator 更新Base协作者
// @Summary 更新Base协作者角色
// @Description 更新Base协作者的角色
// @Tags 协作者
// @Accept json
// @Produce json
// @Param baseId path string true "Base ID"
// @Param collaboratorId path string true "Collaborator ID"
// @Param request body dto.UpdateCollaboratorRequest true "更新协作者请求"
// @Success 200 {object} response.APIResponse{data=dto.CollaboratorResponse}
// @Failure 400 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 404 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/bases/{baseId}/collaborators/{collaboratorId} [patch]
// @Security BearerAuth
func (h *CollaboratorHandler) UpdateBaseCollaborator(c *gin.Context) {
	collaboratorID := c.Param("collaboratorId")

	var req dto.UpdateCollaboratorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	result, err := h.service.UpdateCollaborator(c.Request.Context(), collaboratorID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, result, "更新Base协作者成功")
}

// RemoveBaseCollaborator 移除Base协作者
// @Summary 移除Base协作者
// @Description 从Base移除协作者
// @Tags 协作者
// @Accept json
// @Produce json
// @Param baseId path string true "Base ID"
// @Param collaboratorId path string true "Collaborator ID"
// @Success 200 {object} response.APIResponse
// @Failure 403 {object} response.APIResponse
// @Failure 500 {object} response.APIResponse
// @Router /api/v1/bases/{baseId}/collaborators/{collaboratorId} [delete]
// @Security BearerAuth
func (h *CollaboratorHandler) RemoveBaseCollaborator(c *gin.Context) {
	collaboratorID := c.Param("collaboratorId")

	if err := h.service.RemoveCollaborator(c.Request.Context(), collaboratorID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "移除Base协作者成功")
}
