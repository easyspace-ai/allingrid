package http

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// OrganizationHandler 组织处理器
type OrganizationHandler struct {
	service *application.OrganizationService
}

// NewOrganizationHandler 创建组织处理器
func NewOrganizationHandler(service *application.OrganizationService) *OrganizationHandler {
	return &OrganizationHandler{
		service: service,
	}
}

// CreateOrganization 创建组织
func (h *OrganizationHandler) CreateOrganization(c *gin.Context) {
	var org models.Organization
	if err := c.ShouldBindJSON(&org); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 从上下文中获取用户ID
	userID, exists := c.Get("user_id")
	if !exists {
		response.Error(c, errors.ErrUnauthorized.WithDetails("User not authenticated"))
		return
	}

	org.CreatedBy = userID.(string)

	// 调用服务创建组织
	if err := h.service.Create(c.Request.Context(), &org); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, org, "创建组织成功")
}

// GetOrganization 获取组织
func (h *OrganizationHandler) GetOrganization(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Organization ID is required"))
		return
	}

	// 调用服务获取组织
	org, err := h.service.GetByID(c.Request.Context(), id)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, org, "获取组织成功")
}

// ListOrganizations 列出组织
func (h *OrganizationHandler) ListOrganizations(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	// 调用服务获取列表
	orgs, total, err := h.service.List(c.Request.Context(), page, limit)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 使用分页响应
	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      int(total),
		TotalPages: (int(total) + limit - 1) / limit,
	}

	response.PaginatedSuccess(c, orgs, pagination, "获取组织列表成功")
}

// UpdateOrganization 更新组织
func (h *OrganizationHandler) UpdateOrganization(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Organization ID is required"))
		return
	}

	var org models.Organization
	if err := c.ShouldBindJSON(&org); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	org.ID = id

	// 调用服务更新组织
	if err := h.service.Update(c.Request.Context(), id, &org); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, org, "更新组织成功")
}

// DeleteOrganization 删除组织
func (h *OrganizationHandler) DeleteOrganization(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Organization ID is required"))
		return
	}

	// 调用服务删除组织
	if err := h.service.Delete(c.Request.Context(), id); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除组织成功")
}

// GetOrganizationStats 获取组织统计信息
func (h *OrganizationHandler) GetOrganizationStats(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Organization ID is required"))
		return
	}

	// 调用服务获取统计信息
	stats, err := h.service.GetStats(c.Request.Context(), id)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, stats, "获取组织统计成功")
}

// AddOrganizationUser 添加组织用户
func (h *OrganizationHandler) AddOrganizationUser(c *gin.Context) {
	orgID := c.Param("id")
	if orgID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Organization ID is required"))
		return
	}

	var user models.OrganizationUser
	if err := c.ShouldBindJSON(&user); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	user.OrganizationID = orgID

	// 调用服务添加用户
	if err := h.service.AddUser(c.Request.Context(), &user); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, user, "添加用户成功")
}

// ListOrganizationUsers 列出组织用户
func (h *OrganizationHandler) ListOrganizationUsers(c *gin.Context) {
	orgID := c.Param("id")
	if orgID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Organization ID is required"))
		return
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	// 调用服务获取用户列表
	users, total, err := h.service.ListUsers(c.Request.Context(), orgID, page, limit)
	if err != nil {
		response.Error(c, err)
		return
	}

	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      int(total),
		TotalPages: (int(total) + limit - 1) / limit,
	}

	response.PaginatedSuccess(c, users, pagination, "获取组织用户列表成功")
}
