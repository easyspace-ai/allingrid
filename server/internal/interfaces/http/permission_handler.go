package http

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/permission"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// PermissionHandler 权限HTTP处理器
type PermissionHandler struct {
	permissionService permission.Service
	logger            *zap.Logger
}

// NewPermissionHandler 创建权限HTTP处理器
func NewPermissionHandler(permissionService permission.Service, logger *zap.Logger) *PermissionHandler {
	return &PermissionHandler{
		permissionService: permissionService,
		logger:            logger,
	}
}

// GrantPermissionRequest 授予权限请求
type GrantPermissionRequest struct {
	UserID       string `json:"user_id" binding:"required"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceID   string `json:"resource_id" binding:"required"`
	Role         string `json:"role" binding:"required"`
}

// GrantPermission 授予权限
// @Summary 授予权限
// @Description 为用户授予指定资源的权限
// @Tags 权限管理
// @Accept json
// @Produce json
// @Param request body GrantPermissionRequest true "授予权限请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions [post]
func (h *PermissionHandler) GrantPermission(c *gin.Context) {
	var req GrantPermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 获取当前用户ID
	grantedBy, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	// 授予权限
	if err := h.permissionService.GrantPermission(
		c.Request.Context(),
		req.UserID,
		req.ResourceType,
		req.ResourceID,
		permission.Role(req.Role),
		grantedBy.(string),
	); err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// RevokePermissionRequest 撤销权限请求
type RevokePermissionRequest struct {
	UserID       string `json:"user_id" binding:"required"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceID   string `json:"resource_id" binding:"required"`
}

// RevokePermission 撤销权限
// @Summary 撤销权限
// @Description 撤销用户对指定资源的权限
// @Tags 权限管理
// @Accept json
// @Produce json
// @Param request body RevokePermissionRequest true "撤销权限请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions [delete]
func (h *PermissionHandler) RevokePermission(c *gin.Context) {
	var req RevokePermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 撤销权限
	if err := h.permissionService.RevokePermission(
		c.Request.Context(),
		req.UserID,
		req.ResourceType,
		req.ResourceID,
	); err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// UpdatePermissionRequest 更新权限请求
type UpdatePermissionRequest struct {
	UserID       string `json:"user_id" binding:"required"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceID   string `json:"resource_id" binding:"required"`
	Role         string `json:"role" binding:"required"`
}

// UpdatePermission 更新权限
// @Summary 更新权限
// @Description 更新用户对指定资源的权限
// @Tags 权限管理
// @Accept json
// @Produce json
// @Param request body UpdatePermissionRequest true "更新权限请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions [put]
func (h *PermissionHandler) UpdatePermission(c *gin.Context) {
	var req UpdatePermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 获取当前用户ID
	updatedBy, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	// 更新权限
	if err := h.permissionService.UpdatePermission(
		c.Request.Context(),
		req.UserID,
		req.ResourceType,
		req.ResourceID,
		permission.Role(req.Role),
		updatedBy.(string),
	); err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// GetUserPermissions 获取用户权限
// @Summary 获取用户权限
// @Description 获取指定用户的所有权限
// @Tags 权限管理
// @Produce json
// @Param user_id query string true "用户ID"
// @Success 200 {object} Response{data=[]permission.Permission}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/user [get]
func (h *PermissionHandler) GetUserPermissions(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("User ID is required"))
		return
	}

	// 获取用户权限
	permissions, err := h.permissionService.GetUserPermissions(c.Request.Context(), userID)
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, permissions, "")
}

// GetResourcePermissions 获取资源权限
// @Summary 获取资源权限
// @Description 获取指定资源的所有权限
// @Tags 权限管理
// @Produce json
// @Param resource_type query string true "资源类型"
// @Param resource_id query string true "资源ID"
// @Success 200 {object} Response{data=[]permission.Permission}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/resource [get]
func (h *PermissionHandler) GetResourcePermissions(c *gin.Context) {
	resourceType := c.Query("resource_type")
	resourceID := c.Query("resource_id")
	if resourceType == "" || resourceID == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Resource type and ID are required"))
		return
	}

	// 获取资源权限
	permissions, err := h.permissionService.GetResourcePermissions(c.Request.Context(), resourceType, resourceID)
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, permissions, "")
}

// CheckPermissionRequest 检查权限请求
type CheckPermissionRequest struct {
	UserID       string `json:"user_id" binding:"required"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceID   string `json:"resource_id" binding:"required"`
	Action       string `json:"action" binding:"required"`
}

// CheckPermission 检查权限
// @Summary 检查权限
// @Description 检查用户是否有指定资源的权限
// @Tags 权限管理
// @Accept json
// @Produce json
// @Param request body CheckPermissionRequest true "检查权限请求"
// @Success 200 {object} Response{data=object}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/check [post]
func (h *PermissionHandler) CheckPermission(c *gin.Context) {
	var req CheckPermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 检查权限
	hasPermission, err := h.permissionService.CheckPermission(
		c.Request.Context(),
		req.UserID,
		req.ResourceType,
		req.ResourceID,
		permission.Action(req.Action),
	)
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, map[string]interface{}{"has_permission": hasPermission}, "")
}

// GetUserRole 获取用户角色
// @Summary 获取用户角色
// @Description 获取用户在指定资源中的角色
// @Tags 权限管理
// @Produce json
// @Param user_id query string true "用户ID"
// @Param resource_type query string true "资源类型"
// @Param resource_id query string true "资源ID"
// @Success 200 {object} Response{data=object}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/role [get]
func (h *PermissionHandler) GetUserRole(c *gin.Context) {
	userID := c.Query("user_id")
	resourceType := c.Query("resource_type")
	resourceID := c.Query("resource_id")
	if userID == "" || resourceType == "" || resourceID == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("User ID, resource type and ID are required"))
		return
	}

	// 获取用户角色
	role, err := h.permissionService.GetUserEffectiveRole(c.Request.Context(), userID, resourceType, resourceID)
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, map[string]interface{}{"role": string(role)}, "")
}

// GetUserResources 获取用户资源
// @Summary 获取用户资源
// @Description 获取用户有权限访问的资源列表
// @Tags 权限管理
// @Produce json
// @Param user_id query string true "用户ID"
// @Param resource_type query string false "资源类型"
// @Success 200 {object} Response{data=[]string}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/resources [get]
func (h *PermissionHandler) GetUserResources(c *gin.Context) {
	userID := c.Query("user_id")
	resourceType := c.Query("resource_type")
	if userID == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("User ID is required"))
		return
	}

	// 获取用户资源
	resources, err := h.permissionService.GetUserResources(c.Request.Context(), userID, resourceType)
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, resources, "")
}

// GetResourceCollaborators 获取资源协作者
// @Summary 获取资源协作者
// @Description 获取指定资源的所有协作者
// @Tags 权限管理
// @Produce json
// @Param resource_type query string true "资源类型"
// @Param resource_id query string true "资源ID"
// @Success 200 {object} Response{data=[]permission.CollaboratorInfo}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/collaborators [get]
func (h *PermissionHandler) GetResourceCollaborators(c *gin.Context) {
	resourceType := c.Query("resource_type")
	resourceID := c.Query("resource_id")
	if resourceType == "" || resourceID == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Resource type and ID are required"))
		return
	}

	// 获取资源协作者
	collaborators, err := h.permissionService.GetResourceCollaborators(c.Request.Context(), resourceType, resourceID)
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, collaborators, "")
}

// TransferOwnershipRequest 转移所有权请求
type TransferOwnershipRequest struct {
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceID   string `json:"resource_id" binding:"required"`
	ToUserID     string `json:"to_user_id" binding:"required"`
}

// TransferOwnership 转移所有权
// @Summary 转移所有权
// @Description 将资源的所有权转移给其他用户
// @Tags 权限管理
// @Accept json
// @Produce json
// @Param request body TransferOwnershipRequest true "转移所有权请求"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/transfer [post]
func (h *PermissionHandler) TransferOwnership(c *gin.Context) {
	var req TransferOwnershipRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails(err.Error()))
		return
	}

	// 获取当前用户ID
	fromUserID, exists := c.Get("user_id")
	if !exists {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	// 转移所有权
	if err := h.permissionService.TransferOwnership(
		c.Request.Context(),
		req.ResourceType,
		req.ResourceID,
		fromUserID.(string),
		req.ToUserID,
	); err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// GetPermissionStats 获取权限统计
// @Summary 获取权限统计
// @Description 获取权限系统的统计信息
// @Tags 权限管理
// @Produce json
// @Success 200 {object} Response{data=permission.PermissionStats}
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/stats [get]
func (h *PermissionHandler) GetPermissionStats(c *gin.Context) {
	// 获取权限统计
	stats, err := h.permissionService.GetPermissionStats(c.Request.Context())
	if err != nil {
		h.handleError(c, errors.ErrInternalServer.WithDetails(err.Error()))
		return
	}

	response.SuccessWithMessage(c, stats, "")
}

// GetRolePermissions 获取角色权限
// @Summary 获取角色权限
// @Description 获取指定角色的所有权限
// @Tags 权限管理
// @Produce json
// @Param role query string true "角色"
// @Success 200 {object} Response{data=[]permission.Action}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/role-permissions [get]
func (h *PermissionHandler) GetRolePermissions(c *gin.Context) {
	roleStr := c.Query("role")
	if roleStr == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Role is required"))
		return
	}

	role := permission.Role(roleStr)
	permissions := permission.GetRolePermissions(role)

	response.SuccessWithMessage(c, permissions, "")
}

// CheckRolePermission 检查角色权限
// @Summary 检查角色权限
// @Description 检查角色是否有指定权限
// @Tags 权限管理
// @Produce json
// @Param role query string true "角色"
// @Param action query string true "权限动作"
// @Success 200 {object} Response{data=object}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/role-check [get]
func (h *PermissionHandler) CheckRolePermission(c *gin.Context) {
	roleStr := c.Query("role")
	actionStr := c.Query("action")
	if roleStr == "" || actionStr == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Role and action are required"))
		return
	}

	role := permission.Role(roleStr)
	action := permission.Action(actionStr)
	hasPermission := permission.HasRolePermission(role, action)

	response.SuccessWithMessage(c, map[string]interface{}{"has_permission": hasPermission}, "")
}

// GetRoleLevel 获取角色级别
// @Summary 获取角色级别
// @Description 获取指定角色的权限级别
// @Tags 权限管理
// @Produce json
// @Param role query string true "角色"
// @Success 200 {object} Response{data=object}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/role-level [get]
func (h *PermissionHandler) GetRoleLevel(c *gin.Context) {
	roleStr := c.Query("role")
	if roleStr == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Role is required"))
		return
	}

	role := permission.Role(roleStr)
	level := permission.GetRoleLevel(role)

	response.SuccessWithMessage(c, map[string]interface{}{"role": string(role), "level": level}, "")
}

// CompareRoles 比较角色
// @Summary 比较角色
// @Description 比较两个角色的权限级别
// @Tags 权限管理
// @Produce json
// @Param role1 query string true "角色1"
// @Param role2 query string true "角色2"
// @Success 200 {object} Response{data=object}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/permissions/compare-roles [get]
func (h *PermissionHandler) CompareRoles(c *gin.Context) {
	role1Str := c.Query("role1")
	role2Str := c.Query("role2")
	if role1Str == "" || role2Str == "" {
		h.handleError(c, errors.ErrInvalidRequest.WithDetails("Both roles are required"))
		return
	}

	role1 := permission.Role(role1Str)
	role2 := permission.Role(role2Str)

	level1 := permission.GetRoleLevel(role1)
	level2 := permission.GetRoleLevel(role2)

	response.SuccessWithMessage(c, map[string]interface{}{
		"role1":     string(role1),
		"role2":     string(role2),
		"level1":    level1,
		"level2":    level2,
		"is_higher": permission.IsHigherRole(role1, role2),
		"is_lower":  permission.IsLowerRole(role1, role2),
		"is_equal":  permission.IsEqualRole(role1, role2),
		"can_grant": permission.CanGrantRole(role1, role2),
	}, "")
}

func (h *PermissionHandler) handleError(c *gin.Context, err error) {
	response.Error(c, err)
}
