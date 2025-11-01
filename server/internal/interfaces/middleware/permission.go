package middleware

import (
	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/permission"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// PermissionMiddleware 权限中间件工厂
type PermissionMiddleware struct {
	permissionService *application.PermissionServiceV2
}

// NewPermissionMiddleware 创建权限中间件
func NewPermissionMiddleware(permissionService *application.PermissionServiceV2) *PermissionMiddleware {
	return &PermissionMiddleware{
		permissionService: permissionService,
	}
}

// RequireSpaceAccess 要求Space访问权限
func (m *PermissionMiddleware) RequireSpaceAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		spaceID := c.Param("spaceId")

		if spaceID == "" {
			response.Error(c, errors.ErrBadRequest.WithDetails("spaceId is required"))
			c.Abort()
			return
		}

		if !m.permissionService.CanAccessSpace(c.Request.Context(), userID, spaceID) {
			response.Error(c, errors.ErrForbidden.WithDetails("no permission to access this space"))
			c.Abort()
			return
		}

		c.Next()
	}
}

// RequireBaseAccess 要求Base访问权限
func (m *PermissionMiddleware) RequireBaseAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		baseID := c.Param("baseId")

		if baseID == "" {
			response.Error(c, errors.ErrBadRequest.WithDetails("baseId is required"))
			c.Abort()
			return
		}

		if !m.permissionService.CanAccessBase(c.Request.Context(), userID, baseID) {
			response.Error(c, errors.ErrForbidden.WithDetails("no permission to access this base"))
			c.Abort()
			return
		}

		c.Next()
	}
}

// RequireTableAccess 要求Table访问权限
func (m *PermissionMiddleware) RequireTableAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		tableID := c.Param("tableId")

		if tableID == "" {
			response.Error(c, errors.ErrBadRequest.WithDetails("tableId is required"))
			c.Abort()
			return
		}

		if !m.permissionService.CanAccessTable(c.Request.Context(), userID, tableID) {
			response.Error(c, errors.ErrForbidden.WithDetails("no permission to access this table"))
			c.Abort()
			return
		}

		c.Next()
	}
}

// RequireAction 要求特定动作权限（通用方法）
// 使用示例：
//
//	router.GET("/bases/:baseId", permMw.RequireAction(entity.ResourceTypeBase, permission.ActionBaseRead))
func (m *PermissionMiddleware) RequireAction(resourceType entity.ResourceType, action permission.Action) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")

		// 根据资源类型从参数中获取资源ID
		var resourceID string
		switch resourceType {
		case entity.ResourceTypeSpace:
			resourceID = c.Param("spaceId")
		case entity.ResourceTypeBase:
			resourceID = c.Param("baseId")
		default:
			response.Error(c, errors.ErrInternalServer.WithDetails("unsupported resource type"))
			c.Abort()
			return
		}

		if resourceID == "" {
			response.Error(c, errors.ErrBadRequest.WithDetails("resource id is required"))
			c.Abort()
			return
		}

		// 检查权限
		if !m.permissionService.Can(c.Request.Context(), userID, resourceID, resourceType, action) {
			response.Error(c, errors.ErrForbidden.WithDetails("no permission to perform this action"))
			c.Abort()
			return
		}

		c.Next()
	}
}

// RequireSpaceRole 要求特定Space角色
func (m *PermissionMiddleware) RequireSpaceRole(minRole entity.RoleName) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		spaceID := c.Param("spaceId")

		if spaceID == "" {
			response.Error(c, errors.ErrBadRequest.WithDetails("spaceId is required"))
			c.Abort()
			return
		}

		// 获取用户角色
		role, err := m.permissionService.GetUserRole(c.Request.Context(), userID, spaceID)
		if err != nil {
			response.Error(c, errors.ErrForbidden.WithDetails("no access to this space"))
			c.Abort()
			return
		}

		// 检查角色等级（简化实现，实际应该有角色等级对比逻辑）
		if !hasRoleLevel(role, minRole) {
			response.Error(c, errors.ErrForbidden.WithDetails("insufficient role"))
			c.Abort()
			return
		}

		// 将角色存入context，供后续Handler使用
		c.Set("user_role", string(role))
		c.Next()
	}
}

// hasRoleLevel 检查角色等级（简化版）
// 角色等级: Owner > Creator > Editor > Viewer > Commenter
func hasRoleLevel(userRole, requiredRole entity.RoleName) bool {
	roleLevel := map[entity.RoleName]int{
		entity.RoleOwner:     5,
		entity.RoleCreator:   4,
		entity.RoleEditor:    3,
		entity.RoleViewer:    2,
		entity.RoleCommenter: 1,
	}

	return roleLevel[userRole] >= roleLevel[requiredRole]
}

// RequireOwner 要求Owner角色的便捷方法
func (m *PermissionMiddleware) RequireOwner() gin.HandlerFunc {
	return m.RequireSpaceRole(entity.RoleOwner)
}

// RequireEditor 要求Editor及以上角色的便捷方法
func (m *PermissionMiddleware) RequireEditor() gin.HandlerFunc {
	return m.RequireSpaceRole(entity.RoleEditor)
}
