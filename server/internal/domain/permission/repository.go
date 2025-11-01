package permission

import (
	"context"
	"time"
)

// Repository 权限仓储接口
type Repository interface {
	// 权限管理
	CreatePermission(ctx context.Context, permission *Permission) error
	GetPermission(ctx context.Context, id string) (*Permission, error)
	GetPermissionsByUser(ctx context.Context, userID string) ([]*Permission, error)
	GetPermissionsByResource(ctx context.Context, resourceType, resourceID string) ([]*Permission, error)
	GetUserPermission(ctx context.Context, userID, resourceType, resourceID string) (*Permission, error)
	UpdatePermission(ctx context.Context, permission *Permission) error
	DeletePermission(ctx context.Context, id string) error
	DeleteUserPermissions(ctx context.Context, userID, resourceType, resourceID string) error

	// 空间协作者管理
	CreateSpaceCollaborator(ctx context.Context, collaborator *SpaceCollaborator) error
	GetSpaceCollaborator(ctx context.Context, id string) (*SpaceCollaborator, error)
	GetSpaceCollaborators(ctx context.Context, spaceID string) ([]*SpaceCollaborator, error)
	GetUserSpaceCollaborator(ctx context.Context, userID, spaceID string) (*SpaceCollaborator, error)
	UpdateSpaceCollaborator(ctx context.Context, collaborator *SpaceCollaborator) error
	DeleteSpaceCollaborator(ctx context.Context, id string) error
	DeleteUserSpaceCollaborator(ctx context.Context, userID, spaceID string) error

	// 基础表协作者管理
	CreateBaseCollaborator(ctx context.Context, collaborator *BaseCollaborator) error
	GetBaseCollaborator(ctx context.Context, id string) (*BaseCollaborator, error)
	GetBaseCollaborators(ctx context.Context, baseID string) ([]*BaseCollaborator, error)
	GetUserBaseCollaborator(ctx context.Context, userID, baseID string) (*BaseCollaborator, error)
	UpdateBaseCollaborator(ctx context.Context, collaborator *BaseCollaborator) error
	DeleteBaseCollaborator(ctx context.Context, id string) error
	DeleteUserBaseCollaborator(ctx context.Context, userID, baseID string) error

	// 批量操作
	GetUserRoles(ctx context.Context, userID string) (map[string]Role, error) // resourceType -> role
	GetResourceCollaborators(ctx context.Context, resourceType, resourceID string) ([]*CollaboratorInfo, error)
	GetUserResources(ctx context.Context, userID, resourceType string) ([]string, error) // 返回资源ID列表

	// 权限检查
	CheckUserPermission(ctx context.Context, userID, resourceType, resourceID string, action Action) (bool, error)
	GetUserEffectiveRole(ctx context.Context, userID, resourceType, resourceID string) (Role, error)

	// 统计
	GetPermissionStats(ctx context.Context) (*PermissionStats, error)
}

// CollaboratorInfo 协作者信息
type CollaboratorInfo struct {
	ID        string     `json:"id"`
	UserID    string     `json:"user_id"`
	Role      Role       `json:"role"`
	Email     *string    `json:"email,omitempty"`
	InvitedBy string     `json:"invited_by"`
	InvitedAt time.Time  `json:"invited_at"`
	JoinedAt  *time.Time `json:"joined_at,omitempty"`
	IsActive  bool       `json:"is_active"`
}

// PermissionStats 权限统计信息
type PermissionStats struct {
	TotalPermissions         int64          `json:"total_permissions"`
	ActivePermissions        int64          `json:"active_permissions"`
	ExpiredPermissions       int64          `json:"expired_permissions"`
	TotalSpaceCollaborators  int64          `json:"total_space_collaborators"`
	ActiveSpaceCollaborators int64          `json:"active_space_collaborators"`
	TotalBaseCollaborators   int64          `json:"total_base_collaborators"`
	ActiveBaseCollaborators  int64          `json:"active_base_collaborators"`
	RoleDistribution         map[Role]int64 `json:"role_distribution"`
}
