package entity

import (
	"time"

	"github.com/google/uuid"
)

// ResourceType 资源类型
type ResourceType string

const (
	ResourceTypeSpace ResourceType = "space"
	ResourceTypeBase  ResourceType = "base"
)

// PrincipalType 主体类型
type PrincipalType string

const (
	PrincipalTypeUser       PrincipalType = "user"
	PrincipalTypeDepartment PrincipalType = "department"
)

// RoleName 角色名称（参考原 Teable 项目）
type RoleName string

const (
	RoleOwner     RoleName = "owner"     // 所有者：完全控制
	RoleCreator   RoleName = "creator"   // 创建者：可创建内容
	RoleEditor    RoleName = "editor"    // 编辑者：可编辑内容
	RoleViewer    RoleName = "viewer"    // 查看者：只读
	RoleCommenter RoleName = "commenter" // 评论者：可查看和评论
)

// Collaborator 协作者实体
// 表示用户或部门对某个资源（Space或Base）的访问权限
type Collaborator struct {
	id            string
	resourceID    string
	resourceType  ResourceType
	principalID   string
	principalType PrincipalType
	role          RoleName
	createdBy     string
	createdAt     time.Time
	updatedAt     time.Time
}

// NewCollaborator 创建新的协作者
func NewCollaborator(
	resourceID string,
	resourceType ResourceType,
	principalID string,
	principalType PrincipalType,
	role RoleName,
	createdBy string,
) (*Collaborator, error) {
	// 验证
	if resourceID == "" || principalID == "" || createdBy == "" {
		return nil, &ValidationError{Message: "resourceID, principalID, and createdBy cannot be empty"}
	}

	if !isValidResourceType(resourceType) {
		return nil, &ValidationError{Message: "invalid resource type"}
	}

	if !isValidPrincipalType(principalType) {
		return nil, &ValidationError{Message: "invalid principal type"}
	}

	if !isValidRole(role) {
		return nil, &ValidationError{Message: "invalid role"}
	}

	now := time.Now()
	return &Collaborator{
		id:            uuid.New().String(),
		resourceID:    resourceID,
		resourceType:  resourceType,
		principalID:   principalID,
		principalType: principalType,
		role:          role,
		createdBy:     createdBy,
		createdAt:     now,
		updatedAt:     now,
	}, nil
}

// Getters
func (c *Collaborator) ID() string                   { return c.id }
func (c *Collaborator) ResourceID() string           { return c.resourceID }
func (c *Collaborator) ResourceType() ResourceType   { return c.resourceType }
func (c *Collaborator) PrincipalID() string          { return c.principalID }
func (c *Collaborator) PrincipalType() PrincipalType { return c.principalType }
func (c *Collaborator) Role() RoleName               { return c.role }
func (c *Collaborator) CreatedBy() string            { return c.createdBy }
func (c *Collaborator) CreatedAt() time.Time         { return c.createdAt }
func (c *Collaborator) UpdatedAt() time.Time         { return c.updatedAt }

// UpdateRole 更新角色
func (c *Collaborator) UpdateRole(newRole RoleName) error {
	if !isValidRole(newRole) {
		return &ValidationError{Message: "invalid role"}
	}
	c.role = newRole
	c.updatedAt = time.Now()
	return nil
}

// 验证函数
func isValidResourceType(rt ResourceType) bool {
	return rt == ResourceTypeSpace || rt == ResourceTypeBase
}

func isValidPrincipalType(pt PrincipalType) bool {
	return pt == PrincipalTypeUser || pt == PrincipalTypeDepartment
}

func isValidRole(role RoleName) bool {
	validRoles := []RoleName{RoleOwner, RoleCreator, RoleEditor, RoleViewer, RoleCommenter}
	for _, r := range validRoles {
		if role == r {
			return true
		}
	}
	return false
}

// ValidationError 验证错误
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}
