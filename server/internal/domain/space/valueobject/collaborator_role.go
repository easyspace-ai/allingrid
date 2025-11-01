package valueobject

import "github.com/easyspace-ai/luckdb/server/internal/domain/space"

// CollaboratorRole 协作者角色值对象
type CollaboratorRole struct {
	value string
}

const (
	RoleOwner  = "owner"
	RoleAdmin  = "admin"
	RoleEditor = "editor"
	RoleViewer = "viewer"
	RoleGuest  = "guest"
)

// NewCollaboratorRole 创建协作者角色
func NewCollaboratorRole(value string) (CollaboratorRole, error) {
	if !isValidRole(value) {
		return CollaboratorRole{}, space.ErrInvalidRole
	}

	return CollaboratorRole{value: value}, nil
}

// Owner 返回拥有者角色
func Owner() CollaboratorRole {
	return CollaboratorRole{value: RoleOwner}
}

// Admin 返回管理员角色
func Admin() CollaboratorRole {
	return CollaboratorRole{value: RoleAdmin}
}

// Editor 返回编辑者角色
func Editor() CollaboratorRole {
	return CollaboratorRole{value: RoleEditor}
}

// Viewer 返回查看者角色
func Viewer() CollaboratorRole {
	return CollaboratorRole{value: RoleViewer}
}

// String 获取字符串值
func (cr CollaboratorRole) String() string {
	return cr.value
}

// Equals 比较两个角色是否相等
func (cr CollaboratorRole) Equals(other CollaboratorRole) bool {
	return cr.value == other.value
}

// IsOwner 是否为拥有者
func (cr CollaboratorRole) IsOwner() bool {
	return cr.value == RoleOwner
}

// IsAdmin 是否为管理员
func (cr CollaboratorRole) IsAdmin() bool {
	return cr.value == RoleAdmin
}

// IsEditor 是否为编辑者
func (cr CollaboratorRole) IsEditor() bool {
	return cr.value == RoleEditor
}

// IsViewer 是否为查看者
func (cr CollaboratorRole) IsViewer() bool {
	return cr.value == RoleViewer
}

// CanManageMembers 是否可以管理成员
func (cr CollaboratorRole) CanManageMembers() bool {
	return cr.value == RoleOwner || cr.value == RoleAdmin
}

// CanEdit 是否可以编辑
func (cr CollaboratorRole) CanEdit() bool {
	return cr.value == RoleOwner || cr.value == RoleAdmin || cr.value == RoleEditor
}

// CanView 是否可以查看
func (cr CollaboratorRole) CanView() bool {
	return true // 所有角色都可以查看
}

// GetPermissionLevel 获取权限级别（数字越大权限越高）
func (cr CollaboratorRole) GetPermissionLevel() int {
	switch cr.value {
	case RoleOwner:
		return 100
	case RoleAdmin:
		return 80
	case RoleEditor:
		return 60
	case RoleViewer:
		return 40
	case RoleGuest:
		return 20
	default:
		return 0
	}
}

// IsHigherThan 是否比另一个角色权限更高
func (cr CollaboratorRole) IsHigherThan(other CollaboratorRole) bool {
	return cr.GetPermissionLevel() > other.GetPermissionLevel()
}

// isValidRole 检查角色是否有效
func isValidRole(value string) bool {
	validRoles := map[string]bool{
		RoleOwner:  true,
		RoleAdmin:  true,
		RoleEditor: true,
		RoleViewer: true,
		RoleGuest:  true,
	}

	return validRoles[value]
}
