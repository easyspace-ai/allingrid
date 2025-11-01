package permission

import (
	"errors"
	"time"
)

// Role 角色类型
type Role string

const (
	// 空间角色
	RoleOwner   Role = "owner"   // 空间所有者
	RoleCreator Role = "creator" // 创建者
	RoleEditor  Role = "editor"  // 编辑者
	RoleViewer  Role = "viewer"  // 查看者

	// 基础表角色
	RoleBaseOwner   Role = "base_owner"   // 基础表所有者
	RoleBaseCreator Role = "base_creator" // 基础表创建者
	RoleBaseEditor  Role = "base_editor"  // 基础表编辑者
	RoleBaseViewer  Role = "base_viewer"  // 基础表查看者

	// 系统角色
	RoleSystem Role = "system" // 系统管理员
	RoleAdmin  Role = "admin"  // 管理员
)

// Action 操作类型
type Action string

const (
	// 空间操作
	ActionSpaceCreate      Action = "space|create"
	ActionSpaceDelete      Action = "space|delete"
	ActionSpaceRead        Action = "space|read"
	ActionSpaceUpdate      Action = "space|update"
	ActionSpaceInviteEmail Action = "space|invite_email"
	ActionSpaceInviteLink  Action = "space|invite_link"
	ActionSpaceGrantRole   Action = "space|grant_role"

	// 基础表操作
	ActionBaseCreate                Action = "base|create"
	ActionBaseDelete                Action = "base|delete"
	ActionBaseRead                  Action = "base|read"
	ActionBaseUpdate                Action = "base|update"
	ActionBaseInviteEmail           Action = "base|invite_email"
	ActionBaseInviteLink            Action = "base|invite_link"
	ActionBaseTableImport           Action = "base|table_import"
	ActionBaseTableExport           Action = "base|table_export"
	ActionBaseAuthorityMatrixConfig Action = "base|authority_matrix_config"
	ActionBaseDBConnection          Action = "base|db_connection"
	ActionBaseQueryData             Action = "base|query_data"
	ActionBaseReadAll               Action = "base|read_all"

	// 表格操作
	ActionTableCreate      Action = "table|create"
	ActionTableRead        Action = "table|read"
	ActionTableDelete      Action = "table|delete"
	ActionTableUpdate      Action = "table|update"
	ActionTableImport      Action = "table|import"
	ActionTableExport      Action = "table|export"
	ActionTableTrashRead   Action = "table|trash_read"
	ActionTableTrashUpdate Action = "table|trash_update"
	ActionTableTrashReset  Action = "table|trash_reset"

	// 表格记录历史操作
	ActionTableRecordHistoryRead Action = "table_record_history|read"

	// 视图操作
	ActionViewCreate Action = "view|create"
	ActionViewDelete Action = "view|delete"
	ActionViewRead   Action = "view|read"
	ActionViewUpdate Action = "view|update"
	ActionViewShare  Action = "view|share"

	// 字段操作
	ActionFieldCreate Action = "field|create"
	ActionFieldDelete Action = "field|delete"
	ActionFieldRead   Action = "field|read"
	ActionFieldUpdate Action = "field|update"

	// 记录操作
	ActionRecordCreate  Action = "record|create"
	ActionRecordComment Action = "record|comment"
	ActionRecordDelete  Action = "record|delete"
	ActionRecordRead    Action = "record|read"
	ActionRecordUpdate  Action = "record|update"

	// 自动化操作
	ActionAutomationCreate Action = "automation|create"
	ActionAutomationDelete Action = "automation|delete"
	ActionAutomationRead   Action = "automation|read"
	ActionAutomationUpdate Action = "automation|update"

	// 用户操作
	ActionUserEmailRead Action = "user|email_read"

	// 实例操作
	ActionInstanceRead   Action = "instance|read"
	ActionInstanceUpdate Action = "instance|update"

	// 企业操作
	ActionEnterpriseRead   Action = "enterprise|read"
	ActionEnterpriseUpdate Action = "enterprise|update"
)

// Permission 权限实体
type Permission struct {
	ID           string
	UserID       string
	ResourceType string // space, base, table, view, field, record
	ResourceID   string
	Role         Role
	GrantedBy    string
	GrantedAt    time.Time
	ExpiresAt    *time.Time
	IsActive     bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SpaceCollaborator 空间协作者
type SpaceCollaborator struct {
	ID        string
	SpaceID   string
	UserID    string
	Role      Role
	Email     *string
	InvitedBy string
	InvitedAt time.Time
	JoinedAt  *time.Time
	IsActive  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// BaseCollaborator 基础表协作者
type BaseCollaborator struct {
	ID        string
	BaseID    string
	UserID    string
	Role      Role
	Email     *string
	InvitedBy string
	InvitedAt time.Time
	JoinedAt  *time.Time
	IsActive  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// 业务规则错误
var (
	ErrPermissionNotFound     = errors.New("permission not found")
	ErrPermissionDenied       = errors.New("permission denied")
	ErrInvalidRole            = errors.New("invalid role")
	ErrInvalidResourceType    = errors.New("invalid resource type")
	ErrCollaboratorExists     = errors.New("collaborator already exists")
	ErrCollaboratorNotFound   = errors.New("collaborator not found")
	ErrCannotRemoveOwner      = errors.New("cannot remove owner")
	ErrCannotChangeOwnerRole  = errors.New("cannot change owner role")
	ErrInsufficientPermission = errors.New("insufficient permission")
)

// NewPermission 创建新权限
func NewPermission(userID, resourceType, resourceID string, role Role, grantedBy string) *Permission {
	return &Permission{
		ID:           generatePermissionID(),
		UserID:       userID,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Role:         role,
		GrantedBy:    grantedBy,
		GrantedAt:    time.Now(),
		IsActive:     true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

// NewSpaceCollaborator 创建空间协作者
func NewSpaceCollaborator(spaceID, userID string, role Role, invitedBy string, email *string) *SpaceCollaborator {
	return &SpaceCollaborator{
		ID:        generateCollaboratorID(),
		SpaceID:   spaceID,
		UserID:    userID,
		Role:      role,
		Email:     email,
		InvitedBy: invitedBy,
		InvitedAt: time.Now(),
		IsActive:  true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// NewBaseCollaborator 创建基础表协作者
func NewBaseCollaborator(baseID, userID string, role Role, invitedBy string, email *string) *BaseCollaborator {
	return &BaseCollaborator{
		ID:        generateCollaboratorID(),
		BaseID:    baseID,
		UserID:    userID,
		Role:      role,
		Email:     email,
		InvitedBy: invitedBy,
		InvitedAt: time.Now(),
		IsActive:  true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// HasPermission 检查权限
func (p *Permission) HasPermission(action Action) bool {
	return HasRolePermission(p.Role, action)
}

// IsExpired 检查权限是否过期
func (p *Permission) IsExpired() bool {
	if p.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*p.ExpiresAt)
}

// IsValid 检查权限是否有效
func (p *Permission) IsValid() bool {
	return p.IsActive && !p.IsExpired()
}

// Deactivate 停用权限
func (p *Permission) Deactivate() {
	p.IsActive = false
	p.UpdatedAt = time.Now()
}

// Join 协作者加入
func (sc *SpaceCollaborator) Join() {
	now := time.Now()
	sc.JoinedAt = &now
	sc.UpdatedAt = now
}

// Leave 协作者离开
func (sc *SpaceCollaborator) Leave() {
	sc.IsActive = false
	sc.UpdatedAt = time.Now()
}

// UpdateRole 更新角色
func (sc *SpaceCollaborator) UpdateRole(role Role) {
	sc.Role = role
	sc.UpdatedAt = time.Now()
}

// Join 协作者加入
func (bc *BaseCollaborator) Join() {
	now := time.Now()
	bc.JoinedAt = &now
	bc.UpdatedAt = now
}

// Leave 协作者离开
func (bc *BaseCollaborator) Leave() {
	bc.IsActive = false
	bc.UpdatedAt = time.Now()
}

// UpdateRole 更新角色
func (bc *BaseCollaborator) UpdateRole(role Role) {
	bc.Role = role
	bc.UpdatedAt = time.Now()
}

// 辅助函数

// generatePermissionID 生成权限ID
func generatePermissionID() string {
	// 使用NanoID生成
	return "perm_" + generateNanoID()
}

// generateCollaboratorID 生成协作者ID
func generateCollaboratorID() string {
	// 使用NanoID生成
	return "collab_" + generateNanoID()
}

// generateNanoID 生成NanoID
func generateNanoID() string {
	// 这里应该调用实际的NanoID生成函数
	// 暂时返回一个简单的实现
	return "temp_id"
}
