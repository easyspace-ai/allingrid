package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/valueobject"
	"github.com/google/uuid"
)

// Collaborator 协作者实体
type Collaborator struct {
	id        string
	spaceID   valueobject.SpaceID
	userID    string
	role      valueobject.CollaboratorRole
	joinedAt  time.Time
	updatedAt time.Time
}

// NewCollaborator 创建协作者
func NewCollaborator(
	spaceID valueobject.SpaceID,
	userID string,
	role valueobject.CollaboratorRole,
) (*Collaborator, error) {
	// 验证
	if userID == "" {
		return nil, space.NewDomainError(
			"INVALID_USER_ID",
			"user ID cannot be empty",
			nil,
		)
	}

	now := time.Now()

	return &Collaborator{
		id:        uuid.New().String(),
		spaceID:   spaceID,
		userID:    userID,
		role:      role,
		joinedAt:  now,
		updatedAt: now,
	}, nil
}

// ==================== 访问器方法 ====================

func (c *Collaborator) ID() string                         { return c.id }
func (c *Collaborator) SpaceID() valueobject.SpaceID       { return c.spaceID }
func (c *Collaborator) UserID() string                     { return c.userID }
func (c *Collaborator) Role() valueobject.CollaboratorRole { return c.role }
func (c *Collaborator) JoinedAt() time.Time                { return c.joinedAt }
func (c *Collaborator) UpdatedAt() time.Time               { return c.updatedAt }

// IsOwner 是否为拥有者
func (c *Collaborator) IsOwner() bool {
	return c.role.IsOwner()
}

// IsAdmin 是否为管理员
func (c *Collaborator) IsAdmin() bool {
	return c.role.IsAdmin()
}

// CanEdit 是否可以编辑
func (c *Collaborator) CanEdit() bool {
	return c.role.CanEdit()
}

// CanManageMembers 是否可以管理成员
func (c *Collaborator) CanManageMembers() bool {
	return c.role.CanManageMembers()
}

// ==================== 业务方法 ====================

// ChangeRole 变更角色
func (c *Collaborator) ChangeRole(newRole valueobject.CollaboratorRole) error {
	// 拥有者角色不能变更（需要通过转让所有权）
	if c.IsOwner() {
		return space.NewDomainError(
			"CANNOT_CHANGE_OWNER_ROLE",
			"owner role cannot be changed directly, use transfer ownership instead",
			nil,
		)
	}

	c.role = newRole
	c.updatedAt = time.Now()

	return nil
}
