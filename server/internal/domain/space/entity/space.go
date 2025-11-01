package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/valueobject"
)

// Space 空间实体（充血模型）
type Space struct {
	// 基础属性（私有）
	id          valueobject.SpaceID
	name        valueobject.SpaceName
	description *string
	icon        *string

	// 拥有者
	ownerID string

	// 审计字段
	createdBy string
	createdAt time.Time
	updatedAt time.Time
	deletedAt *time.Time

	// 版本控制
	version int
}

// NewSpace 创建新空间（工厂方法）
func NewSpace(
	name valueobject.SpaceName,
	ownerID string,
) (*Space, error) {
	// 验证
	if ownerID == "" {
		return nil, space.NewDomainError(
			"INVALID_OWNER_ID",
			"owner ID cannot be empty",
			nil,
		)
	}

	now := time.Now()

	return &Space{
		id:        valueobject.NewSpaceID(""),
		name:      name,
		ownerID:   ownerID,
		createdBy: ownerID,
		createdAt: now,
		updatedAt: now,
		version:   1,
	}, nil
}

// ReconstructSpace 重建空间（从数据库加载）
func ReconstructSpace(
	id valueobject.SpaceID,
	name valueobject.SpaceName,
	description *string,
	icon *string,
	ownerID string,
	createdBy string,
	createdAt, updatedAt time.Time,
	deletedAt *time.Time,
	version int,
) *Space {
	return &Space{
		id:          id,
		name:        name,
		description: description,
		icon:        icon,
		ownerID:     ownerID,
		createdBy:   createdBy,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
		deletedAt:   deletedAt,
		version:     version,
	}
}

// ==================== 访问器方法 ====================

func (s *Space) ID() valueobject.SpaceID     { return s.id }
func (s *Space) Name() valueobject.SpaceName { return s.name }
func (s *Space) Description() *string        { return s.description }
func (s *Space) Icon() *string               { return s.icon }
func (s *Space) OwnerID() string             { return s.ownerID }
func (s *Space) CreatedBy() string           { return s.createdBy }
func (s *Space) CreatedAt() time.Time        { return s.createdAt }
func (s *Space) UpdatedAt() time.Time        { return s.updatedAt }
func (s *Space) DeletedAt() *time.Time       { return s.deletedAt }
func (s *Space) Version() int                { return s.version }

// IsDeleted 是否已删除
func (s *Space) IsDeleted() bool {
	return s.deletedAt != nil
}

// IsOwner 检查是否为拥有者
func (s *Space) IsOwner(userID string) bool {
	return s.ownerID == userID
}

// ==================== 业务方法 ====================

// Rename 重命名空间
func (s *Space) Rename(newName valueobject.SpaceName) error {
	if s.IsDeleted() {
		return space.ErrCannotModifyDeletedSpace
	}

	s.name = newName
	s.updatedAt = time.Now()
	s.incrementVersion()

	return nil
}

// UpdateDescription 更新描述
func (s *Space) UpdateDescription(description string) error {
	if s.IsDeleted() {
		return space.ErrCannotModifyDeletedSpace
	}

	s.description = &description
	s.updatedAt = time.Now()

	return nil
}

// UpdateIcon 更新图标
func (s *Space) UpdateIcon(icon string) error {
	if s.IsDeleted() {
		return space.ErrCannotModifyDeletedSpace
	}

	s.icon = &icon
	s.updatedAt = time.Now()

	return nil
}

// TransferOwnership 转让所有权
func (s *Space) TransferOwnership(newOwnerID string, currentOwnerID string) error {
	if s.IsDeleted() {
		return space.ErrCannotModifyDeletedSpace
	}

	// 验证当前用户是否为拥有者
	if !s.IsOwner(currentOwnerID) {
		return space.ErrNotSpaceOwner
	}

	// 验证新拥有者ID
	if newOwnerID == "" {
		return space.NewDomainError(
			"INVALID_NEW_OWNER",
			"new owner ID cannot be empty",
			nil,
		)
	}

	s.ownerID = newOwnerID
	s.updatedAt = time.Now()
	s.incrementVersion()

	return nil
}

// SoftDelete 软删除空间
func (s *Space) SoftDelete() error {
	if s.IsDeleted() {
		return space.ErrSpaceAlreadyDeleted
	}

	now := time.Now()
	s.deletedAt = &now
	s.updatedAt = now

	return nil
}

// Restore 恢复已删除的空间
func (s *Space) Restore() error {
	if !s.IsDeleted() {
		return space.NewDomainError(
			"SPACE_NOT_DELETED",
			"space is not deleted",
			nil,
		)
	}

	s.deletedAt = nil
	s.updatedAt = time.Now()

	return nil
}

// ==================== 私有辅助方法 ====================

// incrementVersion 递增版本号
func (s *Space) incrementVersion() {
	s.version++
}
