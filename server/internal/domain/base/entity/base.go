package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Base 基地聚合根（对齐原版Base实体）
// Base是数据组织的核心单元，一个Space可以包含多个Base
// 一个Base包含多个Table
type Base struct {
	ID        string    // Base唯一标识
	Name      string    // Base名称
	Icon      string    // Base图标
	SpaceID   string    // 所属Space ID
	CreatedBy string    // 创建者ID
	CreatedAt time.Time // 创建时间
	UpdatedAt time.Time // 更新时间
}

// NewBase 创建新Base（对齐原版）
func NewBase(name, icon, spaceID, createdBy string) (*Base, error) {
	if name == "" {
		return nil, errors.New("base name cannot be empty")
	}

	if len(name) > 100 {
		return nil, errors.New("base name too long (max 100 chars)")
	}

	if spaceID == "" {
		return nil, errors.New("space ID cannot be empty")
	}

	if createdBy == "" {
		return nil, errors.New("creator ID cannot be empty")
	}

	now := time.Now()

	return &Base{
		ID:        uuid.New().String(),
		Name:      name,
		Icon:      icon,
		SpaceID:   spaceID,
		CreatedBy: createdBy,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// UpdateName 更新Base名称（业务方法）
func (b *Base) UpdateName(name string) error {
	if name == "" {
		return errors.New("base name cannot be empty")
	}

	if len(name) > 100 {
		return errors.New("base name too long (max 100 chars)")
	}

	b.Name = name
	b.UpdatedAt = time.Now()
	return nil
}

// UpdateIcon 更新Base图标（业务方法）
func (b *Base) UpdateIcon(icon string) {
	b.Icon = icon
	b.UpdatedAt = time.Now()
}

// BelongsToSpace 检查Base是否属于指定Space
func (b *Base) BelongsToSpace(spaceID string) bool {
	return b.SpaceID == spaceID
}

// IsCreatedBy 检查是否由指定用户创建
func (b *Base) IsCreatedBy(userID string) bool {
	return b.CreatedBy == userID
}
