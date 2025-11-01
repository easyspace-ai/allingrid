package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/table"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"
)

// Table 表格实体（充血模型，只负责表格本身）
type Table struct {
	// 基础属性（私有）
	id          valueobject.TableID
	baseID      string
	name        valueobject.TableName
	description *string
	icon        *string
	dbTableName *string // ✅ 物理表名（完全动态表架构）例如：bse_xxx.tbl_yyy

	// 审计字段
	createdBy string
	createdAt time.Time
	updatedAt time.Time
	deletedAt *time.Time

	// 版本控制
	version int
}

// NewTable 创建新表格（工厂方法）
func NewTable(
	baseID string,
	name valueobject.TableName,
	createdBy string,
) (*Table, error) {
	// 业务规则验证
	if baseID == "" {
		return nil, table.NewDomainError(
			"INVALID_BASE_ID",
			"base id cannot be empty",
			nil,
		)
	}

	if name.IsEmpty() {
		return nil, table.ErrTableNameEmpty
	}

	now := time.Now()

	return &Table{
		id:        valueobject.NewTableID(""),
		baseID:    baseID,
		name:      name,
		createdBy: createdBy,
		createdAt: now,
		updatedAt: now,
		version:   1,
	}, nil
}

// ReconstructTable 重建表格（从数据库加载）
func ReconstructTable(
	id valueobject.TableID,
	baseID string,
	name valueobject.TableName,
	description *string,
	icon *string,
	dbTableName *string,
	createdBy string,
	createdAt time.Time,
	updatedAt time.Time,
	deletedAt *time.Time,
	version int,
) *Table {
	return &Table{
		id:          id,
		baseID:      baseID,
		name:        name,
		description: description,
		icon:        icon,
		dbTableName: dbTableName,
		createdBy:   createdBy,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
		deletedAt:   deletedAt,
		version:     version,
	}
}

// ==================== 访问器方法 ====================

// ID 获取表格ID
func (t *Table) ID() valueobject.TableID {
	return t.id
}

// BaseID 获取Base ID
func (t *Table) BaseID() string {
	return t.baseID
}

// Name 获取表格名称
func (t *Table) Name() valueobject.TableName {
	return t.name
}

// Description 获取描述
func (t *Table) Description() *string {
	return t.description
}

// Icon 获取图标
func (t *Table) Icon() *string {
	return t.icon
}

// DBTableName 获取物理表名（完全动态表架构）
func (t *Table) DBTableName() *string {
	return t.dbTableName
}

// CreatedBy 获取创建者
func (t *Table) CreatedBy() string {
	return t.createdBy
}

// CreatedAt 获取创建时间
func (t *Table) CreatedAt() time.Time {
	return t.createdAt
}

// UpdatedAt 获取更新时间
func (t *Table) UpdatedAt() time.Time {
	return t.updatedAt
}

// DeletedAt 获取删除时间
func (t *Table) DeletedAt() *time.Time {
	return t.deletedAt
}

// Version 获取版本号
func (t *Table) Version() int {
	return t.version
}

// IsDeleted 是否已删除
func (t *Table) IsDeleted() bool {
	return t.deletedAt != nil
}

// ==================== 业务方法 ====================

// Rename 重命名表格
func (t *Table) Rename(newName valueobject.TableName) error {
	if t.IsDeleted() {
		return table.ErrCannotModifyDeletedTable
	}

	t.name = newName
	t.updatedAt = time.Now()
	t.incrementVersion()

	return nil
}

// UpdateDescription 更新描述
func (t *Table) UpdateDescription(description string) error {
	if t.IsDeleted() {
		return table.ErrCannotModifyDeletedTable
	}

	t.description = &description
	t.updatedAt = time.Now()

	return nil
}

// UpdateIcon 更新图标
func (t *Table) UpdateIcon(icon string) error {
	if t.IsDeleted() {
		return table.ErrCannotModifyDeletedTable
	}

	t.icon = &icon
	t.updatedAt = time.Now()

	return nil
}

// SetDBTableName 设置物理表名（完全动态表架构）
// 由TableService在创建物理表后调用
func (t *Table) SetDBTableName(dbTableName string) {
	t.dbTableName = &dbTableName
	t.updatedAt = time.Now()
}

// SoftDelete 软删除表格
func (t *Table) SoftDelete() error {
	if t.IsDeleted() {
		return table.ErrTableAlreadyDeleted
	}

	now := time.Now()
	t.deletedAt = &now
	t.updatedAt = now

	return nil
}

// Restore 恢复已删除的表格
func (t *Table) Restore() error {
	if !t.IsDeleted() {
		return table.NewDomainError(
			"TABLE_NOT_DELETED",
			"table is not deleted",
			nil,
		)
	}

	t.deletedAt = nil
	t.updatedAt = time.Now()

	return nil
}

// ==================== 私有辅助方法 ====================

// incrementVersion 递增版本号
func (t *Table) incrementVersion() {
	t.version++
}
