package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
)

// Record 记录实体（充血模型）
// 表示表格中的一条数据记录
type Record struct {
	// 基础属性（私有）
	id      valueobject.RecordID
	tableID string
	data    valueobject.RecordData
	version valueobject.RecordVersion

	// 审计字段
	createdBy string
	updatedBy string
	createdAt time.Time
	updatedAt time.Time
	deletedAt *time.Time
}

// NewRecord 创建新记录（工厂方法）
func NewRecord(
	tableID string,
	data valueobject.RecordData,
	createdBy string,
) (*Record, error) {
	// 验证
	if tableID == "" {
		return nil, record.NewDomainError(
			"INVALID_TABLE_ID",
			"table ID cannot be empty",
			nil,
		)
	}

	if data.IsEmpty() {
		return nil, record.ErrEmptyRecordData
	}

	now := time.Now()

	return &Record{
		id:        valueobject.NewRecordID(""),
		tableID:   tableID,
		data:      data,
		version:   valueobject.InitialVersion(),
		createdBy: createdBy,
		updatedBy: createdBy,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// ReconstructRecord 重建记录（从数据库加载）
func ReconstructRecord(
	id valueobject.RecordID,
	tableID string,
	data valueobject.RecordData,
	version valueobject.RecordVersion,
	createdBy, updatedBy string,
	createdAt, updatedAt time.Time,
	deletedAt *time.Time,
) *Record {
	return &Record{
		id:        id,
		tableID:   tableID,
		data:      data,
		version:   version,
		createdBy: createdBy,
		updatedBy: updatedBy,
		createdAt: createdAt,
		updatedAt: updatedAt,
		deletedAt: deletedAt,
	}
}

// ==================== 访问器方法 ====================

func (r *Record) ID() valueobject.RecordID           { return r.id }
func (r *Record) TableID() string                    { return r.tableID }
func (r *Record) Data() valueobject.RecordData       { return r.data }
func (r *Record) Version() valueobject.RecordVersion { return r.version }
func (r *Record) CreatedBy() string                  { return r.createdBy }
func (r *Record) UpdatedBy() string                  { return r.updatedBy }
func (r *Record) CreatedAt() time.Time               { return r.createdAt }
func (r *Record) UpdatedAt() time.Time               { return r.updatedAt }
func (r *Record) DeletedAt() *time.Time              { return r.deletedAt }

// IsDeleted 是否已删除
func (r *Record) IsDeleted() bool {
	return r.deletedAt != nil
}

// GetFieldValue 获取字段值
func (r *Record) GetFieldValue(fieldName string) (interface{}, bool) {
	return r.data.Get(fieldName)
}

// ==================== 业务方法 ====================

// Update 更新记录数据
func (r *Record) Update(newData valueobject.RecordData, updatedBy string) error {
	if r.IsDeleted() {
		return record.ErrCannotModifyDeletedRecord
	}

	// 合并数据
	mergedData, err := r.data.Merge(newData)
	if err != nil {
		return err
	}

	r.data = mergedData
	r.updatedBy = updatedBy
	r.updatedAt = time.Now()
	r.version = r.version.Increment()

	return nil
}

// SetFieldValue 设置单个字段值
func (r *Record) SetFieldValue(fieldName string, value interface{}, updatedBy string) error {
	if r.IsDeleted() {
		return record.ErrCannotModifyDeletedRecord
	}

	// 更新数据
	newData, err := r.data.Set(fieldName, value)
	if err != nil {
		return err
	}

	r.data = newData
	r.updatedBy = updatedBy
	r.updatedAt = time.Now()
	r.version = r.version.Increment()

	return nil
}

// DeleteFieldValue 删除字段值
func (r *Record) DeleteFieldValue(fieldName string, updatedBy string) error {
	if r.IsDeleted() {
		return record.ErrCannotModifyDeletedRecord
	}

	newData, err := r.data.Delete(fieldName)
	if err != nil {
		return err
	}

	r.data = newData
	r.updatedBy = updatedBy
	r.updatedAt = time.Now()
	r.version = r.version.Increment()

	return nil
}

// SoftDelete 软删除记录
func (r *Record) SoftDelete() error {
	if r.IsDeleted() {
		return record.ErrRecordAlreadyDeleted
	}

	now := time.Now()
	r.deletedAt = &now
	r.updatedAt = now

	return nil
}

// Restore 恢复已删除的记录
func (r *Record) Restore() error {
	if !r.IsDeleted() {
		return record.NewDomainError(
			"RECORD_NOT_DELETED",
			"record is not deleted",
			nil,
		)
	}

	r.deletedAt = nil
	r.updatedAt = time.Now()

	return nil
}

// HasChangedSince 检查记录是否在指定版本后发生变更
func (r *Record) HasChangedSince(version valueobject.RecordVersion) bool {
	return r.version.IsGreaterThan(version)
}
