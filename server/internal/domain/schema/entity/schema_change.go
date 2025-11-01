package entity

import (
	"time"

	"github.com/google/uuid"
)

// SchemaChangeType Schema变更类型
type SchemaChangeType string

const (
	ChangeTypeAddField    SchemaChangeType = "add_field"
	ChangeTypeRemoveField SchemaChangeType = "remove_field"
	ChangeTypeModifyField SchemaChangeType = "modify_field"
	ChangeTypeRenameField SchemaChangeType = "rename_field"
	ChangeTypeChangeType  SchemaChangeType = "change_field_type"
)

// SchemaChange Schema变更实体
type SchemaChange struct {
	id         string
	tableID    string
	changeType SchemaChangeType
	fieldID    string
	oldValue   interface{}
	newValue   interface{}
	status     ChangeStatus
	error      *string
	appliedAt  *time.Time
	createdBy  string
	createdAt  time.Time
}

// ChangeStatus 变更状态
type ChangeStatus string

const (
	StatusPending    ChangeStatus = "pending"
	StatusApplied    ChangeStatus = "applied"
	StatusFailed     ChangeStatus = "failed"
	StatusRolledBack ChangeStatus = "rolled_back"
)

// NewSchemaChange 创建Schema变更
func NewSchemaChange(
	tableID string,
	changeType SchemaChangeType,
	fieldID string,
	oldValue, newValue interface{},
	createdBy string,
) *SchemaChange {
	return &SchemaChange{
		id:         uuid.New().String(),
		tableID:    tableID,
		changeType: changeType,
		fieldID:    fieldID,
		oldValue:   oldValue,
		newValue:   newValue,
		status:     StatusPending,
		createdBy:  createdBy,
		createdAt:  time.Now(),
	}
}

// ==================== 访问器方法 ====================

func (sc *SchemaChange) ID() string                   { return sc.id }
func (sc *SchemaChange) TableID() string              { return sc.tableID }
func (sc *SchemaChange) ChangeType() SchemaChangeType { return sc.changeType }
func (sc *SchemaChange) FieldID() string              { return sc.fieldID }
func (sc *SchemaChange) OldValue() interface{}        { return sc.oldValue }
func (sc *SchemaChange) NewValue() interface{}        { return sc.newValue }
func (sc *SchemaChange) Status() ChangeStatus         { return sc.status }
func (sc *SchemaChange) Error() *string               { return sc.error }
func (sc *SchemaChange) AppliedAt() *time.Time        { return sc.appliedAt }
func (sc *SchemaChange) CreatedBy() string            { return sc.createdBy }
func (sc *SchemaChange) CreatedAt() time.Time         { return sc.createdAt }

// ==================== 业务方法 ====================

// MarkAsApplied 标记为已应用
func (sc *SchemaChange) MarkAsApplied() {
	sc.status = StatusApplied
	now := time.Now()
	sc.appliedAt = &now
}

// MarkAsFailed 标记为失败
func (sc *SchemaChange) MarkAsFailed(errMsg string) {
	sc.status = StatusFailed
	sc.error = &errMsg
}

// MarkAsRolledBack 标记为已回滚
func (sc *SchemaChange) MarkAsRolledBack() {
	sc.status = StatusRolledBack
}

// IsPending 是否待处理
func (sc *SchemaChange) IsPending() bool {
	return sc.status == StatusPending
}

// IsApplied 是否已应用
func (sc *SchemaChange) IsApplied() bool {
	return sc.status == StatusApplied
}

// IsFailed 是否失败
func (sc *SchemaChange) IsFailed() bool {
	return sc.status == StatusFailed
}
