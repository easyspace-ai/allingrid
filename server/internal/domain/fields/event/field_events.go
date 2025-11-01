package event

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"

	"github.com/google/uuid"
)

// DomainEvent 领域事件接口
type DomainEvent interface {
	EventID() string
	EventType() string
	OccurredAt() time.Time
	AggregateID() string
}

// BaseDomainEvent 基础领域事件
type BaseDomainEvent struct {
	eventID     string
	eventType   string
	occurredAt  time.Time
	aggregateID string
}

func newBaseDomainEvent(eventType string, aggregateID string) BaseDomainEvent {
	return BaseDomainEvent{
		eventID:     uuid.New().String(),
		eventType:   eventType,
		occurredAt:  time.Now(),
		aggregateID: aggregateID,
	}
}

func (e BaseDomainEvent) EventID() string       { return e.eventID }
func (e BaseDomainEvent) EventType() string     { return e.eventType }
func (e BaseDomainEvent) OccurredAt() time.Time { return e.occurredAt }
func (e BaseDomainEvent) AggregateID() string   { return e.aggregateID }

// FieldCreated 字段创建事件
type FieldCreated struct {
	BaseDomainEvent
	fieldID   valueobject.FieldID
	tableID   string
	fieldName valueobject.FieldName
	fieldType valueobject.FieldType
	createdBy string
}

// NewFieldCreated 创建字段创建事件
func NewFieldCreated(
	fieldID valueobject.FieldID,
	tableID string,
	fieldName valueobject.FieldName,
	fieldType valueobject.FieldType,
	createdBy string,
) *FieldCreated {
	return &FieldCreated{
		BaseDomainEvent: newBaseDomainEvent("field.created", fieldID.String()),
		fieldID:         fieldID,
		tableID:         tableID,
		fieldName:       fieldName,
		fieldType:       fieldType,
		createdBy:       createdBy,
	}
}

func (e *FieldCreated) FieldID() valueobject.FieldID     { return e.fieldID }
func (e *FieldCreated) TableID() string                  { return e.tableID }
func (e *FieldCreated) FieldName() valueobject.FieldName { return e.fieldName }
func (e *FieldCreated) FieldType() valueobject.FieldType { return e.fieldType }
func (e *FieldCreated) CreatedBy() string                { return e.createdBy }

// FieldUpdated 字段更新事件
type FieldUpdated struct {
	BaseDomainEvent
	fieldID   valueobject.FieldID
	tableID   string
	oldName   valueobject.FieldName
	newName   valueobject.FieldName
	updatedBy string
}

// NewFieldUpdated 创建字段更新事件
func NewFieldUpdated(
	fieldID valueobject.FieldID,
	tableID string,
	oldName, newName valueobject.FieldName,
	updatedBy string,
) *FieldUpdated {
	return &FieldUpdated{
		BaseDomainEvent: newBaseDomainEvent("field.updated", fieldID.String()),
		fieldID:         fieldID,
		tableID:         tableID,
		oldName:         oldName,
		newName:         newName,
		updatedBy:       updatedBy,
	}
}

func (e *FieldUpdated) FieldID() valueobject.FieldID   { return e.fieldID }
func (e *FieldUpdated) TableID() string                { return e.tableID }
func (e *FieldUpdated) OldName() valueobject.FieldName { return e.oldName }
func (e *FieldUpdated) NewName() valueobject.FieldName { return e.newName }
func (e *FieldUpdated) UpdatedBy() string              { return e.updatedBy }

// FieldDeleted 字段删除事件
type FieldDeleted struct {
	BaseDomainEvent
	fieldID   valueobject.FieldID
	tableID   string
	fieldName valueobject.FieldName
	deletedBy string
}

// NewFieldDeleted 创建字段删除事件
func NewFieldDeleted(
	fieldID valueobject.FieldID,
	tableID string,
	fieldName valueobject.FieldName,
	deletedBy string,
) *FieldDeleted {
	return &FieldDeleted{
		BaseDomainEvent: newBaseDomainEvent("field.deleted", fieldID.String()),
		fieldID:         fieldID,
		tableID:         tableID,
		fieldName:       fieldName,
		deletedBy:       deletedBy,
	}
}

func (e *FieldDeleted) FieldID() valueobject.FieldID     { return e.fieldID }
func (e *FieldDeleted) TableID() string                  { return e.tableID }
func (e *FieldDeleted) FieldName() valueobject.FieldName { return e.fieldName }
func (e *FieldDeleted) DeletedBy() string                { return e.deletedBy }

// FieldTypeChanged 字段类型变更事件
type FieldTypeChanged struct {
	BaseDomainEvent
	fieldID   valueobject.FieldID
	tableID   string
	oldType   valueobject.FieldType
	newType   valueobject.FieldType
	changedBy string
}

// NewFieldTypeChanged 创建字段类型变更事件
func NewFieldTypeChanged(
	fieldID valueobject.FieldID,
	tableID string,
	oldType, newType valueobject.FieldType,
	changedBy string,
) *FieldTypeChanged {
	return &FieldTypeChanged{
		BaseDomainEvent: newBaseDomainEvent("field.type_changed", fieldID.String()),
		fieldID:         fieldID,
		tableID:         tableID,
		oldType:         oldType,
		newType:         newType,
		changedBy:       changedBy,
	}
}

func (e *FieldTypeChanged) FieldID() valueobject.FieldID   { return e.fieldID }
func (e *FieldTypeChanged) TableID() string                { return e.tableID }
func (e *FieldTypeChanged) OldType() valueobject.FieldType { return e.oldType }
func (e *FieldTypeChanged) NewType() valueobject.FieldType { return e.newType }
func (e *FieldTypeChanged) ChangedBy() string              { return e.changedBy }

// FieldOptionsUpdated 字段选项更新事件
type FieldOptionsUpdated struct {
	BaseDomainEvent
	fieldID   valueobject.FieldID
	tableID   string
	updatedBy string
}

// NewFieldOptionsUpdated 创建字段选项更新事件
func NewFieldOptionsUpdated(
	fieldID valueobject.FieldID,
	tableID string,
	updatedBy string,
) *FieldOptionsUpdated {
	return &FieldOptionsUpdated{
		BaseDomainEvent: newBaseDomainEvent("field.options_updated", fieldID.String()),
		fieldID:         fieldID,
		tableID:         tableID,
		updatedBy:       updatedBy,
	}
}

func (e *FieldOptionsUpdated) FieldID() valueobject.FieldID { return e.fieldID }
func (e *FieldOptionsUpdated) TableID() string              { return e.tableID }
func (e *FieldOptionsUpdated) UpdatedBy() string            { return e.updatedBy }
