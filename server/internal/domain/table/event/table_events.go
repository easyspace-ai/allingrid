package event

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	tableVO "github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"

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

// TableCreated 表格创建事件
type TableCreated struct {
	BaseDomainEvent
	tableID   tableVO.TableID
	tableName tableVO.TableName
	baseID    string
	createdBy string
}

// NewTableCreated 创建表格创建事件
func NewTableCreated(
	tableID tableVO.TableID,
	tableName tableVO.TableName,
	baseID string,
	createdBy string,
) *TableCreated {
	return &TableCreated{
		BaseDomainEvent: newBaseDomainEvent("table.created", tableID.String()),
		tableID:         tableID,
		tableName:       tableName,
		baseID:          baseID,
		createdBy:       createdBy,
	}
}

func (e *TableCreated) TableID() tableVO.TableID     { return e.tableID }
func (e *TableCreated) TableName() tableVO.TableName { return e.tableName }
func (e *TableCreated) BaseID() string               { return e.baseID }
func (e *TableCreated) CreatedBy() string            { return e.createdBy }

// TableDeleted 表格删除事件
type TableDeleted struct {
	BaseDomainEvent
	tableID   tableVO.TableID
	tableName tableVO.TableName
	deletedBy string
}

// NewTableDeleted 创建表格删除事件
func NewTableDeleted(
	tableID tableVO.TableID,
	tableName tableVO.TableName,
	deletedBy string,
) *TableDeleted {
	return &TableDeleted{
		BaseDomainEvent: newBaseDomainEvent("table.deleted", tableID.String()),
		tableID:         tableID,
		tableName:       tableName,
		deletedBy:       deletedBy,
	}
}

func (e *TableDeleted) TableID() tableVO.TableID     { return e.tableID }
func (e *TableDeleted) TableName() tableVO.TableName { return e.tableName }
func (e *TableDeleted) DeletedBy() string            { return e.deletedBy }

// FieldAddedToTable 字段添加到表格事件
type FieldAddedToTable struct {
	BaseDomainEvent
	tableID   tableVO.TableID
	fieldID   valueobject.FieldID
	fieldName valueobject.FieldName
}

// NewFieldAddedToTable 创建字段添加到表格事件
func NewFieldAddedToTable(
	tableID tableVO.TableID,
	fieldID valueobject.FieldID,
	fieldName valueobject.FieldName,
) *FieldAddedToTable {
	return &FieldAddedToTable{
		BaseDomainEvent: newBaseDomainEvent("table.field_added", tableID.String()),
		tableID:         tableID,
		fieldID:         fieldID,
		fieldName:       fieldName,
	}
}

func (e *FieldAddedToTable) TableID() tableVO.TableID         { return e.tableID }
func (e *FieldAddedToTable) FieldID() valueobject.FieldID     { return e.fieldID }
func (e *FieldAddedToTable) FieldName() valueobject.FieldName { return e.fieldName }

// FieldRemovedFromTable 字段从表格移除事件
type FieldRemovedFromTable struct {
	BaseDomainEvent
	tableID   tableVO.TableID
	fieldID   valueobject.FieldID
	fieldName valueobject.FieldName
}

// NewFieldRemovedFromTable 创建字段从表格移除事件
func NewFieldRemovedFromTable(
	tableID tableVO.TableID,
	fieldID valueobject.FieldID,
	fieldName valueobject.FieldName,
) *FieldRemovedFromTable {
	return &FieldRemovedFromTable{
		BaseDomainEvent: newBaseDomainEvent("table.field_removed", tableID.String()),
		tableID:         tableID,
		fieldID:         fieldID,
		fieldName:       fieldName,
	}
}

func (e *FieldRemovedFromTable) TableID() tableVO.TableID         { return e.tableID }
func (e *FieldRemovedFromTable) FieldID() valueobject.FieldID     { return e.fieldID }
func (e *FieldRemovedFromTable) FieldName() valueobject.FieldName { return e.fieldName }
