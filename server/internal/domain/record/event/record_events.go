package event

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"

	"github.com/google/uuid"
)

// BaseDomainEvent 基础领域事件
type BaseDomainEvent struct {
	eventID       string
	eventType     string
	occurredAt    time.Time
	aggregateID   string
	aggregateType string
	version       int64
	data          map[string]interface{}
	metadata      map[string]interface{}
}

func newBaseDomainEvent(eventType string, aggregateID string) BaseDomainEvent {
	return BaseDomainEvent{
		eventID:       uuid.New().String(),
		eventType:     eventType,
		occurredAt:    time.Now(),
		aggregateID:   aggregateID,
		aggregateType: "record",
		version:       1,
		data:          make(map[string]interface{}),
		metadata:      make(map[string]interface{}),
	}
}

func (e BaseDomainEvent) EventID() string                  { return e.eventID }
func (e BaseDomainEvent) EventType() string                { return e.eventType }
func (e BaseDomainEvent) OccurredAt() time.Time            { return e.occurredAt }
func (e BaseDomainEvent) AggregateID() string              { return e.aggregateID }
func (e BaseDomainEvent) AggregateType() string            { return e.aggregateType }
func (e BaseDomainEvent) Version() int64                   { return e.version }
func (e BaseDomainEvent) Data() map[string]interface{}     { return e.data }
func (e BaseDomainEvent) Metadata() map[string]interface{} { return e.metadata }

// RecordCreated 记录创建事件
type RecordCreated struct {
	BaseDomainEvent
	recordID  valueobject.RecordID
	tableID   string
	createdBy string
}

// NewRecordCreated 创建记录创建事件
func NewRecordCreated(
	recordID valueobject.RecordID,
	tableID string,
	createdBy string,
) *RecordCreated {
	return &RecordCreated{
		BaseDomainEvent: newBaseDomainEvent("record.created", recordID.String()),
		recordID:        recordID,
		tableID:         tableID,
		createdBy:       createdBy,
	}
}

func (e *RecordCreated) RecordID() valueobject.RecordID { return e.recordID }
func (e *RecordCreated) TableID() string                { return e.tableID }
func (e *RecordCreated) CreatedBy() string              { return e.createdBy }

// RecordUpdated 记录更新事件
type RecordUpdated struct {
	BaseDomainEvent
	recordID      valueobject.RecordID
	tableID       string
	updatedBy     string
	changedFields []string
}

// NewRecordUpdated 创建记录更新事件
func NewRecordUpdated(
	recordID valueobject.RecordID,
	tableID string,
	updatedBy string,
	changedFields []string,
) *RecordUpdated {
	return &RecordUpdated{
		BaseDomainEvent: newBaseDomainEvent("record.updated", recordID.String()),
		recordID:        recordID,
		tableID:         tableID,
		updatedBy:       updatedBy,
		changedFields:   changedFields,
	}
}

func (e *RecordUpdated) RecordID() valueobject.RecordID { return e.recordID }
func (e *RecordUpdated) TableID() string                { return e.tableID }
func (e *RecordUpdated) UpdatedBy() string              { return e.updatedBy }
func (e *RecordUpdated) ChangedFields() []string        { return e.changedFields }

// RecordDeleted 记录删除事件
type RecordDeleted struct {
	BaseDomainEvent
	recordID  valueobject.RecordID
	tableID   string
	deletedBy string
}

// NewRecordDeleted 创建记录删除事件
func NewRecordDeleted(
	recordID valueobject.RecordID,
	tableID string,
	deletedBy string,
) *RecordDeleted {
	return &RecordDeleted{
		BaseDomainEvent: newBaseDomainEvent("record.deleted", recordID.String()),
		recordID:        recordID,
		tableID:         tableID,
		deletedBy:       deletedBy,
	}
}

func (e *RecordDeleted) RecordID() valueobject.RecordID { return e.recordID }
func (e *RecordDeleted) TableID() string                { return e.tableID }
func (e *RecordDeleted) DeletedBy() string              { return e.deletedBy }
