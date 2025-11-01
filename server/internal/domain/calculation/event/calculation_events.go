package event

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/google/uuid"
)

// CalculationRequested 计算请求事件
type CalculationRequested struct {
	BaseDomainEvent
	tableID     string
	recordID    valueobject.RecordID
	fieldIDs    []string
	priority    int
	requestedBy string
}

// NewCalculationRequested 创建计算请求事件
func NewCalculationRequested(
	tableID string,
	recordID valueobject.RecordID,
	fieldIDs []string,
	priority int,
	requestedBy string,
) *CalculationRequested {
	return &CalculationRequested{
		BaseDomainEvent: newBaseDomainEvent("calculation.requested", recordID.String()),
		tableID:         tableID,
		recordID:        recordID,
		fieldIDs:        fieldIDs,
		priority:        priority,
		requestedBy:     requestedBy,
	}
}

func (e *CalculationRequested) TableID() string                { return e.tableID }
func (e *CalculationRequested) RecordID() valueobject.RecordID { return e.recordID }
func (e *CalculationRequested) FieldIDs() []string             { return e.fieldIDs }
func (e *CalculationRequested) Priority() int                  { return e.priority }
func (e *CalculationRequested) RequestedBy() string            { return e.requestedBy }
func (e *CalculationRequested) AggregateType() string          { return "calculation" }
func (e *CalculationRequested) Data() map[string]interface{} {
	return map[string]interface{}{
		"table_id":     e.tableID,
		"record_id":    e.recordID.String(),
		"field_ids":    e.fieldIDs,
		"priority":     e.priority,
		"requested_by": e.requestedBy,
	}
}
func (e *CalculationRequested) Metadata() map[string]interface{} { return make(map[string]interface{}) }
func (e *CalculationRequested) Version() int64                   { return 1 }

// CalculationCompleted 计算完成事件
type CalculationCompleted struct {
	BaseDomainEvent
	tableID          string
	recordID         valueobject.RecordID
	calculatedFields map[string]interface{}
	completedBy      string
	duration         time.Duration
}

// NewCalculationCompleted 创建计算完成事件
func NewCalculationCompleted(
	tableID string,
	recordID valueobject.RecordID,
	calculatedFields map[string]interface{},
	completedBy string,
	duration time.Duration,
) *CalculationCompleted {
	return &CalculationCompleted{
		BaseDomainEvent:  newBaseDomainEvent("calculation.completed", recordID.String()),
		tableID:          tableID,
		recordID:         recordID,
		calculatedFields: calculatedFields,
		completedBy:      completedBy,
		duration:         duration,
	}
}

func (e *CalculationCompleted) TableID() string                          { return e.tableID }
func (e *CalculationCompleted) RecordID() valueobject.RecordID           { return e.recordID }
func (e *CalculationCompleted) CalculatedFields() map[string]interface{} { return e.calculatedFields }
func (e *CalculationCompleted) CompletedBy() string                      { return e.completedBy }
func (e *CalculationCompleted) Duration() time.Duration                  { return e.duration }
func (e *CalculationCompleted) AggregateType() string                    { return "calculation" }
func (e *CalculationCompleted) Data() map[string]interface{} {
	return map[string]interface{}{
		"table_id":          e.tableID,
		"record_id":         e.recordID.String(),
		"calculated_fields": e.calculatedFields,
		"completed_by":      e.completedBy,
		"duration":          e.duration.String(),
	}
}
func (e *CalculationCompleted) Metadata() map[string]interface{} { return make(map[string]interface{}) }
func (e *CalculationCompleted) Version() int64                   { return 1 }

// CalculationFailed 计算失败事件
type CalculationFailed struct {
	BaseDomainEvent
	tableID    string
	recordID   valueobject.RecordID
	fieldIDs   []string
	error      string
	failedBy   string
	retryCount int
}

// NewCalculationFailed 创建计算失败事件
func NewCalculationFailed(
	tableID string,
	recordID valueobject.RecordID,
	fieldIDs []string,
	error string,
	failedBy string,
	retryCount int,
) *CalculationFailed {
	return &CalculationFailed{
		BaseDomainEvent: newBaseDomainEvent("calculation.failed", recordID.String()),
		tableID:         tableID,
		recordID:        recordID,
		fieldIDs:        fieldIDs,
		error:           error,
		failedBy:        failedBy,
		retryCount:      retryCount,
	}
}

func (e *CalculationFailed) TableID() string                { return e.tableID }
func (e *CalculationFailed) RecordID() valueobject.RecordID { return e.recordID }
func (e *CalculationFailed) FieldIDs() []string             { return e.fieldIDs }
func (e *CalculationFailed) Error() string                  { return e.error }
func (e *CalculationFailed) FailedBy() string               { return e.failedBy }
func (e *CalculationFailed) RetryCount() int                { return e.retryCount }
func (e *CalculationFailed) AggregateType() string          { return "calculation" }
func (e *CalculationFailed) Data() map[string]interface{} {
	return map[string]interface{}{
		"table_id":    e.tableID,
		"record_id":   e.recordID.String(),
		"field_ids":   e.fieldIDs,
		"error":       e.error,
		"failed_by":   e.failedBy,
		"retry_count": e.retryCount,
	}
}
func (e *CalculationFailed) Metadata() map[string]interface{} { return make(map[string]interface{}) }
func (e *CalculationFailed) Version() int64                   { return 1 }

// 计算优先级常量
const (
	LowPriority    = 1
	NormalPriority = 5
	HighPriority   = 10
	UrgentPriority = 20
)

// BaseDomainEvent 基础领域事件（复用记录事件的定义）
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
