package events

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// DomainEvent 领域事件接口
// 表示业务领域中发生的重要事件
type DomainEvent interface {
	// EventID 事件唯一标识
	EventID() string

	// EventType 事件类型
	EventType() string

	// AggregateID 聚合根ID
	AggregateID() string

	// AggregateType 聚合根类型
	AggregateType() string

	// OccurredAt 事件发生时间
	OccurredAt() time.Time

	// Version 事件版本
	Version() int64

	// Data 事件数据
	Data() map[string]interface{}

	// Metadata 事件元数据
	Metadata() map[string]interface{}
}

// BaseDomainEvent 领域事件基础实现
type BaseDomainEvent struct {
	eventID       string
	eventType     string
	aggregateID   string
	aggregateType string
	occurredAt    time.Time
	version       int64
	data          map[string]interface{}
	metadata      map[string]interface{}
}

// NewBaseDomainEvent 创建基础领域事件
func NewBaseDomainEvent(
	eventType string,
	aggregateID string,
	aggregateType string,
	data map[string]interface{},
) *BaseDomainEvent {
	return &BaseDomainEvent{
		eventID:       utils.GenerateID(),
		eventType:     eventType,
		aggregateID:   aggregateID,
		aggregateType: aggregateType,
		occurredAt:    time.Now(),
		version:       1,
		data:          data,
		metadata:      make(map[string]interface{}),
	}
}

// EventID 事件唯一标识
func (e *BaseDomainEvent) EventID() string {
	return e.eventID
}

// EventType 事件类型
func (e *BaseDomainEvent) EventType() string {
	return e.eventType
}

// AggregateID 聚合根ID
func (e *BaseDomainEvent) AggregateID() string {
	return e.aggregateID
}

// AggregateType 聚合根类型
func (e *BaseDomainEvent) AggregateType() string {
	return e.aggregateType
}

// OccurredAt 事件发生时间
func (e *BaseDomainEvent) OccurredAt() time.Time {
	return e.occurredAt
}

// Version 事件版本
func (e *BaseDomainEvent) Version() int64 {
	return e.version
}

// Data 事件数据
func (e *BaseDomainEvent) Data() map[string]interface{} {
	return e.data
}

// Metadata 事件元数据
func (e *BaseDomainEvent) Metadata() map[string]interface{} {
	return e.metadata
}

// SetMetadata 设置元数据
func (e *BaseDomainEvent) SetMetadata(key string, value interface{}) {
	if e.metadata == nil {
		e.metadata = make(map[string]interface{})
	}
	e.metadata[key] = value
}

// SetVersion 设置版本
func (e *BaseDomainEvent) SetVersion(version int64) {
	e.version = version
}

// EventHandler 事件处理器接口
type EventHandler interface {
	// Handle 处理事件
	Handle(ctx context.Context, event DomainEvent) error

	// EventType 处理器支持的事件类型
	EventType() string

	// Priority 处理器优先级（数字越小优先级越高）
	Priority() int
}

// EventPublisher 事件发布器接口
type EventPublisher interface {
	// Publish 发布事件
	Publish(ctx context.Context, event DomainEvent) error

	// PublishBatch 批量发布事件
	PublishBatch(ctx context.Context, events []DomainEvent) error
}

// EventSubscriber 事件订阅器接口
type EventSubscriber interface {
	// Subscribe 订阅事件
	Subscribe(eventType string, handler EventHandler) error

	// Unsubscribe 取消订阅
	Unsubscribe(eventType string, handler EventHandler) error

	// Start 启动订阅器
	Start(ctx context.Context) error

	// Stop 停止订阅器
	Stop(ctx context.Context) error
}

// EventStore 事件存储接口
type EventStore interface {
	// Save 保存事件
	Save(ctx context.Context, event DomainEvent) error

	// SaveBatch 批量保存事件
	SaveBatch(ctx context.Context, events []DomainEvent) error

	// GetEvents 获取事件
	GetEvents(ctx context.Context, aggregateID string, fromVersion int64) ([]DomainEvent, error)

	// GetEventsByType 根据类型获取事件
	GetEventsByType(ctx context.Context, eventType string, limit int) ([]DomainEvent, error)
}

// EventBus 事件总线接口
type EventBus interface {
	EventPublisher
	EventSubscriber
	EventStore
}

// 预定义的事件类型常量
const (
	// 记录相关事件
	EventTypeRecordCreated  = "record.created"
	EventTypeRecordUpdated  = "record.updated"
	EventTypeRecordDeleted  = "record.deleted"
	EventTypeRecordRestored = "record.restored"

	// 字段相关事件
	EventTypeFieldCreated = "field.created"
	EventTypeFieldUpdated = "field.updated"
	EventTypeFieldDeleted = "field.deleted"

	// 表格相关事件
	EventTypeTableCreated    = "table.created"
	EventTypeTableUpdated    = "table.updated"
	EventTypeTableDeleted    = "table.deleted"
	EventTypeTableDuplicated = "table.duplicated"

	// 视图相关事件
	EventTypeViewCreated = "view.created"
	EventTypeViewUpdated = "view.updated"
	EventTypeViewDeleted = "view.deleted"

	// 空间相关事件
	EventTypeSpaceCreated = "space.created"
	EventTypeSpaceUpdated = "space.updated"
	EventTypeSpaceDeleted = "space.deleted"

	// 用户相关事件
	EventTypeUserCreated = "user.created"
	EventTypeUserUpdated = "user.updated"
	EventTypeUserDeleted = "user.deleted"

	// 计算相关事件
	EventTypeCalculationStarted   = "calculation.started"
	EventTypeCalculationCompleted = "calculation.completed"
	EventTypeCalculationFailed    = "calculation.failed"

	// 缓存相关事件
	EventTypeCacheInvalidated = "cache.invalidated"
	EventTypeCacheUpdated     = "cache.updated"
)

// 预定义的聚合根类型常量
const (
	AggregateTypeRecord = "record"
	AggregateTypeField  = "field"
	AggregateTypeTable  = "table"
	AggregateTypeView   = "view"
	AggregateTypeSpace  = "space"
	AggregateTypeUser   = "user"
)
