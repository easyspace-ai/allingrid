package database

import (
	"context"
	"sync"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// Event 事务事件接口
type Event interface {
	Type() string      // 事件类型
	TableID() string   // 表ID
	RecordID() string  // 记录ID（可选）
	Data() interface{} // 事件数据
}

// RecordEvent 记录事件
type RecordEvent struct {
	EventType  string                 // create, update, delete
	TID        string                 // 表ID
	RID        string                 // 记录ID
	Fields     map[string]interface{} // 字段数据
	UserID     string                 // 操作用户
	WindowID   string                 // WebSocket窗口ID
	OldVersion int64                  // 旧版本号
	NewVersion int64                  // 新版本号
}

// Type 实现 Event 接口
func (e *RecordEvent) Type() string {
	return e.EventType
}

// TableID 实现 Event 接口
func (e *RecordEvent) TableID() string {
	return e.TID
}

// RecordID 实现 Event 接口
func (e *RecordEvent) RecordID() string {
	return e.RID
}

// Data 实现 Event 接口
func (e *RecordEvent) Data() interface{} {
	return e.Fields
}

// FieldEvent 字段事件
type FieldEvent struct {
	EventType string                 // create, update, delete
	TID       string                 // 表ID
	FID       string                 // 字段ID
	FieldData map[string]interface{} // 字段元数据
	UserID    string                 // 操作用户
}

// Type 实现 Event 接口
func (e *FieldEvent) Type() string {
	return e.EventType
}

// TableID 实现 Event 接口
func (e *FieldEvent) TableID() string {
	return e.TID
}

// RecordID 实现 Event 接口
func (e *FieldEvent) RecordID() string {
	return e.FID
}

// Data 实现 Event 接口
func (e *FieldEvent) Data() interface{} {
	return e.FieldData
}

// ViewEvent 视图事件
type ViewEvent struct {
	EventType string                 // create, update, delete
	TID       string                 // 表ID
	VID       string                 // 视图ID
	ViewData  map[string]interface{} // 视图元数据
	UserID    string                 // 操作用户
}

// Type 实现 Event 接口
func (e *ViewEvent) Type() string {
	return e.EventType
}

// TableID 实现 Event 接口
func (e *ViewEvent) TableID() string {
	return e.TID
}

// RecordID 实现 Event 接口
func (e *ViewEvent) RecordID() string {
	return e.VID
}

// Data 实现 Event 接口
func (e *ViewEvent) Data() interface{} {
	return e.ViewData
}

// EventQueue 事务事件队列
// 在事务中收集所有事件，事务提交成功后统一发布
type EventQueue struct {
	events []Event
	mu     sync.Mutex
}

// NewEventQueue 创建事件队列
func NewEventQueue() *EventQueue {
	return &EventQueue{
		events: make([]Event, 0),
	}
}

// Add 添加事件
func (q *EventQueue) Add(event Event) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.events = append(q.events, event)

	logger.Debug("事件已添加到队列",
		logger.String("type", event.Type()),
		logger.String("table_id", event.TableID()),
		logger.String("record_id", event.RecordID()))
}

// GetAll 获取所有事件
func (q *EventQueue) GetAll() []Event {
	q.mu.Lock()
	defer q.mu.Unlock()

	// 返回副本
	events := make([]Event, len(q.events))
	copy(events, q.events)
	return events
}

// Clear 清空队列
func (q *EventQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.events = make([]Event, 0)
}

// Size 获取队列大小
func (q *EventQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.events)
}

// Deduplicate 去重（根据类型、表ID、记录ID）
// 保留最后一个事件
func (q *EventQueue) Deduplicate() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.events) == 0 {
		return
	}

	// 使用 map 去重，key = type:tableID:recordID
	eventMap := make(map[string]Event)
	keys := make([]string, 0, len(q.events))

	for _, event := range q.events {
		key := event.Type() + ":" + event.TableID() + ":" + event.RecordID()

		// 如果key已存在，只更新value（保留最后一个）
		if _, exists := eventMap[key]; !exists {
			keys = append(keys, key)
		}
		eventMap[key] = event
	}

	// 重建事件列表（保持顺序）
	deduped := make([]Event, 0, len(eventMap))
	for _, key := range keys {
		deduped = append(deduped, eventMap[key])
	}

	originalSize := len(q.events)
	q.events = deduped

	if len(deduped) < originalSize {
		logger.Debug("事件队列去重",
			logger.Int("original", originalSize),
			logger.Int("deduplicated", len(deduped)))
	}
}

// GetEventQueue 从上下文获取或创建事件队列
func GetEventQueue(ctx context.Context) *EventQueue {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		// 从事务上下文中获取事件队列
		if len(txCtx.Events) == 0 {
			return NewEventQueue()
		}

		// 将事务上下文中的事件转换为 EventQueue
		queue := NewEventQueue()
		for _, e := range txCtx.Events {
			if event, ok := e.(Event); ok {
				queue.Add(event)
			}
		}
		return queue
	}

	// 不在事务中，返回新队列
	return NewEventQueue()
}

// AddEventToTx 添加事件到事务上下文
func AddEventToTx(ctx context.Context, event Event) {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		txCtx.AddEvent(event)

		logger.Debug("事件已添加到事务上下文",
			logger.String("tx_id", txCtx.ID),
			logger.String("event_type", event.Type()),
			logger.String("table_id", event.TableID()),
			logger.String("record_id", event.RecordID()))
	} else {
		logger.Warn("不在事务中，无法添加事件到事务上下文",
			logger.String("event_type", event.Type()))
	}
}

// GetEventsFromTx 从事务上下文获取所有事件
func GetEventsFromTx(ctx context.Context) []Event {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		events := make([]Event, 0, len(txCtx.Events))
		for _, e := range txCtx.Events {
			if event, ok := e.(Event); ok {
				events = append(events, event)
			}
		}
		return events
	}
	return nil
}

// ClearEventsInTx 清空事务上下文中的事件
func ClearEventsInTx(ctx context.Context) {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		txCtx.mu.Lock()
		defer txCtx.mu.Unlock()
		txCtx.Events = make([]interface{}, 0)
	}
}

// EventPublisher 事件发布器接口
type EventPublisher interface {
	PublishEvents(events []Event) error
	PublishEvent(event Event) error
}

// EventPublisherFunc 事件发布器函数类型
type EventPublisherFunc func(events []Event) error

// PublishEvents 实现 EventPublisher 接口
func (f EventPublisherFunc) PublishEvents(events []Event) error {
	return f(events)
}

// PublishEvent 实现 EventPublisher 接口
func (f EventPublisherFunc) PublishEvent(event Event) error {
	return f([]Event{event})
}

// PublishEventsAfterTx 在事务提交后发布事件
// 使用事务回调机制
func PublishEventsAfterTx(ctx context.Context, publisher EventPublisher) {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		// 添加事务提交后回调
		txCtx.AddCallback(func() {
			events := GetEventsFromTx(ctx)
			if len(events) == 0 {
				return
			}

			// 去重
			queue := NewEventQueue()
			for _, event := range events {
				queue.Add(event)
			}
			queue.Deduplicate()

			// 发布事件
			deduped := queue.GetAll()
			if len(deduped) > 0 {
				logger.Info("事务提交后发布事件",
					logger.Int("count", len(deduped)))

				if err := publisher.PublishEvents(deduped); err != nil {
					logger.Error("事件发布失败",
						logger.Int("count", len(deduped)),
						logger.ErrorField(err))
				} else {
					logger.Debug("事件发布成功",
						logger.Int("count", len(deduped)))
				}
			}
		})
	} else {
		// 不在事务中，立即发布
		logger.Warn("不在事务中，无法使用事务后回调")
	}
}
