package application

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/events"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// EventBus 事件总线实现
// 负责事件的发布、订阅和存储
type EventBus struct {
	// 事件处理器映射
	handlers map[string][]events.EventHandler

	// 事件存储
	eventStore events.EventStore

	// 错误处理服务
	errorService *ErrorService

	// 并发控制
	mu sync.RWMutex

	// 配置
	config *EventBusConfig

	// 运行状态
	running bool
	stopCh  chan struct{}
}

// EventBusConfig 事件总线配置
type EventBusConfig struct {
	// 异步处理配置
	AsyncEnabled   bool `json:"async_enabled"`
	WorkerPoolSize int  `json:"worker_pool_size"`
	QueueSize      int  `json:"queue_size"`

	// 重试配置
	MaxRetries int           `json:"max_retries"`
	RetryDelay time.Duration `json:"retry_delay"`

	// 持久化配置
	PersistEnabled bool          `json:"persist_enabled"`
	BatchSize      int           `json:"batch_size"`
	FlushInterval  time.Duration `json:"flush_interval"`
}

// DefaultEventBusConfig 默认事件总线配置
func DefaultEventBusConfig() *EventBusConfig {
	return &EventBusConfig{
		AsyncEnabled:   true,
		WorkerPoolSize: 10,
		QueueSize:      1000,
		MaxRetries:     3,
		RetryDelay:     100 * time.Millisecond,
		PersistEnabled: true,
		BatchSize:      100,
		FlushInterval:  1 * time.Second,
	}
}

// NewEventBus 创建事件总线
func NewEventBus(
	eventStore events.EventStore,
	errorService *ErrorService,
	config *EventBusConfig,
) *EventBus {
	if config == nil {
		config = DefaultEventBusConfig()
	}

	return &EventBus{
		handlers:     make(map[string][]events.EventHandler),
		eventStore:   eventStore,
		errorService: errorService,
		config:       config,
		stopCh:       make(chan struct{}),
	}
}

// Publish 发布事件
func (eb *EventBus) Publish(ctx context.Context, event events.DomainEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	logger.Debug("publishing event",
		logger.String("event_type", event.EventType()),
		logger.String("aggregate_id", event.AggregateID()),
		logger.String("event_id", event.EventID()))

	// 持久化事件
	if eb.config.PersistEnabled && eb.eventStore != nil {
		if err := eb.eventStore.Save(ctx, event); err != nil {
			return eb.errorService.HandleDatabaseError(ctx, "EventStore.Save", err)
		}
	}

	// 发布事件
	if eb.config.AsyncEnabled {
		return eb.publishAsync(ctx, event)
	}
	return eb.publishSync(ctx, event)
}

// PublishBatch 批量发布事件
func (eb *EventBus) PublishBatch(ctx context.Context, eventList []events.DomainEvent) error {
	if len(eventList) == 0 {
		return nil
	}

	logger.Debug("publishing batch events",
		logger.Int("event_count", len(eventList)))

	// 持久化事件
	if eb.config.PersistEnabled && eb.eventStore != nil {
		if err := eb.eventStore.SaveBatch(ctx, eventList); err != nil {
			return eb.errorService.HandleDatabaseError(ctx, "EventStore.SaveBatch", err)
		}
	}

	// 发布事件
	if eb.config.AsyncEnabled {
		return eb.publishBatchAsync(ctx, eventList)
	}
	return eb.publishBatchSync(ctx, eventList)
}

// Subscribe 订阅事件
func (eb *EventBus) Subscribe(eventType string, handler events.EventHandler) error {
	if eventType == "" || handler == nil {
		return fmt.Errorf("eventType and handler cannot be empty")
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	// 检查是否已存在相同的处理器
	for _, h := range eb.handlers[eventType] {
		if h == handler {
			logger.Warn("handler already subscribed",
				logger.String("event_type", eventType))
			return nil
		}
	}

	// 添加处理器
	eb.handlers[eventType] = append(eb.handlers[eventType], handler)

	logger.Info("event handler subscribed",
		logger.String("event_type", eventType),
		logger.String("handler_type", fmt.Sprintf("%T", handler)))

	return nil
}

// Unsubscribe 取消订阅
func (eb *EventBus) Unsubscribe(eventType string, handler events.EventHandler) error {
	if eventType == "" || handler == nil {
		return fmt.Errorf("eventType and handler cannot be empty")
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	handlers := eb.handlers[eventType]
	for i, h := range handlers {
		if h == handler {
			// 移除处理器
			eb.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)

			logger.Info("event handler unsubscribed",
				logger.String("event_type", eventType),
				logger.String("handler_type", fmt.Sprintf("%T", handler)))
			return nil
		}
	}

	return fmt.Errorf("handler not found for event type: %s", eventType)
}

// Start 启动事件总线
func (eb *EventBus) Start(ctx context.Context) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.running {
		return fmt.Errorf("event bus is already running")
	}

	eb.running = true
	eb.stopCh = make(chan struct{})

	logger.Info("event bus started",
		logger.Bool("async_enabled", eb.config.AsyncEnabled),
		logger.Int("worker_pool_size", eb.config.WorkerPoolSize))

	return nil
}

// Stop 停止事件总线
func (eb *EventBus) Stop(ctx context.Context) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if !eb.running {
		return fmt.Errorf("event bus is not running")
	}

	eb.running = false
	close(eb.stopCh)

	logger.Info("event bus stopped")
	return nil
}

// GetEvents 获取事件
func (eb *EventBus) GetEvents(ctx context.Context, aggregateID string, fromVersion int64) ([]events.DomainEvent, error) {
	if eb.eventStore == nil {
		return nil, fmt.Errorf("event store not configured")
	}

	return eb.eventStore.GetEvents(ctx, aggregateID, fromVersion)
}

// GetEventsByType 根据类型获取事件
func (eb *EventBus) GetEventsByType(ctx context.Context, eventType string, limit int) ([]events.DomainEvent, error) {
	if eb.eventStore == nil {
		return nil, fmt.Errorf("event store not configured")
	}

	return eb.eventStore.GetEventsByType(ctx, eventType, limit)
}

// publishSync 同步发布事件
func (eb *EventBus) publishSync(ctx context.Context, event events.DomainEvent) error {
	eb.mu.RLock()
	handlers := eb.handlers[event.EventType()]
	eb.mu.RUnlock()

	if len(handlers) == 0 {
		logger.Debug("no handlers for event type",
			logger.String("event_type", event.EventType()))
		return nil
	}

	// 按优先级排序处理器
	handlers = eb.sortHandlersByPriority(handlers)

	// 同步执行所有处理器
	for _, handler := range handlers {
		if err := eb.executeHandler(ctx, handler, event); err != nil {
			logger.Error("event handler failed",
				logger.String("event_type", event.EventType()),
				logger.String("handler_type", fmt.Sprintf("%T", handler)),
				logger.ErrorField(err))
			// 继续执行其他处理器
		}
	}

	return nil
}

// publishAsync 异步发布事件
func (eb *EventBus) publishAsync(ctx context.Context, event events.DomainEvent) error {
	// 这里可以实现异步处理逻辑
	// 暂时使用同步处理
	return eb.publishSync(ctx, event)
}

// publishBatchSync 同步批量发布事件
func (eb *EventBus) publishBatchSync(ctx context.Context, eventList []events.DomainEvent) error {
	for _, event := range eventList {
		if err := eb.publishSync(ctx, event); err != nil {
			logger.Error("batch event publish failed",
				logger.String("event_type", event.EventType()),
				logger.String("event_id", event.EventID()),
				logger.ErrorField(err))
			// 继续处理其他事件
		}
	}
	return nil
}

// publishBatchAsync 异步批量发布事件
func (eb *EventBus) publishBatchAsync(ctx context.Context, eventList []events.DomainEvent) error {
	// 这里可以实现异步批量处理逻辑
	// 暂时使用同步处理
	return eb.publishBatchSync(ctx, eventList)
}

// executeHandler 执行事件处理器
func (eb *EventBus) executeHandler(ctx context.Context, handler events.EventHandler, event events.DomainEvent) error {
	// 重试机制
	for attempt := 0; attempt <= eb.config.MaxRetries; attempt++ {
		err := handler.Handle(ctx, event)
		if err == nil {
			return nil
		}

		if attempt < eb.config.MaxRetries {
			logger.Warn("event handler retry",
				logger.String("event_type", event.EventType()),
				logger.String("handler_type", fmt.Sprintf("%T", handler)),
				logger.Int("attempt", attempt+1),
				logger.ErrorField(err))

			time.Sleep(eb.config.RetryDelay)
		} else {
			return fmt.Errorf("event handler failed after %d attempts: %w", eb.config.MaxRetries+1, err)
		}
	}

	return nil
}

// sortHandlersByPriority 按优先级排序处理器
func (eb *EventBus) sortHandlersByPriority(handlers []events.EventHandler) []events.EventHandler {
	// 简单的冒泡排序（按优先级升序）
	sorted := make([]events.EventHandler, len(handlers))
	copy(sorted, handlers)

	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j].Priority() > sorted[j+1].Priority() {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	return sorted
}

// GetStats 获取事件总线统计信息
func (eb *EventBus) GetStats() map[string]interface{} {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	stats := map[string]interface{}{
		"running":          eb.running,
		"async_enabled":    eb.config.AsyncEnabled,
		"worker_pool_size": eb.config.WorkerPoolSize,
		"persist_enabled":  eb.config.PersistEnabled,
		"event_types":      len(eb.handlers),
	}

	// 统计每种事件类型的处理器数量
	handlerCounts := make(map[string]int)
	for eventType, handlers := range eb.handlers {
		handlerCounts[eventType] = len(handlers)
	}
	stats["handler_counts"] = handlerCounts

	return stats
}
