package application

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/events"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CacheInvalidationHandler 缓存失效事件处理器
type CacheInvalidationHandler struct {
	cacheService *CacheService
	priority     int
}

// NewCacheInvalidationHandler 创建缓存失效事件处理器
func NewCacheInvalidationHandler(cacheService *CacheService) *CacheInvalidationHandler {
	return &CacheInvalidationHandler{
		cacheService: cacheService,
		priority:     1, // 高优先级
	}
}

// Handle 处理缓存失效事件
func (h *CacheInvalidationHandler) Handle(ctx context.Context, event events.DomainEvent) error {
	eventData := event.Data()

	// 根据事件类型执行不同的缓存失效策略
	switch event.EventType() {
	case events.EventTypeRecordCreated, events.EventTypeRecordUpdated, events.EventTypeRecordDeleted:
		if tableID, ok := eventData["table_id"].(string); ok {
			return h.cacheService.InvalidateTableCache(ctx, tableID)
		}

	case events.EventTypeFieldCreated, events.EventTypeFieldUpdated, events.EventTypeFieldDeleted:
		if fieldID, ok := eventData["field_id"].(string); ok {
			return h.cacheService.InvalidateFieldCache(ctx, fieldID)
		}

	case events.EventTypeTableCreated, events.EventTypeTableUpdated, events.EventTypeTableDeleted:
		if tableID, ok := eventData["table_id"].(string); ok {
			return h.cacheService.InvalidateTableCache(ctx, tableID)
		}

	case events.EventTypeViewCreated, events.EventTypeViewUpdated, events.EventTypeViewDeleted:
		if tableID, ok := eventData["table_id"].(string); ok {
			return h.cacheService.InvalidateTableCache(ctx, tableID)
		}

	default:
		logger.Debug("no cache invalidation strategy for event type",
			logger.String("event_type", event.EventType()))
	}

	return nil
}

// EventType 处理器支持的事件类型
func (h *CacheInvalidationHandler) EventType() string {
	return "*" // 支持所有事件类型
}

// Priority 处理器优先级
func (h *CacheInvalidationHandler) Priority() int {
	return h.priority
}

// CalculationEventHandler 计算事件处理器
type CalculationEventHandler struct {
	calculationOrchestrator *CalculationOrchestrator
	priority                int
}

// NewCalculationEventHandler 创建计算事件处理器
func NewCalculationEventHandler(calculationOrchestrator *CalculationOrchestrator) *CalculationEventHandler {
	return &CalculationEventHandler{
		calculationOrchestrator: calculationOrchestrator,
		priority:                2, // 中等优先级
	}
}

// Handle 处理计算事件
func (h *CalculationEventHandler) Handle(ctx context.Context, event events.DomainEvent) error {
	eventData := event.Data()

	// 根据事件类型触发相应的计算
	switch event.EventType() {
	case events.EventTypeRecordCreated, events.EventTypeRecordUpdated:
		// 触发记录字段计算
		if recordID, ok := eventData["record_id"].(string); ok {
			if tableID, ok := eventData["table_id"].(string); ok {
				// 这里需要根据实际的Record实体来创建记录对象
				// 暂时记录日志
				logger.Info("triggering calculation for record",
					logger.String("record_id", recordID),
					logger.String("table_id", tableID))
			}
		}

	case events.EventTypeFieldCreated, events.EventTypeFieldUpdated:
		// 触发字段依赖的计算
		if fieldID, ok := eventData["field_id"].(string); ok {
			if tableID, ok := eventData["table_id"].(string); ok {
				logger.Info("triggering calculation for field change",
					logger.String("field_id", fieldID),
					logger.String("table_id", tableID))
			}
		}

	default:
		logger.Debug("no calculation strategy for event type",
			logger.String("event_type", event.EventType()))
	}

	return nil
}

// EventType 处理器支持的事件类型
func (h *CalculationEventHandler) EventType() string {
	return "*" // 支持所有事件类型
}

// Priority 处理器优先级
func (h *CalculationEventHandler) Priority() int {
	return h.priority
}

// AuditEventHandler 审计事件处理器
type AuditEventHandler struct {
	// 这里可以注入审计服务
	priority int
}

// NewAuditEventHandler 创建审计事件处理器
func NewAuditEventHandler() *AuditEventHandler {
	return &AuditEventHandler{
		priority: 4, // 最低优先级
	}
}

// Handle 处理审计事件
func (h *AuditEventHandler) Handle(ctx context.Context, event events.DomainEvent) error {
	// 记录审计日志
	logger.Info("audit event",
		logger.String("event_type", event.EventType()),
		logger.String("aggregate_id", event.AggregateID()),
		logger.String("aggregate_type", event.AggregateType()),
		logger.String("event_id", event.EventID()),
		logger.String("occurred_at", event.OccurredAt().Format("2006-01-02 15:04:05")),
		logger.Any("data", event.Data()),
		logger.Any("metadata", event.Metadata()))

	// 这里可以实现更复杂的审计逻辑，比如：
	// 1. 存储到专门的审计表
	// 2. 发送到外部审计系统
	// 3. 生成审计报告

	return nil
}

// EventType 处理器支持的事件类型
func (h *AuditEventHandler) EventType() string {
	return "*" // 支持所有事件类型
}

// Priority 处理器优先级
func (h *AuditEventHandler) Priority() int {
	return h.priority
}

// EventHandlerRegistry 事件处理器注册表
type EventHandlerRegistry struct {
	handlers map[string][]events.EventHandler
}

// NewEventHandlerRegistry 创建事件处理器注册表
func NewEventHandlerRegistry() *EventHandlerRegistry {
	return &EventHandlerRegistry{
		handlers: make(map[string][]events.EventHandler),
	}
}

// RegisterHandler 注册事件处理器
func (r *EventHandlerRegistry) RegisterHandler(eventType string, handler events.EventHandler) {
	r.handlers[eventType] = append(r.handlers[eventType], handler)
}

// GetHandlers 获取事件处理器
func (r *EventHandlerRegistry) GetHandlers(eventType string) []events.EventHandler {
	return r.handlers[eventType]
}

// GetAllHandlers 获取所有事件处理器
func (r *EventHandlerRegistry) GetAllHandlers() map[string][]events.EventHandler {
	return r.handlers
}

// RegisterDefaultHandlers 注册默认的事件处理器
func RegisterDefaultHandlers(
	registry *EventHandlerRegistry,
	cacheService *CacheService,
	calculationOrchestrator *CalculationOrchestrator,
) {
	// 注册缓存失效处理器
	cacheHandler := NewCacheInvalidationHandler(cacheService)
	registry.RegisterHandler("*", cacheHandler)

	// 注册计算事件处理器
	calcHandler := NewCalculationEventHandler(calculationOrchestrator)
	registry.RegisterHandler("*", calcHandler)

	// 注册审计事件处理器
	auditHandler := NewAuditEventHandler()
	registry.RegisterHandler("*", auditHandler)

	logger.Info("default event handlers registered",
		logger.Int("handler_count", 4))
}
