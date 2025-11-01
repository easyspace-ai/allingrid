package handlers

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/events"
	recordEvent "github.com/easyspace-ai/luckdb/server/internal/domain/record/event"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// RecordEventHandler 记录事件处理器
// 负责处理记录相关的领域事件
type RecordEventHandler struct {
	// 可以注入需要的服务
	// calculationService *CalculationService
	// cacheService       *CacheService
}

// NewRecordEventHandler 创建记录事件处理器
func NewRecordEventHandler() *RecordEventHandler {
	return &RecordEventHandler{}
}

// Handle 处理事件
func (h *RecordEventHandler) Handle(ctx context.Context, event events.DomainEvent) error {
	logger.Info("处理记录事件",
		logger.String("event_type", event.EventType()),
		logger.String("event_id", event.EventID()),
		logger.String("aggregate_id", event.AggregateID()))

	switch e := event.(type) {
	case *recordEvent.RecordCreated:
		return h.handleRecordCreated(ctx, e)
	case *recordEvent.RecordUpdated:
		return h.handleRecordUpdated(ctx, e)
	case *recordEvent.RecordDeleted:
		return h.handleRecordDeleted(ctx, e)
	default:
		logger.Warn("未知的记录事件类型",
			logger.String("event_type", event.EventType()))
		return nil
	}
}

// EventType 处理器支持的事件类型
func (h *RecordEventHandler) EventType() string {
	return "record.*" // 支持所有记录相关事件
}

// Priority 处理器优先级
func (h *RecordEventHandler) Priority() int {
	return 10 // 中等优先级
}

// handleRecordCreated 处理记录创建事件
func (h *RecordEventHandler) handleRecordCreated(ctx context.Context, event *recordEvent.RecordCreated) error {
	logger.Info("处理记录创建事件",
		logger.String("record_id", event.RecordID().String()),
		logger.String("table_id", event.TableID()),
		logger.String("created_by", event.CreatedBy()))

	// 1. 触发虚拟字段计算
	// 这里可以发布计算请求事件
	// calcEvent := calculationEvent.NewCalculationRequested(
	//     event.TableID(),
	//     event.RecordID(),
	//     []string{}, // 计算所有虚拟字段
	//     calculationEvent.NormalPriority,
	//     "system",
	// )
	// return h.eventBus.Publish(ctx, calcEvent)

	// 2. 更新缓存
	// h.cacheService.InvalidateRecord(event.TableID(), event.RecordID().String())

	// 3. 发送 WebSocket 通知
	// h.websocketService.BroadcastRecordCreate(event.TableID(), event.RecordID().String(), fields)

	logger.Info("记录创建事件处理完成",
		logger.String("record_id", event.RecordID().String()))
	return nil
}

// handleRecordUpdated 处理记录更新事件
func (h *RecordEventHandler) handleRecordUpdated(ctx context.Context, event *recordEvent.RecordUpdated) error {
	logger.Info("处理记录更新事件",
		logger.String("record_id", event.RecordID().String()),
		logger.String("table_id", event.TableID()),
		logger.String("updated_by", event.UpdatedBy()),
		logger.Strings("changed_fields", event.ChangedFields()))

	// 1. 触发受影响的虚拟字段重算
	// 这里可以发布计算请求事件
	// calcEvent := calculationEvent.NewCalculationRequested(
	//     event.TableID(),
	//     event.RecordID(),
	//     event.ChangedFields(),
	//     calculationEvent.NormalPriority,
	//     "system",
	// )
	// return h.eventBus.Publish(ctx, calcEvent)

	// 2. 更新缓存
	// h.cacheService.InvalidateRecord(event.TableID(), event.RecordID().String())

	// 3. 发送 WebSocket 通知
	// h.websocketService.BroadcastRecordUpdate(event.TableID(), event.RecordID().String(), fields)

	logger.Info("记录更新事件处理完成",
		logger.String("record_id", event.RecordID().String()))
	return nil
}

// handleRecordDeleted 处理记录删除事件
func (h *RecordEventHandler) handleRecordDeleted(ctx context.Context, event *recordEvent.RecordDeleted) error {
	logger.Info("处理记录删除事件",
		logger.String("record_id", event.RecordID().String()),
		logger.String("table_id", event.TableID()),
		logger.String("deleted_by", event.DeletedBy()))

	// 1. 清理缓存
	// h.cacheService.InvalidateRecord(event.TableID(), event.RecordID().String())

	// 2. 发送 WebSocket 通知
	// h.websocketService.BroadcastRecordDelete(event.TableID(), event.RecordID().String())

	logger.Info("记录删除事件处理完成",
		logger.String("record_id", event.RecordID().String()))
	return nil
}

// CalculationEventHandler 计算事件处理器
// 负责处理计算相关的领域事件
type CalculationEventHandler struct {
	// 可以注入计算服务
	// calculationService *CalculationService
}

// NewCalculationEventHandler 创建计算事件处理器
func NewCalculationEventHandler() *CalculationEventHandler {
	return &CalculationEventHandler{}
}

// Handle 处理事件
func (h *CalculationEventHandler) Handle(ctx context.Context, event events.DomainEvent) error {
	logger.Info("处理计算事件",
		logger.String("event_type", event.EventType()),
		logger.String("event_id", event.EventID()))

	// 这里可以根据具体的事件类型进行处理
	// 例如：处理计算请求、计算完成、计算失败等事件

	return nil
}

// EventType 处理器支持的事件类型
func (h *CalculationEventHandler) EventType() string {
	return "calculation.*" // 支持所有计算相关事件
}

// Priority 处理器优先级
func (h *CalculationEventHandler) Priority() int {
	return 5 // 高优先级，计算事件需要优先处理
}
