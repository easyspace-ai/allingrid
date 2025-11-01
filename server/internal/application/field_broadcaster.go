package application

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/events"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// FieldBroadcasterImpl 字段广播器实现
type FieldBroadcasterImpl struct{ businessEvents events.BusinessEventPublisher }

// NewFieldBroadcaster 创建字段广播器
func NewFieldBroadcaster(businessEvents events.BusinessEventPublisher) *FieldBroadcasterImpl {
	return &FieldBroadcasterImpl{businessEvents: businessEvents}
}

// BroadcastFieldCreate 广播字段创建事件
func (b *FieldBroadcasterImpl) BroadcastFieldCreate(tableID string, field *entity.Field) {
	// 旧 WebSocket 广播已移除；改用业务事件系统

	// 2. 发布到统一业务事件系统（支持SSE、WebSocket、Yjs）
	if b.businessEvents != nil {
		ctx := context.Background()
		err := b.businessEvents.PublishFieldEvent(
			ctx,
			events.BusinessEventTypeFieldCreate,
			tableID,
			field.ID().String(),
			field,
			"", // 字段创建通常没有特定用户ID
		)

		if err != nil {
			logger.Error("发布字段创建业务事件失败",
				logger.String("table_id", tableID),
				logger.String("field_id", field.ID().String()),
				logger.ErrorField(err))
		} else {
			logger.Info("字段创建业务事件已发布",
				logger.String("table_id", tableID),
				logger.String("field_id", field.ID().String()))
		}
	}
}

// BroadcastFieldUpdate 广播字段更新事件
func (b *FieldBroadcasterImpl) BroadcastFieldUpdate(tableID string, field *entity.Field) {
	// 旧 WebSocket 广播已移除；改用业务事件系统

	// 2. 发布到统一业务事件系统（支持SSE、WebSocket、Yjs）
	if b.businessEvents != nil {
		ctx := context.Background()
		err := b.businessEvents.PublishFieldEvent(
			ctx,
			events.BusinessEventTypeFieldUpdate,
			tableID,
			field.ID().String(),
			field,
			"", // 字段更新通常没有特定用户ID
		)

		if err != nil {
			logger.Error("发布字段更新业务事件失败",
				logger.String("table_id", tableID),
				logger.String("field_id", field.ID().String()),
				logger.ErrorField(err))
		} else {
			logger.Info("字段更新业务事件已发布",
				logger.String("table_id", tableID),
				logger.String("field_id", field.ID().String()))
		}
	}
}

// BroadcastFieldDelete 广播字段删除事件
func (b *FieldBroadcasterImpl) BroadcastFieldDelete(tableID, fieldID string) {
	// 旧 WebSocket 广播已移除；改用业务事件系统

	// 2. 发布到统一业务事件系统（支持SSE、WebSocket、Yjs）
	if b.businessEvents != nil {
		ctx := context.Background()
		err := b.businessEvents.PublishFieldEvent(
			ctx,
			events.BusinessEventTypeFieldDelete,
			tableID,
			fieldID,
			map[string]interface{}{
				"field_id": fieldID,
			},
			"", // 字段删除通常没有特定用户ID
		)

		if err != nil {
			logger.Error("发布字段删除业务事件失败",
				logger.String("table_id", tableID),
				logger.String("field_id", fieldID),
				logger.ErrorField(err))
		} else {
			logger.Info("字段删除业务事件已发布",
				logger.String("table_id", tableID),
				logger.String("field_id", fieldID))
		}
	}
}
