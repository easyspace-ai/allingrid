package sharedb

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/events"
	"go.uber.org/zap"
)

// OpsToEventsConverter ShareDB 操作到事件转换器
type OpsToEventsConverter struct {
	eventManager *events.BusinessEventManager
	logger       *zap.Logger
}

// NewOpsToEventsConverter 创建操作到事件转换器
func NewOpsToEventsConverter(eventManager *events.BusinessEventManager, logger *zap.Logger) *OpsToEventsConverter {
	return &OpsToEventsConverter{
		eventManager: eventManager,
		logger:       logger,
	}
}

// ConvertOpsToEvents 将 ShareDB 操作转换为业务事件
func (c *OpsToEventsConverter) ConvertOpsToEvents(ops []*Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	for _, op := range ops {
		events := c.convertSingleOpToEvents(op)
		businessEvents = append(businessEvents, events...)
	}

	c.logger.Debug("Converted ShareDB operations to business events",
		zap.Int("ops_count", len(ops)),
		zap.Int("events_count", len(businessEvents)))

	return businessEvents
}

// convertSingleOpToEvents 转换单个操作
func (c *OpsToEventsConverter) convertSingleOpToEvents(op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	// 解析集合信息
	collectionInfo := ParseCollection(op.Collection)

	// 根据操作类型和文档类型创建事件
	switch op.Type {
	case OpTypeCreate:
		event := c.createCreateEvent(collectionInfo, op)
		if event != nil {
			businessEvents = append(businessEvents, event)
		}

	case OpTypeEdit:
		events := c.createEditEvents(collectionInfo, op)
		businessEvents = append(businessEvents, events...)

	case OpTypeDelete:
		event := c.createDeleteEvent(collectionInfo, op)
		if event != nil {
			businessEvents = append(businessEvents, event)
		}
	}

	return businessEvents
}

// createCreateEvent 创建创建事件
func (c *OpsToEventsConverter) createCreateEvent(collectionInfo *CollectionInfo, op *Operation) *events.BusinessEvent {
	eventType := c.getEventType(collectionInfo.Type, "created")

	event := &events.BusinessEvent{
		Type:      events.BusinessEventType(eventType),
		TableID:   collectionInfo.TableID,
		Timestamp: time.Now().Unix(),
		Data:      op.Create.Data,
	}

	// 设置特定ID
	switch collectionInfo.Type {
	case DocumentTypeRecord:
		event.RecordID = op.DocID
	case DocumentTypeField:
		event.FieldID = op.DocID
	case DocumentTypeView:
		// ViewID 字段不存在，使用 TableID 存储视图ID
		event.TableID = op.DocID
	}

	return event
}

// createEditEvents 创建编辑事件
func (c *OpsToEventsConverter) createEditEvents(collectionInfo *CollectionInfo, op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	// 为每个操作创建事件
	for _, otOp := range op.Op {
		event := c.createEditEventFromOTOp(collectionInfo, op, otOp)
		if event != nil {
			businessEvents = append(businessEvents, event)
		}
	}

	// 如果没有具体的操作，创建一个通用更新事件
	if len(businessEvents) == 0 {
		eventType := c.getEventType(collectionInfo.Type, "updated")
		event := &events.BusinessEvent{
			Type:      events.BusinessEventType(eventType),
			TableID:   collectionInfo.TableID,
			Timestamp: time.Now().Unix(),
			Data:      op.Op,
		}

		// 设置特定ID
		switch collectionInfo.Type {
		case DocumentTypeRecord:
			event.RecordID = op.DocID
		case DocumentTypeField:
			event.FieldID = op.DocID
		case DocumentTypeView:
			// ViewID 字段不存在，使用 TableID 存储视图ID
			event.TableID = op.DocID
		}

		businessEvents = append(businessEvents, event)
	}

	return businessEvents
}

// createEditEventFromOTOp 从 OT 操作创建编辑事件
func (c *OpsToEventsConverter) createEditEventFromOTOp(collectionInfo *CollectionInfo, op *Operation, otOp OTOperation) *events.BusinessEvent {
	// 解析路径
	path, ok := otOp["p"].([]interface{})
	if !ok || len(path) == 0 {
		return nil
	}

	eventType := c.getEventType(collectionInfo.Type, "updated")
	event := &events.BusinessEvent{
		Type:      events.BusinessEventType(eventType),
		TableID:   collectionInfo.TableID,
		Timestamp: time.Now().Unix(),
		Data:      otOp,
	}

	// 设置特定ID
	switch collectionInfo.Type {
	case DocumentTypeRecord:
		event.RecordID = op.DocID
		// 如果是字段更新，设置字段ID
		if len(path) >= 2 && path[0] == "fields" {
			event.FieldID = path[1].(string)
		}
	case DocumentTypeField:
		event.FieldID = op.DocID
	case DocumentTypeView:
		// ViewID 字段不存在，使用 TableID 存储视图ID
		event.TableID = op.DocID
	}

	return event
}

// createDeleteEvent 创建删除事件
func (c *OpsToEventsConverter) createDeleteEvent(collectionInfo *CollectionInfo, op *Operation) *events.BusinessEvent {
	eventType := c.getEventType(collectionInfo.Type, "deleted")

	event := &events.BusinessEvent{
		Type:      events.BusinessEventType(eventType),
		TableID:   collectionInfo.TableID,
		Timestamp: time.Now().Unix(),
	}

	// 设置特定ID
	switch collectionInfo.Type {
	case DocumentTypeRecord:
		event.RecordID = op.DocID
	case DocumentTypeField:
		event.FieldID = op.DocID
	case DocumentTypeView:
		// ViewID 字段不存在，使用 TableID 存储视图ID
		event.TableID = op.DocID
	}

	return event
}

// getEventType 获取事件类型
func (c *OpsToEventsConverter) getEventType(docType DocumentType, action string) string {
	switch docType {
	case DocumentTypeRecord:
		return "record." + action
	case DocumentTypeField:
		return "field." + action
	case DocumentTypeView:
		return "view." + action
	case DocumentTypeTable:
		return "table." + action
	default:
		return "document." + action
	}
}

// PublishEvents 发布事件
func (c *OpsToEventsConverter) PublishEvents(ctx context.Context, events []*events.BusinessEvent) error {
	for _, event := range events {
		if err := c.eventManager.Publish(event); err != nil {
			c.logger.Error("Failed to publish business event",
				zap.Error(err),
				zap.String("event_type", string(event.Type)),
				zap.String("table_id", event.TableID))
			return err
		}
	}

	c.logger.Debug("Published business events",
		zap.Int("events_count", len(events)))

	return nil
}
