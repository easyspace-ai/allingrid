package sharedb

import (
	"sync"

	"github.com/easyspace-ai/luckdb/server/internal/events"
	"go.uber.org/zap"
)

// TransactionHook ShareDB 事务钩子
type TransactionHook struct {
	eventManager *events.BusinessEventManager
	logger       *zap.Logger
	mu           sync.RWMutex
}

// NewTransactionHook 创建事务钩子
func NewTransactionHook(eventManager *events.BusinessEventManager, logger *zap.Logger) *TransactionHook {
	return &TransactionHook{
		eventManager: eventManager,
		logger:       logger,
	}
}

// AfterCommit 在事务提交后执行
func (h *TransactionHook) AfterCommit(collection, docID string, op *Operation) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 将 ShareDB 操作转换为业务事件
	businessEvents := h.convertOpToEvents(collection, docID, op)

	// 发布业务事件
	for _, event := range businessEvents {
		if err := h.eventManager.Publish(event); err != nil {
			h.logger.Error("Failed to publish business event",
				zap.Error(err),
				zap.String("event_type", string(event.Type)),
				zap.String("collection", collection),
				zap.String("doc_id", docID))
		}
	}

	h.logger.Debug("ShareDB operation converted to business events",
		zap.String("collection", collection),
		zap.String("doc_id", docID),
		zap.String("op_type", string(op.Type)),
		zap.Int("events_count", len(businessEvents)))
}

// convertOpToEvents 将 ShareDB 操作转换为业务事件
func (h *TransactionHook) convertOpToEvents(collection, docID string, op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	// 解析集合类型
	collectionInfo := ParseCollection(collection)

	switch collectionInfo.Type {
	case DocumentTypeRecord:
		businessEvents = h.convertRecordOpToEvents(collectionInfo.TableID, docID, op)
	case DocumentTypeField:
		businessEvents = h.convertFieldOpToEvents(collectionInfo.TableID, docID, op)
	case DocumentTypeView:
		businessEvents = h.convertViewOpToEvents(collectionInfo.TableID, docID, op)
	case DocumentTypeTable:
		businessEvents = h.convertTableOpToEvents(collectionInfo.TableID, docID, op)
	}

	return businessEvents
}

// convertRecordOpToEvents 转换记录操作
func (h *TransactionHook) convertRecordOpToEvents(tableID, recordID string, op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	switch op.Type {
	case OpTypeCreate:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeRecordCreate,
			TableID:   tableID,
			RecordID:  recordID,
			Data:      op.Create.Data,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeEdit:
		// 解析操作中的字段变更
		for _, otOp := range op.Op {
			if path, ok := otOp["p"].([]interface{}); ok && len(path) >= 2 {
				if path[0] == "fields" {
					fieldID := path[1].(string)
					event := &events.BusinessEvent{
						Type:      events.BusinessEventTypeRecordUpdate,
						TableID:   tableID,
						RecordID:  recordID,
						FieldID:   fieldID,
						Data:      otOp,
						Timestamp: op.Version,
					}
					businessEvents = append(businessEvents, event)
				}
			}
		}

	case OpTypeDelete:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeRecordDelete,
			TableID:   tableID,
			RecordID:  recordID,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)
	}

	return businessEvents
}

// convertFieldOpToEvents 转换字段操作
func (h *TransactionHook) convertFieldOpToEvents(tableID, fieldID string, op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	switch op.Type {
	case OpTypeCreate:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeFieldCreate,
			TableID:   tableID,
			FieldID:   fieldID,
			Data:      op.Create.Data,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeEdit:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeFieldUpdate,
			TableID:   tableID,
			FieldID:   fieldID,
			Data:      op.Op,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeDelete:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeFieldDelete,
			TableID:   tableID,
			FieldID:   fieldID,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)
	}

	return businessEvents
}

// convertViewOpToEvents 转换视图操作
func (h *TransactionHook) convertViewOpToEvents(tableID, viewID string, op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	switch op.Type {
	case OpTypeCreate:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeViewCreate,
			TableID:   tableID,
			Data:      op.Create.Data,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeEdit:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeViewUpdate,
			TableID:   tableID,
			Data:      op.Op,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeDelete:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeViewDelete,
			TableID:   tableID,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)
	}

	return businessEvents
}

// convertTableOpToEvents 转换表操作
func (h *TransactionHook) convertTableOpToEvents(tableID, docID string, op *Operation) []*events.BusinessEvent {
	var businessEvents []*events.BusinessEvent

	switch op.Type {
	case OpTypeCreate:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeTableCreate,
			TableID:   tableID,
			Data:      op.Create.Data,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeEdit:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeTableUpdate,
			TableID:   tableID,
			Data:      op.Op,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)

	case OpTypeDelete:
		event := &events.BusinessEvent{
			Type:      events.BusinessEventTypeTableDelete,
			TableID:   tableID,
			Timestamp: op.Version,
		}
		businessEvents = append(businessEvents, event)
	}

	return businessEvents
}
