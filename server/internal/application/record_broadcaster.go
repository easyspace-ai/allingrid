package application

import (
	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// RecordBroadcasterImpl 记录广播器实现
type RecordBroadcasterImpl struct {
	sharedbService *sharedb.ShareDBService
}

// NewRecordBroadcaster 创建新的记录广播器
func NewRecordBroadcaster(sharedbService *sharedb.ShareDBService) *RecordBroadcasterImpl {
	return &RecordBroadcasterImpl{
		sharedbService: sharedbService,
	}
}

// BroadcastRecordCreate 广播记录创建操作
func (b *RecordBroadcasterImpl) BroadcastRecordCreate(tableID, recordID string, fields map[string]interface{}) {
	logger.Info("🎯 RecordBroadcaster: 开始广播记录创建事件",
		logger.String("table_id", tableID),
		logger.String("record_id", recordID))

	// 旧 WS 广播已移除；记录到日志，依赖业务事件由 RealtimeManager(SSE/YJS) 处理
	logger.Info("Record created (broadcast via business events)",
		logger.String("table_id", tableID),
		logger.String("record_id", recordID))
}

// BroadcastRecordUpdate 广播记录更新操作
func (b *RecordBroadcasterImpl) BroadcastRecordUpdate(tableID, recordID string, fields map[string]interface{}) {
	logger.Info("🎯 RecordBroadcaster: 开始广播记录更新事件",
		logger.String("table_id", tableID),
		logger.String("record_id", recordID))

	// 发送ShareDB操作消息
	if b.sharedbService != nil {
		// 创建ShareDB操作
		op := sharedb.OTOperation{
			"p": []interface{}{"data", fields}, // 设置路径操作
		}

		// 发送ShareDB操作消息
		err := b.sharedbService.BroadcastOperation(tableID, recordID, []sharedb.OTOperation{op})
		if err != nil {
			logger.Error("ShareDB广播失败", logger.String("error", err.Error()))
		} else {
			logger.Info("✅ ShareDB操作已广播",
				logger.String("table_id", tableID),
				logger.String("record_id", recordID))
		}
	}

	logger.Info("Record updated (broadcast via business events)",
		logger.String("table_id", tableID),
		logger.String("record_id", recordID))
}

// BroadcastRecordDelete 广播记录删除操作
func (b *RecordBroadcasterImpl) BroadcastRecordDelete(tableID, recordID string) {
	logger.Info("Record deleted (broadcast via business events)",
		logger.String("table_id", tableID),
		logger.String("record_id", recordID))
}
