package sharedb

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// RecordAdapter 记录适配器
type RecordAdapter struct {
	db         *gorm.DB
	logger     *zap.Logger
	recordRepo repository.RecordRepository
}

// NewRecordAdapter 创建记录适配器
func NewRecordAdapter(db *gorm.DB, logger *zap.Logger, recordRepo repository.RecordRepository) *RecordAdapter {
	return &RecordAdapter{
		db:         db,
		logger:     logger,
		recordRepo: recordRepo,
	}
}

// GetSnapshot 获取记录快照
func (a *RecordAdapter) GetSnapshot(ctx context.Context, tableID, recordID string, projection map[string]bool) (*Snapshot, error) {
	a.logger.Info("📸 GetSnapshot called",
		zap.String("table_id", tableID),
		zap.String("record_id", recordID))

	// 查询数据库获取记录数据
	record, err := a.recordRepo.FindByTableAndID(ctx, tableID, valueobject.NewRecordID(recordID))
	if err != nil {
		a.logger.Error("❌ GetByID failed",
			zap.String("table_id", tableID),
			zap.String("record_id", recordID),
			zap.Error(err))
		return nil, fmt.Errorf("failed to get record: %w", err)
	}

	if record == nil {
		a.logger.Error("❌ Record not found",
			zap.String("table_id", tableID),
			zap.String("record_id", recordID))
		return nil, fmt.Errorf("record not found: %s", recordID)
	}

	// 构建快照数据
	// 客户端期望的数据格式：{ "data": { "fieldId": "value" } }
	// 这与客户端操作路径 ["data", fieldId] 保持一致
	recordDataMap := record.Data().ToMap()
	snapshotData := map[string]interface{}{
		"data": recordDataMap, // 直接使用 data 字段，与客户端操作路径一致
	}

	a.logger.Info("✅ GetSnapshot success",
		zap.String("table_id", tableID),
		zap.String("record_id", recordID),
		zap.Int64("version", int64(record.Version().Value())),
		zap.Int("field_count", len(recordDataMap)))

	return &Snapshot{
		ID:      recordID,
		Type:    "json0",
		Version: int64(record.Version().Value()),
		Data:    snapshotData,
	}, nil
}

// GetSnapshotBulk 批量获取记录快照
func (a *RecordAdapter) GetSnapshotBulk(ctx context.Context, tableID string, ids []string, projection map[string]bool) ([]*Snapshot, error) {
	snapshots := make([]*Snapshot, 0, len(ids))

	for _, id := range ids {
		snapshot, err := a.GetSnapshot(ctx, tableID, id, projection)
		if err != nil {
			a.logger.Error("Failed to get record snapshot",
				zap.Error(err),
				zap.String("table_id", tableID),
				zap.String("record_id", id))
			continue
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

// GetDocIDsByQuery 根据查询获取文档ID列表
func (a *RecordAdapter) GetDocIDsByQuery(ctx context.Context, tableID string, query interface{}) ([]string, error) {
	// 查询数据库获取记录ID列表
	records, err := a.recordRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get records by table ID: %w", err)
	}

	// 提取记录ID
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID().String())
	}

	return ids, nil
}
