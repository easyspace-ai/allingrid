package sharedb

import (
	"context"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TableAdapter 表适配器
type TableAdapter struct {
	db     *gorm.DB
	logger *zap.Logger
}

// NewTableAdapter 创建表适配器
func NewTableAdapter(db *gorm.DB, logger *zap.Logger) *TableAdapter {
	return &TableAdapter{
		db:     db,
		logger: logger,
	}
}

// GetSnapshot 获取表快照
func (a *TableAdapter) GetSnapshot(ctx context.Context, tableID, docID string, projection map[string]bool) (*Snapshot, error) {
	// 这里应该查询数据库获取表数据
	// 暂时返回一个模拟的快照
	return &Snapshot{
		ID:      docID,
		Type:    "json0",
		Version: 1,
		Data: map[string]interface{}{
			"id":          docID,
			"name":        "Table " + docID,
			"description": "A sample table",
		},
	}, nil
}

// GetSnapshotBulk 批量获取表快照
func (a *TableAdapter) GetSnapshotBulk(ctx context.Context, tableID string, ids []string, projection map[string]bool) ([]*Snapshot, error) {
	snapshots := make([]*Snapshot, 0, len(ids))

	for _, id := range ids {
		snapshot, err := a.GetSnapshot(ctx, tableID, id, projection)
		if err != nil {
			a.logger.Error("Failed to get table snapshot",
				zap.Error(err),
				zap.String("table_id", tableID),
				zap.String("doc_id", id))
			continue
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

// GetDocIDsByQuery 根据查询获取文档ID列表
func (a *TableAdapter) GetDocIDsByQuery(ctx context.Context, tableID string, query interface{}) ([]string, error) {
	// 这里应该根据查询条件从数据库获取表ID列表
	// 暂时返回一个模拟的ID列表
	return []string{"table_1", "table_2", "table_3"}, nil
}
