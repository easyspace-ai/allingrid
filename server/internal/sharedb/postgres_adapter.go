package sharedb

import (
	"context"
	"fmt"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// PostgresAdapter PostgreSQL 适配器
type PostgresAdapter struct {
	db     *gorm.DB
	logger *zap.Logger

	// 各类型文档适配器
	recordAdapter *RecordAdapter
	fieldAdapter  *FieldAdapter
	viewAdapter   *ViewAdapter
	tableAdapter  *TableAdapter
}

// NewPostgresAdapter 创建 PostgreSQL 适配器
func NewPostgresAdapter(db *gorm.DB, logger *zap.Logger, recordRepo repository.RecordRepository) Adapter {
	return &PostgresAdapter{
		db:            db,
		logger:        logger,
		recordAdapter: NewRecordAdapter(db, logger, recordRepo),
		fieldAdapter:  NewFieldAdapter(db, logger),
		viewAdapter:   NewViewAdapter(db, logger),
		tableAdapter:  NewTableAdapter(db, logger),
	}
}

// Query 查询文档
func (a *PostgresAdapter) Query(ctx context.Context, collection string, query interface{}, projection map[string]bool) ([]string, error) {
	collectionInfo := ParseCollection(collection)

	switch collectionInfo.Type {
	case DocumentTypeRecord:
		return a.recordAdapter.GetDocIDsByQuery(ctx, collectionInfo.TableID, query)
	case DocumentTypeField:
		return a.fieldAdapter.GetDocIDsByQuery(ctx, collectionInfo.TableID, query)
	case DocumentTypeView:
		return a.viewAdapter.GetDocIDsByQuery(ctx, collectionInfo.TableID, query)
	case DocumentTypeTable:
		return a.tableAdapter.GetDocIDsByQuery(ctx, collectionInfo.TableID, query)
	default:
		return nil, fmt.Errorf("unknown document type: %s", collectionInfo.Type)
	}
}

// GetSnapshot 获取文档快照
func (a *PostgresAdapter) GetSnapshot(ctx context.Context, collection string, id string, projection map[string]bool) (*Snapshot, error) {
	collectionInfo := ParseCollection(collection)

	switch collectionInfo.Type {
	case DocumentTypeRecord:
		return a.recordAdapter.GetSnapshot(ctx, collectionInfo.TableID, id, projection)
	case DocumentTypeField:
		return a.fieldAdapter.GetSnapshot(ctx, collectionInfo.TableID, id, projection)
	case DocumentTypeView:
		return a.viewAdapter.GetSnapshot(ctx, collectionInfo.TableID, id, projection)
	case DocumentTypeTable:
		return a.tableAdapter.GetSnapshot(ctx, collectionInfo.TableID, id, projection)
	default:
		return nil, fmt.Errorf("unknown document type: %s", collectionInfo.Type)
	}
}

// GetSnapshotBulk 批量获取文档快照
func (a *PostgresAdapter) GetSnapshotBulk(ctx context.Context, collection string, ids []string, projection map[string]bool) (map[string]*Snapshot, error) {
	collectionInfo := ParseCollection(collection)

	switch collectionInfo.Type {
	case DocumentTypeRecord:
		snapshots, err := a.recordAdapter.GetSnapshotBulk(ctx, collectionInfo.TableID, ids, projection)
		if err != nil {
			return nil, err
		}
		return a.snapshotsToMap(snapshots), nil
	case DocumentTypeField:
		snapshots, err := a.fieldAdapter.GetSnapshotBulk(ctx, collectionInfo.TableID, ids, projection)
		if err != nil {
			return nil, err
		}
		return a.snapshotsToMap(snapshots), nil
	case DocumentTypeView:
		snapshots, err := a.viewAdapter.GetSnapshotBulk(ctx, collectionInfo.TableID, ids, projection)
		if err != nil {
			return nil, err
		}
		return a.snapshotsToMap(snapshots), nil
	case DocumentTypeTable:
		snapshots, err := a.tableAdapter.GetSnapshotBulk(ctx, collectionInfo.TableID, ids, projection)
		if err != nil {
			return nil, err
		}
		return a.snapshotsToMap(snapshots), nil
	default:
		return nil, fmt.Errorf("unknown document type: %s", collectionInfo.Type)
	}
}

// GetOps 获取操作历史
func (a *PostgresAdapter) GetOps(ctx context.Context, collection string, id string, from, to int64) ([]*opbuilder.Operation, error) {
	// 这里应该实现操作历史查询
	// 暂时返回空操作列表
	return []*opbuilder.Operation{}, nil
}

// SkipPoll 跳过轮询优化
func (a *PostgresAdapter) SkipPoll(ctx context.Context, collection string, id string, op *opbuilder.Operation, query interface{}) bool {
	// 如果操作没有实际内容，可以跳过轮询
	if op.Path == nil {
		return true
	}
	return false
}

// Close 关闭适配器
func (a *PostgresAdapter) Close() error {
	// 关闭数据库连接
	if a.db != nil {
		sqlDB, err := a.db.DB()
		if err == nil {
			return sqlDB.Close()
		}
	}
	return nil
}

// snapshotsToMap 将快照列表转换为映射
func (a *PostgresAdapter) snapshotsToMap(snapshots []*Snapshot) map[string]*Snapshot {
	result := make(map[string]*Snapshot, len(snapshots))
	for _, snapshot := range snapshots {
		result[snapshot.ID] = snapshot
	}
	return result
}

// ParseCollection 解析集合名称
func ParseCollection(collection string) *CollectionInfo {
	parts := strings.Split(collection, "_")
	if len(parts) < 2 {
		return &CollectionInfo{
			Type:       DocumentTypeRecord,
			TableID:    "default",
			DocumentID: collection,
		}
	}

	// 处理 "rec_" 前缀的集合名称
	// collection 格式：rec_tbl_xxx
	var docType DocumentType
	if parts[0] == "rec" {
		docType = DocumentTypeRecord
	} else if parts[0] == "field" {
		docType = DocumentTypeField
	} else if parts[0] == "view" {
		docType = DocumentTypeView
	} else if parts[0] == "table" {
		docType = DocumentTypeTable
	} else {
		// 默认作为 record 处理
		docType = DocumentTypeRecord
	}

	// 对于 collection 格式：rec_tbl_xxx，tableID 应该是 tbl_xxx（parts[1] 及其后面的部分）
	var tableID string
	if len(parts) >= 3 {
		// 合并 parts[1] 及其后面的所有部分，例如 "tbl_WBPaMKaMZ7xq0hSKpXPRh"
		tableID = strings.Join(parts[1:], "_")
	} else {
		tableID = parts[1]
	}

	return &CollectionInfo{
		Type:       docType,
		TableID:    tableID,
		DocumentID: collection,
	}
}
