package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
)

// RecordRepository 记录仓储接口
type RecordRepository interface {
	// Save 保存记录（新增或更新）
	Save(ctx context.Context, record *entity.Record) error

	// FindByID 根据ID查找记录
	// ⚠️ 已废弃：请使用 FindByIDs，因为需要 tableID
	FindByID(ctx context.Context, id valueobject.RecordID) (*entity.Record, error)

	// FindByIDs 根据ID列表查找记录（需要提供 tableID）
	// ✅ 对齐 Teable 架构：所有记录操作都需要 tableID
	FindByIDs(ctx context.Context, tableID string, ids []valueobject.RecordID) ([]*entity.Record, error)

	// FindByTableAndID 根据表ID和记录ID查找记录
	// ✅ 对齐 Teable 架构：所有记录操作都需要 tableID
	FindByTableAndID(ctx context.Context, tableID string, id valueobject.RecordID) (*entity.Record, error)

	// FindByTableID 查找表的所有记录
	FindByTableID(ctx context.Context, tableID string) ([]*entity.Record, error)

	// Delete 删除记录（物理删除）
	Delete(ctx context.Context, id valueobject.RecordID) error

	// DeleteByTableAndID 根据表ID和记录ID删除记录
	// ✅ 对齐 Teable 架构：所有记录操作都需要 tableID
	DeleteByTableAndID(ctx context.Context, tableID string, id valueobject.RecordID) error

	// Exists 检查记录是否存在
	Exists(ctx context.Context, id valueobject.RecordID) (bool, error)

	// List 列出记录（支持过滤和分页）
	List(ctx context.Context, filter RecordFilter) ([]*entity.Record, int64, error)

	// BatchSave 批量保存记录
	BatchSave(ctx context.Context, records []*entity.Record) error

	// BatchDelete 批量删除记录
	BatchDelete(ctx context.Context, ids []valueobject.RecordID) error

	// CountByTableID 统计表的记录数
	CountByTableID(ctx context.Context, tableID string) (int64, error)

	// FindWithVersion 根据ID和版本查找记录（用于乐观锁）
	FindWithVersion(ctx context.Context, tableID string, id valueobject.RecordID, version valueobject.RecordVersion) (*entity.Record, error)

	// NextID 生成下一个记录ID
	NextID() valueobject.RecordID
}

// RecordFilter 记录过滤器
type RecordFilter struct {
	TableID      *string
	CreatedBy    *string
	UpdatedBy    *string
	IsDeleted    *bool
	FieldFilters map[string]interface{} // 字段过滤条件
	OrderBy      string                 // created_at, updated_at, field_name
	OrderDir     string                 // asc, desc
	Limit        int
	Offset       int
}
