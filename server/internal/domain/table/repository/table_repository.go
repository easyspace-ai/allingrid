package repository

import (
	"context"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	tableEntity "github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"
)

// TableRepository 表格仓储接口
type TableRepository interface {
	// Save 保存表格
	Save(ctx context.Context, table *tableEntity.Table) error

	// GetByID 根据ID获取表格
	GetByID(ctx context.Context, id string) (*tableEntity.Table, error)

	// GetByBaseID 获取Base下的所有表格
	GetByBaseID(ctx context.Context, baseID string) ([]*tableEntity.Table, error)

	// Delete 删除表格
	Delete(ctx context.Context, id string) error

	// Update 更新表格
	Update(ctx context.Context, table *tableEntity.Table) error

	// Exists 检查表格是否存在
	Exists(ctx context.Context, id string) (bool, error)

	// ExistsByNameInBase 检查Base下是否存在指定名称的表格
	ExistsByNameInBase(ctx context.Context, baseID string, name valueobject.TableName, excludeID *string) (bool, error)

	// Count 统计表格数量
	Count(ctx context.Context, baseID string) (int64, error)
}

// FieldRepository 字段仓储接口（用于Table领域服务）
type FieldRepository interface {
	// GetByID 根据ID获取字段
	GetByID(ctx context.Context, id string) (*fieldEntity.Field, error)

	// GetByTableID 获取表格的所有字段
	GetByTableID(ctx context.Context, tableID string) ([]*fieldEntity.Field, error)

	// Save 保存字段
	Save(ctx context.Context, field *fieldEntity.Field) error

	// Update 更新字段
	Update(ctx context.Context, field *fieldEntity.Field) error

	// Delete 删除字段
	Delete(ctx context.Context, id string) error

	// List 列表查询字段
	List(ctx context.Context, filter *ListFieldFilter) ([]*fieldEntity.Field, error)

	// Count 统计字段数量
	Count(ctx context.Context, filter *ListFieldFilter) (int64, error)
}

// ListFieldFilter 字段列表过滤器
type ListFieldFilter struct {
	TableID    string
	FieldTypes []string
	IsVirtual  *bool
	Limit      int
	Offset     int
}

// RecordRepository 记录仓储接口（用于Table领域服务）
type RecordRepository interface {
	// GetByID 根据ID获取记录
	GetByID(ctx context.Context, tableID, recordID string) (RecordData, error)

	// GetMany 批量获取记录
	GetMany(ctx context.Context, tableID string, recordIDs []string) ([]RecordData, error)

	// GetLinkedRecords 获取关联记录
	GetLinkedRecords(ctx context.Context, tableID, recordID, linkFieldID string) ([]RecordData, error)

	// FindReferencingRecords 查找引用指定记录的所有记录
	FindReferencingRecords(ctx context.Context, targetTableID, targetRecordID string) ([]RecordReference, error)

	// Save 保存记录
	Save(ctx context.Context, tableID string, record RecordData) error

	// Update 更新记录
	Update(ctx context.Context, tableID string, recordID string, data map[string]interface{}) error

	// Delete 删除记录
	Delete(ctx context.Context, tableID, recordID string) error
}

// RecordData 记录数据接口
type RecordData interface {
	GetID() string
	GetTableID() string
	GetData() map[string]interface{}
	SetData(data map[string]interface{})
}

// RecordReference 记录引用
type RecordReference struct {
	TableID   string
	RecordID  string
	FieldID   string
	FieldName string
	FieldType string
}
