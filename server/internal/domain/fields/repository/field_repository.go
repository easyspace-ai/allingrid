package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// FieldRepository 字段仓储接口
// 定义在领域层，实现在基础设施层
type FieldRepository interface {
	// Save 保存字段（新增或更新）
	Save(ctx context.Context, field *entity.Field) error

	// FindByID 根据ID查找字段
	FindByID(ctx context.Context, id valueobject.FieldID) (*entity.Field, error)

	// FindByTableID 查找表的所有字段
	FindByTableID(ctx context.Context, tableID string) ([]*entity.Field, error)

	// FindByName 根据表ID和字段名查找字段
	FindByName(ctx context.Context, tableID string, name valueobject.FieldName) (*entity.Field, error)

	// Delete 删除字段（物理删除）
	Delete(ctx context.Context, id valueobject.FieldID) error

	// Exists 检查字段是否存在
	Exists(ctx context.Context, id valueobject.FieldID) (bool, error)

	// ExistsByName 检查表中是否存在同名字段
	ExistsByName(ctx context.Context, tableID string, name valueobject.FieldName, excludeID *valueobject.FieldID) (bool, error)

	// List 列出字段（支持过滤和分页）
	List(ctx context.Context, filter FieldFilter) ([]*entity.Field, int64, error)

	// BatchSave 批量保存字段
	BatchSave(ctx context.Context, fields []*entity.Field) error

	// BatchDelete 批量删除字段
	BatchDelete(ctx context.Context, ids []valueobject.FieldID) error

	// GetVirtualFields 获取表的所有虚拟字段
	GetVirtualFields(ctx context.Context, tableID string) ([]*entity.Field, error)

	// GetComputedFields 获取表的所有计算字段
	GetComputedFields(ctx context.Context, tableID string) ([]*entity.Field, error)

	// GetFieldsByType 根据字段类型查找字段
	GetFieldsByType(ctx context.Context, tableID string, fieldType valueobject.FieldType) ([]*entity.Field, error)

	// UpdateOrder 更新字段排序
	UpdateOrder(ctx context.Context, fieldID valueobject.FieldID, order float64) error

	// GetMaxOrder 获取表中字段的最大order值（用于新字段排序）
	GetMaxOrder(ctx context.Context, tableID string) (float64, error)

	// NextID 生成下一个字段ID
	NextID() valueobject.FieldID
}

// FieldFilter 字段过滤器
type FieldFilter struct {
	TableID    *string
	FieldType  *valueobject.FieldType
	Name       *string
	IsVirtual  *bool
	IsComputed *bool
	IsDeleted  *bool
	CreatedBy  *string
	OrderBy    string // name, created_at, updated_at, order
	OrderDir   string // asc, desc
	Limit      int
	Offset     int
}
