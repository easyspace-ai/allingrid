package dto

import (
	"time"

	tableEntity "github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
)

// ViewConfigDTO 视图配置DTO
type ViewConfigDTO struct {
	Name        string                   `json:"name" binding:"required"`
	Type        string                   `json:"type" binding:"required"`
	Description string                   `json:"description,omitempty"`
	ColumnMeta  []map[string]interface{} `json:"columnMeta,omitempty"`
}

// FieldConfigDTO 字段配置DTO
type FieldConfigDTO struct {
	Name        string                 `json:"name" binding:"required"`
	Type        string                 `json:"type" binding:"required"`
	Description string                 `json:"description,omitempty"`
	Required    bool                   `json:"required"`
	Unique      bool                   `json:"unique"`
	IsPrimary   bool                   `json:"isPrimary,omitempty"`
	Options     map[string]interface{} `json:"options,omitempty"`
}

// CreateTableRequest 创建表请求
// 对齐 Teable 的 ICreateTableRo 和 ICreateTableWithDefault
type CreateTableRequest struct {
	Name        string           `json:"name" binding:"required"`
	Description string           `json:"description"`
	BaseID      string           `json:"baseId" binding:"required"` // ✅ 统一使用 camelCase
	Views       []ViewConfigDTO  `json:"views,omitempty"`           // ✅ 视图配置数组（可选，不传时使用默认值）
	Fields      []FieldConfigDTO `json:"fields,omitempty"`          // ✅ 字段配置数组（可选，不传时使用默认值）
}

// UpdateTableRequest 更新表请求
type UpdateTableRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

// RenameTableRequest 重命名表请求
type RenameTableRequest struct {
	Name string `json:"name" binding:"required"`
}

// DuplicateTableRequest 复制表请求
type DuplicateTableRequest struct {
	Name       string `json:"name" binding:"required"`
	WithData   bool   `json:"withData"`   // 是否复制数据
	WithViews  bool   `json:"withViews"`  // 是否复制视图
	WithFields bool   `json:"withFields"` // 是否复制字段配置
}

// TableUsageResponse 表用量响应
type TableUsageResponse struct {
	RecordCount     int64   `json:"recordCount"`     // 记录数量
	MaxRecords      int64   `json:"maxRecords"`      // 最大记录数限制
	UsagePercentage float64 `json:"usagePercentage"` // 使用百分比
	StorageSize     int64   `json:"storageSize"`     // 存储大小（字节）
	MaxStorageSize  int64   `json:"maxStorageSize"`  // 最大存储限制（字节）
}

// TableListFilter 表列表过滤器
type TableListFilter struct {
	BaseID    *string    `json:"baseId"`
	Name      *string    `json:"name"`
	CreatedAt *time.Time `json:"createdAt"`
	Page      int        `json:"page"`
	PageSize  int        `json:"pageSize"`
	Offset    int        `json:"offset"`
	Limit     int        `json:"limit"`
}

// TableResponse 表响应
type TableResponse struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	BaseID        string    `json:"baseId"`
	DefaultViewID *string   `json:"defaultViewId,omitempty"` // ✅ 默认视图ID（可选）
	FieldCount    int       `json:"fieldCount"`
	RecordCount   int64     `json:"recordCount"`
	CreatedBy     string    `json:"createdBy"`
	UpdatedBy     string    `json:"updatedBy"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// TableListResponse 表列表响应
type TableListResponse struct {
	Tables     []*TableResponse    `json:"tables"`
	Pagination *PaginationResponse `json:"pagination"`
}

// FromTableEntity 从Domain实体转换为DTO
func FromTableEntity(table *tableEntity.Table) *TableResponse {
	if table == nil {
		return nil
	}

	desc := ""
	if table.Description() != nil {
		desc = *table.Description()
	}

	// UpdatedBy 通常应该从操作上下文中获取
	// 如果没有专门的 UpdatedBy 字段，可以使用 CreatedBy 作为默认值
	// 在实际应用中，应该从请求上下文中获取当前操作用户
	updatedBy := table.CreatedBy() // 默认使用创建者

	return &TableResponse{
		ID:          table.ID().String(),
		Name:        table.Name().String(),
		Description: desc,
		BaseID:      table.BaseID(),
		CreatedBy:   table.CreatedBy(),
		UpdatedBy:   updatedBy,
		CreatedAt:   table.CreatedAt(),
		UpdatedAt:   table.UpdatedAt(),
	}
}
