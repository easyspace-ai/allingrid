package models

import (
	"time"

	"gorm.io/gorm"
)

// Table 数据表模型
type Table struct {
	ID          string  `gorm:"primaryKey;type:varchar(50)" json:"id"`
	BaseID      string  `gorm:"type:varchar(50);not null;index" json:"base_id"`
	Name        string  `gorm:"type:varchar(255);not null" json:"name"`
	Description *string `gorm:"type:text" json:"description"`
	Icon        *string `gorm:"type:varchar(255)" json:"icon"`
	// IsSystem 字段已从数据库中移除（见 cmd/server/main.go 第383行）
	CreatedBy        string         `gorm:"type:varchar(50);not null;index" json:"created_by"`
	CreatedTime      time.Time      `gorm:"not null" json:"created_time"`
	DeletedTime      gorm.DeletedAt `gorm:"index" json:"deleted_time"`
	LastModifiedTime *time.Time     `json:"last_modified_time"`
	DBTableName      *string        `gorm:"column:db_table_name;type:varchar(255);index" json:"db_table_name"`
	Version          *int           `gorm:"column:version;default:1" json:"version"`
	Order            *float64       `gorm:"column:order;index" json:"order"`
	LastModifiedBy   *string        `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
}

// TableName 指定表名
func (Table) TableName() string {
	return "table_meta"
}

// FieldDependency 字段依赖关系模型
type FieldDependency struct {
	ID               string    `gorm:"primaryKey;type:varchar(50)" json:"id"`
	SourceFieldID    string    `gorm:"column:source_field_id;type:varchar(30);not null;index" json:"source_field_id"`
	DependentFieldID string    `gorm:"column:dependent_field_id;type:varchar(30);not null;index" json:"dependent_field_id"`
	DependencyType   string    `gorm:"column:dependency_type;type:varchar(50);not null" json:"dependency_type"`
	CreatedTime      time.Time `gorm:"column:created_time;default:CURRENT_TIMESTAMP" json:"created_time"`
}

// TableName 指定表名
func (FieldDependency) TableName() string {
	return "field_dependency"
}

// VirtualFieldCache 虚拟字段缓存模型
type VirtualFieldCache struct {
	ID          string     `gorm:"primaryKey;type:varchar(50)" json:"id"`
	RecordID    string     `gorm:"column:record_id;type:varchar(30);not null;index" json:"record_id"`
	FieldID     string     `gorm:"column:field_id;type:varchar(30);not null;index" json:"field_id"`
	CachedValue *string    `gorm:"column:cached_value;type:text" json:"cached_value"`
	ValueType   *string    `gorm:"column:value_type;type:varchar(50)" json:"value_type"`
	CachedAt    time.Time  `gorm:"column:cached_at;default:CURRENT_TIMESTAMP" json:"cached_at"`
	ExpiresAt   *time.Time `gorm:"column:expires_at;index" json:"expires_at"`
}

// TableName 指定表名
func (VirtualFieldCache) TableName() string {
	return "virtual_field_cache"
}
