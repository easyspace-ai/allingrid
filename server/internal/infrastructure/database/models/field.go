package models

import (
	"time"

	"gorm.io/gorm"
)

// Field 字段定义
type Field struct {
	ID                  string         `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TableID             string         `gorm:"type:varchar(50);not null;index" json:"table_id"`
	Name                string         `gorm:"type:varchar(255);not null" json:"name"`
	Description         *string        `gorm:"type:text" json:"description"`
	Type                string         `gorm:"type:varchar(50);not null" json:"type"`
	CellValueType       string         `gorm:"type:varchar(50);not null" json:"cell_value_type"`
	IsMultipleCellValue *bool          `gorm:"default:false" json:"is_multiple_cell_value"`
	DBFieldType         string         `gorm:"type:varchar(50);not null" json:"db_field_type"`
	DBFieldName         string         `gorm:"type:varchar(255);not null" json:"db_field_name"`
	NotNull             *bool          `gorm:"default:false" json:"not_null"`
	Unique              *bool          `gorm:"default:false" json:"unique"`
	IsPrimary           *bool          `gorm:"default:false" json:"is_primary"`
	IsComputed          *bool          `gorm:"default:false" json:"is_computed"`
	IsLookup            *bool          `gorm:"default:false" json:"is_lookup"`
	FieldOrder          float64        `gorm:"column:field_order;type:numeric(10,2);default:0" json:"field_order"`
	Order               *float64       `gorm:"column:order" json:"order"`
	Version             *int           `gorm:"default:1" json:"version"`
	CreatedBy           string         `gorm:"type:varchar(50);not null;index" json:"created_by"`
	CreatedTime         time.Time      `gorm:"not null" json:"created_time"`
	LastModifiedTime    *time.Time     `json:"last_modified_time"`
	IsRequired          bool           `gorm:"default:false" json:"is_required"`
	IsUnique            bool           `gorm:"default:false" json:"is_unique"`
	DefaultValue        *string        `gorm:"type:text" json:"default_value"`
	Options             *string        `gorm:"type:text" json:"options"`
	DeletedTime         gorm.DeletedAt `gorm:"index" json:"deleted_time"`
	// 虚拟字段和AI字段支持
	AIConfig            *string `gorm:"column:ai_config;type:text" json:"ai_config"`
	LookupLinkedFieldID *string `gorm:"column:lookup_linked_field_id;type:varchar(30)" json:"lookup_linked_field_id"`
	LookupOptions       *string `gorm:"column:lookup_options;type:text" json:"lookup_options"`
	HasError            *bool   `gorm:"column:has_error;default:false" json:"has_error"`
	IsPending           *bool   `gorm:"column:is_pending;default:false" json:"is_pending"`
}

func (Field) TableName() string { return "field" }
