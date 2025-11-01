package models

import (
	"time"

	"gorm.io/datatypes"
)

// View 视图模型（数据库表）
type View struct {
	ID               string         `gorm:"column:id;type:varchar(30);primaryKey"`
	Name             string         `gorm:"column:name;type:varchar(100);not null"`
	Description      *string        `gorm:"column:description;type:text"`
	TableID          string         `gorm:"column:table_id;type:varchar(30);not null;index:idx_view_table_id"`
	Type             string         `gorm:"column:type;type:varchar(20);not null"` // grid, kanban, gallery, form, calendar
	Filter           datatypes.JSON `gorm:"column:filter;type:jsonb"`
	Sort             datatypes.JSON `gorm:"column:sort;type:jsonb"`
	Group            datatypes.JSON `gorm:"column:group;type:jsonb"`
	ColumnMeta       datatypes.JSON `gorm:"column:column_meta;type:jsonb"`
	Options          datatypes.JSON `gorm:"column:options;type:jsonb"`
	Order            *float64       `gorm:"column:order"`
	Version          int            `gorm:"column:version;type:int;default:1"`
	IsLocked         bool           `gorm:"column:is_locked;type:boolean;default:false"`
	EnableShare      bool           `gorm:"column:enable_share;type:boolean;default:false"`
	ShareID          *string        `gorm:"column:share_id;type:varchar(50);uniqueIndex"`
	ShareMeta        datatypes.JSON `gorm:"column:share_meta;type:jsonb"`
	CreatedBy        string         `gorm:"column:created_by;type:varchar(30);not null"`
	CreatedTime      time.Time      `gorm:"column:created_time;type:timestamp;not null;autoCreateTime"`
	LastModifiedTime *time.Time     `gorm:"column:last_modified_time;type:timestamp;autoUpdateTime"`
	DeletedTime      *time.Time     `gorm:"column:deleted_time;type:timestamp;index"`
}

// TableName 指定表名
func (View) TableName() string {
	return "view"
}
