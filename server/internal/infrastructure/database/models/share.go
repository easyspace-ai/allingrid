package models

import (
	"time"

	"gorm.io/gorm"
)

// ShareView 分享视图模型
type ShareView struct {
	ID          string         `gorm:"primaryKey;type:varchar(30)" json:"id"`
	ViewID      string         `gorm:"not null;type:varchar(30);index" json:"view_id"`
	TableID     string         `gorm:"not null;type:varchar(30);index" json:"table_id"`
	ShareID     string         `gorm:"not null;type:varchar(30);uniqueIndex" json:"share_id"`
	EnableShare bool           `gorm:"default:false;index" json:"enable_share"`
	ShareMeta   *string        `gorm:"type:json" json:"share_meta,omitempty"`
	CreatedBy   string         `gorm:"not null;type:varchar(30)" json:"created_by"`
	CreatedTime time.Time      `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	UpdatedTime time.Time      `gorm:"autoUpdateTime;column:updated_time" json:"updated_time"`
	DeletedTime gorm.DeletedAt `gorm:"column:deleted_time" json:"deleted_time,omitempty"`
}

// TableName 指定表名
func (ShareView) TableName() string {
	return "share_views"
}
