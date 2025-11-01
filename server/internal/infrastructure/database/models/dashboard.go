package models

import (
	"time"
)

// Dashboard 仪表板模型
type Dashboard struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string    `gorm:"type:varchar(255);not null" json:"name"`
	BaseID           string    `gorm:"column:base_id;type:varchar(30);not null" json:"base_id"`
	Layout           *string   `gorm:"type:text" json:"layout"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
}

// TableName 指定表名
func (Dashboard) TableName() string {
	return "dashboard"
}
