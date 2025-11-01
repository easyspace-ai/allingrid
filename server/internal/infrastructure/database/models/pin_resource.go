package models

import (
	"time"
)

// PinResource 置顶资源模型
type PinResource struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Type        string    `gorm:"type:varchar(50);not null" json:"type"`
	ResourceID  string    `gorm:"column:resource_id;type:varchar(50);not null" json:"resource_id"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy   string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	Order       float64   `gorm:"column:order;not null" json:"order"`
}

// TableName 指定表名
func (PinResource) TableName() string {
	return "pin_resource"
}
