package models

import (
	"time"
)

// Integration 集成模型
type Integration struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	ResourceID       string    `gorm:"column:resource_id;type:varchar(30);unique;not null" json:"resource_id"`
	Config           string    `gorm:"type:text;not null" json:"config"`
	Type             string    `gorm:"type:varchar(50);not null" json:"type"`
	Enable           *bool     `gorm:"type:boolean" json:"enable"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
}

// TableName 指定表名
func (Integration) TableName() string {
	return "integration"
}
