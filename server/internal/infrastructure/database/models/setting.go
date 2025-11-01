package models

import (
	"time"
)

// Setting 系统设置模型
type Setting struct {
	Name             string    `gorm:"primaryKey;type:varchar(100)" json:"name"`
	Content          *string   `gorm:"type:text" json:"content"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
}

// TableName 指定表名
func (Setting) TableName() string {
	return "setting"
}
