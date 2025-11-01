package models

import (
	"time"
)

// Reference 引用关系模型
type Reference struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	FromFieldID string    `gorm:"column:from_field_id;type:varchar(30);not null" json:"from_field_id"`
	ToFieldID   string    `gorm:"column:to_field_id;type:varchar(30);not null" json:"to_field_id"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

// TableName 指定表名
func (Reference) TableName() string {
	return "reference"
}
