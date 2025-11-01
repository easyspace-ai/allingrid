package models

import (
	"time"
)

// Ops 操作记录模型
type Ops struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Collection  string    `gorm:"type:varchar(100);not null" json:"collection"`
	DocID       string    `gorm:"column:doc_id;type:varchar(50);not null" json:"doc_id"`
	DocType     string    `gorm:"column:doc_type;type:varchar(50);not null" json:"doc_type"`
	Version     int       `gorm:"not null" json:"version"`
	Operation   string    `gorm:"type:text;not null" json:"operation"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy   string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
}

// TableName 指定表名
func (Ops) TableName() string {
	return "ops"
}
