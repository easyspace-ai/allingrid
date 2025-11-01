package models

import (
	"time"
)

// Comment 评论模型
type Comment struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TableID          string     `gorm:"column:table_id;type:varchar(30);not null" json:"table_id"`
	RecordID         string     `gorm:"column:record_id;type:varchar(30);not null" json:"record_id"`
	QuoteID          *string    `gorm:"column:quote_id;type:varchar(30)" json:"quote_id"`
	Content          *string    `gorm:"type:text" json:"content"`
	Reaction         *string    `gorm:"type:text" json:"reaction"`
	DeletedTime      *time.Time `gorm:"column:deleted_time" json:"deleted_time"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedTime time.Time  `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
}

// TableName 指定表名
func (Comment) TableName() string {
	return "comment"
}

// CommentSubscription 评论订阅模型
type CommentSubscription struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TableID     string    `gorm:"column:table_id;type:varchar(30);not null" json:"table_id"`
	RecordID    string    `gorm:"column:record_id;type:varchar(30);not null" json:"record_id"`
	CreatedBy   string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

// TableName 指定表名
func (CommentSubscription) TableName() string {
	return "comment_subscription"
}
