package models

import (
	"time"
)

// UserLastVisit 用户访问记录模型
type UserLastVisit struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	UserID           string    `gorm:"column:user_id;type:varchar(30);not null" json:"user_id"`
	ResourceType     string    `gorm:"column:resource_type;type:varchar(50);not null" json:"resource_type"`
	ResourceID       string    `gorm:"column:resource_id;type:varchar(30);not null" json:"resource_id"`
	ParentResourceID string    `gorm:"column:parent_resource_id;type:varchar(30);not null" json:"parent_resource_id"`
	LastVisitTime    time.Time `gorm:"autoCreateTime;column:last_visit_time" json:"last_visit_time"`
}

// TableName 指定表名
func (UserLastVisit) TableName() string {
	return "user_last_visit"
}

// Waitlist 等待列表模型
type Waitlist struct {
	Email       string     `gorm:"primaryKey;type:varchar(255)" json:"email"`
	Invite      *bool      `gorm:"type:boolean" json:"invite"`
	InviteTime  *time.Time `gorm:"column:invite_time" json:"invite_time"`
	CreatedTime time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

// TableName 指定表名
func (Waitlist) TableName() string {
	return "waitlist"
}

// AttachmentLink 附件关联模型
type AttachmentLink struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	AttachmentID     string     `gorm:"column:attachment_id;type:varchar(30);not null" json:"attachment_id"`
	Name             string     `gorm:"type:varchar(255);not null" json:"name"`
	Token            string     `gorm:"type:varchar(50);not null" json:"token"`
	TableID          string     `gorm:"column:table_id;type:varchar(30);not null" json:"table_id"`
	RecordID         string     `gorm:"column:record_id;type:varchar(30);not null" json:"record_id"`
	FieldID          string     `gorm:"column:field_id;type:varchar(30);not null" json:"field_id"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedBy   *string    `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
}

// TableName 指定表名
func (AttachmentLink) TableName() string {
	return "attachments_table"
}
