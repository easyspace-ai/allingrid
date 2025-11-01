package models

import "time"

// 为了与 Prisma 兼容，增加 Attachments 与 AttachmentsTable
type Attachments struct {
	ID             string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Token          string     `gorm:"uniqueIndex;type:varchar(100);not null" json:"token"`
	Hash           string     `gorm:"type:varchar(255);not null" json:"hash"`
	Size           int        `gorm:"not null" json:"size"`
	Mimetype       string     `gorm:"type:varchar(100);not null" json:"mimetype"`
	Path           string     `gorm:"type:varchar(500);not null" json:"path"`
	Width          *int       `gorm:"type:int" json:"width"`
	Height         *int       `gorm:"type:int" json:"height"`
	DeletedTime    *time.Time `gorm:"column:deleted_time" json:"deleted_time"`
	CreatedTime    time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy      string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedBy *string    `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	ThumbnailPath  *string    `gorm:"column:thumbnail_path;type:varchar(500)" json:"thumbnail_path"`
}

func (Attachments) TableName() string { return "attachments" }

type AttachmentsTable struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	AttachmentID     string     `gorm:"column:attachment_id;type:varchar(30);index;not null" json:"attachment_id"`
	Name             string     `gorm:"type:varchar(255);not null" json:"name"`
	Token            string     `gorm:"type:varchar(100);not null" json:"token"`
	TableID          string     `gorm:"column:table_id;type:varchar(30);not null;index:idx_att_table_field,priority:1;index:idx_att_table_record,priority:1" json:"table_id"`
	RecordID         string     `gorm:"column:record_id;type:varchar(30);not null;index:idx_att_table_record,priority:2" json:"record_id"`
	FieldID          string     `gorm:"column:field_id;type:varchar(30);not null;index:idx_att_table_field,priority:2" json:"field_id"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedBy   *string    `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
}

func (AttachmentsTable) TableName() string { return "attachments_table" }
