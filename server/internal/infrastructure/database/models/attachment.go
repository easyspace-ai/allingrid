package models

import (
	"time"

	"gorm.io/gorm"
)

// Attachment 附件模型
type Attachment struct {
	ID             string         `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name           string         `gorm:"not null;type:varchar(255)" json:"name"`
	Path           string         `gorm:"not null;type:varchar(500)" json:"path"`
	Token          string         `gorm:"not null;type:varchar(50);uniqueIndex" json:"token"`
	Size           int64          `gorm:"not null" json:"size"`
	MimeType       string         `gorm:"not null;type:varchar(100)" json:"mime_type"`
	PresignedURL   *string        `gorm:"type:varchar(500)" json:"presigned_url,omitempty"`
	Width          *int           `gorm:"type:int" json:"width,omitempty"`
	Height         *int           `gorm:"type:int" json:"height,omitempty"`
	SmallThumbnail *string        `gorm:"type:varchar(500)" json:"small_thumbnail,omitempty"`
	LargeThumbnail *string        `gorm:"type:varchar(500)" json:"large_thumbnail,omitempty"`
	TableID        string         `gorm:"not null;type:varchar(30);index" json:"table_id"`
	FieldID        string         `gorm:"not null;type:varchar(30);index" json:"field_id"`
	RecordID       string         `gorm:"not null;type:varchar(30);index" json:"record_id"`
	CreatedBy      string         `gorm:"not null;type:varchar(30)" json:"created_by"`
	CreatedTime    time.Time      `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	UpdatedTime    time.Time      `gorm:"autoUpdateTime;column:updated_time" json:"updated_time"`
	DeletedTime    gorm.DeletedAt `gorm:"column:deleted_time" json:"deleted_time,omitempty"`
}

// TableName 指定表名
func (Attachment) TableName() string {
	return "attachments"
}

// UploadToken 上传令牌模型
type UploadToken struct {
	Token        string    `gorm:"primaryKey;type:varchar(50)" json:"token"`
	UserID       string    `gorm:"not null;type:varchar(30);index" json:"user_id"`
	TableID      string    `gorm:"not null;type:varchar(30);index" json:"table_id"`
	FieldID      string    `gorm:"not null;type:varchar(30);index" json:"field_id"`
	RecordID     string    `gorm:"not null;type:varchar(30);index" json:"record_id"`
	ExpiresAt    time.Time `gorm:"not null;index" json:"expires_at"`
	MaxSize      int64     `gorm:"not null" json:"max_size"`
	AllowedTypes *string   `gorm:"type:json" json:"allowed_types,omitempty"`
	CreatedTime  time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

// TableName 指定表名
func (UploadToken) TableName() string {
	return "upload_tokens"
}
