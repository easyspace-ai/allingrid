package models

import (
	"time"

	"gorm.io/gorm"
)

// RecordVersion 记录版本模型
type RecordVersion struct {
	ID         string                 `gorm:"primaryKey;type:varchar(36)" json:"id"`
	RecordID   string                 `gorm:"type:varchar(36);index:idx_record_versions_record" json:"record_id"`
	Version    int64                  `gorm:"type:bigint;index" json:"version"`
	Data       map[string]interface{} `gorm:"serializer:json;type:jsonb" json:"data"`
	ChangeType string                 `gorm:"type:varchar(50)" json:"change_type"` // create, update, delete, backup_before_restore, restore
	ChangedBy  string                 `gorm:"type:varchar(36)" json:"changed_by"`
	ChangedAt  time.Time              `gorm:"type:timestamp;index:idx_record_versions_time" json:"changed_at"`
	CreatedAt  time.Time              `gorm:"type:timestamp;autoCreateTime" json:"created_at"`
}

// TableName 指定表名
func (RecordVersion) TableName() string {
	return "record_versions"
}

// BeforeCreate GORM钩子：创建前
func (rv *RecordVersion) BeforeCreate(tx *gorm.DB) error {
	if rv.ChangedAt.IsZero() {
		rv.ChangedAt = time.Now()
	}
	return nil
}
