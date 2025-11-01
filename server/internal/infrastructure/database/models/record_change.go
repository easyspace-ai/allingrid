package models

import (
	"time"

	"gorm.io/gorm"
)

// RecordChange 记录变更模型
type RecordChange struct {
	ID         string                 `gorm:"primaryKey;type:varchar(36)" json:"id"`
	RecordID   string                 `gorm:"type:varchar(36);index:idx_record_changes_record" json:"record_id"`
	TableID    string                 `gorm:"type:varchar(36);index:idx_record_changes_table" json:"table_id"`
	ChangeType string                 `gorm:"type:varchar(20);index" json:"change_type"` // create, update, delete
	OldData    map[string]interface{} `gorm:"serializer:json;type:jsonb" json:"old_data,omitempty"`
	NewData    map[string]interface{} `gorm:"serializer:json;type:jsonb" json:"new_data,omitempty"`
	ChangedBy  string                 `gorm:"type:varchar(36);index:idx_record_changes_user" json:"changed_by"`
	ChangedAt  time.Time              `gorm:"type:timestamp;index:idx_record_changes_time" json:"changed_at"`
	Version    int64                  `gorm:"type:bigint;default:0" json:"version"`
	CreatedAt  time.Time              `gorm:"type:timestamp;autoCreateTime" json:"created_at"`
}

// TableName 指定表名
func (RecordChange) TableName() string {
	return "record_changes"
}

// BeforeCreate GORM钩子：创建前
func (rc *RecordChange) BeforeCreate(tx *gorm.DB) error {
	if rc.ChangedAt.IsZero() {
		rc.ChangedAt = time.Now()
	}
	return nil
}
