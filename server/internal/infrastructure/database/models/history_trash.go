package models

import "time"

// RecordHistory 记录历史模型（✅ Phase 3: 完整元数据版本）
// 参考旧系统: teable-develop/apps/nestjs-backend/src/db-main-prisma/schema.prisma
type RecordHistory struct {
	ID          string              `gorm:"primaryKey;type:varchar(50)" json:"id"`
	TableID     string              `gorm:"type:varchar(50);not null;index:idx_record_history_table_record_time" json:"table_id"`
	RecordID    string              `gorm:"type:varchar(50);not null;index:idx_record_history_table_record_time" json:"record_id"`
	FieldID     string              `gorm:"type:varchar(50);not null;index:idx_record_history_table_field_time" json:"field_id"`
	Before      *RecordHistoryState `gorm:"serializer:json;type:jsonb" json:"before"`
	After       *RecordHistoryState `gorm:"serializer:json;type:jsonb" json:"after"`
	CreatedTime time.Time           `gorm:"type:timestamp;not null;default:CURRENT_TIMESTAMP;index:idx_record_history_table_record_time" json:"created_time"`
	CreatedBy   string              `gorm:"type:varchar(50);not null" json:"created_by"`
}

// RecordHistoryState 记录历史状态
type RecordHistoryState struct {
	Meta FieldMeta   `json:"meta"`
	Data interface{} `json:"data"`
}

// FieldMeta 字段元数据
type FieldMeta struct {
	Type          string      `json:"type"`
	Name          string      `json:"name"`
	CellValueType string      `json:"cellValueType"`
	Options       interface{} `json:"options"`
}

func (RecordHistory) TableName() string { return "record_history" }

type Trash struct {
	ID           string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	ResourceType string    `gorm:"column:resource_type;type:varchar(50);uniqueIndex:uq_trash_resource,priority:1;not null" json:"resource_type"`
	ResourceID   string    `gorm:"column:resource_id;type:varchar(30);uniqueIndex:uq_trash_resource,priority:2;not null" json:"resource_id"`
	ParentID     *string   `gorm:"column:parent_id;type:varchar(30)" json:"parent_id"`
	DeletedTime  time.Time `gorm:"autoCreateTime;column:deleted_time;not null" json:"deleted_time"`
	DeletedBy    string    `gorm:"column:deleted_by;type:varchar(30);not null" json:"deleted_by"`
}

func (Trash) TableName() string { return "trash" }

type TableTrash struct {
	ID           string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TableID      string    `gorm:"column:table_id;type:varchar(30);index;not null" json:"table_id"`
	ResourceType string    `gorm:"column:resource_type;type:varchar(50);not null" json:"resource_type"`
	Snapshot     string    `gorm:"column:snapshot;type:text;not null" json:"snapshot"`
	CreatedTime  time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy    string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
}

func (TableTrash) TableName() string { return "table_trash" }

type RecordTrash struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TableID     string    `gorm:"column:table_id;type:varchar(30);index;not null" json:"table_id"`
	RecordID    string    `gorm:"column:record_id;type:varchar(30);index;not null" json:"record_id"`
	Snapshot    string    `gorm:"column:snapshot;type:text;not null" json:"snapshot"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy   string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
}

func (RecordTrash) TableName() string { return "record_trash" }
