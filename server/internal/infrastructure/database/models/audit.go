package models

import (
	"time"

	"gorm.io/gorm"
)

// AuditLog 审计日志表
type AuditLog struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Action           string     `gorm:"type:text;not null" json:"action"`        // create, read, update, delete, login, logout, etc.
	ResourceType     string     `gorm:"type:text;not null" json:"resource_type"` // user, space, base, record, etc.
	ResourceID       *string    `gorm:"type:text" json:"resource_id"`
	ResourceName     *string    `gorm:"type:text" json:"resource_name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Status           string     `gorm:"type:text;not null;default:'success'" json:"status"` // success, failure, error
	Severity         string     `gorm:"type:text;not null;default:'info'" json:"severity"`  // info, warning, error, critical
	UserID           *string    `gorm:"type:text" json:"user_id"`
	UserName         *string    `gorm:"type:text" json:"user_name"`
	UserEmail        *string    `gorm:"type:text" json:"user_email"`
	IPAddress        *string    `gorm:"type:text" json:"ip_address"`
	UserAgent        *string    `gorm:"type:text" json:"user_agent"`
	SessionID        *string    `gorm:"type:text" json:"session_id"`
	RequestID        *string    `gorm:"type:text" json:"request_id"`
	OrganizationID   *string    `gorm:"type:text" json:"organization_id"`
	SpaceID          *string    `gorm:"type:text" json:"space_id"`
	BaseID           *string    `gorm:"type:text" json:"base_id"`
	TableID          *string    `gorm:"type:text" json:"table_id"`
	RecordID         *string    `gorm:"type:text" json:"record_id"`
	FieldID          *string    `gorm:"type:text" json:"field_id"`
	OldValues        *string    `gorm:"type:jsonb" json:"old_values"`
	NewValues        *string    `gorm:"type:jsonb" json:"new_values"`
	ChangedFields    *string    `gorm:"type:jsonb" json:"changed_fields"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	Duration         *int64     `gorm:"type:bigint" json:"duration"` // milliseconds
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	User         *User         `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"user,omitempty"`
	Organization *Organization `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"organization,omitempty"`
	Space        *Space        `gorm:"foreignKey:SpaceID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"space,omitempty"`
	Base         *Base         `gorm:"foreignKey:BaseID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"base,omitempty"`
	Table        *Table        `gorm:"foreignKey:TableID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"table,omitempty"`
	Record       *Record       `gorm:"foreignKey:RecordID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"record,omitempty"`
	Field        *Field        `gorm:"foreignKey:FieldID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"field,omitempty"`
}

// TableName 返回表名
func (AuditLog) TableName() string {
	return "audit_log"
}

// BeforeCreate 创建前钩子
func (al *AuditLog) BeforeCreate(tx *gorm.DB) error {
	if al.CreatedTime.IsZero() {
		al.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (al *AuditLog) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	al.LastModifiedTime = &now
	return nil
}

// AuditLogConfig 审计日志配置表
type AuditLogConfig struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	ResourceType     string     `gorm:"type:text;not null" json:"resource_type"`
	Action           string     `gorm:"type:text;not null" json:"action"`
	IsEnabled        bool       `gorm:"type:boolean;not null;default:true" json:"is_enabled"`
	LogLevel         string     `gorm:"type:text;not null;default:'info'" json:"log_level"` // info, warning, error, critical
	IncludeOldValues bool       `gorm:"type:boolean;not null;default:true" json:"include_old_values"`
	IncludeNewValues bool       `gorm:"type:boolean;not null;default:true" json:"include_new_values"`
	IncludeMetadata  bool       `gorm:"type:boolean;not null;default:true" json:"include_metadata"`
	RetentionDays    int        `gorm:"type:integer;not null;default:90" json:"retention_days"`
	Conditions       *string    `gorm:"type:jsonb" json:"conditions"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
}

// TableName 返回表名
func (AuditLogConfig) TableName() string {
	return "audit_log_config"
}

// BeforeCreate 创建前钩子
func (alc *AuditLogConfig) BeforeCreate(tx *gorm.DB) error {
	if alc.CreatedTime.IsZero() {
		alc.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (alc *AuditLogConfig) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	alc.LastModifiedTime = &now
	return nil
}

// AuditLogSummary 审计日志汇总表
type AuditLogSummary struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Date             time.Time  `gorm:"type:date;not null" json:"date"`
	ResourceType     string     `gorm:"type:text;not null" json:"resource_type"`
	Action           string     `gorm:"type:text;not null" json:"action"`
	Status           string     `gorm:"type:text;not null" json:"status"`
	Severity         string     `gorm:"type:text;not null" json:"severity"`
	Count            int        `gorm:"type:integer;not null;default:0" json:"count"`
	UniqueUsers      int        `gorm:"type:integer;not null;default:0" json:"unique_users"`
	AvgDuration      float64    `gorm:"type:decimal(10,2)" json:"avg_duration"`
	MinDuration      int64      `gorm:"type:bigint" json:"min_duration"`
	MaxDuration      int64      `gorm:"type:bigint" json:"max_duration"`
	ErrorCount       int        `gorm:"type:integer;not null;default:0" json:"error_count"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
}

// TableName 返回表名
func (AuditLogSummary) TableName() string {
	return "audit_log_summary"
}

// BeforeCreate 创建前钩子
func (als *AuditLogSummary) BeforeCreate(tx *gorm.DB) error {
	if als.CreatedTime.IsZero() {
		als.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (als *AuditLogSummary) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	als.LastModifiedTime = &now
	return nil
}
