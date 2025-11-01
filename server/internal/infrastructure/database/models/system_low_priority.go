package models

import (
	"time"

	"gorm.io/gorm"
)

// SystemSetting 系统设置表
type SystemSetting struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Key              string     `gorm:"type:text;not null" json:"key"`
	Value            *string    `gorm:"type:text" json:"value"`
	Type             string     `gorm:"type:text;not null;default:'string'" json:"type"` // string, number, boolean, json
	Category         string     `gorm:"type:text;not null;default:'general'" json:"category"`
	Description      *string    `gorm:"type:text" json:"description"`
	IsPublic         bool       `gorm:"type:boolean;not null;default:false" json:"is_public"`
	IsEncrypted      bool       `gorm:"type:boolean;not null;default:false" json:"is_encrypted"`
	IsRequired       bool       `gorm:"type:boolean;not null;default:false" json:"is_required"`
	DefaultValue     *string    `gorm:"type:text" json:"default_value"`
	Validation       *string    `gorm:"type:jsonb" json:"validation"`
	SortOrder        int        `gorm:"type:integer;not null;default:0" json:"sort_order"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (SystemSetting) TableName() string {
	return "system_setting"
}

// BeforeCreate 创建前钩子
func (ss *SystemSetting) BeforeCreate(tx *gorm.DB) error {
	if ss.CreatedTime.IsZero() {
		ss.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (ss *SystemSetting) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	ss.LastModifiedTime = &now
	return nil
}

// SystemLog 系统日志表
type SystemLog struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Level            string     `gorm:"type:text;not null" json:"level"` // debug, info, warn, error, fatal
	Message          string     `gorm:"type:text;not null" json:"message"`
	Source           *string    `gorm:"type:text" json:"source"`
	Category         *string    `gorm:"type:text" json:"category"`
	UserID           *string    `gorm:"type:text" json:"user_id"`
	SessionID        *string    `gorm:"type:text" json:"session_id"`
	RequestID        *string    `gorm:"type:text" json:"request_id"`
	IPAddress        *string    `gorm:"type:text" json:"ip_address"`
	UserAgent        *string    `gorm:"type:text" json:"user_agent"`
	OrganizationID   *string    `gorm:"type:text" json:"organization_id"`
	SpaceID          *string    `gorm:"type:text" json:"space_id"`
	BaseID           *string    `gorm:"type:text" json:"base_id"`
	TableID          *string    `gorm:"type:text" json:"table_id"`
	RecordID         *string    `gorm:"type:text" json:"record_id"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	StackTrace       *string    `gorm:"type:text" json:"stack_trace"`
	Context          *string    `gorm:"type:jsonb" json:"context"`
	Tags             *string    `gorm:"type:jsonb" json:"tags"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
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
}

// TableName 返回表名
func (SystemLog) TableName() string {
	return "system_log"
}

// BeforeCreate 创建前钩子
func (sl *SystemLog) BeforeCreate(tx *gorm.DB) error {
	if sl.CreatedTime.IsZero() {
		sl.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (sl *SystemLog) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	sl.LastModifiedTime = &now
	return nil
}

// SystemMetrics 系统指标表
type SystemMetrics struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Value            float64    `gorm:"type:decimal(20,6);not null" json:"value"`
	Unit             *string    `gorm:"type:text" json:"unit"`
	Type             string     `gorm:"type:text;not null" json:"type"` // counter, gauge, histogram, summary
	Category         *string    `gorm:"type:text" json:"category"`
	Labels           *string    `gorm:"type:jsonb" json:"labels"`
	Timestamp        time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"timestamp"`
	Source           *string    `gorm:"type:text" json:"source"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (SystemMetrics) TableName() string {
	return "system_metrics"
}

// BeforeCreate 创建前钩子
func (sm *SystemMetrics) BeforeCreate(tx *gorm.DB) error {
	if sm.CreatedTime.IsZero() {
		sm.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (sm *SystemMetrics) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	sm.LastModifiedTime = &now
	return nil
}

// SystemHealth 系统健康检查表
type SystemHealth struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Service          string     `gorm:"type:text;not null" json:"service"`
	Status           string     `gorm:"type:text;not null" json:"status"` // healthy, unhealthy, degraded
	Version          *string    `gorm:"type:text" json:"version"`
	Uptime           *int64     `gorm:"type:bigint" json:"uptime"` // seconds
	LastCheckTime    time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"last_check_time"`
	ResponseTime     *int64     `gorm:"type:bigint" json:"response_time"` // milliseconds
	ErrorCount       int        `gorm:"type:integer;not null;default:0" json:"error_count"`
	SuccessCount     int        `gorm:"type:integer;not null;default:0" json:"success_count"`
	LastErrorTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_error_time"`
	LastErrorMessage *string    `gorm:"type:text" json:"last_error_message"`
	HealthCheck      *string    `gorm:"type:jsonb" json:"health_check"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (SystemHealth) TableName() string {
	return "system_health"
}

// BeforeCreate 创建前钩子
func (sh *SystemHealth) BeforeCreate(tx *gorm.DB) error {
	if sh.CreatedTime.IsZero() {
		sh.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (sh *SystemHealth) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	sh.LastModifiedTime = &now
	return nil
}

// SystemBackup 系统备份表
type SystemBackup struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Type             string     `gorm:"type:text;not null" json:"type"`                     // full, incremental, differential
	Status           string     `gorm:"type:text;not null;default:'pending'" json:"status"` // pending, running, completed, failed
	Size             *int64     `gorm:"type:bigint" json:"size"`                            // bytes
	Location         string     `gorm:"type:text;not null" json:"location"`
	Checksum         *string    `gorm:"type:text" json:"checksum"`
	Encrypted        bool       `gorm:"type:boolean;not null;default:false" json:"encrypted"`
	Compressed       bool       `gorm:"type:boolean;not null;default:false" json:"compressed"`
	StartedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"started_time"`
	CompletedTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"completed_time"`
	Duration         *int64     `gorm:"type:bigint" json:"duration"` // milliseconds
	RetentionDays    int        `gorm:"type:integer;not null;default:30" json:"retention_days"`
	ExpiryTime       *time.Time `gorm:"type:timestamp(3) without time zone" json:"expiry_time"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (SystemBackup) TableName() string {
	return "system_backup"
}

// BeforeCreate 创建前钩子
func (sb *SystemBackup) BeforeCreate(tx *gorm.DB) error {
	if sb.CreatedTime.IsZero() {
		sb.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (sb *SystemBackup) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	sb.LastModifiedTime = &now
	return nil
}
