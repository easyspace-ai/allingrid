package models

import (
	"time"

	"gorm.io/gorm"
)

// ApiKey API密钥表
type ApiKey struct {
	ID                 string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name               string     `gorm:"type:text;not null" json:"name"`
	Description        *string    `gorm:"type:text" json:"description"`
	Key                string     `gorm:"type:text;not null" json:"key"`
	Secret             string     `gorm:"type:text;not null" json:"secret"`
	Type               string     `gorm:"type:text;not null;default:'api'" json:"type"`      // api, webhook, integration
	Status             string     `gorm:"type:text;not null;default:'active'" json:"status"` // active, inactive, revoked
	Permissions        *string    `gorm:"type:jsonb" json:"permissions"`
	RateLimit          int        `gorm:"type:integer;not null;default:1000" json:"rate_limit"` // requests per hour
	ExpiryDate         *time.Time `gorm:"type:timestamp(3) without time zone" json:"expiry_date"`
	LastUsedTime       *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_used_time"`
	UsageCount         int        `gorm:"type:integer;not null;default:0" json:"usage_count"`
	IPWhitelist        *string    `gorm:"type:jsonb" json:"ip_whitelist"`
	UserAgentWhitelist *string    `gorm:"type:jsonb" json:"user_agent_whitelist"`
	WebhookURL         *string    `gorm:"type:text" json:"webhook_url"`
	WebhookSecret      *string    `gorm:"type:text" json:"webhook_secret"`
	Metadata           *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy          string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime        time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime   *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy     *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime        *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (ApiKey) TableName() string {
	return "api_key"
}

// BeforeCreate 创建前钩子
func (ak *ApiKey) BeforeCreate(tx *gorm.DB) error {
	if ak.CreatedTime.IsZero() {
		ak.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (ak *ApiKey) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	ak.LastModifiedTime = &now
	return nil
}

// ApiUsage API使用记录表
type ApiUsage struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	ApiKeyID         *string    `gorm:"type:text" json:"api_key_id"`
	UserID           *string    `gorm:"type:text" json:"user_id"`
	Endpoint         string     `gorm:"type:text;not null" json:"endpoint"`
	Method           string     `gorm:"type:text;not null" json:"method"`
	Status           int        `gorm:"type:integer;not null" json:"status"`
	ResponseTime     int64      `gorm:"type:bigint;not null" json:"response_time"` // milliseconds
	RequestSize      *int64     `gorm:"type:bigint" json:"request_size"`
	ResponseSize     *int64     `gorm:"type:bigint" json:"response_size"`
	IPAddress        *string    `gorm:"type:text" json:"ip_address"`
	UserAgent        *string    `gorm:"type:text" json:"user_agent"`
	Referer          *string    `gorm:"type:text" json:"referer"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	RequestID        *string    `gorm:"type:text" json:"request_id"`
	SessionID        *string    `gorm:"type:text" json:"session_id"`
	OrganizationID   *string    `gorm:"type:text" json:"organization_id"`
	SpaceID          *string    `gorm:"type:text" json:"space_id"`
	BaseID           *string    `gorm:"type:text" json:"base_id"`
	TableID          *string    `gorm:"type:text" json:"table_id"`
	RecordID         *string    `gorm:"type:text" json:"record_id"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	ApiKey       *ApiKey       `gorm:"foreignKey:ApiKeyID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"api_key,omitempty"`
	User         *User         `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"user,omitempty"`
	Organization *Organization `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"organization,omitempty"`
	Space        *Space        `gorm:"foreignKey:SpaceID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"space,omitempty"`
	Base         *Base         `gorm:"foreignKey:BaseID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"base,omitempty"`
	Table        *Table        `gorm:"foreignKey:TableID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"table,omitempty"`
	Record       *Record       `gorm:"foreignKey:RecordID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"record,omitempty"`
}

// TableName 返回表名
func (ApiUsage) TableName() string {
	return "api_usage"
}

// BeforeCreate 创建前钩子
func (au *ApiUsage) BeforeCreate(tx *gorm.DB) error {
	if au.CreatedTime.IsZero() {
		au.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (au *ApiUsage) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	au.LastModifiedTime = &now
	return nil
}

// ApiRateLimit API速率限制表
type ApiRateLimit struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	ApiKeyID         *string    `gorm:"type:text" json:"api_key_id"`
	UserID           *string    `gorm:"type:text" json:"user_id"`
	IPAddress        *string    `gorm:"type:text" json:"ip_address"`
	Endpoint         *string    `gorm:"type:text" json:"endpoint"`
	Method           *string    `gorm:"type:text" json:"method"`
	WindowSize       int        `gorm:"type:integer;not null;default:3600" json:"window_size"` // seconds
	MaxRequests      int        `gorm:"type:integer;not null;default:1000" json:"max_requests"`
	CurrentRequests  int        `gorm:"type:integer;not null;default:0" json:"current_requests"`
	WindowStart      time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"window_start"`
	WindowEnd        time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"window_end"`
	IsBlocked        bool       `gorm:"type:boolean;not null;default:false" json:"is_blocked"`
	BlockedUntil     *time.Time `gorm:"type:timestamp(3) without time zone" json:"blocked_until"`
	LastRequestTime  *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_request_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	ApiKey *ApiKey `gorm:"foreignKey:ApiKeyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"api_key,omitempty"`
	User   *User   `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"user,omitempty"`
}

// TableName 返回表名
func (ApiRateLimit) TableName() string {
	return "api_rate_limit"
}

// BeforeCreate 创建前钩子
func (arl *ApiRateLimit) BeforeCreate(tx *gorm.DB) error {
	if arl.CreatedTime.IsZero() {
		arl.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (arl *ApiRateLimit) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	arl.LastModifiedTime = &now
	return nil
}

// ApiWebhookLog API Webhook日志表
type ApiWebhookLog struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	ApiKeyID         string     `gorm:"type:text;not null" json:"api_key_id"`
	EventType        string     `gorm:"type:text;not null" json:"event_type"`
	Payload          *string    `gorm:"type:jsonb" json:"payload"`
	URL              string     `gorm:"type:text;not null" json:"url"`
	Method           string     `gorm:"type:text;not null;default:'POST'" json:"method"`
	Headers          *string    `gorm:"type:jsonb" json:"headers"`
	Status           int        `gorm:"type:integer;not null" json:"status"`
	ResponseTime     int64      `gorm:"type:bigint;not null" json:"response_time"` // milliseconds
	ResponseBody     *string    `gorm:"type:text" json:"response_body"`
	ResponseHeaders  *string    `gorm:"type:jsonb" json:"response_headers"`
	RetryCount       int        `gorm:"type:integer;not null;default:0" json:"retry_count"`
	MaxRetries       int        `gorm:"type:integer;not null;default:3" json:"max_retries"`
	NextRetryTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"next_retry_time"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	RequestID        *string    `gorm:"type:text" json:"request_id"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	ApiKey *ApiKey `gorm:"foreignKey:ApiKeyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"api_key,omitempty"`
}

// TableName 返回表名
func (ApiWebhookLog) TableName() string {
	return "api_webhook_log"
}

// BeforeCreate 创建前钩子
func (awl *ApiWebhookLog) BeforeCreate(tx *gorm.DB) error {
	if awl.CreatedTime.IsZero() {
		awl.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (awl *ApiWebhookLog) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	awl.LastModifiedTime = &now
	return nil
}
