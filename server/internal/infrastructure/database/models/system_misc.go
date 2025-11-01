package models

import (
	"time"

	"gorm.io/gorm"
)

// VisitLog 访问日志表
type VisitLog struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	UserID           *string    `gorm:"type:text" json:"user_id"`
	SessionID        *string    `gorm:"type:text" json:"session_id"`
	IPAddress        *string    `gorm:"type:text" json:"ip_address"`
	UserAgent        *string    `gorm:"type:text" json:"user_agent"`
	Referer          *string    `gorm:"type:text" json:"referer"`
	URL              string     `gorm:"type:text;not null" json:"url"`
	Method           string     `gorm:"type:text;not null;default:'GET'" json:"method"`
	Status           int        `gorm:"type:integer;not null;default:200" json:"status"`
	Duration         *int64     `gorm:"type:bigint" json:"duration"` // milliseconds
	ResponseSize     *int64     `gorm:"type:bigint" json:"response_size"`
	Country          *string    `gorm:"type:text" json:"country"`
	City             *string    `gorm:"type:text" json:"city"`
	Device           *string    `gorm:"type:text" json:"device"`
	Browser          *string    `gorm:"type:text" json:"browser"`
	OS               *string    `gorm:"type:text" json:"os"`
	IsMobile         bool       `gorm:"type:boolean;not null;default:false" json:"is_mobile"`
	IsBot            bool       `gorm:"type:boolean;not null;default:false" json:"is_bot"`
	OrganizationID   *string    `gorm:"type:text" json:"organization_id"`
	SpaceID          *string    `gorm:"type:text" json:"space_id"`
	BaseID           *string    `gorm:"type:text" json:"base_id"`
	TableID          *string    `gorm:"type:text" json:"table_id"`
	RecordID         *string    `gorm:"type:text" json:"record_id"`
	Action           *string    `gorm:"type:text" json:"action"`
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
func (VisitLog) TableName() string {
	return "visit_log"
}

// BeforeCreate 创建前钩子
func (vl *VisitLog) BeforeCreate(tx *gorm.DB) error {
	if vl.CreatedTime.IsZero() {
		vl.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (vl *VisitLog) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	vl.LastModifiedTime = &now
	return nil
}

// Subscription 订阅表
type Subscription struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Type             string     `gorm:"type:text;not null" json:"type"`                       // newsletter, updates, notifications, etc.
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"`    // active, inactive, cancelled
	Frequency        string     `gorm:"type:text;not null;default:'weekly'" json:"frequency"` // daily, weekly, monthly, quarterly
	IsPublic         bool       `gorm:"type:boolean;not null;default:true" json:"is_public"`
	IsDefault        bool       `gorm:"type:boolean;not null;default:false" json:"is_default"`
	SubscriberCount  int        `gorm:"type:integer;not null;default:0" json:"subscriber_count"`
	Tags             *string    `gorm:"type:jsonb" json:"tags"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (Subscription) TableName() string {
	return "subscription"
}

// BeforeCreate 创建前钩子
func (s *Subscription) BeforeCreate(tx *gorm.DB) error {
	if s.CreatedTime.IsZero() {
		s.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (s *Subscription) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	s.LastModifiedTime = &now
	return nil
}

// SpaceAdvancedSetting 空间高级设置表
type SpaceAdvancedSetting struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	SpaceID          string     `gorm:"type:text;not null" json:"space_id"`
	Key              string     `gorm:"type:text;not null" json:"key"`
	Value            *string    `gorm:"type:text" json:"value"`
	Type             string     `gorm:"type:text;not null;default:'string'" json:"type"` // string, number, boolean, json
	Description      *string    `gorm:"type:text" json:"description"`
	IsPublic         bool       `gorm:"type:boolean;not null;default:false" json:"is_public"`
	IsEncrypted      bool       `gorm:"type:boolean;not null;default:false" json:"is_encrypted"`
	Category         *string    `gorm:"type:text" json:"category"`
	SortOrder        int        `gorm:"type:integer;not null;default:0" json:"sort_order"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Space *Space `gorm:"foreignKey:SpaceID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"space,omitempty"`
}

// TableName 返回表名
func (SpaceAdvancedSetting) TableName() string {
	return "space_advanced_setting"
}

// BeforeCreate 创建前钩子
func (sas *SpaceAdvancedSetting) BeforeCreate(tx *gorm.DB) error {
	if sas.CreatedTime.IsZero() {
		sas.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (sas *SpaceAdvancedSetting) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	sas.LastModifiedTime = &now
	return nil
}
