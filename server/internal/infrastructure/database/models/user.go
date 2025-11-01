package models

import (
	"time"

	"gorm.io/gorm"
)

// User 用户模型
type User struct {
	ID                   string         `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name                 string         `gorm:"not null;type:varchar(255)" json:"name"`
	Email                string         `gorm:"unique;not null;type:varchar(255)" json:"email"`
	Password             *string        `gorm:"type:varchar(255)" json:"-"`
	Salt                 *string        `gorm:"type:varchar(255)" json:"-"`
	Phone                *string        `gorm:"unique;type:varchar(50)" json:"phone"`
	Avatar               *string        `gorm:"type:varchar(500)" json:"avatar"`
	IsSystem             *bool          `gorm:"column:is_system;default:false" json:"is_system"`
	IsAdmin              *bool          `gorm:"column:is_admin;default:false" json:"is_admin"`
	IsTrialUsed          *bool          `gorm:"column:is_trial_used;default:false" json:"is_trial_used"`
	NotifyMeta           *string        `gorm:"type:text;column:notify_meta" json:"notify_meta"`
	LastSignTime         *time.Time     `gorm:"column:last_sign_time" json:"last_sign_time"`
	DeactivatedTime      *time.Time     `gorm:"column:deactivated_time" json:"deactivated_time"`
	CreatedTime          time.Time      `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	DeletedTime          gorm.DeletedAt `gorm:"column:deleted_time" json:"deleted_time,omitempty"`
	LastModifiedTime     *time.Time     `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	PermanentDeletedTime *time.Time     `gorm:"column:permanent_deleted_time" json:"permanent_deleted_time"`
	RefMeta              *string        `gorm:"type:text;column:ref_meta" json:"ref_meta"`

	// 关联关系
	Accounts []Account `gorm:"foreignKey:UserID" json:"accounts,omitempty"`
}

// TableName 指定表名
func (User) TableName() string {
	return "users"
}

// Account 账户模型(第三方登录)
type Account struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	UserID      string    `gorm:"column:user_id;not null" json:"user_id"`
	Type        string    `gorm:"not null" json:"type"`
	Provider    string    `gorm:"not null;index:uq_provider_provider_id,unique" json:"provider"`
	ProviderID  string    `gorm:"column:provider_id;not null;index:uq_provider_provider_id,unique" json:"provider_id"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`

	// 关联关系
	User User `gorm:"foreignKey:UserID;references:ID" json:"user,omitempty"`
}

// TableName 指定表名
func (Account) TableName() string {
	return "account"
}

// BeforeCreate GORM钩子 - 创建前
func (u *User) BeforeCreate(tx *gorm.DB) error {
	if u.CreatedTime.IsZero() {
		u.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate GORM钩子 - 更新前
func (u *User) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	u.LastModifiedTime = &now
	return nil
}

// IsDeleted 检查用户是否已删除
func (u *User) IsDeleted() bool {
	return u.DeletedTime.Valid
}

// IsActive 检查用户是否激活
func (u *User) IsActive() bool {
	return u.DeactivatedTime == nil && !u.IsDeleted()
}

// IsSuperAdmin 检查是否为超级管理员
func (u *User) IsSuperAdmin() bool {
	return u.IsSystem != nil && *u.IsSystem
}

// IsAdminUser 检查是否为管理员
func (u *User) IsAdminUser() bool {
	return u.IsAdmin != nil && *u.IsAdmin
}

// GetDisplayName 获取显示名称
func (u *User) GetDisplayName() string {
	if u.Name != "" {
		return u.Name
	}
	return u.Email
}

// SanitizeForJSON 清理敏感信息用于JSON序列化
func (u *User) SanitizeForJSON() *User {
	cleaned := *u
	cleaned.Password = nil
	cleaned.Salt = nil
	return &cleaned
}
