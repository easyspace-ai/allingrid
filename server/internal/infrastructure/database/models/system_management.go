package models

import (
	"time"

	"gorm.io/gorm"
)

// App 应用表
type App struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Type             string     `gorm:"type:text;not null" json:"type"`                    // web, mobile, desktop, api
	Platform         string     `gorm:"type:text;not null" json:"platform"`                // web, ios, android, windows, mac, linux
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"` // active, inactive, deprecated
	Version          string     `gorm:"type:text;not null;default:'1.0.0'" json:"version"`
	BuildNumber      *string    `gorm:"type:text" json:"build_number"`
	BundleID         *string    `gorm:"type:text" json:"bundle_id"`
	PackageName      *string    `gorm:"type:text" json:"package_name"`
	AppStoreURL      *string    `gorm:"type:text" json:"app_store_url"`
	PlayStoreURL     *string    `gorm:"type:text" json:"play_store_url"`
	DownloadURL      *string    `gorm:"type:text" json:"download_url"`
	Icon             *string    `gorm:"type:text" json:"icon"`
	Screenshot       *string    `gorm:"type:jsonb" json:"screenshot"`
	Features         *string    `gorm:"type:jsonb" json:"features"`
	Requirements     *string    `gorm:"type:jsonb" json:"requirements"`
	Permissions      *string    `gorm:"type:jsonb" json:"permissions"`
	IsPublic         bool       `gorm:"type:boolean;not null;default:true" json:"is_public"`
	IsFeatured       bool       `gorm:"type:boolean;not null;default:false" json:"is_featured"`
	DownloadCount    int        `gorm:"type:integer;not null;default:0" json:"download_count"`
	Rating           float64    `gorm:"type:decimal(3,2);not null;default:0" json:"rating"`
	ReviewCount      int        `gorm:"type:integer;not null;default:0" json:"review_count"`
	Size             *int64     `gorm:"type:bigint" json:"size"` // bytes
	MinOSVersion     *string    `gorm:"type:text" json:"min_os_version"`
	Tags             *string    `gorm:"type:jsonb" json:"tags"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	Versions []AppVersion `gorm:"foreignKey:AppID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"versions,omitempty"`
}

// TableName 返回表名
func (App) TableName() string {
	return "app"
}

// BeforeCreate 创建前钩子
func (a *App) BeforeCreate(tx *gorm.DB) error {
	if a.CreatedTime.IsZero() {
		a.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (a *App) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	a.LastModifiedTime = &now
	return nil
}

// AppVersion 应用版本表
type AppVersion struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	AppID            string     `gorm:"type:text;not null" json:"app_id"`
	Version          string     `gorm:"type:text;not null" json:"version"`
	BuildNumber      *string    `gorm:"type:text" json:"build_number"`
	ReleaseNotes     *string    `gorm:"type:text" json:"release_notes"`
	Status           string     `gorm:"type:text;not null;default:'draft'" json:"status"` // draft, testing, released, deprecated
	Type             string     `gorm:"type:text;not null;default:'release'" json:"type"` // release, beta, alpha
	Size             *int64     `gorm:"type:bigint" json:"size"`                          // bytes
	Checksum         *string    `gorm:"type:text" json:"checksum"`
	DownloadURL      *string    `gorm:"type:text" json:"download_url"`
	MinOSVersion     *string    `gorm:"type:text" json:"min_os_version"`
	MaxOSVersion     *string    `gorm:"type:text" json:"max_os_version"`
	IsForced         bool       `gorm:"type:boolean;not null;default:false" json:"is_forced"`
	IsCritical       bool       `gorm:"type:boolean;not null;default:false" json:"is_critical"`
	ReleaseDate      *time.Time `gorm:"type:timestamp(3) without time zone" json:"release_date"`
	ExpiryDate       *time.Time `gorm:"type:timestamp(3) without time zone" json:"expiry_date"`
	DownloadCount    int        `gorm:"type:integer;not null;default:0" json:"download_count"`
	InstallCount     int        `gorm:"type:integer;not null;default:0" json:"install_count"`
	CrashCount       int        `gorm:"type:integer;not null;default:0" json:"crash_count"`
	Rating           float64    `gorm:"type:decimal(3,2);not null;default:0" json:"rating"`
	ReviewCount      int        `gorm:"type:integer;not null;default:0" json:"review_count"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	App *App `gorm:"foreignKey:AppID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"app,omitempty"`
}

// TableName 返回表名
func (AppVersion) TableName() string {
	return "app_version"
}

// BeforeCreate 创建前钩子
func (av *AppVersion) BeforeCreate(tx *gorm.DB) error {
	if av.CreatedTime.IsZero() {
		av.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (av *AppVersion) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	av.LastModifiedTime = &now
	return nil
}

// Authentication 认证表
type Authentication struct {
	ID                 string     `gorm:"primaryKey;type:text;not null" json:"id"`
	UserID             string     `gorm:"type:text;not null" json:"user_id"`
	Provider           string     `gorm:"type:text;not null" json:"provider"` // email, google, github, etc.
	ProviderID         string     `gorm:"type:text;not null" json:"provider_id"`
	ProviderData       *string    `gorm:"type:jsonb" json:"provider_data"`
	AccessToken        *string    `gorm:"type:text" json:"access_token"`
	RefreshToken       *string    `gorm:"type:text" json:"refresh_token"`
	TokenExpiry        *time.Time `gorm:"type:timestamp(3) without time zone" json:"token_expiry"`
	IsVerified         bool       `gorm:"type:boolean;not null;default:false" json:"is_verified"`
	VerificationCode   *string    `gorm:"type:text" json:"verification_code"`
	VerificationExpiry *time.Time `gorm:"type:timestamp(3) without time zone" json:"verification_expiry"`
	LastLoginTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_login_time"`
	LoginCount         int        `gorm:"type:integer;not null;default:0" json:"login_count"`
	FailedLoginCount   int        `gorm:"type:integer;not null;default:0" json:"failed_login_count"`
	LastFailedLogin    *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_failed_login"`
	IsLocked           bool       `gorm:"type:boolean;not null;default:false" json:"is_locked"`
	LockedUntil        *time.Time `gorm:"type:timestamp(3) without time zone" json:"locked_until"`
	Metadata           *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy          string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime        time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime   *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy     *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime        *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	User *User `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"user,omitempty"`
}

// TableName 返回表名
func (Authentication) TableName() string {
	return "authentication"
}

// BeforeCreate 创建前钩子
func (a *Authentication) BeforeCreate(tx *gorm.DB) error {
	if a.CreatedTime.IsZero() {
		a.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (a *Authentication) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	a.LastModifiedTime = &now
	return nil
}

// DomainVerification 域名验证表
type DomainVerification struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Domain           string     `gorm:"type:text;not null" json:"domain"`
	VerificationType string     `gorm:"type:text;not null" json:"verification_type"` // dns, file, html
	VerificationCode string     `gorm:"type:text;not null" json:"verification_code"`
	VerificationFile *string    `gorm:"type:text" json:"verification_file"`
	VerificationPath *string    `gorm:"type:text" json:"verification_path"`
	Status           string     `gorm:"type:text;not null;default:'pending'" json:"status"` // pending, verified, failed, expired
	VerifiedTime     *time.Time `gorm:"type:timestamp(3) without time zone" json:"verified_time"`
	ExpiryTime       *time.Time `gorm:"type:timestamp(3) without time zone" json:"expiry_time"`
	LastCheckedTime  *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_checked_time"`
	CheckCount       int        `gorm:"type:integer;not null;default:0" json:"check_count"`
	MaxChecks        int        `gorm:"type:integer;not null;default:10" json:"max_checks"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
}

// TableName 返回表名
func (DomainVerification) TableName() string {
	return "domain_verification"
}

// BeforeCreate 创建前钩子
func (dv *DomainVerification) BeforeCreate(tx *gorm.DB) error {
	if dv.CreatedTime.IsZero() {
		dv.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (dv *DomainVerification) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	dv.LastModifiedTime = &now
	return nil
}
