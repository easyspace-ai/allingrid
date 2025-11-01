package models

import (
	"time"

	"gorm.io/gorm"
)

// License 许可证表
type License struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Type             string     `gorm:"type:text;not null" json:"type"` // personal, team, enterprise
	Plan             string     `gorm:"type:text;not null" json:"plan"` // basic, pro, enterprise
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"`
	MaxUsers         int        `gorm:"type:integer;not null;default:1" json:"max_users"`
	MaxSpaces        int        `gorm:"type:integer;not null;default:1" json:"max_spaces"`
	MaxStorage       int64      `gorm:"type:bigint;not null;default:1073741824" json:"max_storage"`  // 1GB in bytes
	MaxFileSize      int64      `gorm:"type:bigint;not null;default:104857600" json:"max_file_size"` // 100MB in bytes
	Features         *string    `gorm:"type:jsonb" json:"features"`
	Price            float64    `gorm:"type:decimal(10,2);not null;default:0" json:"price"`
	Currency         string     `gorm:"type:text;not null;default:'USD'" json:"currency"`
	BillingCycle     string     `gorm:"type:text;not null;default:'monthly'" json:"billing_cycle"` // monthly, yearly
	IsPublic         bool       `gorm:"type:boolean;not null;default:true" json:"is_public"`
	IsActive         bool       `gorm:"type:boolean;not null;default:true" json:"is_active"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Customers     []LicenseCustomer     `gorm:"foreignKey:LicenseID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"customers,omitempty"`
	Subscriptions []LicenseSubscription `gorm:"foreignKey:LicenseID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"subscriptions,omitempty"`
}

// TableName 返回表名
func (License) TableName() string {
	return "license"
}

// BeforeCreate 创建前钩子
func (l *License) BeforeCreate(tx *gorm.DB) error {
	if l.CreatedTime.IsZero() {
		l.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (l *License) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	l.LastModifiedTime = &now
	return nil
}

// LicenseCustomer 许可证客户表
type LicenseCustomer struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	LicenseID        string     `gorm:"type:text;not null" json:"license_id"`
	UserID           string     `gorm:"type:text;not null" json:"user_id"`
	OrganizationID   *string    `gorm:"type:text" json:"organization_id"`
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"`
	StartDate        time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"start_date"`
	EndDate          *time.Time `gorm:"type:timestamp(3) without time zone" json:"end_date"`
	IsTrial          bool       `gorm:"type:boolean;not null;default:false" json:"is_trial"`
	TrialEndDate     *time.Time `gorm:"type:timestamp(3) without time zone" json:"trial_end_date"`
	AutoRenew        bool       `gorm:"type:boolean;not null;default:true" json:"auto_renew"`
	BillingEmail     *string    `gorm:"type:text" json:"billing_email"`
	PaymentMethodID  *string    `gorm:"type:text" json:"payment_method_id"`
	LastPaymentDate  *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_payment_date"`
	NextBillingDate  *time.Time `gorm:"type:timestamp(3) without time zone" json:"next_billing_date"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	License       *License              `gorm:"foreignKey:LicenseID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"license,omitempty"`
	User          *User                 `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"user,omitempty"`
	Organization  *Organization         `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"organization,omitempty"`
	Subscriptions []LicenseSubscription `gorm:"foreignKey:CustomerID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"subscriptions,omitempty"`
}

// TableName 返回表名
func (LicenseCustomer) TableName() string {
	return "license_customer"
}

// BeforeCreate 创建前钩子
func (lc *LicenseCustomer) BeforeCreate(tx *gorm.DB) error {
	if lc.CreatedTime.IsZero() {
		lc.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (lc *LicenseCustomer) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	lc.LastModifiedTime = &now
	return nil
}

// LicenseSubscription 许可证订阅表
type LicenseSubscription struct {
	ID                   string     `gorm:"primaryKey;type:text;not null" json:"id"`
	LicenseID            string     `gorm:"type:text;not null" json:"license_id"`
	CustomerID           string     `gorm:"type:text;not null" json:"customer_id"`
	Status               string     `gorm:"type:text;not null;default:'active'" json:"status"`
	Plan                 string     `gorm:"type:text;not null" json:"plan"`
	BillingCycle         string     `gorm:"type:text;not null;default:'monthly'" json:"billing_cycle"`
	Price                float64    `gorm:"type:decimal(10,2);not null" json:"price"`
	Currency             string     `gorm:"type:text;not null;default:'USD'" json:"currency"`
	StartDate            time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"start_date"`
	EndDate              *time.Time `gorm:"type:timestamp(3) without time zone" json:"end_date"`
	NextBillingDate      *time.Time `gorm:"type:timestamp(3) without time zone" json:"next_billing_date"`
	LastBillingDate      *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_billing_date"`
	PaymentMethodID      *string    `gorm:"type:text" json:"payment_method_id"`
	StripeSubscriptionID *string    `gorm:"type:text" json:"stripe_subscription_id"`
	StripeCustomerID     *string    `gorm:"type:text" json:"stripe_customer_id"`
	AutoRenew            bool       `gorm:"type:boolean;not null;default:true" json:"auto_renew"`
	CancelAtPeriodEnd    bool       `gorm:"type:boolean;not null;default:false" json:"cancel_at_period_end"`
	CancelledAt          *time.Time `gorm:"type:timestamp(3) without time zone" json:"cancelled_at"`
	CancelReason         *string    `gorm:"type:text" json:"cancel_reason"`
	CreatedBy            string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime          time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime     *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy       *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime          *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata             *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	License  *License         `gorm:"foreignKey:LicenseID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"license,omitempty"`
	Customer *LicenseCustomer `gorm:"foreignKey:CustomerID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"customer,omitempty"`
}

// TableName 返回表名
func (LicenseSubscription) TableName() string {
	return "license_subscription"
}

// BeforeCreate 创建前钩子
func (ls *LicenseSubscription) BeforeCreate(tx *gorm.DB) error {
	if ls.CreatedTime.IsZero() {
		ls.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (ls *LicenseSubscription) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	ls.LastModifiedTime = &now
	return nil
}

// EnterpriseLicense 企业许可证表
type EnterpriseLicense struct {
	ID                string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name              string     `gorm:"type:text;not null" json:"name"`
	Description       *string    `gorm:"type:text" json:"description"`
	CompanyName       string     `gorm:"type:text;not null" json:"company_name"`
	ContactEmail      string     `gorm:"type:text;not null" json:"contact_email"`
	ContactPhone      *string    `gorm:"type:text" json:"contact_phone"`
	MaxUsers          int        `gorm:"type:integer;not null;default:100" json:"max_users"`
	MaxSpaces         int        `gorm:"type:integer;not null;default:50" json:"max_spaces"`
	MaxStorage        int64      `gorm:"type:bigint;not null;default:107374182400" json:"max_storage"` // 100GB in bytes
	MaxFileSize       int64      `gorm:"type:bigint;not null;default:1073741824" json:"max_file_size"` // 1GB in bytes
	Features          *string    `gorm:"type:jsonb" json:"features"`
	CustomDomain      *string    `gorm:"type:text" json:"custom_domain"`
	SSOEnabled        bool       `gorm:"type:boolean;not null;default:false" json:"sso_enabled"`
	APIEnabled        bool       `gorm:"type:boolean;not null;default:false" json:"api_enabled"`
	WhiteLabelEnabled bool       `gorm:"type:boolean;not null;default:false" json:"white_label_enabled"`
	SupportLevel      string     `gorm:"type:text;not null;default:'standard'" json:"support_level"`
	Status            string     `gorm:"type:text;not null;default:'active'" json:"status"`
	StartDate         time.Time  `gorm:"type:timestamp(3) without time zone;not null" json:"start_date"`
	EndDate           *time.Time `gorm:"type:timestamp(3) without time zone" json:"end_date"`
	Price             float64    `gorm:"type:decimal(10,2);not null" json:"price"`
	Currency          string     `gorm:"type:text;not null;default:'USD'" json:"currency"`
	BillingCycle      string     `gorm:"type:text;not null;default:'yearly'" json:"billing_cycle"`
	CreatedBy         string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime       time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime  *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy    *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime       *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata          *string    `gorm:"type:jsonb" json:"metadata"`
}

// TableName 返回表名
func (EnterpriseLicense) TableName() string {
	return "enterprise_license"
}

// BeforeCreate 创建前钩子
func (el *EnterpriseLicense) BeforeCreate(tx *gorm.DB) error {
	if el.CreatedTime.IsZero() {
		el.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (el *EnterpriseLicense) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	el.LastModifiedTime = &now
	return nil
}
