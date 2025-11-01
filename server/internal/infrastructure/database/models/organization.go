package models

import (
	"time"

	"gorm.io/gorm"
)

// Organization 组织表
type Organization struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Logo             *string    `gorm:"type:text" json:"logo"`
	Domain           *string    `gorm:"type:text" json:"domain"`
	ContactEmail     *string    `gorm:"type:text" json:"contact_email"`
	ContactPhone     *string    `gorm:"type:text" json:"contact_phone"`
	Address          *string    `gorm:"type:text" json:"address"`
	Website          *string    `gorm:"type:text" json:"website"`
	Industry         *string    `gorm:"type:text" json:"industry"`
	Size             *string    `gorm:"type:text" json:"size"`
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"`
	Plan             *string    `gorm:"type:text" json:"plan"`
	SubscriptionID   *string    `gorm:"type:text" json:"subscription_id"`
	MaxUsers         *int       `gorm:"type:integer" json:"max_users"`
	MaxSpaces        *int       `gorm:"type:integer" json:"max_spaces"`
	MaxStorage       *int64     `gorm:"type:bigint" json:"max_storage"`
	UsedStorage      int64      `gorm:"type:bigint;default:0" json:"used_storage"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Settings         *string    `gorm:"type:jsonb" json:"settings"`
	BillingInfo      *string    `gorm:"type:jsonb" json:"billing_info"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Departments []OrganizationDepartment `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"departments,omitempty"`
	Spaces      []OrganizationSpace      `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"spaces,omitempty"`
	Users       []OrganizationUser       `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"users,omitempty"`
	SettingsRel []OrganizationSetting    `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"settings_rel,omitempty"`
}

// TableName 返回表名
func (Organization) TableName() string {
	return "organization"
}

// BeforeCreate 创建前钩子
func (o *Organization) BeforeCreate(tx *gorm.DB) error {
	if o.CreatedTime.IsZero() {
		o.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (o *Organization) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	o.LastModifiedTime = &now
	return nil
}

// OrganizationDepartment 组织部门表
type OrganizationDepartment struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	OrganizationID   string     `gorm:"type:text;not null" json:"organization_id"`
	ParentID         *string    `gorm:"type:text" json:"parent_id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Code             *string    `gorm:"type:text" json:"code"`
	Level            int        `gorm:"type:integer;not null;default:1" json:"level"`
	Path             *string    `gorm:"type:text" json:"path"`
	SortOrder        int        `gorm:"type:integer;not null;default:0" json:"sort_order"`
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Settings         *string    `gorm:"type:jsonb" json:"settings"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Organization *Organization                `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"organization,omitempty"`
	Parent       *OrganizationDepartment      `gorm:"foreignKey:ParentID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"parent,omitempty"`
	Children     []OrganizationDepartment     `gorm:"foreignKey:ParentID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"children,omitempty"`
	Users        []OrganizationUserDepartment `gorm:"foreignKey:DepartmentID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"users,omitempty"`
}

// TableName 返回表名
func (OrganizationDepartment) TableName() string {
	return "organization_department"
}

// BeforeCreate 创建前钩子
func (od *OrganizationDepartment) BeforeCreate(tx *gorm.DB) error {
	if od.CreatedTime.IsZero() {
		od.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (od *OrganizationDepartment) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	od.LastModifiedTime = &now
	return nil
}

// OrganizationSetting 组织设置表
type OrganizationSetting struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	OrganizationID   string     `gorm:"type:text;not null" json:"organization_id"`
	Key              string     `gorm:"type:text;not null" json:"key"`
	Value            *string    `gorm:"type:text" json:"value"`
	Type             string     `gorm:"type:text;not null;default:'string'" json:"type"`
	Description      *string    `gorm:"type:text" json:"description"`
	IsPublic         bool       `gorm:"type:boolean;not null;default:false" json:"is_public"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Organization *Organization `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"organization,omitempty"`
}

// TableName 返回表名
func (OrganizationSetting) TableName() string {
	return "organization_setting"
}

// BeforeCreate 创建前钩子
func (os *OrganizationSetting) BeforeCreate(tx *gorm.DB) error {
	if os.CreatedTime.IsZero() {
		os.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (os *OrganizationSetting) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	os.LastModifiedTime = &now
	return nil
}

// OrganizationSpace 组织空间表
type OrganizationSpace struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	OrganizationID   string     `gorm:"type:text;not null" json:"organization_id"`
	SpaceID          string     `gorm:"type:text;not null" json:"space_id"`
	Role             string     `gorm:"type:text;not null;default:'member'" json:"role"`
	Permissions      *string    `gorm:"type:jsonb" json:"permissions"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Organization *Organization `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"organization,omitempty"`
	Space        *Space        `gorm:"foreignKey:SpaceID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"space,omitempty"`
}

// TableName 返回表名
func (OrganizationSpace) TableName() string {
	return "organization_space"
}

// BeforeCreate 创建前钩子
func (os *OrganizationSpace) BeforeCreate(tx *gorm.DB) error {
	if os.CreatedTime.IsZero() {
		os.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (os *OrganizationSpace) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	os.LastModifiedTime = &now
	return nil
}

// OrganizationUser 组织用户表
type OrganizationUser struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	OrganizationID   string     `gorm:"type:text;not null" json:"organization_id"`
	UserID           string     `gorm:"type:text;not null" json:"user_id"`
	Role             string     `gorm:"type:text;not null;default:'member'" json:"role"`
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"`
	JoinedTime       time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"joined_time"`
	InvitedBy        *string    `gorm:"type:text" json:"invited_by"`
	InvitedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"invited_time"`
	AcceptedTime     *time.Time `gorm:"type:timestamp(3) without time zone" json:"accepted_time"`
	LastActiveTime   *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_active_time"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Organization *Organization                `gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"organization,omitempty"`
	User         *User                        `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"user,omitempty"`
	Departments  []OrganizationUserDepartment `gorm:"foreignKey:OrganizationUserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"departments,omitempty"`
}

// TableName 返回表名
func (OrganizationUser) TableName() string {
	return "organization_user"
}

// BeforeCreate 创建前钩子
func (ou *OrganizationUser) BeforeCreate(tx *gorm.DB) error {
	if ou.CreatedTime.IsZero() {
		ou.CreatedTime = time.Now()
	}
	if ou.JoinedTime.IsZero() {
		ou.JoinedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (ou *OrganizationUser) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	ou.LastModifiedTime = &now
	return nil
}

// OrganizationUserDepartment 组织用户部门表
type OrganizationUserDepartment struct {
	ID                 string     `gorm:"primaryKey;type:text;not null" json:"id"`
	OrganizationUserID string     `gorm:"type:text;not null" json:"organization_user_id"`
	DepartmentID       string     `gorm:"type:text;not null" json:"department_id"`
	Role               string     `gorm:"type:text;not null;default:'member'" json:"role"`
	IsPrimary          bool       `gorm:"type:boolean;not null;default:false" json:"is_primary"`
	JoinedTime         time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"joined_time"`
	LeftTime           *time.Time `gorm:"type:timestamp(3) without time zone" json:"left_time"`
	CreatedBy          string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime        time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime   *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy     *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime        *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata           *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	OrganizationUser *OrganizationUser       `gorm:"foreignKey:OrganizationUserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"organization_user,omitempty"`
	Department       *OrganizationDepartment `gorm:"foreignKey:DepartmentID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"department,omitempty"`
}

// TableName 返回表名
func (OrganizationUserDepartment) TableName() string {
	return "organization_user_department"
}

// BeforeCreate 创建前钩子
func (oud *OrganizationUserDepartment) BeforeCreate(tx *gorm.DB) error {
	if oud.CreatedTime.IsZero() {
		oud.CreatedTime = time.Now()
	}
	if oud.JoinedTime.IsZero() {
		oud.JoinedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (oud *OrganizationUserDepartment) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	oud.LastModifiedTime = &now
	return nil
}
