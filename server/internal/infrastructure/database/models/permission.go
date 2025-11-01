package models

import (
	"time"
)

// Permission 权限模型
type Permission struct {
	ID           string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	UserID       string     `gorm:"not null;type:varchar(30);index" json:"user_id"`
	ResourceType string     `gorm:"not null;type:varchar(50);index" json:"resource_type"`
	ResourceID   string     `gorm:"not null;type:varchar(30);index" json:"resource_id"`
	Role         string     `gorm:"not null;type:varchar(50)" json:"role"`
	GrantedBy    string     `gorm:"not null;type:varchar(30)" json:"granted_by"`
	GrantedAt    time.Time  `gorm:"not null" json:"granted_at"`
	ExpiresAt    *time.Time `gorm:"index" json:"expires_at,omitempty"`
	IsActive     bool       `gorm:"default:true;index" json:"is_active"`
	CreatedAt    time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time  `gorm:"autoUpdateTime" json:"updated_at"`

	// 复合索引
	// UNIQUE(user_id, resource_type, resource_id)
}

// TableName 指定表名
func (Permission) TableName() string {
	return "permissions"
}

// BaseCollaborator 基础表协作者模型
type BaseCollaborator struct {
	ID        string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	BaseID    string     `gorm:"not null;type:varchar(30);index" json:"base_id"`
	UserID    string     `gorm:"not null;type:varchar(30);index" json:"user_id"`
	Role      string     `gorm:"not null;type:varchar(50)" json:"role"`
	Email     *string    `gorm:"type:varchar(255)" json:"email,omitempty"`
	InvitedBy string     `gorm:"not null;type:varchar(30)" json:"invited_by"`
	InvitedAt time.Time  `gorm:"not null" json:"invited_at"`
	JoinedAt  *time.Time `json:"joined_at,omitempty"`
	IsActive  bool       `gorm:"default:true;index" json:"is_active"`
	CreatedAt time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time  `gorm:"autoUpdateTime" json:"updated_at"`

	// 复合索引
	// UNIQUE(base_id, user_id)
}

// TableName 指定表名
func (BaseCollaborator) TableName() string {
	return "base_collaborators"
}

// Collaborator（与 Prisma 一致的通用协作表）
type Collaborator struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	RoleName         string     `gorm:"column:role_name;type:varchar(50);not null" json:"role_name"`
	ResourceType     string     `gorm:"column:resource_type;type:varchar(50);not null;uniqueIndex:uq_collab_rt_rid_pid_pt,priority:4" json:"resource_type"`
	ResourceID       string     `gorm:"column:resource_id;type:varchar(30);not null;index;uniqueIndex:uq_collab_rt_rid_pid_pt,priority:3" json:"resource_id"`
	PrincipalID      string     `gorm:"column:principal_id;type:varchar(30);not null;index;uniqueIndex:uq_collab_rt_rid_pid_pt,priority:1" json:"principal_id"`
	PrincipalType    string     `gorm:"column:principal_type;type:varchar(50);not null;uniqueIndex:uq_collab_rt_rid_pid_pt,priority:2" json:"principal_type"`
	CreatedBy        string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
}

func (Collaborator) TableName() string { return "collaborator" }

// Invitation（与 Prisma 一致）
type Invitation struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	BaseID           *string    `gorm:"column:base_id;type:varchar(30);index" json:"base_id"`
	SpaceID          *string    `gorm:"column:space_id;type:varchar(30);index" json:"space_id"`
	Type             string     `gorm:"column:type;type:varchar(50);not null" json:"type"`
	Role             string     `gorm:"type:varchar(50);not null" json:"role"`
	InvitationCode   string     `gorm:"column:invitation_code;type:varchar(100);not null" json:"invitation_code"`
	ExpiredTime      *time.Time `gorm:"column:expired_time" json:"expired_time"`
	CreatedBy        string     `gorm:"column:create_by;type:varchar(30);not null" json:"create_by"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"column:deleted_time" json:"deleted_time"`
}

func (Invitation) TableName() string { return "invitation" }

// InvitationRecord（与 Prisma 一致）
type InvitationRecord struct {
	ID           string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	InvitationID string    `gorm:"column:invitation_id;type:varchar(30);index;not null" json:"invitation_id"`
	BaseID       *string   `gorm:"column:base_id;type:varchar(30);index" json:"base_id"`
	SpaceID      *string   `gorm:"column:space_id;type:varchar(30);index" json:"space_id"`
	Type         string    `gorm:"column:type;type:varchar(50);not null" json:"type"`
	Inviter      string    `gorm:"column:inviter;type:varchar(30);not null" json:"inviter"`
	Accepter     string    `gorm:"column:accepter;type:varchar(30);not null" json:"accepter"`
	CreatedTime  time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

func (InvitationRecord) TableName() string { return "invitation_record" }
