package models

import (
	"time"

	"gorm.io/gorm"
)

// Space 工作空间模型
type Space struct {
	ID               string         `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string         `gorm:"not null;type:varchar(255)" json:"name"`
	Description      *string        `gorm:"type:text" json:"description"`
	Icon             *string        `gorm:"type:varchar(100)" json:"icon"`
	CreatedBy        string         `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	CreatedTime      time.Time      `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	DeletedTime      gorm.DeletedAt `gorm:"column:deleted_time" json:"deleted_time,omitempty"`
	LastModifiedTime *time.Time     `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string        `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	Credit           *int           `gorm:"column:credit" json:"credit"`
	IsTemplate       *bool          `gorm:"column:is_template" json:"is_template"`
}

func (Space) TableName() string {
	return "space"
}

// SpaceCollaborator 空间协作者模型
type SpaceCollaborator struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	SpaceID     string    `gorm:"column:space_id;index;type:varchar(30);not null" json:"space_id"`
	UserID      string    `gorm:"column:user_id;index;type:varchar(30);not null" json:"user_id"`
	Role        string    `gorm:"type:varchar(20);not null" json:"role"` // owner, editor, viewer
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

func (SpaceCollaborator) TableName() string {
	return "space_collaborator"
}
