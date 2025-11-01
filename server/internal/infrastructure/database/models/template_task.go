package models

import (
	"time"
)

// Template 模板模型
type Template struct {
	ID                  string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	BaseID              *string   `gorm:"column:base_id;type:varchar(30)" json:"base_id"`
	Cover               *string   `gorm:"type:varchar(500)" json:"cover"`
	Name                *string   `gorm:"type:varchar(255)" json:"name"`
	Description         *string   `gorm:"type:text" json:"description"`
	MarkdownDescription *string   `gorm:"column:markdown_description;type:text" json:"markdown_description"`
	CategoryID          *string   `gorm:"column:category_id;type:varchar(30)" json:"category_id"`
	CreatedTime         time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy           string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedTime    time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy      *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	IsSystem            *bool     `gorm:"column:is_system" json:"is_system"`
	IsPublished         *bool     `gorm:"column:is_published" json:"is_published"`
	Snapshot            *string   `gorm:"type:text" json:"snapshot"`
	Order               float64   `gorm:"column:order;not null" json:"order"`
	UsageCount          int       `gorm:"column:usage_count;default:0" json:"usage_count"`
}

// TableName 指定表名
func (Template) TableName() string {
	return "template"
}

// TemplateCategory 模板分类模型
type TemplateCategory struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string    `gorm:"type:varchar(255);unique;not null" json:"name"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`
	Order            float64   `gorm:"column:order;not null" json:"order"`
}

// TableName 指定表名
func (TemplateCategory) TableName() string {
	return "template_category"
}

// Task 任务模型
type Task struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Type             string    `gorm:"column:type;type:varchar(50);not null" json:"type"`
	Status           string    `gorm:"column:status;type:varchar(20);not null" json:"status"`
	Snapshot         *string   `gorm:"type:text" json:"snapshot"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`

	// 关联关系
	Runs []TaskRun `gorm:"foreignKey:TaskID" json:"runs,omitempty"`
}

// TableName 指定表名
func (Task) TableName() string {
	return "task"
}

// TaskRun 任务运行模型
type TaskRun struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TaskID           string     `gorm:"column:task_id;type:varchar(30);not null" json:"task_id"`
	Status           string     `gorm:"column:status;type:varchar(20);not null" json:"status"`
	Snapshot         string     `gorm:"type:text;not null" json:"snapshot"`
	Spent            *int       `gorm:"type:integer" json:"spent"`
	ErrorMsg         *string    `gorm:"column:error_msg;type:text" json:"error_msg"`
	StartedTime      *time.Time `gorm:"column:started_time" json:"started_time"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time  `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`

	// 关联关系
	Task Task `gorm:"foreignKey:TaskID;references:ID" json:"task,omitempty"`
}

// TableName 指定表名
func (TaskRun) TableName() string {
	return "task_run"
}

// TaskReference 任务引用模型
type TaskReference struct {
	ID          string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	FromFieldID string    `gorm:"column:from_field_id;type:varchar(30);not null" json:"from_field_id"`
	ToFieldID   string    `gorm:"column:to_field_id;type:varchar(30);not null" json:"to_field_id"`
	CreatedTime time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
}

// TableName 指定表名
func (TaskReference) TableName() string {
	return "task_reference"
}
