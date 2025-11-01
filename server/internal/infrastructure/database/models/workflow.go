package models

import (
	"time"

	"gorm.io/gorm"
)

// Workflow 工作流表
type Workflow struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Type             string     `gorm:"type:text;not null" json:"type"`                   // automation, approval, data_processing, etc.
	Status           string     `gorm:"type:text;not null;default:'draft'" json:"status"` // draft, active, paused, archived
	Version          int        `gorm:"type:integer;not null;default:1" json:"version"`
	IsActive         bool       `gorm:"type:boolean;not null;default:false" json:"is_active"`
	IsPublic         bool       `gorm:"type:boolean;not null;default:false" json:"is_public"`
	TriggerType      string     `gorm:"type:text;not null" json:"trigger_type"` // manual, scheduled, event, webhook
	TriggerConfig    *string    `gorm:"type:jsonb" json:"trigger_config"`
	ExecutionMode    string     `gorm:"type:text;not null;default:'sequential'" json:"execution_mode"` // sequential, parallel
	Timeout          int        `gorm:"type:integer;not null;default:3600" json:"timeout"`             // seconds
	RetryCount       int        `gorm:"type:integer;not null;default:0" json:"retry_count"`
	MaxRetries       int        `gorm:"type:integer;not null;default:3" json:"max_retries"`
	Concurrency      int        `gorm:"type:integer;not null;default:1" json:"concurrency"`
	Tags             *string    `gorm:"type:jsonb" json:"tags"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	Nodes     []WorkflowNode     `gorm:"foreignKey:WorkflowID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"nodes,omitempty"`
	Runs      []WorkflowRun      `gorm:"foreignKey:WorkflowID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"runs,omitempty"`
	Snapshots []WorkflowSnapshot `gorm:"foreignKey:WorkflowID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"snapshots,omitempty"`
}

// TableName 返回表名
func (Workflow) TableName() string {
	return "workflow"
}

// BeforeCreate 创建前钩子
func (w *Workflow) BeforeCreate(tx *gorm.DB) error {
	if w.CreatedTime.IsZero() {
		w.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (w *Workflow) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	w.LastModifiedTime = &now
	return nil
}

// WorkflowNode 工作流节点表
type WorkflowNode struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	WorkflowID       string     `gorm:"type:text;not null" json:"workflow_id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Type             string     `gorm:"type:text;not null" json:"type"` // start, end, action, condition, loop, etc.
	Description      *string    `gorm:"type:text" json:"description"`
	Position         *string    `gorm:"type:jsonb" json:"position"` // x, y coordinates
	Config           *string    `gorm:"type:jsonb" json:"config"`
	InputSchema      *string    `gorm:"type:jsonb" json:"input_schema"`
	OutputSchema     *string    `gorm:"type:jsonb" json:"output_schema"`
	Condition        *string    `gorm:"type:jsonb" json:"condition"`
	Action           *string    `gorm:"type:jsonb" json:"action"`
	Timeout          int        `gorm:"type:integer;not null;default:300" json:"timeout"` // seconds
	RetryCount       int        `gorm:"type:integer;not null;default:0" json:"retry_count"`
	MaxRetries       int        `gorm:"type:integer;not null;default:3" json:"max_retries"`
	SortOrder        int        `gorm:"type:integer;not null;default:0" json:"sort_order"`
	IsEnabled        bool       `gorm:"type:boolean;not null;default:true" json:"is_enabled"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	Workflow *Workflow            `gorm:"foreignKey:WorkflowID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"workflow,omitempty"`
	Secrets  []WorkflowNodeSecret `gorm:"foreignKey:NodeID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"secrets,omitempty"`
	RunSteps []WorkflowRunStep    `gorm:"foreignKey:NodeID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"run_steps,omitempty"`
}

// TableName 返回表名
func (WorkflowNode) TableName() string {
	return "workflow_node"
}

// BeforeCreate 创建前钩子
func (wn *WorkflowNode) BeforeCreate(tx *gorm.DB) error {
	if wn.CreatedTime.IsZero() {
		wn.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (wn *WorkflowNode) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	wn.LastModifiedTime = &now
	return nil
}

// WorkflowNodeSecret 工作流节点密钥表
type WorkflowNodeSecret struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	NodeID           string     `gorm:"type:text;not null" json:"node_id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Value            string     `gorm:"type:text;not null" json:"value"`
	Type             string     `gorm:"type:text;not null;default:'text'" json:"type"` // text, password, token, key
	IsEncrypted      bool       `gorm:"type:boolean;not null;default:true" json:"is_encrypted"`
	Description      *string    `gorm:"type:text" json:"description"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Node *WorkflowNode `gorm:"foreignKey:NodeID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"node,omitempty"`
}

// TableName 返回表名
func (WorkflowNodeSecret) TableName() string {
	return "workflow_node_secret"
}

// BeforeCreate 创建前钩子
func (wns *WorkflowNodeSecret) BeforeCreate(tx *gorm.DB) error {
	if wns.CreatedTime.IsZero() {
		wns.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (wns *WorkflowNodeSecret) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	wns.LastModifiedTime = &now
	return nil
}

// WorkflowRun 工作流运行表
type WorkflowRun struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	WorkflowID       string     `gorm:"type:text;not null" json:"workflow_id"`
	TriggerType      string     `gorm:"type:text;not null" json:"trigger_type"`
	TriggerData      *string    `gorm:"type:jsonb" json:"trigger_data"`
	Status           string     `gorm:"type:text;not null;default:'pending'" json:"status"` // pending, running, completed, failed, cancelled
	Progress         int        `gorm:"type:integer;not null;default:0" json:"progress"`    // 0-100
	StartedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"started_time"`
	CompletedTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"completed_time"`
	Duration         *int64     `gorm:"type:bigint" json:"duration"` // milliseconds
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	Input            *string    `gorm:"type:jsonb" json:"input"`
	Output           *string    `gorm:"type:jsonb" json:"output"`
	Logs             *string    `gorm:"type:jsonb" json:"logs"`
	RetryCount       int        `gorm:"type:integer;not null;default:0" json:"retry_count"`
	MaxRetries       int        `gorm:"type:integer;not null;default:3" json:"max_retries"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Workflow *Workflow         `gorm:"foreignKey:WorkflowID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"workflow,omitempty"`
	Steps    []WorkflowRunStep `gorm:"foreignKey:RunID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"steps,omitempty"`
}

// TableName 返回表名
func (WorkflowRun) TableName() string {
	return "workflow_run"
}

// BeforeCreate 创建前钩子
func (wr *WorkflowRun) BeforeCreate(tx *gorm.DB) error {
	if wr.CreatedTime.IsZero() {
		wr.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (wr *WorkflowRun) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	wr.LastModifiedTime = &now
	return nil
}

// WorkflowRunStep 工作流运行步骤表
type WorkflowRunStep struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	RunID            string     `gorm:"type:text;not null" json:"run_id"`
	NodeID           string     `gorm:"type:text;not null" json:"node_id"`
	StepOrder        int        `gorm:"type:integer;not null" json:"step_order"`
	Status           string     `gorm:"type:text;not null;default:'pending'" json:"status"` // pending, running, completed, failed, skipped
	StartedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"started_time"`
	CompletedTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"completed_time"`
	Duration         *int64     `gorm:"type:bigint" json:"duration"` // milliseconds
	Input            *string    `gorm:"type:jsonb" json:"input"`
	Output           *string    `gorm:"type:jsonb" json:"output"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	Logs             *string    `gorm:"type:jsonb" json:"logs"`
	RetryCount       int        `gorm:"type:integer;not null;default:0" json:"retry_count"`
	MaxRetries       int        `gorm:"type:integer;not null;default:3" json:"max_retries"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Run  *WorkflowRun  `gorm:"foreignKey:RunID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"run,omitempty"`
	Node *WorkflowNode `gorm:"foreignKey:NodeID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"node,omitempty"`
}

// TableName 返回表名
func (WorkflowRunStep) TableName() string {
	return "workflow_run_step"
}

// BeforeCreate 创建前钩子
func (wrs *WorkflowRunStep) BeforeCreate(tx *gorm.DB) error {
	if wrs.CreatedTime.IsZero() {
		wrs.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (wrs *WorkflowRunStep) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	wrs.LastModifiedTime = &now
	return nil
}

// WorkflowSnapshot 工作流快照表
type WorkflowSnapshot struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	WorkflowID       string     `gorm:"type:text;not null" json:"workflow_id"`
	Version          int        `gorm:"type:integer;not null" json:"version"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Config           *string    `gorm:"type:jsonb" json:"config"`
	Nodes            *string    `gorm:"type:jsonb" json:"nodes"`
	Connections      *string    `gorm:"type:jsonb" json:"connections"`
	IsActive         bool       `gorm:"type:boolean;not null;default:false" json:"is_active"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Workflow *Workflow `gorm:"foreignKey:WorkflowID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"workflow,omitempty"`
}

// TableName 返回表名
func (WorkflowSnapshot) TableName() string {
	return "workflow_snapshot"
}

// BeforeCreate 创建前钩子
func (ws *WorkflowSnapshot) BeforeCreate(tx *gorm.DB) error {
	if ws.CreatedTime.IsZero() {
		ws.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (ws *WorkflowSnapshot) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	ws.LastModifiedTime = &now
	return nil
}
