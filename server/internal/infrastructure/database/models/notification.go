package models

import (
	"time"

	"gorm.io/gorm"
)

// Notification 通知模型
type Notification struct {
	ID          string     `gorm:"primaryKey;type:varchar(20)" json:"id"`
	UserID      string     `gorm:"type:varchar(20);not null;index" json:"user_id"`
	Type        string     `gorm:"type:varchar(50);not null;index" json:"type"`
	Title       string     `gorm:"type:varchar(255);not null" json:"title"`
	Content     string     `gorm:"type:text;not null" json:"content"`
	Data        string     `gorm:"type:json" json:"data"` // JSON格式存储
	Status      string     `gorm:"type:varchar(20);not null;default:'unread';index" json:"status"`
	Priority    string     `gorm:"type:varchar(20);not null;default:'normal';index" json:"priority"`
	SourceID    string     `gorm:"type:varchar(20);index" json:"source_id"`
	SourceType  string     `gorm:"type:varchar(50);index" json:"source_type"`
	ActionURL   string     `gorm:"type:varchar(500)" json:"action_url"`
	ExpiresAt   *time.Time `gorm:"index" json:"expires_at"`
	ReadAt      *time.Time `json:"read_at"`
	CreatedTime time.Time  `gorm:"autoCreateTime;index" json:"created_time"`
	UpdatedTime time.Time  `gorm:"autoUpdateTime" json:"updated_time"`
}

// TableName 返回表名
func (Notification) TableName() string {
	return "notifications"
}

// NotificationTemplate 通知模板模型
type NotificationTemplate struct {
	ID          string    `gorm:"primaryKey;type:varchar(20)" json:"id"`
	Type        string    `gorm:"type:varchar(50);not null;uniqueIndex" json:"type"`
	Name        string    `gorm:"type:varchar(100);not null" json:"name"`
	Title       string    `gorm:"type:varchar(255);not null" json:"title"`
	Content     string    `gorm:"type:text;not null" json:"content"`
	Variables   string    `gorm:"type:json" json:"variables"`    // JSON格式存储
	DefaultData string    `gorm:"type:json" json:"default_data"` // JSON格式存储
	IsActive    bool      `gorm:"default:true;index" json:"is_active"`
	CreatedTime time.Time `gorm:"autoCreateTime" json:"created_time"`
	UpdatedTime time.Time `gorm:"autoUpdateTime" json:"updated_time"`
}

// TableName 返回表名
func (NotificationTemplate) TableName() string {
	return "notification_templates"
}

// NotificationSubscription 通知订阅模型
type NotificationSubscription struct {
	ID          string    `gorm:"primaryKey;type:varchar(20)" json:"id"`
	UserID      string    `gorm:"type:varchar(20);not null;index" json:"user_id"`
	Type        string    `gorm:"type:varchar(50);not null;index" json:"type"`
	SourceID    string    `gorm:"type:varchar(20);index" json:"source_id"`
	SourceType  string    `gorm:"type:varchar(50);index" json:"source_type"`
	Channels    string    `gorm:"type:json;not null" json:"channels"` // JSON格式存储
	Settings    string    `gorm:"type:json" json:"settings"`          // JSON格式存储
	IsActive    bool      `gorm:"default:true;index" json:"is_active"`
	CreatedTime time.Time `gorm:"autoCreateTime" json:"created_time"`
	UpdatedTime time.Time `gorm:"autoUpdateTime" json:"updated_time"`
}

// TableName 返回表名
func (NotificationSubscription) TableName() string {
	return "notification_subscriptions"
}

// BeforeCreate 创建前钩子
func (n *Notification) BeforeCreate(tx *gorm.DB) error {
	if n.ID == "" {
		// 这里应该生成ID，但通常由应用层处理
	}
	return nil
}

// BeforeCreate 创建前钩子
func (nt *NotificationTemplate) BeforeCreate(tx *gorm.DB) error {
	if nt.ID == "" {
		// 这里应该生成ID，但通常由应用层处理
	}
	return nil
}

// BeforeCreate 创建前钩子
func (ns *NotificationSubscription) BeforeCreate(tx *gorm.DB) error {
	if ns.ID == "" {
		// 这里应该生成ID，但通常由应用层处理
	}
	return nil
}

// NotificationDelivery 通知发送记录表
type NotificationDelivery struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	NotificationID   string     `gorm:"type:text;not null" json:"notification_id"`
	UserID           string     `gorm:"type:text;not null" json:"user_id"`
	Channel          string     `gorm:"type:text;not null" json:"channel"`                  // email, push, sms, webhook
	Status           string     `gorm:"type:text;not null;default:'pending'" json:"status"` // pending, sent, delivered, failed
	ExternalID       *string    `gorm:"type:text" json:"external_id"`
	SentTime         *time.Time `gorm:"type:timestamp(3) without time zone" json:"sent_time"`
	DeliveredTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"delivered_time"`
	ReadTime         *time.Time `gorm:"type:timestamp(3) without time zone" json:"read_time"`
	ErrorCode        *string    `gorm:"type:text" json:"error_code"`
	ErrorMessage     *string    `gorm:"type:text" json:"error_message"`
	RetryCount       int        `gorm:"type:integer;not null;default:0" json:"retry_count"`
	MaxRetries       int        `gorm:"type:integer;not null;default:3" json:"max_retries"`
	NextRetryTime    *time.Time `gorm:"type:timestamp(3) without time zone" json:"next_retry_time"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Notification *Notification `gorm:"foreignKey:NotificationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"notification,omitempty"`
	User         *User         `gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"user,omitempty"`
}

// TableName 返回表名
func (NotificationDelivery) TableName() string {
	return "notification_delivery"
}

// BeforeCreate 创建前钩子
func (nd *NotificationDelivery) BeforeCreate(tx *gorm.DB) error {
	if nd.CreatedTime.IsZero() {
		nd.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (nd *NotificationDelivery) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	nd.LastModifiedTime = &now
	return nil
}
