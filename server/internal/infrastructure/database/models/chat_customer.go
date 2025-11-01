package models

import (
	"time"

	"gorm.io/gorm"
)

// Chat 聊天表
type Chat struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             *string    `gorm:"type:text" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	Type             string     `gorm:"type:text;not null;default:'direct'" json:"type"`   // direct, group, channel
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"` // active, archived, deleted
	IsPrivate        bool       `gorm:"type:boolean;not null;default:false" json:"is_private"`
	IsPinned         bool       `gorm:"type:boolean;not null;default:false" json:"is_pinned"`
	LastMessageID    *string    `gorm:"type:text" json:"last_message_id"`
	LastMessageTime  *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_message_time"`
	UnreadCount      int        `gorm:"type:integer;not null;default:0" json:"unread_count"`
	ParticipantCount int        `gorm:"type:integer;not null;default:0" json:"participant_count"`
	Avatar           *string    `gorm:"type:text" json:"avatar"`
	Tags             *string    `gorm:"type:jsonb" json:"tags"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`

	// 关联关系
	Messages []ChatMessage `gorm:"foreignKey:ChatID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"messages,omitempty"`
}

// TableName 返回表名
func (Chat) TableName() string {
	return "chat"
}

// BeforeCreate 创建前钩子
func (c *Chat) BeforeCreate(tx *gorm.DB) error {
	if c.CreatedTime.IsZero() {
		c.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (c *Chat) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	c.LastModifiedTime = &now
	return nil
}

// ChatMessage 聊天消息表
type ChatMessage struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	ChatID           string     `gorm:"type:text;not null" json:"chat_id"`
	ParentID         *string    `gorm:"type:text" json:"parent_id"`
	Content          string     `gorm:"type:text;not null" json:"content"`
	Type             string     `gorm:"type:text;not null;default:'text'" json:"type"`   // text, image, file, link, etc.
	Status           string     `gorm:"type:text;not null;default:'sent'" json:"status"` // sent, delivered, read, failed
	IsEdited         bool       `gorm:"type:boolean;not null;default:false" json:"is_edited"`
	IsDeleted        bool       `gorm:"type:boolean;not null;default:false" json:"is_deleted"`
	IsPinned         bool       `gorm:"type:boolean;not null;default:false" json:"is_pinned"`
	AttachmentID     *string    `gorm:"type:text" json:"attachment_id"`
	AttachmentURL    *string    `gorm:"type:text" json:"attachment_url"`
	AttachmentType   *string    `gorm:"type:text" json:"attachment_type"`
	AttachmentSize   *int64     `gorm:"type:bigint" json:"attachment_size"`
	LinkPreview      *string    `gorm:"type:jsonb" json:"link_preview"`
	Mentions         *string    `gorm:"type:jsonb" json:"mentions"`
	Reactions        *string    `gorm:"type:jsonb" json:"reactions"`
	EditedTime       *time.Time `gorm:"type:timestamp(3) without time zone" json:"edited_time"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	ReadBy           *string    `gorm:"type:jsonb" json:"read_by"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Chat    *Chat         `gorm:"foreignKey:ChatID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"chat,omitempty"`
	Parent  *ChatMessage  `gorm:"foreignKey:ParentID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"parent,omitempty"`
	Replies []ChatMessage `gorm:"foreignKey:ParentID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"replies,omitempty"`
}

// TableName 返回表名
func (ChatMessage) TableName() string {
	return "chat_message"
}

// BeforeCreate 创建前钩子
func (cm *ChatMessage) BeforeCreate(tx *gorm.DB) error {
	if cm.CreatedTime.IsZero() {
		cm.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (cm *ChatMessage) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	cm.LastModifiedTime = &now

	// 如果消息被编辑，设置编辑时间
	if cm.IsEdited && cm.EditedTime == nil {
		cm.EditedTime = &now
	}

	// 如果消息被删除，设置删除时间
	if cm.IsDeleted && cm.DeletedTime == nil {
		cm.DeletedTime = &now
	}

	return nil
}

// Customer 客户表
type Customer struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	Name             string     `gorm:"type:text;not null" json:"name"`
	Email            *string    `gorm:"type:text" json:"email"`
	Phone            *string    `gorm:"type:text" json:"phone"`
	Company          *string    `gorm:"type:text" json:"company"`
	Title            *string    `gorm:"type:text" json:"title"`
	Industry         *string    `gorm:"type:text" json:"industry"`
	Website          *string    `gorm:"type:text" json:"website"`
	Address          *string    `gorm:"type:text" json:"address"`
	City             *string    `gorm:"type:text" json:"city"`
	State            *string    `gorm:"type:text" json:"state"`
	Country          *string    `gorm:"type:text" json:"country"`
	PostalCode       *string    `gorm:"type:text" json:"postal_code"`
	Status           string     `gorm:"type:text;not null;default:'active'" json:"status"` // active, inactive, potential, lost
	Source           *string    `gorm:"type:text" json:"source"`                           // website, referral, ad, etc.
	Tags             *string    `gorm:"type:jsonb" json:"tags"`
	Notes            *string    `gorm:"type:text" json:"notes"`
	Avatar           *string    `gorm:"type:text" json:"avatar"`
	LastContactTime  *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_contact_time"`
	NextContactTime  *time.Time `gorm:"type:timestamp(3) without time zone" json:"next_contact_time"`
	AssignedTo       *string    `gorm:"type:text" json:"assigned_to"`
	Priority         string     `gorm:"type:text;not null;default:'normal'" json:"priority"` // low, normal, high, urgent
	Value            float64    `gorm:"type:decimal(10,2);not null;default:0" json:"value"`
	Currency         string     `gorm:"type:text;not null;default:'USD'" json:"currency"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	CreditHistory []CreditHistory `gorm:"foreignKey:CustomerID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"credit_history,omitempty"`
}

// TableName 返回表名
func (Customer) TableName() string {
	return "customer"
}

// BeforeCreate 创建前钩子
func (c *Customer) BeforeCreate(tx *gorm.DB) error {
	if c.CreatedTime.IsZero() {
		c.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (c *Customer) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	c.LastModifiedTime = &now
	return nil
}

// CreditHistory 积分历史表
type CreditHistory struct {
	ID               string     `gorm:"primaryKey;type:text;not null" json:"id"`
	CustomerID       string     `gorm:"type:text;not null" json:"customer_id"`
	TransactionType  string     `gorm:"type:text;not null" json:"transaction_type"` // earn, spend, refund, bonus, penalty
	Amount           int        `gorm:"type:integer;not null" json:"amount"`
	Balance          int        `gorm:"type:integer;not null" json:"balance"`
	Description      string     `gorm:"type:text;not null" json:"description"`
	ReferenceType    *string    `gorm:"type:text" json:"reference_type"` // purchase, referral, activity, etc.
	ReferenceID      *string    `gorm:"type:text" json:"reference_id"`
	ExpiryDate       *time.Time `gorm:"type:timestamp(3) without time zone" json:"expiry_date"`
	IsExpired        bool       `gorm:"type:boolean;not null;default:false" json:"is_expired"`
	CreatedBy        string     `gorm:"type:text;not null" json:"created_by"`
	CreatedTime      time.Time  `gorm:"type:timestamp(3) without time zone;not null;default:CURRENT_TIMESTAMP" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"type:timestamp(3) without time zone" json:"last_modified_time"`
	LastModifiedBy   *string    `gorm:"type:text" json:"last_modified_by"`
	DeletedTime      *time.Time `gorm:"type:timestamp(3) without time zone" json:"deleted_time"`
	Metadata         *string    `gorm:"type:jsonb" json:"metadata"`

	// 关联关系
	Customer *Customer `gorm:"foreignKey:CustomerID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"customer,omitempty"`
}

// TableName 返回表名
func (CreditHistory) TableName() string {
	return "credit_history"
}

// BeforeCreate 创建前钩子
func (ch *CreditHistory) BeforeCreate(tx *gorm.DB) error {
	if ch.CreatedTime.IsZero() {
		ch.CreatedTime = time.Now()
	}
	return nil
}

// BeforeUpdate 更新前钩子
func (ch *CreditHistory) BeforeUpdate(tx *gorm.DB) error {
	now := time.Now()
	ch.LastModifiedTime = &now
	return nil
}
