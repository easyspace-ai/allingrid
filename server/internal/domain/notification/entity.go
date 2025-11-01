package notification

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// NotificationType 通知类型
type NotificationType string

const (
	NotificationTypeSystem   NotificationType = "system"   // 系统通知
	NotificationTypeUser     NotificationType = "user"     // 用户通知
	NotificationTypeSpace    NotificationType = "space"    // 空间通知
	NotificationTypeTable    NotificationType = "table"    // 表格通知
	NotificationTypeRecord   NotificationType = "record"   // 记录通知
	NotificationTypeComment  NotificationType = "comment"  // 评论通知
	NotificationTypeMention  NotificationType = "mention"  // 提及通知
	NotificationTypeShare    NotificationType = "share"    // 分享通知
	NotificationTypeInvite   NotificationType = "invite"   // 邀请通知
	NotificationTypeReminder NotificationType = "reminder" // 提醒通知
)

// NotificationStatus 通知状态
type NotificationStatus string

const (
	NotificationStatusUnread   NotificationStatus = "unread"   // 未读
	NotificationStatusRead     NotificationStatus = "read"     // 已读
	NotificationStatusArchived NotificationStatus = "archived" // 已归档
)

// NotificationPriority 通知优先级
type NotificationPriority string

const (
	NotificationPriorityLow    NotificationPriority = "low"    // 低优先级
	NotificationPriorityNormal NotificationPriority = "normal" // 普通优先级
	NotificationPriorityHigh   NotificationPriority = "high"   // 高优先级
	NotificationPriorityUrgent NotificationPriority = "urgent" // 紧急优先级
)

// Notification 通知实体
type Notification struct {
	ID          string                 `json:"id"`
	UserID      string                 `json:"user_id"`
	Type        NotificationType       `json:"type"`
	Title       string                 `json:"title"`
	Content     string                 `json:"content"`
	Data        map[string]interface{} `json:"data,omitempty"`
	Status      NotificationStatus     `json:"status"`
	Priority    NotificationPriority   `json:"priority"`
	SourceID    string                 `json:"source_id,omitempty"`   // 来源ID（如表格ID、记录ID等）
	SourceType  string                 `json:"source_type,omitempty"` // 来源类型
	ActionURL   string                 `json:"action_url,omitempty"`  // 操作链接
	ExpiresAt   *time.Time             `json:"expires_at,omitempty"`  // 过期时间
	ReadAt      *time.Time             `json:"read_at,omitempty"`     // 阅读时间
	CreatedTime time.Time              `json:"created_time"`
	UpdatedTime time.Time              `json:"updated_time"`
}

// NewNotification 创建新通知
func NewNotification(userID string, notificationType NotificationType, title, content string) *Notification {
	return &Notification{
		ID:          utils.GenerateNanoID(10),
		UserID:      userID,
		Type:        notificationType,
		Title:       title,
		Content:     content,
		Data:        make(map[string]interface{}),
		Status:      NotificationStatusUnread,
		Priority:    NotificationPriorityNormal,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// SetData 设置通知数据
func (n *Notification) SetData(key string, value interface{}) {
	if n.Data == nil {
		n.Data = make(map[string]interface{})
	}
	n.Data[key] = value
	n.UpdatedTime = time.Now()
}

// SetPriority 设置通知优先级
func (n *Notification) SetPriority(priority NotificationPriority) {
	n.Priority = priority
	n.UpdatedTime = time.Now()
}

// SetSource 设置通知来源
func (n *Notification) SetSource(sourceID, sourceType string) {
	n.SourceID = sourceID
	n.SourceType = sourceType
	n.UpdatedTime = time.Now()
}

// SetActionURL 设置操作链接
func (n *Notification) SetActionURL(url string) {
	n.ActionURL = url
	n.UpdatedTime = time.Now()
}

// SetExpiresAt 设置过期时间
func (n *Notification) SetExpiresAt(expiresAt time.Time) {
	n.ExpiresAt = &expiresAt
	n.UpdatedTime = time.Now()
}

// MarkAsRead 标记为已读
func (n *Notification) MarkAsRead() {
	n.Status = NotificationStatusRead
	now := time.Now()
	n.ReadAt = &now
	n.UpdatedTime = time.Now()
}

// MarkAsArchived 标记为已归档
func (n *Notification) MarkAsArchived() {
	n.Status = NotificationStatusArchived
	n.UpdatedTime = time.Now()
}

// IsExpired 检查是否过期
func (n *Notification) IsExpired() bool {
	if n.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*n.ExpiresAt)
}

// NotificationTemplate 通知模板
type NotificationTemplate struct {
	ID          string                 `json:"id"`
	Type        NotificationType       `json:"type"`
	Name        string                 `json:"name"`
	Title       string                 `json:"title"`
	Content     string                 `json:"content"`
	Variables   []string               `json:"variables"`    // 模板变量
	DefaultData map[string]interface{} `json:"default_data"` // 默认数据
	IsActive    bool                   `json:"is_active"`
	CreatedTime time.Time              `json:"created_time"`
	UpdatedTime time.Time              `json:"updated_time"`
}

// NewNotificationTemplate 创建通知模板
func NewNotificationTemplate(notificationType NotificationType, name, title, content string) *NotificationTemplate {
	return &NotificationTemplate{
		ID:          utils.GenerateNanoID(10),
		Type:        notificationType,
		Name:        name,
		Title:       title,
		Content:     content,
		Variables:   make([]string, 0),
		DefaultData: make(map[string]interface{}),
		IsActive:    true,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// AddVariable 添加模板变量
func (t *NotificationTemplate) AddVariable(variable string) {
	t.Variables = append(t.Variables, variable)
	t.UpdatedTime = time.Now()
}

// SetDefaultData 设置默认数据
func (t *NotificationTemplate) SetDefaultData(key string, value interface{}) {
	if t.DefaultData == nil {
		t.DefaultData = make(map[string]interface{})
	}
	t.DefaultData[key] = value
	t.UpdatedTime = time.Now()
}

// NotificationSubscription 通知订阅
type NotificationSubscription struct {
	ID          string                 `json:"id"`
	UserID      string                 `json:"user_id"`
	Type        NotificationType       `json:"type"`
	SourceID    string                 `json:"source_id,omitempty"`   // 订阅来源ID
	SourceType  string                 `json:"source_type,omitempty"` // 订阅来源类型
	Channels    []string               `json:"channels"`              // 通知渠道：email, push, in_app
	Settings    map[string]interface{} `json:"settings"`              // 订阅设置
	IsActive    bool                   `json:"is_active"`
	CreatedTime time.Time              `json:"created_time"`
	UpdatedTime time.Time              `json:"updated_time"`
}

// NewNotificationSubscription 创建通知订阅
func NewNotificationSubscription(userID string, notificationType NotificationType) *NotificationSubscription {
	return &NotificationSubscription{
		ID:          utils.GenerateNanoID(10),
		UserID:      userID,
		Type:        notificationType,
		Channels:    []string{"in_app"}, // 默认应用内通知
		Settings:    make(map[string]interface{}),
		IsActive:    true,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// AddChannel 添加通知渠道
func (s *NotificationSubscription) AddChannel(channel string) {
	for _, c := range s.Channels {
		if c == channel {
			return // 已存在
		}
	}
	s.Channels = append(s.Channels, channel)
	s.UpdatedTime = time.Now()
}

// RemoveChannel 移除通知渠道
func (s *NotificationSubscription) RemoveChannel(channel string) {
	for i, c := range s.Channels {
		if c == channel {
			s.Channels = append(s.Channels[:i], s.Channels[i+1:]...)
			s.UpdatedTime = time.Now()
			return
		}
	}
}

// SetSetting 设置订阅设置
func (s *NotificationSubscription) SetSetting(key string, value interface{}) {
	if s.Settings == nil {
		s.Settings = make(map[string]interface{})
	}
	s.Settings[key] = value
	s.UpdatedTime = time.Now()
}

// NotificationStats 通知统计
type NotificationStats struct {
	TotalNotifications int64                          `json:"total_notifications"`
	UnreadCount        int64                          `json:"unread_count"`
	ReadCount          int64                          `json:"read_count"`
	ArchivedCount      int64                          `json:"archived_count"`
	ByType             map[NotificationType]int64     `json:"by_type"`
	ByPriority         map[NotificationPriority]int64 `json:"by_priority"`
	RecentActivity     []*Notification                `json:"recent_activity"`
}

// CreateNotificationRequest 创建通知请求
type CreateNotificationRequest struct {
	UserID     string                 `json:"user_id" binding:"required"`
	Type       NotificationType       `json:"type" binding:"required"`
	Title      string                 `json:"title" binding:"required"`
	Content    string                 `json:"content" binding:"required"`
	Data       map[string]interface{} `json:"data,omitempty"`
	Priority   NotificationPriority   `json:"priority,omitempty"`
	SourceID   string                 `json:"source_id,omitempty"`
	SourceType string                 `json:"source_type,omitempty"`
	ActionURL  string                 `json:"action_url,omitempty"`
	ExpiresAt  *time.Time             `json:"expires_at,omitempty"`
}

// UpdateNotificationRequest 更新通知请求
type UpdateNotificationRequest struct {
	Status NotificationStatus     `json:"status,omitempty"`
	Data   map[string]interface{} `json:"data,omitempty"`
}

// ListNotificationsRequest 列出通知请求
type ListNotificationsRequest struct {
	UserID     string                `json:"user_id" binding:"required"`
	Type       *NotificationType     `json:"type,omitempty"`
	Status     *NotificationStatus   `json:"status,omitempty"`
	Priority   *NotificationPriority `json:"priority,omitempty"`
	SourceID   string                `json:"source_id,omitempty"`
	SourceType string                `json:"source_type,omitempty"`
	Page       int                   `json:"page,omitempty"`
	PageSize   int                   `json:"page_size,omitempty"`
	SortBy     string                `json:"sort_by,omitempty"`
	SortOrder  string                `json:"sort_order,omitempty"`
}

// ListNotificationsResponse 列出通知响应
type ListNotificationsResponse struct {
	Notifications []*Notification `json:"notifications"`
	Total         int64           `json:"total"`
	Page          int             `json:"page"`
	PageSize      int             `json:"page_size"`
	TotalPages    int             `json:"total_pages"`
}

// MarkNotificationsReadRequest 标记通知为已读请求
type MarkNotificationsReadRequest struct {
	NotificationIDs []string `json:"notification_ids" binding:"required"`
}

// CreateSubscriptionRequest 创建订阅请求
type CreateSubscriptionRequest struct {
	UserID     string                 `json:"user_id" binding:"required"`
	Type       NotificationType       `json:"type" binding:"required"`
	SourceID   string                 `json:"source_id,omitempty"`
	SourceType string                 `json:"source_type,omitempty"`
	Channels   []string               `json:"channels" binding:"required"`
	Settings   map[string]interface{} `json:"settings,omitempty"`
}

// UpdateSubscriptionRequest 更新订阅请求
type UpdateSubscriptionRequest struct {
	Channels []string               `json:"channels,omitempty"`
	Settings map[string]interface{} `json:"settings,omitempty"`
	IsActive *bool                  `json:"is_active,omitempty"`
}
