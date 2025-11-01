package notification

import (
	"context"
)

// Repository 通知仓储接口
type Repository interface {
	// CreateNotification 创建通知
	CreateNotification(ctx context.Context, notification *Notification) error
	// GetNotification 获取通知
	GetNotification(ctx context.Context, id string) (*Notification, error)
	// UpdateNotification 更新通知
	UpdateNotification(ctx context.Context, notification *Notification) error
	// DeleteNotification 删除通知
	DeleteNotification(ctx context.Context, id string) error
	// ListNotifications 列出通知
	ListNotifications(ctx context.Context, req *ListNotificationsRequest) (*ListNotificationsResponse, error)
	// MarkNotificationsRead 标记通知为已读
	MarkNotificationsRead(ctx context.Context, notificationIDs []string) error
	// MarkAllNotificationsRead 标记所有通知为已读
	MarkAllNotificationsRead(ctx context.Context, userID string) error
	// GetNotificationStats 获取通知统计
	GetNotificationStats(ctx context.Context, userID string) (*NotificationStats, error)
	// CleanupExpiredNotifications 清理过期通知
	CleanupExpiredNotifications(ctx context.Context) error
}

// TemplateRepository 通知模板仓储接口
type TemplateRepository interface {
	// CreateTemplate 创建模板
	CreateTemplate(ctx context.Context, template *NotificationTemplate) error
	// GetTemplate 获取模板
	GetTemplate(ctx context.Context, id string) (*NotificationTemplate, error)
	// GetTemplateByType 根据类型获取模板
	GetTemplateByType(ctx context.Context, notificationType NotificationType) (*NotificationTemplate, error)
	// UpdateTemplate 更新模板
	UpdateTemplate(ctx context.Context, template *NotificationTemplate) error
	// DeleteTemplate 删除模板
	DeleteTemplate(ctx context.Context, id string) error
	// ListTemplates 列出模板
	ListTemplates(ctx context.Context, notificationType *NotificationType, isActive *bool) ([]*NotificationTemplate, error)
}

// SubscriptionRepository 通知订阅仓储接口
type SubscriptionRepository interface {
	// CreateSubscription 创建订阅
	CreateSubscription(ctx context.Context, subscription *NotificationSubscription) error
	// GetSubscription 获取订阅
	GetSubscription(ctx context.Context, id string) (*NotificationSubscription, error)
	// GetUserSubscriptions 获取用户订阅
	GetUserSubscriptions(ctx context.Context, userID string, notificationType *NotificationType) ([]*NotificationSubscription, error)
	// GetSubscriptionsBySource 根据来源获取订阅
	GetSubscriptionsBySource(ctx context.Context, sourceID, sourceType string, notificationType *NotificationType) ([]*NotificationSubscription, error)
	// UpdateSubscription 更新订阅
	UpdateSubscription(ctx context.Context, subscription *NotificationSubscription) error
	// DeleteSubscription 删除订阅
	DeleteSubscription(ctx context.Context, id string) error
	// DeleteUserSubscriptions 删除用户订阅
	DeleteUserSubscriptions(ctx context.Context, userID string, notificationType *NotificationType) error
}
