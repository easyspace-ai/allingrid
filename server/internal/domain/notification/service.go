package notification

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	pkgErrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// Service 通知服务接口
type Service interface {
	// CreateNotification 创建通知
	CreateNotification(ctx context.Context, req *CreateNotificationRequest) (*Notification, error)
	// GetNotification 获取通知
	GetNotification(ctx context.Context, id string) (*Notification, error)
	// UpdateNotification 更新通知
	UpdateNotification(ctx context.Context, id string, req *UpdateNotificationRequest) (*Notification, error)
	// DeleteNotification 删除通知
	DeleteNotification(ctx context.Context, id string) error
	// ListNotifications 列出通知
	ListNotifications(ctx context.Context, req *ListNotificationsRequest) (*ListNotificationsResponse, error)
	// MarkNotificationsRead 标记通知为已读
	MarkNotificationsRead(ctx context.Context, req *MarkNotificationsReadRequest) error
	// MarkAllNotificationsRead 标记所有通知为已读
	MarkAllNotificationsRead(ctx context.Context, userID string) error
	// GetNotificationStats 获取通知统计
	GetNotificationStats(ctx context.Context, userID string) (*NotificationStats, error)
	// CleanupExpiredNotifications 清理过期通知
	CleanupExpiredNotifications(ctx context.Context) error

	// Template management
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

	// Subscription management
	// CreateSubscription 创建订阅
	CreateSubscription(ctx context.Context, req *CreateSubscriptionRequest) (*NotificationSubscription, error)
	// GetSubscription 获取订阅
	GetSubscription(ctx context.Context, id string) (*NotificationSubscription, error)
	// GetUserSubscriptions 获取用户订阅
	GetUserSubscriptions(ctx context.Context, userID string, notificationType *NotificationType) ([]*NotificationSubscription, error)
	// UpdateSubscription 更新订阅
	UpdateSubscription(ctx context.Context, id string, req *UpdateSubscriptionRequest) (*NotificationSubscription, error)
	// DeleteSubscription 删除订阅
	DeleteSubscription(ctx context.Context, id string) error
	// DeleteUserSubscriptions 删除用户订阅
	DeleteUserSubscriptions(ctx context.Context, userID string, notificationType *NotificationType) error

	// Notification sending
	// SendNotification 发送通知
	SendNotification(ctx context.Context, notification *Notification) error
	// SendBulkNotifications 批量发送通知
	SendBulkNotifications(ctx context.Context, notifications []*Notification) error
	// SendNotificationToSubscribers 向订阅者发送通知
	SendNotificationToSubscribers(ctx context.Context, notificationType NotificationType, sourceID, sourceType string, title, content string, data map[string]interface{}) error
}

// service 通知服务实现
type service struct {
	repo             Repository
	templateRepo     TemplateRepository
	subscriptionRepo SubscriptionRepository
	logger           *zap.Logger
}

// NewService 创建通知服务
func NewService(
	repo Repository,
	templateRepo TemplateRepository,
	subscriptionRepo SubscriptionRepository,
	logger *zap.Logger,
) Service {
	return &service{
		repo:             repo,
		templateRepo:     templateRepo,
		subscriptionRepo: subscriptionRepo,
		logger:           logger,
	}
}

// CreateNotification 创建通知
func (s *service) CreateNotification(ctx context.Context, req *CreateNotificationRequest) (*Notification, error) {
	notification := NewNotification(req.UserID, req.Type, req.Title, req.Content)

	if req.Data != nil {
		notification.Data = req.Data
	}
	if req.Priority != "" {
		notification.Priority = req.Priority
	}
	if req.SourceID != "" {
		notification.SourceID = req.SourceID
	}
	if req.SourceType != "" {
		notification.SourceType = req.SourceType
	}
	if req.ActionURL != "" {
		notification.ActionURL = req.ActionURL
	}
	if req.ExpiresAt != nil {
		notification.ExpiresAt = req.ExpiresAt
	}

	if err := s.repo.CreateNotification(ctx, notification); err != nil {
		s.logger.Error("Failed to create notification",
			zap.String("user_id", req.UserID),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	// 发送通知
	if err := s.SendNotification(ctx, notification); err != nil {
		s.logger.Error("Failed to send notification",
			zap.String("notification_id", notification.ID),
			zap.Error(err))
		// 不返回错误，因为通知已创建成功
	}

	return notification, nil
}

// GetNotification 获取通知
func (s *service) GetNotification(ctx context.Context, id string) (*Notification, error) {
	notification, err := s.repo.GetNotification(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Notification not found")
		}
		s.logger.Error("Failed to get notification",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return notification, nil
}

// UpdateNotification 更新通知
func (s *service) UpdateNotification(ctx context.Context, id string, req *UpdateNotificationRequest) (*Notification, error) {
	notification, err := s.repo.GetNotification(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Notification not found")
		}
		s.logger.Error("Failed to get notification for update",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	if req.Status != "" {
		notification.Status = req.Status
		if req.Status == NotificationStatusRead && notification.ReadAt == nil {
			now := time.Now()
			notification.ReadAt = &now
		}
	}
	if req.Data != nil {
		notification.Data = req.Data
	}
	notification.UpdatedTime = time.Now()

	if err := s.repo.UpdateNotification(ctx, notification); err != nil {
		s.logger.Error("Failed to update notification",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return notification, nil
}

// DeleteNotification 删除通知
func (s *service) DeleteNotification(ctx context.Context, id string) error {
	if err := s.repo.DeleteNotification(ctx, id); err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return pkgErrors.ErrNotFound.WithDetails("Notification not found")
		}
		s.logger.Error("Failed to delete notification",
			zap.String("id", id),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// ListNotifications 列出通知
func (s *service) ListNotifications(ctx context.Context, req *ListNotificationsRequest) (*ListNotificationsResponse, error) {
	// 设置默认值
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}
	if req.SortBy == "" {
		req.SortBy = "created_time"
	}
	if req.SortOrder == "" {
		req.SortOrder = "desc"
	}

	response, err := s.repo.ListNotifications(ctx, req)
	if err != nil {
		s.logger.Error("Failed to list notifications",
			zap.String("user_id", req.UserID),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return response, nil
}

// MarkNotificationsRead 标记通知为已读
func (s *service) MarkNotificationsRead(ctx context.Context, req *MarkNotificationsReadRequest) error {
	if err := s.repo.MarkNotificationsRead(ctx, req.NotificationIDs); err != nil {
		s.logger.Error("Failed to mark notifications as read",
			zap.Strings("notification_ids", req.NotificationIDs),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// MarkAllNotificationsRead 标记所有通知为已读
func (s *service) MarkAllNotificationsRead(ctx context.Context, userID string) error {
	if err := s.repo.MarkAllNotificationsRead(ctx, userID); err != nil {
		s.logger.Error("Failed to mark all notifications as read",
			zap.String("user_id", userID),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// GetNotificationStats 获取通知统计
func (s *service) GetNotificationStats(ctx context.Context, userID string) (*NotificationStats, error) {
	stats, err := s.repo.GetNotificationStats(ctx, userID)
	if err != nil {
		s.logger.Error("Failed to get notification stats",
			zap.String("user_id", userID),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return stats, nil
}

// CleanupExpiredNotifications 清理过期通知
func (s *service) CleanupExpiredNotifications(ctx context.Context) error {
	if err := s.repo.CleanupExpiredNotifications(ctx); err != nil {
		s.logger.Error("Failed to cleanup expired notifications", zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// CreateTemplate 创建模板
func (s *service) CreateTemplate(ctx context.Context, template *NotificationTemplate) error {
	if err := s.templateRepo.CreateTemplate(ctx, template); err != nil {
		s.logger.Error("Failed to create notification template",
			zap.String("type", string(template.Type)),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// GetTemplate 获取模板
func (s *service) GetTemplate(ctx context.Context, id string) (*NotificationTemplate, error) {
	template, err := s.templateRepo.GetTemplate(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Template not found")
		}
		s.logger.Error("Failed to get notification template",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return template, nil
}

// GetTemplateByType 根据类型获取模板
func (s *service) GetTemplateByType(ctx context.Context, notificationType NotificationType) (*NotificationTemplate, error) {
	template, err := s.templateRepo.GetTemplateByType(ctx, notificationType)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Template not found")
		}
		s.logger.Error("Failed to get notification template by type",
			zap.String("type", string(notificationType)),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return template, nil
}

// UpdateTemplate 更新模板
func (s *service) UpdateTemplate(ctx context.Context, template *NotificationTemplate) error {
	if err := s.templateRepo.UpdateTemplate(ctx, template); err != nil {
		s.logger.Error("Failed to update notification template",
			zap.String("id", template.ID),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// DeleteTemplate 删除模板
func (s *service) DeleteTemplate(ctx context.Context, id string) error {
	if err := s.templateRepo.DeleteTemplate(ctx, id); err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return pkgErrors.ErrNotFound.WithDetails("Template not found")
		}
		s.logger.Error("Failed to delete notification template",
			zap.String("id", id),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// ListTemplates 列出模板
func (s *service) ListTemplates(ctx context.Context, notificationType *NotificationType, isActive *bool) ([]*NotificationTemplate, error) {
	templates, err := s.templateRepo.ListTemplates(ctx, notificationType, isActive)
	if err != nil {
		s.logger.Error("Failed to list notification templates", zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return templates, nil
}

// CreateSubscription 创建订阅
func (s *service) CreateSubscription(ctx context.Context, req *CreateSubscriptionRequest) (*NotificationSubscription, error) {
	subscription := NewNotificationSubscription(req.UserID, req.Type)

	if req.SourceID != "" {
		subscription.SourceID = req.SourceID
	}
	if req.SourceType != "" {
		subscription.SourceType = req.SourceType
	}
	if req.Channels != nil {
		subscription.Channels = req.Channels
	}
	if req.Settings != nil {
		subscription.Settings = req.Settings
	}

	if err := s.subscriptionRepo.CreateSubscription(ctx, subscription); err != nil {
		s.logger.Error("Failed to create notification subscription",
			zap.String("user_id", req.UserID),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return subscription, nil
}

// GetSubscription 获取订阅
func (s *service) GetSubscription(ctx context.Context, id string) (*NotificationSubscription, error) {
	subscription, err := s.subscriptionRepo.GetSubscription(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Subscription not found")
		}
		s.logger.Error("Failed to get notification subscription",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return subscription, nil
}

// GetUserSubscriptions 获取用户订阅
func (s *service) GetUserSubscriptions(ctx context.Context, userID string, notificationType *NotificationType) ([]*NotificationSubscription, error) {
	subscriptions, err := s.subscriptionRepo.GetUserSubscriptions(ctx, userID, notificationType)
	if err != nil {
		s.logger.Error("Failed to get user notification subscriptions",
			zap.String("user_id", userID),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return subscriptions, nil
}

// UpdateSubscription 更新订阅
func (s *service) UpdateSubscription(ctx context.Context, id string, req *UpdateSubscriptionRequest) (*NotificationSubscription, error) {
	subscription, err := s.subscriptionRepo.GetSubscription(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Subscription not found")
		}
		s.logger.Error("Failed to get notification subscription for update",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	if req.Channels != nil {
		subscription.Channels = req.Channels
	}
	if req.Settings != nil {
		subscription.Settings = req.Settings
	}
	if req.IsActive != nil {
		subscription.IsActive = *req.IsActive
	}
	subscription.UpdatedTime = time.Now()

	if err := s.subscriptionRepo.UpdateSubscription(ctx, subscription); err != nil {
		s.logger.Error("Failed to update notification subscription",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return subscription, nil
}

// DeleteSubscription 删除订阅
func (s *service) DeleteSubscription(ctx context.Context, id string) error {
	if err := s.subscriptionRepo.DeleteSubscription(ctx, id); err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return pkgErrors.ErrNotFound.WithDetails("Subscription not found")
		}
		s.logger.Error("Failed to delete notification subscription",
			zap.String("id", id),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// DeleteUserSubscriptions 删除用户订阅
func (s *service) DeleteUserSubscriptions(ctx context.Context, userID string, notificationType *NotificationType) error {
	if err := s.subscriptionRepo.DeleteUserSubscriptions(ctx, userID, notificationType); err != nil {
		s.logger.Error("Failed to delete user notification subscriptions",
			zap.String("user_id", userID),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// SendNotification 发送通知
func (s *service) SendNotification(ctx context.Context, notification *Notification) error {
	// 获取用户订阅
	subscriptions, err := s.subscriptionRepo.GetUserSubscriptions(ctx, notification.UserID, &notification.Type)
	if err != nil {
		s.logger.Error("Failed to get user subscriptions for notification",
			zap.String("user_id", notification.UserID),
			zap.String("type", string(notification.Type)),
			zap.Error(err))
		return err
	}

	// 检查是否有相关订阅
	hasSubscription := false
	for _, subscription := range subscriptions {
		if subscription.IsActive &&
			(subscription.SourceID == "" || subscription.SourceID == notification.SourceID) &&
			(subscription.SourceType == "" || subscription.SourceType == notification.SourceType) {
			hasSubscription = true
			break
		}
	}

	if !hasSubscription {
		s.logger.Info("User has no active subscription for notification type",
			zap.String("user_id", notification.UserID),
			zap.String("type", string(notification.Type)))
		return nil
	}

	// 这里应该实现具体的通知发送逻辑
	// 例如：发送到WebSocket、发送邮件、发送推送通知等
	s.logger.Info("Notification sent",
		zap.String("notification_id", notification.ID),
		zap.String("user_id", notification.UserID),
		zap.String("type", string(notification.Type)),
		zap.String("title", notification.Title))

	return nil
}

// SendBulkNotifications 批量发送通知
func (s *service) SendBulkNotifications(ctx context.Context, notifications []*Notification) error {
	for _, notification := range notifications {
		if err := s.SendNotification(ctx, notification); err != nil {
			s.logger.Error("Failed to send bulk notification",
				zap.String("notification_id", notification.ID),
				zap.Error(err))
			// 继续发送其他通知
		}
	}

	return nil
}

// SendNotificationToSubscribers 向订阅者发送通知
func (s *service) SendNotificationToSubscribers(ctx context.Context, notificationType NotificationType, sourceID, sourceType string, title, content string, data map[string]interface{}) error {
	// 获取订阅者
	subscriptions, err := s.subscriptionRepo.GetSubscriptionsBySource(ctx, sourceID, sourceType, &notificationType)
	if err != nil {
		s.logger.Error("Failed to get subscriptions for source",
			zap.String("source_id", sourceID),
			zap.String("source_type", sourceType),
			zap.String("type", string(notificationType)),
			zap.Error(err))
		return err
	}

	// 为每个订阅者创建通知
	notifications := make([]*Notification, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		if !subscription.IsActive {
			continue
		}

		notification := NewNotification(subscription.UserID, notificationType, title, content)
		notification.SourceID = sourceID
		notification.SourceType = sourceType
		if data != nil {
			notification.Data = data
		}

		// 创建通知
		if err := s.repo.CreateNotification(ctx, notification); err != nil {
			s.logger.Error("Failed to create notification for subscriber",
				zap.String("user_id", subscription.UserID),
				zap.Error(err))
			continue
		}

		notifications = append(notifications, notification)
	}

	// 批量发送通知
	if len(notifications) > 0 {
		return s.SendBulkNotifications(ctx, notifications)
	}

	return nil
}
