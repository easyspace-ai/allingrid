package http

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/notification"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// NotificationHandler 通知处理器
type NotificationHandler struct {
	service notification.Service
	logger  *zap.Logger
}

// NewNotificationHandler 创建通知处理器
func NewNotificationHandler(service notification.Service, logger *zap.Logger) *NotificationHandler {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	return &NotificationHandler{
		service: service,
		logger:  logger,
	}
}

// CreateNotification 创建通知
// @Summary 创建通知
// @Description 创建新的通知
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param request body notification.CreateNotificationRequest true "创建通知请求"
// @Success 200 {object} response.Response{data=notification.Notification} "创建成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications [post]
func (h *NotificationHandler) CreateNotification(c *gin.Context) {
	var req notification.CreateNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind create notification request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	notification, err := h.service.CreateNotification(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to create notification",
			zap.String("user_id", req.UserID),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, notification, "通知创建成功")
}

// GetNotification 获取通知详情
// @Summary 获取通知详情
// @Description 根据ID获取通知详情
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param id path string true "通知ID"
// @Success 200 {object} response.Response{data=notification.Notification} "获取成功"
// @Failure 404 {object} response.Response "通知不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/{id} [get]
func (h *NotificationHandler) GetNotification(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("通知ID不能为空"))
		return
	}

	notification, err := h.service.GetNotification(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to get notification",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, notification, "获取通知成功")
}

// UpdateNotification 更新通知
// @Summary 更新通知
// @Description 更新通知信息
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param id path string true "通知ID"
// @Param request body notification.UpdateNotificationRequest true "更新通知请求"
// @Success 200 {object} response.Response{data=notification.Notification} "更新成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 404 {object} response.Response "通知不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/{id} [put]
func (h *NotificationHandler) UpdateNotification(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("通知ID不能为空"))
		return
	}

	var req notification.UpdateNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind update notification request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	notification, err := h.service.UpdateNotification(c.Request.Context(), id, &req)
	if err != nil {
		h.logger.Error("Failed to update notification",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, notification, "通知更新成功")
}

// DeleteNotification 删除通知
// @Summary 删除通知
// @Description 根据ID删除通知
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param id path string true "通知ID"
// @Success 200 {object} response.Response "删除成功"
// @Failure 404 {object} response.Response "通知不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/{id} [delete]
func (h *NotificationHandler) DeleteNotification(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("通知ID不能为空"))
		return
	}

	err := h.service.DeleteNotification(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to delete notification",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知删除成功")
}

// ListNotifications 列出通知
// @Summary 列出通知
// @Description 获取用户的通知列表
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param user_id query string true "用户ID"
// @Param type query string false "通知类型"
// @Param status query string false "通知状态"
// @Param priority query string false "通知优先级"
// @Param source_id query string false "来源ID"
// @Param source_type query string false "来源类型"
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(20)
// @Param sort_by query string false "排序字段" default(created_time)
// @Param sort_order query string false "排序顺序" default(desc)
// @Success 200 {object} response.Response{data=notification.ListNotificationsResponse} "获取成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications [get]
func (h *NotificationHandler) ListNotifications(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("用户ID不能为空"))
		return
	}

	req := &notification.ListNotificationsRequest{
		UserID: userID,
	}

	// 解析查询参数
	if typeStr := c.Query("type"); typeStr != "" {
		notificationType := notification.NotificationType(typeStr)
		req.Type = &notificationType
	}

	if statusStr := c.Query("status"); statusStr != "" {
		notificationStatus := notification.NotificationStatus(statusStr)
		req.Status = &notificationStatus
	}

	if priorityStr := c.Query("priority"); priorityStr != "" {
		notificationPriority := notification.NotificationPriority(priorityStr)
		req.Priority = &notificationPriority
	}

	req.SourceID = c.Query("source_id")
	req.SourceType = c.Query("source_type")

	// 分页参数
	if pageStr := c.Query("page"); pageStr != "" {
		if page, err := strconv.Atoi(pageStr); err == nil {
			req.Page = page
		}
	}
	if pageStr := c.Query("page_size"); pageStr != "" {
		if pageSize, err := strconv.Atoi(pageStr); err == nil {
			req.PageSize = pageSize
		}
	}

	req.SortBy = c.DefaultQuery("sort_by", "created_time")
	req.SortOrder = c.DefaultQuery("sort_order", "desc")

	notifications, err := h.service.ListNotifications(c.Request.Context(), req)
	if err != nil {
		h.logger.Error("Failed to list notifications",
			zap.String("user_id", userID),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, notifications, "获取通知列表成功")
}

// MarkNotificationsRead 标记通知为已读
// @Summary 标记通知为已读
// @Description 批量标记通知为已读状态
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param request body notification.MarkNotificationsReadRequest true "标记已读请求"
// @Success 200 {object} response.Response "标记成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/mark-read [post]
func (h *NotificationHandler) MarkNotificationsRead(c *gin.Context) {
	var req notification.MarkNotificationsReadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind mark notifications read request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	err := h.service.MarkNotificationsRead(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to mark notifications as read",
			zap.Strings("notification_ids", req.NotificationIDs),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知标记为已读成功")
}

// MarkAllNotificationsRead 标记所有通知为已读
// @Summary 标记所有通知为已读
// @Description 标记用户的所有通知为已读状态
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param user_id path string true "用户ID"
// @Success 200 {object} response.Response "标记成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/user/{user_id}/mark-all-read [post]
func (h *NotificationHandler) MarkAllNotificationsRead(c *gin.Context) {
	userID := c.Param("user_id")
	if userID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("用户ID不能为空"))
		return
	}

	err := h.service.MarkAllNotificationsRead(c.Request.Context(), userID)
	if err != nil {
		h.logger.Error("Failed to mark all notifications as read",
			zap.String("user_id", userID),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "所有通知标记为已读成功")
}

// GetNotificationStats 获取通知统计
// @Summary 获取通知统计
// @Description 获取用户的通知统计信息
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param user_id path string true "用户ID"
// @Success 200 {object} response.Response{data=notification.NotificationStats} "获取成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/user/{user_id}/stats [get]
func (h *NotificationHandler) GetNotificationStats(c *gin.Context) {
	userID := c.Param("user_id")
	if userID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("用户ID不能为空"))
		return
	}

	stats, err := h.service.GetNotificationStats(c.Request.Context(), userID)
	if err != nil {
		h.logger.Error("Failed to get notification stats",
			zap.String("user_id", userID),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, stats, "获取通知统计成功")
}

// CreateTemplate 创建通知模板
// @Summary 创建通知模板
// @Description 创建新的通知模板
// @Tags 通知模板管理
// @Accept json
// @Produce json
// @Param request body notification.NotificationTemplate true "创建模板请求"
// @Success 200 {object} response.Response "创建成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-templates [post]
func (h *NotificationHandler) CreateTemplate(c *gin.Context) {
	var template notification.NotificationTemplate
	if err := c.ShouldBindJSON(&template); err != nil {
		h.logger.Error("Failed to bind create template request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	err := h.service.CreateTemplate(c.Request.Context(), &template)
	if err != nil {
		h.logger.Error("Failed to create notification template",
			zap.String("type", string(template.Type)),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知模板创建成功")
}

// GetTemplate 获取通知模板
// @Summary 获取通知模板
// @Description 根据ID获取通知模板
// @Tags 通知模板管理
// @Accept json
// @Produce json
// @Param id path string true "模板ID"
// @Success 200 {object} response.Response{data=notification.NotificationTemplate} "获取成功"
// @Failure 404 {object} response.Response "模板不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-templates/{id} [get]
func (h *NotificationHandler) GetTemplate(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("模板ID不能为空"))
		return
	}

	template, err := h.service.GetTemplate(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to get notification template",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, template, "获取通知模板成功")
}

// GetTemplateByType 根据类型获取通知模板
// @Summary 根据类型获取通知模板
// @Description 根据通知类型获取模板
// @Tags 通知模板管理
// @Accept json
// @Produce json
// @Param type path string true "通知类型"
// @Success 200 {object} response.Response{data=notification.NotificationTemplate} "获取成功"
// @Failure 404 {object} response.Response "模板不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-templates/type/{type} [get]
func (h *NotificationHandler) GetTemplateByType(c *gin.Context) {
	notificationType := notification.NotificationType(c.Param("type"))
	if notificationType == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("通知类型不能为空"))
		return
	}

	template, err := h.service.GetTemplateByType(c.Request.Context(), notificationType)
	if err != nil {
		h.logger.Error("Failed to get notification template by type",
			zap.String("type", string(notificationType)),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, template, "获取通知模板成功")
}

// UpdateTemplate 更新通知模板
// @Summary 更新通知模板
// @Description 更新通知模板信息
// @Tags 通知模板管理
// @Accept json
// @Produce json
// @Param id path string true "模板ID"
// @Param request body notification.NotificationTemplate true "更新模板请求"
// @Success 200 {object} response.Response "更新成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 404 {object} response.Response "模板不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-templates/{id} [put]
func (h *NotificationHandler) UpdateTemplate(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("模板ID不能为空"))
		return
	}

	var template notification.NotificationTemplate
	if err := c.ShouldBindJSON(&template); err != nil {
		h.logger.Error("Failed to bind update template request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	template.ID = id // 确保ID一致

	err := h.service.UpdateTemplate(c.Request.Context(), &template)
	if err != nil {
		h.logger.Error("Failed to update notification template",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知模板更新成功")
}

// DeleteTemplate 删除通知模板
// @Summary 删除通知模板
// @Description 根据ID删除通知模板
// @Tags 通知模板管理
// @Accept json
// @Produce json
// @Param id path string true "模板ID"
// @Success 200 {object} response.Response "删除成功"
// @Failure 404 {object} response.Response "模板不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-templates/{id} [delete]
func (h *NotificationHandler) DeleteTemplate(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("模板ID不能为空"))
		return
	}

	err := h.service.DeleteTemplate(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to delete notification template",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知模板删除成功")
}

// ListTemplates 列出通知模板
// @Summary 列出通知模板
// @Description 获取通知模板列表
// @Tags 通知模板管理
// @Accept json
// @Produce json
// @Param type query string false "通知类型"
// @Param is_active query bool false "是否激活"
// @Success 200 {object} response.Response{data=[]notification.NotificationTemplate} "获取成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-templates [get]
func (h *NotificationHandler) ListTemplates(c *gin.Context) {
	var notificationType *notification.NotificationType
	var isActive *bool

	if typeStr := c.Query("type"); typeStr != "" {
		nt := notification.NotificationType(typeStr)
		notificationType = &nt
	}

	if activeStr := c.Query("is_active"); activeStr != "" {
		if active, err := strconv.ParseBool(activeStr); err == nil {
			isActive = &active
		}
	}

	templates, err := h.service.ListTemplates(c.Request.Context(), notificationType, isActive)
	if err != nil {
		h.logger.Error("Failed to list notification templates", zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, templates, "获取通知模板列表成功")
}

// CreateSubscription 创建通知订阅
// @Summary 创建通知订阅
// @Description 创建新的通知订阅
// @Tags 通知订阅管理
// @Accept json
// @Produce json
// @Param request body notification.CreateSubscriptionRequest true "创建订阅请求"
// @Success 200 {object} response.Response{data=notification.NotificationSubscription} "创建成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-subscriptions [post]
func (h *NotificationHandler) CreateSubscription(c *gin.Context) {
	var req notification.CreateSubscriptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind create subscription request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	subscription, err := h.service.CreateSubscription(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to create notification subscription",
			zap.String("user_id", req.UserID),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, subscription, "通知订阅创建成功")
}

// GetSubscription 获取通知订阅
// @Summary 获取通知订阅
// @Description 根据ID获取通知订阅
// @Tags 通知订阅管理
// @Accept json
// @Produce json
// @Param id path string true "订阅ID"
// @Success 200 {object} response.Response{data=notification.NotificationSubscription} "获取成功"
// @Failure 404 {object} response.Response "订阅不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-subscriptions/{id} [get]
func (h *NotificationHandler) GetSubscription(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("订阅ID不能为空"))
		return
	}

	subscription, err := h.service.GetSubscription(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to get notification subscription",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, subscription, "获取通知订阅成功")
}

// GetUserSubscriptions 获取用户订阅
// @Summary 获取用户订阅
// @Description 获取用户的通知订阅列表
// @Tags 通知订阅管理
// @Accept json
// @Produce json
// @Param user_id path string true "用户ID"
// @Param type query string false "通知类型"
// @Success 200 {object} response.Response{data=[]notification.NotificationSubscription} "获取成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-subscriptions/user/{user_id} [get]
func (h *NotificationHandler) GetUserSubscriptions(c *gin.Context) {
	userID := c.Param("user_id")
	if userID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("用户ID不能为空"))
		return
	}

	var notificationType *notification.NotificationType
	if typeStr := c.Query("type"); typeStr != "" {
		nt := notification.NotificationType(typeStr)
		notificationType = &nt
	}

	subscriptions, err := h.service.GetUserSubscriptions(c.Request.Context(), userID, notificationType)
	if err != nil {
		h.logger.Error("Failed to get user notification subscriptions",
			zap.String("user_id", userID),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, subscriptions, "获取用户通知订阅成功")
}

// UpdateSubscription 更新通知订阅
// @Summary 更新通知订阅
// @Description 更新通知订阅信息
// @Tags 通知订阅管理
// @Accept json
// @Produce json
// @Param id path string true "订阅ID"
// @Param request body notification.UpdateSubscriptionRequest true "更新订阅请求"
// @Success 200 {object} response.Response{data=notification.NotificationSubscription} "更新成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 404 {object} response.Response "订阅不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-subscriptions/{id} [put]
func (h *NotificationHandler) UpdateSubscription(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("订阅ID不能为空"))
		return
	}

	var req notification.UpdateSubscriptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind update subscription request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	subscription, err := h.service.UpdateSubscription(c.Request.Context(), id, &req)
	if err != nil {
		h.logger.Error("Failed to update notification subscription",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, subscription, "通知订阅更新成功")
}

// DeleteSubscription 删除通知订阅
// @Summary 删除通知订阅
// @Description 根据ID删除通知订阅
// @Tags 通知订阅管理
// @Accept json
// @Produce json
// @Param id path string true "订阅ID"
// @Success 200 {object} response.Response "删除成功"
// @Failure 404 {object} response.Response "订阅不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-subscriptions/{id} [delete]
func (h *NotificationHandler) DeleteSubscription(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("订阅ID不能为空"))
		return
	}

	err := h.service.DeleteSubscription(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to delete notification subscription",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知订阅删除成功")
}

// DeleteUserSubscriptions 删除用户订阅
// @Summary 删除用户订阅
// @Description 删除用户的所有或指定类型的通知订阅
// @Tags 通知订阅管理
// @Accept json
// @Produce json
// @Param user_id path string true "用户ID"
// @Param type query string false "通知类型"
// @Success 200 {object} response.Response "删除成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notification-subscriptions/user/{user_id} [delete]
func (h *NotificationHandler) DeleteUserSubscriptions(c *gin.Context) {
	userID := c.Param("user_id")
	if userID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("用户ID不能为空"))
		return
	}

	var notificationType *notification.NotificationType
	if typeStr := c.Query("type"); typeStr != "" {
		nt := notification.NotificationType(typeStr)
		notificationType = &nt
	}

	err := h.service.DeleteUserSubscriptions(c.Request.Context(), userID, notificationType)
	if err != nil {
		h.logger.Error("Failed to delete user notification subscriptions",
			zap.String("user_id", userID),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "用户通知订阅删除成功")
}

// SendNotification 发送通知
// @Summary 发送通知
// @Description 发送通知到订阅者
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param request body notification.CreateNotificationRequest true "发送通知请求"
// @Success 200 {object} response.Response "发送成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/send [post]
func (h *NotificationHandler) SendNotification(c *gin.Context) {
	var req notification.CreateNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind send notification request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	notification, err := h.service.CreateNotification(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to send notification",
			zap.String("user_id", req.UserID),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, notification, "通知发送成功")
}

// SendNotificationToSubscribers 向订阅者发送通知
// @Summary 向订阅者发送通知
// @Description 向指定来源的订阅者发送通知
// @Tags 通知管理
// @Accept json
// @Produce json
// @Param request body SendNotificationToSubscribersRequest true "发送通知请求"
// @Success 200 {object} response.Response "发送成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/send-to-subscribers [post]
func (h *NotificationHandler) SendNotificationToSubscribers(c *gin.Context) {
	var req SendNotificationToSubscribersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind send notification to subscribers request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	err := h.service.SendNotificationToSubscribers(
		c.Request.Context(),
		req.Type,
		req.SourceID,
		req.SourceType,
		req.Title,
		req.Content,
		req.Data,
	)
	if err != nil {
		h.logger.Error("Failed to send notification to subscribers",
			zap.String("type", string(req.Type)),
			zap.String("source_id", req.SourceID),
			zap.String("source_type", req.SourceType),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "通知发送成功")
}

// CleanupExpiredNotifications 清理过期通知
// @Summary 清理过期通知
// @Description 清理所有过期的通知
// @Tags 通知管理
// @Accept json
// @Produce json
// @Success 200 {object} response.Response "清理成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/notifications/cleanup [post]
func (h *NotificationHandler) CleanupExpiredNotifications(c *gin.Context) {
	err := h.service.CleanupExpiredNotifications(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to cleanup expired notifications", zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "过期通知清理成功")
}

// SendNotificationToSubscribersRequest 向订阅者发送通知请求
type SendNotificationToSubscribersRequest struct {
	Type       notification.NotificationType `json:"type" binding:"required"`
	SourceID   string                        `json:"source_id"`
	SourceType string                        `json:"source_type"`
	Title      string                        `json:"title" binding:"required"`
	Content    string                        `json:"content" binding:"required"`
	Data       map[string]interface{}        `json:"data,omitempty"`
}
