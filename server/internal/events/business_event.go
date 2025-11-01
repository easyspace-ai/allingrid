package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/cache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// BusinessEventType 业务事件类型
type BusinessEventType string

const (
	// 表相关事件
	BusinessEventTypeTableCreate BusinessEventType = "table.create"
	BusinessEventTypeTableUpdate BusinessEventType = "table.update"
	BusinessEventTypeTableDelete BusinessEventType = "table.delete"

	// 字段相关事件
	BusinessEventTypeFieldCreate BusinessEventType = "field.create"
	BusinessEventTypeFieldUpdate BusinessEventType = "field.update"
	BusinessEventTypeFieldDelete BusinessEventType = "field.delete"

	// 记录相关事件
	BusinessEventTypeRecordCreate BusinessEventType = "record.create"
	BusinessEventTypeRecordUpdate BusinessEventType = "record.update"
	BusinessEventTypeRecordDelete BusinessEventType = "record.delete"

	// 计算相关事件
	BusinessEventTypeCalculationUpdate BusinessEventType = "calculation.update"

	// 视图相关事件
	BusinessEventTypeViewCreate BusinessEventType = "view.create"
	BusinessEventTypeViewUpdate BusinessEventType = "view.update"
	BusinessEventTypeViewDelete BusinessEventType = "view.delete"
)

// BusinessEvent 业务事件结构
type BusinessEvent struct {
	ID        string            `json:"id"`
	Type      BusinessEventType `json:"type"`
	TableID   string            `json:"table_id,omitempty"`
	RecordID  string            `json:"record_id,omitempty"`
	FieldID   string            `json:"field_id,omitempty"`
	Data      interface{}       `json:"data"`
	UserID    string            `json:"user_id,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Version   int64             `json:"version,omitempty"`
}

// BusinessEventSubscriber 业务事件订阅者接口
type BusinessEventSubscriber interface {
	// Subscribe 订阅业务事件
	Subscribe(ctx context.Context, eventTypes []BusinessEventType) (<-chan *BusinessEvent, error)

	// Unsubscribe 取消订阅
	Unsubscribe(subscriptionID string) error

	// Publish 发布业务事件（内部使用）
	Publish(event *BusinessEvent) error
}

// BusinessEventPublisher 业务事件发布者接口
type BusinessEventPublisher interface {
	// PublishTableEvent 发布表事件
	PublishTableEvent(ctx context.Context, eventType BusinessEventType, tableID string, data interface{}, userID string) error

	// PublishFieldEvent 发布字段事件
	PublishFieldEvent(ctx context.Context, eventType BusinessEventType, tableID, fieldID string, data interface{}, userID string) error

	// PublishRecordEvent 发布记录事件
	PublishRecordEvent(ctx context.Context, eventType BusinessEventType, tableID, recordID string, data interface{}, userID string, version int64) error

	// PublishCalculationEvent 发布计算事件
	PublishCalculationEvent(ctx context.Context, tableID, recordID string, data interface{}, userID string) error
}

// BusinessEventManager 业务事件管理器
type BusinessEventManager struct {
	subscribers map[string]chan *BusinessEvent
	subMutex    sync.RWMutex
	logger      *zap.Logger
	redisClient *redis.Client
	redisPrefix string
	ctx         context.Context
	cancel      context.CancelFunc
	shutdown    bool
	shutdownMux sync.RWMutex
}

// NewBusinessEventManager 创建业务事件管理器
func NewBusinessEventManager(logger *zap.Logger) *BusinessEventManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &BusinessEventManager{
		subscribers: make(map[string]chan *BusinessEvent),
		logger:      logger,
		redisPrefix: "luckdb:events",
		ctx:         ctx,
		cancel:      cancel,
	}
}

// NewBusinessEventManagerWithRedis 创建带Redis分布式广播的业务事件管理器
func NewBusinessEventManagerWithRedis(logger *zap.Logger, redisClient *redis.Client, redisPrefix string) *BusinessEventManager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &BusinessEventManager{
		subscribers: make(map[string]chan *BusinessEvent),
		logger:      logger,
		redisClient: redisClient,
		redisPrefix: redisPrefix,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 启动Redis订阅监听
	if redisClient != nil {
		go manager.startRedisSubscriber()
	}

	return manager
}

// Subscribe 订阅业务事件
func (m *BusinessEventManager) Subscribe(ctx context.Context, eventTypes []BusinessEventType) (<-chan *BusinessEvent, error) {
	subscriptionID := fmt.Sprintf("sub_%d", time.Now().UnixNano())
	eventChan := make(chan *BusinessEvent, 100) // 缓冲通道

	m.subMutex.Lock()
	m.subscribers[subscriptionID] = eventChan
	m.subMutex.Unlock()

	m.logger.Info("Business event subscription created",
		zap.String("subscription_id", subscriptionID),
		zap.Strings("event_types", convertEventTypesToStrings(eventTypes)))

	// 启动清理协程
	go func() {
		<-ctx.Done()
		m.Unsubscribe(subscriptionID)
	}()

	return eventChan, nil
}

// Unsubscribe 取消订阅
func (m *BusinessEventManager) Unsubscribe(subscriptionID string) error {
	m.subMutex.Lock()
	defer m.subMutex.Unlock()

	if eventChan, exists := m.subscribers[subscriptionID]; exists {
		close(eventChan)
		delete(m.subscribers, subscriptionID)
		m.logger.Info("Business event subscription removed",
			zap.String("subscription_id", subscriptionID))
	}

	return nil
}

// Publish 发布业务事件
func (m *BusinessEventManager) Publish(event *BusinessEvent) error {
	// 设置事件ID和时间戳
	if event.ID == "" {
		event.ID = fmt.Sprintf("evt_%d_%d", time.Now().UnixNano(), len(event.Type))
	}
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}

	// 如果配置了Redis，先发布到Redis进行分布式广播
	if m.redisClient != nil {
		if err := m.publishToRedis(event); err != nil {
			m.logger.Error("Failed to publish event to Redis",
				zap.String("event_id", event.ID),
				zap.Error(err))
			// 继续本地广播，不因Redis失败而阻塞
		}
	}

	// 本地广播
	return m.publishLocally(event)
}

// publishToRedis 发布事件到Redis进行分布式广播
func (m *BusinessEventManager) publishToRedis(event *BusinessEvent) error {
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	channel := fmt.Sprintf("%s:broadcast", m.redisPrefix)
	return m.redisClient.Publish(context.Background(), channel, eventData).Err()
}

// publishLocally 本地广播事件
func (m *BusinessEventManager) publishLocally(event *BusinessEvent) error {
	m.subMutex.RLock()
	subscriberCount := len(m.subscribers)
	subscribers := make([]chan *BusinessEvent, 0, subscriberCount)
	for _, ch := range m.subscribers {
		subscribers = append(subscribers, ch)
	}
	m.subMutex.RUnlock()

	// 向所有订阅者发送事件
	for _, ch := range subscribers {
		select {
		case ch <- event:
			// 成功发送
		default:
			// 通道已满，记录警告但不阻塞
			m.logger.Warn("Business event channel full, dropping event",
				zap.String("event_id", event.ID),
				zap.String("event_type", string(event.Type)))
		}
	}

	m.logger.Debug("Business event published",
		zap.String("event_id", event.ID),
		zap.String("event_type", string(event.Type)),
		zap.String("table_id", event.TableID),
		zap.String("record_id", event.RecordID),
		zap.Int("subscriber_count", subscriberCount))

	return nil
}

// PublishTableEvent 发布表事件
func (m *BusinessEventManager) PublishTableEvent(ctx context.Context, eventType BusinessEventType, tableID string, data interface{}, userID string) error {
	event := &BusinessEvent{
		Type:    eventType,
		TableID: tableID,
		Data:    data,
		UserID:  userID,
	}

	return m.Publish(event)
}

// PublishFieldEvent 发布字段事件
func (m *BusinessEventManager) PublishFieldEvent(ctx context.Context, eventType BusinessEventType, tableID, fieldID string, data interface{}, userID string) error {
	event := &BusinessEvent{
		Type:    eventType,
		TableID: tableID,
		FieldID: fieldID,
		Data:    data,
		UserID:  userID,
	}

	return m.Publish(event)
}

// PublishRecordEvent 发布记录事件
func (m *BusinessEventManager) PublishRecordEvent(ctx context.Context, eventType BusinessEventType, tableID, recordID string, data interface{}, userID string, version int64) error {
	event := &BusinessEvent{
		Type:     eventType,
		TableID:  tableID,
		RecordID: recordID,
		Data:     data,
		UserID:   userID,
		Version:  version,
	}

	return m.Publish(event)
}

// PublishCalculationEvent 发布计算事件
func (m *BusinessEventManager) PublishCalculationEvent(ctx context.Context, tableID, recordID string, data interface{}, userID string) error {
	event := &BusinessEvent{
		Type:     BusinessEventTypeCalculationUpdate,
		TableID:  tableID,
		RecordID: recordID,
		Data:     data,
		UserID:   userID,
	}

	return m.Publish(event)
}

// GetSubscriberCount 获取订阅者数量
func (m *BusinessEventManager) GetSubscriberCount() int {
	m.subMutex.RLock()
	defer m.subMutex.RUnlock()
	return len(m.subscribers)
}

// convertEventTypesToStrings 转换事件类型为字符串切片
func convertEventTypesToStrings(eventTypes []BusinessEventType) []string {
	result := make([]string, len(eventTypes))
	for i, et := range eventTypes {
		result[i] = string(et)
	}
	return result
}

// BusinessEventFilter 业务事件过滤器
type BusinessEventFilter struct {
	EventTypes []BusinessEventType `json:"event_types,omitempty"`
	TableID    string              `json:"table_id,omitempty"`
	RecordID   string              `json:"record_id,omitempty"`
	FieldID    string              `json:"field_id,omitempty"`
	UserID     string              `json:"user_id,omitempty"`
}

// Matches 检查事件是否匹配过滤器
func (f *BusinessEventFilter) Matches(event *BusinessEvent) bool {
	// 检查事件类型
	if len(f.EventTypes) > 0 {
		found := false
		for _, et := range f.EventTypes {
			if et == event.Type {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// 检查表ID
	if f.TableID != "" && f.TableID != event.TableID {
		return false
	}

	// 检查记录ID
	if f.RecordID != "" && f.RecordID != event.RecordID {
		return false
	}

	// 检查字段ID
	if f.FieldID != "" && f.FieldID != event.FieldID {
		return false
	}

	// 检查用户ID
	if f.UserID != "" && f.UserID != event.UserID {
		return false
	}

	return true
}

// BusinessEventSubscription 业务事件订阅
type BusinessEventSubscription struct {
	ID        string                `json:"id"`
	Filter    *BusinessEventFilter  `json:"filter"`
	EventChan <-chan *BusinessEvent `json:"-"`
	Cancel    context.CancelFunc    `json:"-"`
}

// NewBusinessEventSubscription 创建业务事件订阅
func NewBusinessEventSubscription(id string, filter *BusinessEventFilter, eventChan <-chan *BusinessEvent, cancel context.CancelFunc) *BusinessEventSubscription {
	return &BusinessEventSubscription{
		ID:        id,
		Filter:    filter,
		EventChan: eventChan,
		Cancel:    cancel,
	}
}

// Close 关闭订阅
func (s *BusinessEventSubscription) Close() {
	if s.Cancel != nil {
		s.Cancel()
	}
}

// startRedisSubscriber 启动Redis订阅监听
func (m *BusinessEventManager) startRedisSubscriber() {
	if m.redisClient == nil {
		return
	}

	channel := fmt.Sprintf("%s:broadcast", m.redisPrefix)

	// 使用错误抑制器包装Redis操作
	err := cache.SuppressRedisErrors(m.logger, func() error {
		pubsub := m.redisClient.Subscribe(m.ctx, channel)
		defer func() {
			// 优雅关闭订阅
			if err := pubsub.Close(); err != nil {
				// 忽略连接关闭错误
				if !strings.Contains(err.Error(), "use of closed network connection") {
					m.logger.Warn("Redis pubsub close error", zap.Error(err))
				}
			}
		}()

		return m.handleRedisSubscription(pubsub)
	})

	if err != nil {
		m.logger.Error("Redis subscription failed", zap.Error(err))
	}
}

// handleRedisSubscription 处理Redis订阅
func (m *BusinessEventManager) handleRedisSubscription(pubsub *redis.PubSub) error {
	channel := fmt.Sprintf("%s:broadcast", m.redisPrefix)
	m.logger.Info("Started Redis event subscriber", zap.String("channel", channel))

	for {
		// 检查关闭状态
		m.shutdownMux.RLock()
		if m.shutdown {
			m.shutdownMux.RUnlock()
			m.logger.Info("Redis event subscriber stopping due to shutdown")
			return nil
		}
		m.shutdownMux.RUnlock()

		select {
		case <-m.ctx.Done():
			m.logger.Info("Redis event subscriber stopping due to context cancellation")
			return nil
		default:
		}

		// 使用带超时的接收，避免长时间阻塞
		ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		msg, err := pubsub.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			// 检查是否是上下文取消错误
			if m.ctx.Err() != nil {
				m.logger.Info("Redis subscription stopped due to context cancellation")
				return nil
			}

			// 检查是否是连接关闭错误
			if strings.Contains(err.Error(), "use of closed network connection") ||
				strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "broken pipe") {
				m.logger.Info("Redis subscription stopped due to connection closure")
				return nil
			}

			// 检查是否是超时错误
			if strings.Contains(err.Error(), "context deadline exceeded") {
				continue // 超时是正常的，继续循环
			}

			m.logger.Error("Redis subscription error", zap.Error(err))
			time.Sleep(time.Second) // 避免快速重连
			continue
		}

		// 解析事件
		var event BusinessEvent
		if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
			m.logger.Error("Failed to unmarshal Redis event",
				zap.String("payload", msg.Payload),
				zap.Error(err))
			continue
		}

		// 本地广播（避免重复发布到Redis）
		if err := m.publishLocally(&event); err != nil {
			m.logger.Error("Failed to broadcast Redis event locally",
				zap.String("event_id", event.ID),
				zap.Error(err))
		}
	}
}

// Shutdown 优雅关闭业务事件管理器
func (m *BusinessEventManager) Shutdown() error {
	m.shutdownMux.Lock()
	if m.shutdown {
		m.shutdownMux.Unlock()
		return nil
	}
	m.shutdown = true
	m.shutdownMux.Unlock()

	// 取消上下文，停止Redis订阅
	if m.cancel != nil {
		m.cancel()
	}

	// 关闭所有订阅者通道
	m.subMutex.Lock()
	for subscriptionID, eventChan := range m.subscribers {
		close(eventChan)
		delete(m.subscribers, subscriptionID)
		m.logger.Info("Business event subscription closed during shutdown",
			zap.String("subscription_id", subscriptionID))
	}
	m.subMutex.Unlock()

	m.logger.Info("Business event manager shutdown completed")
	return nil
}
