package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/events"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// SSEManager Server-Sent Events 管理器
// 用于轻量级的实时数据同步，补充 Yjs 的协作功能
type SSEManager struct {
	clients        map[string]*SSEClient
	channels       map[string]map[string]*SSEClient
	broker         *SSEBroker
	businessEvents events.BusinessEventSubscriber // ✨ 业务事件订阅器
	logger         *zap.Logger
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
}

// SSEClient SSE 客户端
type SSEClient struct {
	ID        string
	Response  http.ResponseWriter
	Request   *http.Request
	Channel   chan *SSEMessage
	UserID    string
	Channels  map[string]bool
	IsActive  bool
	CreatedAt time.Time
	LastSeen  time.Time
	mu        sync.RWMutex
}

// SSEMessage SSE 消息
type SSEMessage struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"timestamp"`
}

// SSEBroker SSE 消息代理
type SSEBroker struct {
	clients  map[string]*SSEClient
	channels map[string]map[string]*SSEClient
	mu       sync.RWMutex
}

// NewSSEManager 创建 SSE 管理器
func NewSSEManager(logger *zap.Logger, businessEvents events.BusinessEventSubscriber) *SSEManager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &SSEManager{
		clients:        make(map[string]*SSEClient),
		channels:       make(map[string]map[string]*SSEClient),
		businessEvents: businessEvents,
		broker: &SSEBroker{
			clients:  make(map[string]*SSEClient),
			channels: make(map[string]map[string]*SSEClient),
		},
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// 启动业务事件监听
	if businessEvents != nil {
		go manager.startBusinessEventSubscription()
	}

	return manager
}

// startBusinessEventSubscription 启动业务事件订阅
func (sm *SSEManager) startBusinessEventSubscription() {
	if sm.businessEvents == nil {
		return
	}

	// 订阅所有业务事件类型
	eventTypes := []events.BusinessEventType{
		events.BusinessEventTypeTableCreate,
		events.BusinessEventTypeTableUpdate,
		events.BusinessEventTypeTableDelete,
		events.BusinessEventTypeFieldCreate,
		events.BusinessEventTypeFieldUpdate,
		events.BusinessEventTypeFieldDelete,
		events.BusinessEventTypeRecordCreate,
		events.BusinessEventTypeRecordUpdate,
		events.BusinessEventTypeRecordDelete,
		events.BusinessEventTypeCalculationUpdate,
		events.BusinessEventTypeViewCreate,
		events.BusinessEventTypeViewUpdate,
		events.BusinessEventTypeViewDelete,
	}

	eventChan, err := sm.businessEvents.Subscribe(sm.ctx, eventTypes)
	if err != nil {
		sm.logger.Error("订阅业务事件失败", zap.Error(err))
		return
	}

	sm.logger.Info("SSE 管理器已订阅业务事件")

	// 监听业务事件
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				sm.logger.Info("业务事件通道已关闭")
				return
			}
			sm.handleBusinessEvent(event)
		case <-sm.ctx.Done():
			sm.logger.Info("SSE 管理器业务事件订阅已停止")
			return
		}
	}
}

// handleBusinessEvent 处理业务事件
func (sm *SSEManager) handleBusinessEvent(event *events.BusinessEvent) {
	// 将业务事件转换为SSE消息
	sseMessage := &SSEMessage{
		ID:        event.ID,
		Type:      string(event.Type),
		Data:      event,
		Timestamp: event.Timestamp,
	}

	// 根据事件类型确定广播策略
	switch event.Type {
	case events.BusinessEventTypeRecordCreate,
		events.BusinessEventTypeRecordUpdate,
		events.BusinessEventTypeRecordDelete,
		events.BusinessEventTypeCalculationUpdate:
		// 记录相关事件：广播到表级别频道
		if event.TableID != "" {
			channel := fmt.Sprintf("table:%s", event.TableID)
			sm.broker.BroadcastToChannel(channel, sseMessage)
		}

		// 也广播到记录级别频道
		if event.RecordID != "" {
			channel := fmt.Sprintf("record:%s", event.RecordID)
			sm.broker.BroadcastToChannel(channel, sseMessage)
		}

	case events.BusinessEventTypeFieldCreate,
		events.BusinessEventTypeFieldUpdate,
		events.BusinessEventTypeFieldDelete:
		// 字段相关事件：广播到表级别频道
		if event.TableID != "" {
			channel := fmt.Sprintf("table:%s", event.TableID)
			sm.broker.BroadcastToChannel(channel, sseMessage)
		}

	case events.BusinessEventTypeTableCreate,
		events.BusinessEventTypeTableUpdate,
		events.BusinessEventTypeTableDelete:
		// 表相关事件：广播到全局频道
		sm.broker.BroadcastToChannel("global", sseMessage)
	}

	sm.logger.Debug("业务事件已转换为SSE消息",
		zap.String("event_type", string(event.Type)),
		zap.String("table_id", event.TableID),
		zap.String("record_id", event.RecordID))
}

// HandleSSE 处理 SSE 连接
func (sm *SSEManager) HandleSSE(c *gin.Context) {
	// 设置 SSE 头部
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Headers", "Cache-Control")

	// 获取客户端ID
	clientID := c.Query("client_id")
	if clientID == "" {
		clientID = fmt.Sprintf("sse_client_%d", time.Now().UnixNano())
	}

	userID := c.Query("user_id")

	// 创建 SSE 客户端
	client := &SSEClient{
		ID:        clientID,
		Response:  c.Writer,
		Request:   c.Request,
		Channel:   make(chan *SSEMessage, 100),
		UserID:    userID,
		Channels:  make(map[string]bool),
		IsActive:  true,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
	}

	// 注册客户端
	sm.registerClient(client)
	defer sm.unregisterClient(client)

	// 发送连接确认消息
	connectMessage := &SSEMessage{
		ID:   clientID,
		Type: "connect",
		Data: map[string]interface{}{
			"clientId": clientID,
			"message":  "Connected to SSE",
		},
		Timestamp: time.Now().Unix(),
	}

	if err := sm.sendMessage(client, connectMessage); err != nil {
		sm.logger.Error("Failed to send connect message", zap.Error(err))
		return
	}

	// 处理客户端消息
	sm.handleClientMessages(client)
}

// HandleSubscription 处理订阅请求
func (sm *SSEManager) HandleSubscription(c *gin.Context) {
	var req struct {
		ClientID      string   `json:"client_id"`
		Subscriptions []string `json:"subscriptions"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	// 验证客户端
	client, exists := sm.getClient(req.ClientID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Client not found"})
		return
	}

	// 更新订阅
	if err := sm.updateSubscriptions(client, req.Subscriptions); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update subscriptions"})
		return
	}

	// 发送确认消息
	confirmMessage := &SSEMessage{
		ID:   client.ID,
		Type: "subscription_updated",
		Data: map[string]interface{}{
			"subscriptions": req.Subscriptions,
		},
		Timestamp: time.Now().Unix(),
	}

	if err := sm.sendMessage(client, confirmMessage); err != nil {
		sm.logger.Error("Failed to send subscription confirmation", zap.Error(err))
	}

	c.Status(http.StatusNoContent)
}

// registerClient 注册客户端
func (sm *SSEManager) registerClient(client *SSEClient) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.clients[client.ID] = client
	sm.broker.RegisterClient(client)

	sm.logger.Info("SSE client registered", zap.String("client_id", client.ID))
}

// unregisterClient 取消注册客户端
func (sm *SSEManager) unregisterClient(client *SSEClient) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.clients, client.ID)
	sm.broker.UnregisterClient(client.ID)

	sm.logger.Info("SSE client unregistered", zap.String("client_id", client.ID))
}

// getClient 获取客户端
func (sm *SSEManager) getClient(clientID string) (*SSEClient, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	client, exists := sm.clients[clientID]
	return client, exists
}

// updateSubscriptions 更新订阅
func (sm *SSEManager) updateSubscriptions(client *SSEClient, channels []string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	// 清除现有订阅
	for channel := range client.Channels {
		sm.broker.UnsubscribeFromChannel(client.ID, channel)
	}

	// 添加新订阅
	client.Channels = make(map[string]bool)
	for _, channel := range channels {
		client.Channels[channel] = true
		sm.broker.SubscribeToChannel(client.ID, channel)
	}

	return nil
}

// handleClientMessages 处理客户端消息
func (sm *SSEManager) handleClientMessages(client *SSEClient) {
	ticker := time.NewTicker(30 * time.Second) // 心跳间隔
	defer ticker.Stop()

	for {
		select {
		case message := <-client.Channel:
			if err := sm.sendMessage(client, message); err != nil {
				sm.logger.Error("Failed to send message",
					zap.String("client_id", client.ID),
					zap.Error(err))
				return
			}

		case <-ticker.C:
			// 发送心跳消息
			heartbeatMessage := &SSEMessage{
				ID:   client.ID,
				Type: "heartbeat",
				Data: map[string]interface{}{
					"timestamp": time.Now().Unix(),
				},
				Timestamp: time.Now().Unix(),
			}

			if err := sm.sendMessage(client, heartbeatMessage); err != nil {
				sm.logger.Error("Failed to send heartbeat",
					zap.String("client_id", client.ID),
					zap.Error(err))
				return
			}

		case <-client.Request.Context().Done():
			// 客户端断开连接
			return
		}
	}
}

// sendMessage 发送消息
func (sm *SSEManager) sendMessage(client *SSEClient, message *SSEMessage) error {
	// 序列化消息
	data, err := json.Marshal(message.Data)
	if err != nil {
		return err
	}

	// 构建 SSE 格式
	sseData := fmt.Sprintf("id: %s\nevent: %s\ndata: %s\n\n",
		message.ID, message.Type, string(data))

	// 发送消息
	_, err = client.Response.Write([]byte(sseData))
	if err != nil {
		return err
	}

	// 刷新响应
	if flusher, ok := client.Response.(http.Flusher); ok {
		flusher.Flush()
	}

	// 更新最后活跃时间
	client.mu.Lock()
	client.LastSeen = time.Now()
	client.mu.Unlock()

	return nil
}

// BroadcastToChannel 向频道广播消息
func (sm *SSEManager) BroadcastToChannel(channel string, data interface{}) error {
	message := &SSEMessage{
		Type: "broadcast",
		Data: map[string]interface{}{
			"channel":   channel,
			"message":   data,
			"timestamp": time.Now().Unix(),
		},
		Timestamp: time.Now().Unix(),
	}

	return sm.broker.BroadcastToChannel(channel, message)
}

// BroadcastToUser 向用户广播消息
func (sm *SSEManager) BroadcastToUser(userID string, data interface{}) error {
	message := &SSEMessage{
		Type: "user_message",
		Data: map[string]interface{}{
			"user_id":   userID,
			"message":   data,
			"timestamp": time.Now().Unix(),
		},
		Timestamp: time.Now().Unix(),
	}

	return sm.broker.BroadcastToUser(userID, message)
}

// BroadcastRecordCreate 广播记录创建事件
func (sm *SSEManager) BroadcastRecordCreate(record interface{}) error {
	message := &SSEMessage{
		Type: "record_create",
		Data: map[string]interface{}{
			"action":    "create",
			"record":    record,
			"timestamp": time.Now().UnixNano(),
		},
		Timestamp: time.Now().Unix(),
	}

	return sm.broker.BroadcastToChannel("records", message)
}

// BroadcastRecordUpdate 广播记录更新事件
func (sm *SSEManager) BroadcastRecordUpdate(record interface{}) error {
	message := &SSEMessage{
		Type: "record_update",
		Data: map[string]interface{}{
			"action":    "update",
			"record":    record,
			"timestamp": time.Now().UnixNano(),
		},
		Timestamp: time.Now().Unix(),
	}

	return sm.broker.BroadcastToChannel("records", message)
}

// BroadcastRecordDelete 广播记录删除事件
func (sm *SSEManager) BroadcastRecordDelete(record interface{}) error {
	message := &SSEMessage{
		Type: "record_delete",
		Data: map[string]interface{}{
			"action":    "delete",
			"record":    record,
			"timestamp": time.Now().UnixNano(),
		},
		Timestamp: time.Now().Unix(),
	}

	return sm.broker.BroadcastToChannel("records", message)
}

// GetStats 获取统计信息
func (sm *SSEManager) GetStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return map[string]interface{}{
		"clients":         len(sm.clients),
		"active_channels": sm.broker.GetChannelCount(),
	}
}

// Shutdown 关闭管理器
func (sm *SSEManager) Shutdown() error {
	sm.cancel()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 关闭所有客户端连接
	for _, client := range sm.clients {
		close(client.Channel)
	}

	// 清空客户端列表
	sm.clients = make(map[string]*SSEClient)

	sm.logger.Info("SSE manager shutdown completed")
	return nil
}

// SSEBroker 方法
func (sb *SSEBroker) RegisterClient(client *SSEClient) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	sb.clients[client.ID] = client
}

func (sb *SSEBroker) UnregisterClient(clientID string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// 从所有频道移除客户端
	for channel, clients := range sb.channels {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(sb.channels, channel)
		}
	}

	// 从客户端列表移除
	delete(sb.clients, clientID)
}

func (sb *SSEBroker) SubscribeToChannel(clientID, channel string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	client, exists := sb.clients[clientID]
	if !exists {
		return
	}

	if sb.channels[channel] == nil {
		sb.channels[channel] = make(map[string]*SSEClient)
	}

	sb.channels[channel][clientID] = client
}

func (sb *SSEBroker) UnsubscribeFromChannel(clientID, channel string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.channels[channel] != nil {
		delete(sb.channels[channel], clientID)
		if len(sb.channels[channel]) == 0 {
			delete(sb.channels, channel)
		}
	}
}

func (sb *SSEBroker) BroadcastToChannel(channel string, message *SSEMessage) error {
	sb.mu.RLock()
	clients := sb.channels[channel]
	sb.mu.RUnlock()

	if clients == nil {
		return nil // 没有订阅者
	}

	var errors []error
	for _, client := range clients {
		if !client.IsActive {
			continue
		}

		select {
		case client.Channel <- message:
		default:
			// 通道已满，标记客户端为非活跃
			client.mu.Lock()
			client.IsActive = false
			client.mu.Unlock()
			errors = append(errors, fmt.Errorf("client channel full: %s", client.ID))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("broadcast errors: %v", errors)
	}

	return nil
}

func (sb *SSEBroker) BroadcastToUser(userID string, message *SSEMessage) error {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	var errors []error
	for _, client := range sb.clients {
		if client.UserID == userID && client.IsActive {
			select {
			case client.Channel <- message:
			default:
				client.mu.Lock()
				client.IsActive = false
				client.mu.Unlock()
				errors = append(errors, fmt.Errorf("client channel full: %s", client.ID))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("broadcast errors: %v", errors)
	}

	return nil
}

func (sb *SSEBroker) GetChannelCount() int {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	return len(sb.channels)
}
