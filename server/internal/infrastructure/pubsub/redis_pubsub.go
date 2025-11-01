package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// Message 消息结构
type Message struct {
	Channel string      `json:"channel"`
	Type    string      `json:"type"`
	Data    interface{} `json:"data"`
}

// MessageHandler 消息处理器
type MessageHandler func(msg *Message) error

// RedisPubSub Redis发布/订阅管理器
// 用于分布式WebSocket消息广播
type RedisPubSub struct {
	client   *redis.Client
	logger   *zap.Logger
	handlers map[string][]MessageHandler // channel -> handlers
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewRedisPubSub 创建Redis发布/订阅管理器
func NewRedisPubSub(client *redis.Client, logger *zap.Logger) *RedisPubSub {
	ctx, cancel := context.WithCancel(context.Background())
	return &RedisPubSub{
		client:   client,
		logger:   logger,
		handlers: make(map[string][]MessageHandler),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Publish 发布消息到指定频道
// 所有订阅该频道的服务实例都会收到消息
func (r *RedisPubSub) Publish(ctx context.Context, channel string, msgType string, data interface{}) error {
	msg := &Message{
		Channel: channel,
		Type:    msgType,
		Data:    data,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		r.logger.Error("Failed to marshal message",
			zap.String("channel", channel),
			zap.Error(err),
		)
		return fmt.Errorf("marshal message: %w", err)
	}

	err = r.client.Publish(ctx, channel, payload).Err()
	if err != nil {
		r.logger.Error("Failed to publish message",
			zap.String("channel", channel),
			zap.Error(err),
		)
		return fmt.Errorf("publish message: %w", err)
	}

	r.logger.Debug("Message published",
		zap.String("channel", channel),
		zap.String("type", msgType),
	)

	return nil
}

// Subscribe 订阅频道并注册消息处理器
// 支持多个handler处理同一频道的消息
func (r *RedisPubSub) Subscribe(channels []string, handler MessageHandler) error {
	if len(channels) == 0 {
		return fmt.Errorf("no channels specified")
	}

	// 注册handler
	r.mu.Lock()
	for _, channel := range channels {
		r.handlers[channel] = append(r.handlers[channel], handler)
	}
	r.mu.Unlock()

	// 启动订阅goroutine
	r.wg.Add(1)
	go r.subscribeLoop(channels)

	return nil
}

// subscribeLoop 订阅循环
func (r *RedisPubSub) subscribeLoop(channels []string) {
	defer r.wg.Done()

	pubsub := r.client.Subscribe(r.ctx, channels...)
	defer pubsub.Close()

	r.logger.Info("Subscribed to channels",
		zap.Strings("channels", channels),
	)

	// 接收消息
	ch := pubsub.Channel()
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("Subscription cancelled")
			return
		case redisMsg, ok := <-ch:
			if !ok {
				r.logger.Warn("Channel closed, resubscribing...")
				// 重新订阅
				pubsub = r.client.Subscribe(r.ctx, channels...)
				ch = pubsub.Channel()
				continue
			}

			// 解析消息
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				r.logger.Error("Failed to unmarshal message",
					zap.String("channel", redisMsg.Channel),
					zap.Error(err),
				)
				continue
			}

			// 调用handlers
			r.handleMessage(&msg)
		}
	}
}

// handleMessage 处理消息
func (r *RedisPubSub) handleMessage(msg *Message) {
	r.mu.RLock()
	handlers, exists := r.handlers[msg.Channel]
	r.mu.RUnlock()

	if !exists || len(handlers) == 0 {
		return
	}

	// 并发调用所有handlers
	var wg sync.WaitGroup
	for _, handler := range handlers {
		wg.Add(1)
		go func(h MessageHandler) {
			defer wg.Done()
			if err := h(msg); err != nil {
				r.logger.Error("Handler error",
					zap.String("channel", msg.Channel),
					zap.String("type", msg.Type),
					zap.Error(err),
				)
			}
		}(handler)
	}
	wg.Wait()
}

// Close 关闭发布/订阅管理器
func (r *RedisPubSub) Close() error {
	r.logger.Info("Closing Redis PubSub...")
	r.cancel()
	r.wg.Wait()
	r.logger.Info("Redis PubSub closed")
	return nil
}

// ===== 便捷方法 =====

// PublishTableUpdate 发布表格更新消息
func (r *RedisPubSub) PublishTableUpdate(ctx context.Context, tableID string, data interface{}) error {
	channel := fmt.Sprintf("table:%s", tableID)
	return r.Publish(ctx, channel, "table_update", data)
}

// PublishRecordUpdate 发布记录更新消息
func (r *RedisPubSub) PublishRecordUpdate(ctx context.Context, tableID, recordID string, data interface{}) error {
	channel := fmt.Sprintf("record:%s:%s", tableID, recordID)
	return r.Publish(ctx, channel, "record_update", data)
}

// PublishFieldUpdate 发布字段更新消息
func (r *RedisPubSub) PublishFieldUpdate(ctx context.Context, tableID, fieldID string, data interface{}) error {
	channel := fmt.Sprintf("field:%s:%s", tableID, fieldID)
	return r.Publish(ctx, channel, "field_update", data)
}

// SubscribeToTableUpdates 订阅表格更新
func (r *RedisPubSub) SubscribeToTableUpdates(tableIDs []string, handler MessageHandler) error {
	channels := make([]string, len(tableIDs))
	for i, tableID := range tableIDs {
		channels[i] = fmt.Sprintf("table:%s", tableID)
	}
	return r.Subscribe(channels, handler)
}

// SubscribeToAllUpdates 订阅所有更新（使用Redis模式订阅）
func (r *RedisPubSub) SubscribeToAllUpdates(pattern string, handler MessageHandler) error {
	// 注册handler
	r.mu.Lock()
	r.handlers[pattern] = append(r.handlers[pattern], handler)
	r.mu.Unlock()

	// 启动模式订阅
	r.wg.Add(1)
	go r.patternSubscribeLoop(pattern)

	return nil
}

// patternSubscribeLoop 模式订阅循环
func (r *RedisPubSub) patternSubscribeLoop(pattern string) {
	defer r.wg.Done()

	pubsub := r.client.PSubscribe(r.ctx, pattern)
	defer pubsub.Close()

	r.logger.Info("Subscribed to pattern",
		zap.String("pattern", pattern),
	)

	// 接收消息
	ch := pubsub.Channel()
	for {
		select {
		case <-r.ctx.Done():
			return
		case redisMsg, ok := <-ch:
			if !ok {
				r.logger.Warn("Pattern channel closed, resubscribing...")
				pubsub = r.client.PSubscribe(r.ctx, pattern)
				ch = pubsub.Channel()
				continue
			}

			// 解析消息
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				r.logger.Error("Failed to unmarshal pattern message",
					zap.String("channel", redisMsg.Channel),
					zap.Error(err),
				)
				continue
			}

			// 调用handlers
			r.handleMessage(&msg)
		}
	}
}
