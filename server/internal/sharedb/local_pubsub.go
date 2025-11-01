package sharedb

import (
	"context"
	"sync"

	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
	"go.uber.org/zap"
)

// LocalPubSub 本地发布订阅实现
type LocalPubSub struct {
	channels sync.Map // channel -> []chan *opbuilder.Operation
	logger   *zap.Logger
	mu       sync.RWMutex
}

// NewLocalPubSub 创建本地发布订阅
func NewLocalPubSub(logger *zap.Logger) PubSub {
	return &LocalPubSub{
		logger: logger,
	}
}

// Publish 发布消息
func (p *LocalPubSub) Publish(ctx context.Context, channels []string, op *opbuilder.Operation) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	totalSubscribers := 0
	for _, channel := range channels {
		// 获取频道的订阅者
		subscribersInterface, exists := p.channels.Load(channel)
		if !exists {
			p.logger.Info("No subscribers for channel", zap.String("channel", channel))
			continue
		}

		subscribers := subscribersInterface.([]chan *opbuilder.Operation)
		totalSubscribers += len(subscribers)

		p.logger.Info("找到订阅者",
			zap.String("channel", channel),
			zap.Int("subscriber_count", len(subscribers)))

		// 向所有订阅者发送消息
		sentCount := 0
		for i, subscriber := range subscribers {
			select {
			case subscriber <- op:
				sentCount++
				// 消息成功发送
			case <-ctx.Done():
				return ctx.Err()
			default:
				// 如果订阅者通道已满，跳过
				p.logger.Warn("Subscriber channel is full, skipping",
					zap.String("channel", channel),
					zap.Int("subscriber_index", i))
			}
		}
		
		p.logger.Info("Message sent to subscribers",
			zap.String("channel", channel),
			zap.Int("total_subscribers", len(subscribers)),
			zap.Int("sent_count", sentCount))
	}

	p.logger.Info("Message published",
		zap.Strings("channels", channels),
		zap.String("op_type", string(op.Type)),
		zap.Int("total_subscribers", totalSubscribers))

	return nil
}

// Subscribe 订阅频道
func (p *LocalPubSub) Subscribe(ctx context.Context, channel string, handler func(*opbuilder.Operation)) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 创建订阅者通道
	subscriber := make(chan *opbuilder.Operation, 100) // 缓冲100个消息

	// 获取或创建频道的订阅者列表（需要加锁保护）
	var subscribers []chan *opbuilder.Operation
	subscribersInterface, loaded := p.channels.Load(channel)
	if loaded {
		subscribers = subscribersInterface.([]chan *opbuilder.Operation)
	} else {
		subscribers = make([]chan *opbuilder.Operation, 0)
	}
	
	// 创建新的切片，避免竞态条件
	newSubscribers := make([]chan *opbuilder.Operation, len(subscribers), len(subscribers)+1)
	copy(newSubscribers, subscribers)
	newSubscribers = append(newSubscribers, subscriber)
	p.channels.Store(channel, newSubscribers)

	p.logger.Info("Subscribed to channel",
		zap.String("channel", channel),
		zap.Int("total_subscribers", len(newSubscribers)),
		zap.Bool("was_loaded", loaded))

	// 启动处理协程
	go func() {
		defer func() {
			// 只移除当前订阅者，不关闭所有订阅者
			p.removeSubscriber(channel, subscriber)
		}()

		for {
			select {
			case <-ctx.Done():
				p.logger.Debug("订阅 context 已取消",
					zap.String("channel", channel))
				return
			case op := <-subscriber:
				if op != nil {
					handler(op)
				}
			}
		}
	}()

	return nil
}

// removeSubscriber 移除特定的订阅者
func (p *LocalPubSub) removeSubscriber(channel string, subscriber chan *opbuilder.Operation) {
	p.mu.Lock()
	defer p.mu.Unlock()

	subscribersInterface, exists := p.channels.Load(channel)
	if !exists {
		return
	}

	subscribers := subscribersInterface.([]chan *opbuilder.Operation)

	// 找到并移除特定订阅者
	newSubscribers := make([]chan *opbuilder.Operation, 0, len(subscribers))
	for _, sub := range subscribers {
		if sub != subscriber {
			newSubscribers = append(newSubscribers, sub)
		}
	}

	// 关闭被移除的订阅者通道
	close(subscriber)

	if len(newSubscribers) == 0 {
		// 如果没有订阅者了，删除频道
		p.channels.Delete(channel)
		p.logger.Debug("Channel deleted (no subscribers)", zap.String("channel", channel))
	} else {
		// 更新订阅者列表
		p.channels.Store(channel, newSubscribers)
		p.logger.Debug("Subscriber removed",
			zap.String("channel", channel),
			zap.Int("remaining_subscribers", len(newSubscribers)))
	}
}

// Unsubscribe 取消订阅（取消整个频道的所有订阅）
func (p *LocalPubSub) Unsubscribe(ctx context.Context, channel string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	subscribersInterface, exists := p.channels.Load(channel)
	if !exists {
		return nil
	}

	subscribers := subscribersInterface.([]chan *opbuilder.Operation)

	// 关闭所有订阅者通道
	for _, subscriber := range subscribers {
		close(subscriber)
	}

	// 删除频道
	p.channels.Delete(channel)

	p.logger.Debug("Unsubscribed from channel", zap.String("channel", channel))
	return nil
}

// Close 关闭发布订阅
func (p *LocalPubSub) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 关闭所有频道
	p.channels.Range(func(key, value interface{}) bool {
		subscribers := value.([]chan *opbuilder.Operation)
		for _, subscriber := range subscribers {
			close(subscriber)
		}
		p.channels.Delete(key)
		return true
	})

	p.logger.Info("Local pub/sub closed")
	return nil
}
