package sharedb

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RedisPubSub Redis 发布订阅实现
type RedisPubSub struct {
	client        *redis.Client
	prefix        string
	listeners     sync.Map                      // channel -> []func(*opbuilder.Operation)
	subscriptions map[string]context.CancelFunc // channel -> cancelFunc
	logger        *zap.Logger
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewRedisPubSub 创建 Redis 发布订阅
func NewRedisPubSub(redisURI string, logger *zap.Logger) (PubSub, error) {
	// 解析 Redis URI
	opt, err := redis.ParseURL(redisURI)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opt)

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	// 创建带取消功能的 context
	ctx, cancel := context.WithCancel(context.Background())

	return &RedisPubSub{
		client:        client,
		prefix:        "sharedb:",
		subscriptions: make(map[string]context.CancelFunc),
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Publish 发布消息
func (p *RedisPubSub) Publish(ctx context.Context, channels []string, op *opbuilder.Operation) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 序列化操作
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	// 发布到所有频道
	for _, channel := range channels {
		fullChannel := p.prefix + channel
		if err := p.client.Publish(ctx, fullChannel, data).Err(); err != nil {
			p.logger.Error("Failed to publish to channel",
				zap.Error(err),
				zap.String("channel", fullChannel))
			continue
		}
	}

	p.logger.Debug("Message published to Redis",
		zap.Strings("channels", channels),
		zap.String("op_type", string(op.Type)))

	return nil
}

// Subscribe 订阅频道
func (p *RedisPubSub) Subscribe(ctx context.Context, channel string, handler func(*opbuilder.Operation)) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	fullChannel := p.prefix + channel

	// 如果已经订阅，先取消
	if cancel, exists := p.subscriptions[fullChannel]; exists {
		cancel()
	}

	// 创建子 context 用于取消订阅
	subCtx, cancel := context.WithCancel(p.ctx)
	p.subscriptions[fullChannel] = cancel

	// 启动订阅协程
	go func() {
		defer func() {
			// 清理订阅记录
			p.mu.Lock()
			delete(p.subscriptions, fullChannel)
			p.mu.Unlock()
		}()

		pubsub := p.client.Subscribe(subCtx, fullChannel)
		defer pubsub.Close()

		// 移除 Debug 日志以减少输出
		// p.logger.Debug("Subscribed to Redis channel", zap.String("channel", fullChannel))

		// 监听消息
		ch := pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case msg := <-ch:
				if msg == nil {
					continue
				}

				// 解析操作
				var op opbuilder.Operation
				if err := json.Unmarshal([]byte(msg.Payload), &op); err != nil {
					p.logger.Error("Failed to unmarshal operation",
						zap.Error(err),
						zap.String("channel", fullChannel))
					continue
				}

				// 调用处理器
				handler(&op)
			}
		}
	}()

	return nil
}

// Unsubscribe 取消订阅
func (p *RedisPubSub) Unsubscribe(ctx context.Context, channel string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	fullChannel := p.prefix + channel

	// 取消订阅
	if cancel, exists := p.subscriptions[fullChannel]; exists {
		cancel()
		delete(p.subscriptions, fullChannel)
	}

	return nil
}

// Close 关闭发布订阅
func (p *RedisPubSub) Close() error {
	// 取消所有订阅
	p.mu.Lock()
	for channel, cancel := range p.subscriptions {
		cancel()
		delete(p.subscriptions, channel)
	}
	p.mu.Unlock()

	// 取消主 context
	if p.cancel != nil {
		p.cancel()
	}

	// 关闭 Redis 客户端
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}
