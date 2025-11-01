package sharedb

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
)

// Adapter ShareDB 适配器接口
type Adapter interface {
	// Query 查询文档
	Query(ctx context.Context, collection string, query interface{}, projection map[string]bool) ([]string, error)

	// GetSnapshot 获取文档快照
	GetSnapshot(ctx context.Context, collection string, id string, projection map[string]bool) (*Snapshot, error)

	// GetSnapshotBulk 批量获取文档快照
	GetSnapshotBulk(ctx context.Context, collection string, ids []string, projection map[string]bool) (map[string]*Snapshot, error)

	// GetOps 获取操作历史
	GetOps(ctx context.Context, collection string, id string, from, to int64) ([]*opbuilder.Operation, error)

	// SkipPoll 跳过轮询优化
	SkipPoll(ctx context.Context, collection string, id string, op *opbuilder.Operation, query interface{}) bool

	// Close 关闭适配器
	Close() error
}

// PubSub 发布订阅接口
type PubSub interface {
	// Publish 发布消息
	Publish(ctx context.Context, channels []string, op *opbuilder.Operation) error

	// Subscribe 订阅频道
	Subscribe(ctx context.Context, channel string, handler func(*opbuilder.Operation)) error

	// Unsubscribe 取消订阅
	Unsubscribe(ctx context.Context, channel string) error

	// Close 关闭发布订阅
	Close() error
}

// Middleware 中间件接口
type Middleware interface {
	// Handle 处理消息
	Handle(ctx context.Context, conn *Connection, msg *Message) error
}

// PresenceManager 在线状态管理器接口
type PresenceManager interface {
	// Submit 提交在线状态
	Submit(ctx context.Context, channel, clientID string, data PresenceData) error

	// GetPresences 获取在线状态
	GetPresences(ctx context.Context, channel string) (map[string]PresenceData, error)

	// RemovePresence 移除在线状态
	RemovePresence(ctx context.Context, channel, clientID string) error

	// Close 关闭管理器
	Close() error
}

// Service ShareDB 服务接口
type Service interface {
	// HandleWebSocket 处理 WebSocket 连接
	HandleWebSocket(ctx context.Context, conn *Connection) error

	// SubmitOp 提交操作
	SubmitOp(ctx context.Context, collection, docID string, op *opbuilder.Operation) error

	// PublishOp 发布操作
	PublishOp(ctx context.Context, collection, docID string, op *opbuilder.Operation) error

	// WithTransaction 在事务中执行操作
	WithTransaction(fn func(context.Context) error) error

	// GetStats 获取统计信息
	GetStats() map[string]interface{}

	// Shutdown 关闭服务
	Shutdown() error
}
