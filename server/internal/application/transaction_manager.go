package application

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/events"
	"github.com/easyspace-ai/luckdb/server/pkg/database"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"gorm.io/gorm"
)

// TransactionManager 统一事务管理器
// 集成事件发布、事务管理、错误处理等功能
type TransactionManager struct {
	db       *gorm.DB
	eventBus *EventBus
	mu       sync.RWMutex
	config   *TransactionManagerConfig
}

// TransactionManagerConfig 事务管理器配置
type TransactionManagerConfig struct {
	// 默认事务选项
	DefaultOptions *database.TransactionOptions

	// 事件发布配置
	PublishEventsOnCommit   bool
	PublishEventsOnRollback bool

	// 超时配置
	DefaultTimeout time.Duration

	// 重试配置
	MaxRetries int
	RetryDelay time.Duration
}

// DefaultTransactionManagerConfig 默认配置
func DefaultTransactionManagerConfig() *TransactionManagerConfig {
	return &TransactionManagerConfig{
		DefaultOptions: &database.TransactionOptions{
			Timeout:    30 * time.Second,
			MaxRetries: 3,
			RetryDelay: 100 * time.Millisecond,
		},
		PublishEventsOnCommit:   true,
		PublishEventsOnRollback: false,
		DefaultTimeout:          30 * time.Second,
		MaxRetries:              3,
		RetryDelay:              100 * time.Millisecond,
	}
}

// NewTransactionManager 创建统一事务管理器
func NewTransactionManager(db *gorm.DB, eventBus *EventBus, config *TransactionManagerConfig) *TransactionManager {
	if config == nil {
		config = DefaultTransactionManagerConfig()
	}

	return &TransactionManager{
		db:       db,
		eventBus: eventBus,
		config:   config,
	}
}

// ExecuteInTransaction 在事务中执行操作
// 支持事件发布、错误处理、重试等功能
func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, fn func(context.Context) error) error {
	return tm.ExecuteInTransactionWithOptions(ctx, nil, fn)
}

// ExecuteInTransactionWithOptions 在事务中执行操作（带选项）
func (tm *TransactionManager) ExecuteInTransactionWithOptions(
	ctx context.Context,
	opts *database.TransactionOptions,
	fn func(context.Context) error,
) error {
	// 使用默认选项
	if opts == nil {
		opts = tm.config.DefaultOptions
	}

	// 检查是否已在事务中
	if database.InTransaction(ctx) {
		logger.Debug("检测到嵌套事务，复用现有事务")
		return fn(ctx)
	}

	// 创建事务上下文，用于收集事件
	txCtx := context.WithValue(ctx, "tx_events", make([]events.DomainEvent, 0))

	// 执行事务
	err := database.Transaction(txCtx, tm.db, opts, func(txCtx context.Context) error {
		// 执行业务逻辑
		if err := fn(txCtx); err != nil {
			return err
		}

		// 收集事务中的事件
		events := tm.collectEventsFromTx(txCtx)
		if len(events) > 0 {
			// 将事件添加到事务上下文中，在提交后发布
			database.AddTxCallback(txCtx, func() {
				tm.publishEventsAfterCommit(ctx, events)
			})
		}

		return nil
	})

	// 处理事务结果
	if err != nil {
		logger.Error("事务执行失败",
			logger.ErrorField(err))
		return err
	}

	logger.Debug("事务执行成功")
	return nil
}

// ExecuteInReadOnlyTransaction 在只读事务中执行操作
func (tm *TransactionManager) ExecuteInReadOnlyTransaction(ctx context.Context, fn func(context.Context) error) error {
	opts := &database.TransactionOptions{
		Timeout:    tm.config.DefaultTimeout,
		ReadOnly:   true,
		MaxRetries: 1, // 只读事务不重试
	}

	return tm.ExecuteInTransactionWithOptions(ctx, opts, fn)
}

// AddEventToTransaction 添加事件到当前事务
func (tm *TransactionManager) AddEventToTransaction(ctx context.Context, event events.DomainEvent) error {
	if !database.InTransaction(ctx) {
		return fmt.Errorf("不在事务中，无法添加事件")
	}

	// 获取事务事件列表
	eventsList, ok := ctx.Value("tx_events").([]events.DomainEvent)
	if !ok {
		eventsList = make([]events.DomainEvent, 0)
	}

	// 添加事件
	eventsList = append(eventsList, event)

	// 更新上下文
	ctx = context.WithValue(ctx, "tx_events", eventsList)

	logger.Debug("事件已添加到事务",
		logger.String("event_type", event.EventType()),
		logger.String("event_id", event.EventID()))

	return nil
}

// collectEventsFromTx 从事务上下文中收集事件
func (tm *TransactionManager) collectEventsFromTx(ctx context.Context) []events.DomainEvent {
	eventsList, ok := ctx.Value("tx_events").([]events.DomainEvent)
	if !ok {
		return make([]events.DomainEvent, 0)
	}
	return eventsList
}

// publishEventsAfterCommit 在事务提交后发布事件
func (tm *TransactionManager) publishEventsAfterCommit(ctx context.Context, events []events.DomainEvent) {
	if len(events) == 0 {
		return
	}

	logger.Info("开始发布事务事件",
		logger.Int("event_count", len(events)))

	// 批量发布事件
	if err := tm.eventBus.PublishBatch(ctx, events); err != nil {
		logger.Error("发布事务事件失败",
			logger.ErrorField(err))
		// 这里可以考虑将失败的事件存储到死信队列或重试队列
	} else {
		logger.Info("事务事件发布成功",
			logger.Int("event_count", len(events)))
	}
}

// GetCurrentTransaction 获取当前事务信息
func (tm *TransactionManager) GetCurrentTransaction(ctx context.Context) *TransactionInfo {
	if !database.InTransaction(ctx) {
		return nil
	}

	txCtx := database.GetTxContext(ctx)
	if txCtx == nil {
		return nil
	}

	return &TransactionInfo{
		ID:         txCtx.ID,
		StartTime:  txCtx.StartTime,
		Duration:   time.Since(txCtx.StartTime),
		EventCount: len(txCtx.Events),
	}
}

// TransactionInfo 事务信息
type TransactionInfo struct {
	ID         string
	StartTime  time.Time
	Duration   time.Duration
	EventCount int
}

// IsInTransaction 检查是否在事务中
func (tm *TransactionManager) IsInTransaction(ctx context.Context) bool {
	return database.InTransaction(ctx)
}

// GetTransactionStats 获取事务统计信息
func (tm *TransactionManager) GetTransactionStats() map[string]interface{} {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return map[string]interface{}{
		"config": map[string]interface{}{
			"default_timeout":     tm.config.DefaultTimeout.String(),
			"max_retries":         tm.config.MaxRetries,
			"retry_delay":         tm.config.RetryDelay.String(),
			"publish_on_commit":   tm.config.PublishEventsOnCommit,
			"publish_on_rollback": tm.config.PublishEventsOnRollback,
		},
	}
}
