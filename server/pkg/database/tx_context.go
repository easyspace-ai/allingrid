package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// TxContextKey 事务上下文键类型
type TxContextKey string

const (
	// TxKey 事务客户端键
	TxKey TxContextKey = "tx"
	// TxIDKey 事务ID键
	TxIDKey TxContextKey = "tx_id"
	// TxTimeKey 事务开始时间键
	TxTimeKey TxContextKey = "tx_time"
	// TxEventsKey 事务事件队列键
	TxEventsKey TxContextKey = "tx_events"
)

// TxContext 事务上下文
type TxContext struct {
	Tx        *gorm.DB
	ID        string
	StartTime time.Time
	Events    []interface{} // 事务内收集的事件
	Callbacks []func()      // 事务提交后回调
	mu        sync.Mutex
}

// AddEvent 添加事务事件
func (tc *TxContext) AddEvent(event interface{}) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.Events = append(tc.Events, event)
}

// AddCallback 添加事务提交后回调
func (tc *TxContext) AddCallback(callback func()) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.Callbacks = append(tc.Callbacks, callback)
}

// ExecuteCallbacks 执行所有回调
func (tc *TxContext) ExecuteCallbacks() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for _, callback := range tc.Callbacks {
		// 捕获 panic，防止单个回调失败影响其他回调
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("事务回调执行失败",
						logger.String("tx_id", tc.ID),
						logger.Any("panic", r))
				}
			}()
			callback()
		}()
	}
}

// WithTx 使用事务客户端
// 如果上下文中已有事务，则复用；否则返回原始 db
func WithTx(ctx context.Context, db *gorm.DB) *gorm.DB {
	if txCtx := GetTxContext(ctx); txCtx != nil && txCtx.Tx != nil {
		return txCtx.Tx
	}
	return db
}

// GetTxContext 从上下文中获取事务上下文
func GetTxContext(ctx context.Context) *TxContext {
	if ctx == nil {
		return nil
	}

	if txCtx, ok := ctx.Value(TxKey).(*TxContext); ok {
		return txCtx
	}

	return nil
}

// SetTxContext 设置事务上下文
func SetTxContext(ctx context.Context, txCtx *TxContext) context.Context {
	return context.WithValue(ctx, TxKey, txCtx)
}

// InTransaction 检查是否在事务中
func InTransaction(ctx context.Context) bool {
	return GetTxContext(ctx) != nil
}

// TransactionOptions 事务选项
type TransactionOptions struct {
	Timeout    time.Duration // 事务超时时间
	Isolation  string        // 隔离级别（可选）
	ReadOnly   bool          // 是否只读（可选）
	MaxRetries int           // 最大重试次数（用于死锁）
	RetryDelay time.Duration // 重试延迟
}

// DefaultTransactionOptions 默认事务选项
var DefaultTransactionOptions = TransactionOptions{
	Timeout:    30 * time.Second,
	MaxRetries: 3,
	RetryDelay: 100 * time.Millisecond,
}

// BigTransactionOptions 大事务选项（用于批量操作）
var BigTransactionOptions = TransactionOptions{
	Timeout:    60 * time.Second,
	MaxRetries: 3,
	RetryDelay: 100 * time.Millisecond,
}

// Transaction 执行事务
// 支持嵌套事务检测、超时控制、死锁重试、事务提交后回调
func Transaction(ctx context.Context, db *gorm.DB, opts *TransactionOptions, fn func(context.Context) error) error {
	// 使用默认选项
	if opts == nil {
		opts = &DefaultTransactionOptions
	}

	// 检查是否已在事务中（支持嵌套事务）
	if InTransaction(ctx) {
		logger.Debug("检测到嵌套事务，复用现有事务")
		return fn(ctx)
	}

	// 设置超时
	txCtx := ctx
	var cancel context.CancelFunc
	if opts.Timeout > 0 {
		txCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// 创建事务上下文
	txContext := &TxContext{
		ID:        generateTxID(),
		StartTime: time.Now(),
		Events:    make([]interface{}, 0),
		Callbacks: make([]func(), 0),
	}

	logger.Debug("开始事务",
		logger.String("tx_id", txContext.ID),
		logger.Duration("timeout", opts.Timeout))

	// 带重试的事务执行
	var lastErr error
	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 {
			// 指数退避
			delay := time.Duration(attempt) * opts.RetryDelay
			logger.Warn("事务重试",
				logger.String("tx_id", txContext.ID),
				logger.Int("attempt", attempt),
				logger.Duration("delay", delay))
			time.Sleep(delay)
		}

		// 执行事务
		err := db.WithContext(txCtx).Transaction(func(tx *gorm.DB) error {
			// 设置事务客户端到上下文
			txContext.Tx = tx
			txCtxWithTx := SetTxContext(txCtx, txContext)

			// 执行业务逻辑
			if err := fn(txCtxWithTx); err != nil {
				return err
			}

			return nil
		})

		if err == nil {
			// 事务成功，执行回调
			duration := time.Since(txContext.StartTime)
			logger.Debug("事务提交成功",
				logger.String("tx_id", txContext.ID),
				logger.Duration("duration", duration),
				logger.Int("events", len(txContext.Events)),
				logger.Int("callbacks", len(txContext.Callbacks)))

			// 执行所有回调
			txContext.ExecuteCallbacks()

			return nil
		}

		lastErr = err

		// 检查是否为死锁错误
		if !IsDeadlock(err) {
			// 不是死锁，不重试
			logger.Error("事务执行失败",
				logger.String("tx_id", txContext.ID),
				logger.ErrorField(err))
			return err
		}

		// 是死锁，记录并准备重试
		logger.Warn("检测到死锁",
			logger.String("tx_id", txContext.ID),
			logger.Int("attempt", attempt),
			logger.ErrorField(err))

		// 如果已达到最大重试次数，退出
		if attempt >= opts.MaxRetries {
			break
		}
	}

	// 重试次数用尽
	logger.Error("事务执行失败，已达最大重试次数",
		logger.String("tx_id", txContext.ID),
		logger.Int("max_retries", opts.MaxRetries),
		logger.ErrorField(lastErr))

	return fmt.Errorf("事务执行失败（已重试%d次）: %w", opts.MaxRetries, lastErr)
}

// TransactionWithCallback 执行事务并在成功后执行回调
func TransactionWithCallback(ctx context.Context, db *gorm.DB, opts *TransactionOptions, fn func(context.Context) error, afterCommit func()) error {
	// 如果已在事务中，将回调添加到现有事务
	if txCtx := GetTxContext(ctx); txCtx != nil {
		txCtx.AddCallback(afterCommit)
		return fn(ctx)
	}

	// 否则创建新事务
	return Transaction(ctx, db, opts, func(txCtx context.Context) error {
		// 添加回调到事务上下文
		if tc := GetTxContext(txCtx); tc != nil {
			tc.AddCallback(afterCommit)
		}

		return fn(txCtx)
	})
}

// AddTxCallback 添加事务提交后回调
func AddTxCallback(ctx context.Context, callback func()) {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		txCtx.AddCallback(callback)
	} else {
		// 如果不在事务中，立即执行
		logger.Warn("不在事务中，立即执行回调")
		callback()
	}
}

// AddTxEvent 添加事务事件
func AddTxEvent(ctx context.Context, event interface{}) {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		txCtx.AddEvent(event)
	} else {
		logger.Warn("不在事务中，无法收集事件")
	}
}

// GetTxEvents 获取事务收集的所有事件
func GetTxEvents(ctx context.Context) []interface{} {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		return txCtx.Events
	}
	return nil
}

// ClearTxEvents 清空事务事件
func ClearTxEvents(ctx context.Context) {
	if txCtx := GetTxContext(ctx); txCtx != nil {
		txCtx.mu.Lock()
		defer txCtx.mu.Unlock()
		txCtx.Events = make([]interface{}, 0)
	}
}

// generateTxID 生成事务ID
func generateTxID() string {
	return fmt.Sprintf("tx_%d", time.Now().UnixNano())
}

// RepositoryWithTx Repository 事务支持接口
type RepositoryWithTx interface {
	WithTx(ctx context.Context) interface{}
}
