package sharedb

import (
	"context"
	"sync"

	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
)

// TransactionContext 事务上下文
type TransactionContext struct {
	RawOpMaps []map[string]opbuilder.Operation `json:"rawOpMaps"`
	CacheKeys []string                         `json:"cacheKeys"`
	mu        sync.RWMutex
}

// NewTransactionContext 创建新的事务上下文
func NewTransactionContext() *TransactionContext {
	return &TransactionContext{
		RawOpMaps: make([]map[string]opbuilder.Operation, 0),
		CacheKeys: make([]string, 0),
	}
}

// AddRawOpMap 添加原始操作映射
func (tc *TransactionContext) AddRawOpMap(opMap map[string]opbuilder.Operation) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.RawOpMaps = append(tc.RawOpMaps, opMap)
}

// AddCacheKey 添加缓存键
func (tc *TransactionContext) AddCacheKey(key string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.CacheKeys = append(tc.CacheKeys, key)
}

// AddCacheKeys 批量添加缓存键
func (tc *TransactionContext) AddCacheKeys(keys []string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.CacheKeys = append(tc.CacheKeys, keys...)
}

// GetRawOpMaps 获取原始操作映射
func (tc *TransactionContext) GetRawOpMaps() []map[string]opbuilder.Operation {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.RawOpMaps
}

// GetCacheKeys 获取缓存键
func (tc *TransactionContext) GetCacheKeys() []string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.CacheKeys
}

// Clear 清空事务上下文
func (tc *TransactionContext) Clear() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.RawOpMaps = make([]map[string]opbuilder.Operation, 0)
	tc.CacheKeys = make([]string, 0)
}

// IsEmpty 检查事务上下文是否为空
func (tc *TransactionContext) IsEmpty() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return len(tc.RawOpMaps) == 0 && len(tc.CacheKeys) == 0
}

// ContextKey 上下文键类型
type ContextKey string

const (
	// TransactionContextKey 事务上下文键
	TransactionContextKey ContextKey = "transaction_context"
)

// WithTransactionContext 将事务上下文添加到上下文中
func WithTransactionContext(ctx context.Context, txCtx *TransactionContext) context.Context {
	return context.WithValue(ctx, TransactionContextKey, txCtx)
}

// GetTransactionContext 从上下文中获取事务上下文
func GetTransactionContext(ctx context.Context) *TransactionContext {
	if txCtx, ok := ctx.Value(TransactionContextKey).(*TransactionContext); ok {
		return txCtx
	}
	return nil
}

// GetOrCreateTransactionContext 获取或创建事务上下文
func GetOrCreateTransactionContext(ctx context.Context) *TransactionContext {
	txCtx := GetTransactionContext(ctx)
	if txCtx == nil {
		txCtx = NewTransactionContext()
		ctx = WithTransactionContext(ctx, txCtx)
	}
	return txCtx
}
