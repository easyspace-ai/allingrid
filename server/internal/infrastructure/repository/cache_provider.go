package repository

import (
	"context"
	"time"
)

// CacheProvider 缓存提供者接口（打破循环依赖）
// 定义缓存操作的最小接口，避免依赖application包
type CacheProvider interface {
	Get(ctx context.Context, key string, dest interface{}) error
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	InvalidatePattern(ctx context.Context, pattern string) error
}

