package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PerformanceCache 高性能缓存实现
type PerformanceCache struct {
	redis      CacheService
	localCache *LocalCache
	logger     *zap.Logger
	config     PerformanceCacheConfig
}

// PerformanceCacheConfig 性能缓存配置
type PerformanceCacheConfig struct {
	// 是否启用本地缓存
	EnableLocalCache bool
	// 本地缓存大小
	LocalCacheSize int
	// 本地缓存默认过期时间
	LocalCacheTTL time.Duration
	// Redis默认过期时间
	RedisTTL time.Duration
	// 缓存预热配置
	EnableWarmup bool
	// 缓存键前缀
	KeyPrefix string
}

// DefaultPerformanceCacheConfig 默认配置
func DefaultPerformanceCacheConfig() PerformanceCacheConfig {
	return PerformanceCacheConfig{
		EnableLocalCache: true,
		LocalCacheSize:   10000,
		LocalCacheTTL:    5 * time.Minute,
		RedisTTL:         30 * time.Minute,
		EnableWarmup:     true,
		KeyPrefix:        "perf:",
	}
}

// NewPerformanceCache 创建性能缓存
func NewPerformanceCache(redis CacheService, config PerformanceCacheConfig) *PerformanceCache {
	var localCache *LocalCache
	if config.EnableLocalCache {
		localCache = NewLocalCache(config.LocalCacheSize, config.LocalCacheTTL)
	}

	return &PerformanceCache{
		redis:      redis,
		localCache: localCache,
		logger:     zap.L(),
		config:     config,
	}
}

// Get 获取缓存（多级缓存）
func (pc *PerformanceCache) Get(ctx context.Context, key string, dest interface{}) error {
	fullKey := pc.buildKey(key)

	// 1. 先尝试本地缓存
	if pc.localCache != nil {
		if err := pc.localCache.Get(fullKey, dest); err == nil {
			// Cache hit - no logging for performance (high frequency operation)
			return nil
		}
	}

	// 2. 尝试Redis缓存
	if err := pc.redis.Get(ctx, fullKey, dest); err == nil {
		// 写入本地缓存
		if pc.localCache != nil {
			pc.localCache.Set(fullKey, dest, pc.config.LocalCacheTTL)
		}
		// Cache hit from Redis - no logging for performance (high frequency)
		return nil
	}

	// Cache miss - no logging for performance (high frequency)
	return ErrCacheNotFound
}

// Set 设置缓存（多级缓存）
func (pc *PerformanceCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	fullKey := pc.buildKey(key)

	// 设置Redis缓存
	if ttl == 0 {
		ttl = pc.config.RedisTTL
	}

	if err := pc.redis.Set(ctx, fullKey, value, ttl); err != nil {
		pc.logger.Error("Failed to set Redis cache",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	// 设置本地缓存
	if pc.localCache != nil {
		localTTL := ttl
		if localTTL > pc.config.LocalCacheTTL {
			localTTL = pc.config.LocalCacheTTL
		}
		pc.localCache.Set(fullKey, value, localTTL)
	}

	// Cache set - no logging for performance (high frequency)
	return nil
}

// Delete 删除缓存（多级缓存）
func (pc *PerformanceCache) Delete(ctx context.Context, keys ...string) error {
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = pc.buildKey(key)
	}

	// 删除Redis缓存
	if err := pc.redis.Delete(ctx, fullKeys...); err != nil {
		pc.logger.Error("Failed to delete Redis cache",
			zap.Strings("keys", keys),
			zap.Error(err),
		)
		return err
	}

	// 删除本地缓存
	if pc.localCache != nil {
		for _, key := range keys {
			pc.localCache.Delete(pc.buildKey(key))
		}
	}

	pc.logger.Info("Cache deleted", // Info level - less frequent operation
		zap.Strings("keys", keys),
	)

	return nil
}

// DeletePattern 按模式删除缓存
func (pc *PerformanceCache) DeletePattern(ctx context.Context, pattern string) error {
	fullPattern := pc.buildKey(pattern)

	// 删除Redis缓存
	if err := pc.redis.DeletePattern(ctx, fullPattern); err != nil {
		pc.logger.Error("Failed to delete Redis cache by pattern",
			zap.String("pattern", pattern),
			zap.Error(err),
		)
		return err
	}

	// 注意：本地缓存无法按模式删除，只能清空所有本地缓存
	// 或者实现一个更复杂的模式匹配机制
	if pc.localCache != nil {
		pc.logger.Warn("Local cache pattern deletion not implemented, consider clearing local cache")
	}

	pc.logger.Info("Cache deleted by pattern", // Info level - less frequent operation
		zap.String("pattern", pattern),
	)

	return nil
}

// GetOrSet 获取缓存，如果不存在则设置
func (pc *PerformanceCache) GetOrSet(ctx context.Context, key string, dest interface{}, setter func() (interface{}, error), ttl time.Duration) error {
	// 尝试获取缓存
	if err := pc.Get(ctx, key, dest); err == nil {
		return nil
	}

	// 缓存不存在，执行setter函数
	value, err := setter()
	if err != nil {
		return err
	}

	// 设置缓存
	if err := pc.Set(ctx, key, value, ttl); err != nil {
		pc.logger.Warn("Failed to set cache after get-or-set",
			zap.String("key", key),
			zap.Error(err),
		)
	}

	// 将值复制到dest
	return copyValue(value, dest)
}

// Exists 检查缓存是否存在
func (pc *PerformanceCache) Exists(ctx context.Context, key string) (bool, error) {
	fullKey := pc.buildKey(key)

	// 检查本地缓存
	if pc.localCache != nil && pc.localCache.Exists(fullKey) {
		return true, nil
	}

	// 检查Redis缓存
	return pc.redis.Exists(ctx, fullKey)
}

// SetNX 设置键值，仅当键不存在时
func (pc *PerformanceCache) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	fullKey := pc.buildKey(key)

	// 如果本地缓存存在且键已存在，返回false
	if pc.localCache != nil && pc.localCache.Exists(fullKey) {
		return false, nil
	}

	// 使用Redis的SetNX
	success, err := pc.redis.SetNX(ctx, fullKey, value, expiration)
	if err != nil {
		return false, err
	}

	// 如果设置成功，更新本地缓存
	if success && pc.localCache != nil {
		pc.localCache.Set(fullKey, value, expiration)
	}

	return success, nil
}

// TTL 获取键的生存时间
func (pc *PerformanceCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	fullKey := pc.buildKey(key)
	return pc.redis.TTL(ctx, fullKey)
}

// Flush 清空所有缓存
func (pc *PerformanceCache) Flush(ctx context.Context) error {
	// 清空本地缓存
	if pc.localCache != nil {
		pc.localCache.Clear()
	}

	// 清空Redis缓存（这里需要根据实际情况实现）
	pc.logger.Warn("Flush operation not fully implemented for Redis")

	return nil
}

// buildKey 构建缓存键
func (pc *PerformanceCache) buildKey(key string) string {
	return fmt.Sprintf("%s%s", pc.config.KeyPrefix, key)
}

// Warmup 缓存预热
func (pc *PerformanceCache) Warmup(ctx context.Context, items []WarmupItem) error {
	if !pc.config.EnableWarmup {
		return nil
	}

	pc.logger.Info("Starting cache warmup",
		zap.Int("items", len(items)),
	)

	for _, item := range items {
		if err := pc.Set(ctx, item.Key, item.Value, item.TTL); err != nil {
			pc.logger.Error("Failed to warmup cache item",
				zap.String("key", item.Key),
				zap.Error(err),
			)
			continue
		}
	}

	pc.logger.Info("Cache warmup completed")
	return nil
}

// WarmupItem 预热项
type WarmupItem struct {
	Key   string
	Value interface{}
	TTL   time.Duration
}

// LocalCache 本地内存缓存
type LocalCache struct {
	items      sync.Map
	maxSize    int
	defaultTTL time.Duration
	mu         sync.RWMutex
	size       int
}

// CacheItem 缓存项
type CacheItem struct {
	Value     interface{}
	ExpiresAt time.Time
}

// NewLocalCache 创建本地缓存
func NewLocalCache(maxSize int, defaultTTL time.Duration) *LocalCache {
	cache := &LocalCache{
		maxSize:    maxSize,
		defaultTTL: defaultTTL,
	}

	// 启动清理goroutine
	go cache.cleanup()

	return cache
}

// Get 获取本地缓存
func (lc *LocalCache) Get(key string, dest interface{}) error {
	if value, ok := lc.items.Load(key); ok {
		item := value.(*CacheItem)

		// 检查是否过期
		if time.Now().After(item.ExpiresAt) {
			lc.items.Delete(key)
			lc.mu.Lock()
			lc.size--
			lc.mu.Unlock()
			return ErrCacheNotFound
		}

		return copyValue(item.Value, dest)
	}

	return ErrCacheNotFound
}

// Set 设置本地缓存
func (lc *LocalCache) Set(key string, value interface{}, ttl time.Duration) {
	if ttl == 0 {
		ttl = lc.defaultTTL
	}

	item := &CacheItem{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}

	lc.items.Store(key, item)

	lc.mu.Lock()
	lc.size++

	// 如果超过最大大小，随机删除一些项目
	if lc.size > lc.maxSize {
		lc.evictRandom()
	}
	lc.mu.Unlock()
}

// Delete 删除本地缓存
func (lc *LocalCache) Delete(key string) {
	if _, ok := lc.items.Load(key); ok {
		lc.items.Delete(key)
		lc.mu.Lock()
		lc.size--
		lc.mu.Unlock()
	}
}

// Exists 检查本地缓存是否存在
func (lc *LocalCache) Exists(key string) bool {
	if value, ok := lc.items.Load(key); ok {
		item := value.(*CacheItem)
		if time.Now().After(item.ExpiresAt) {
			lc.items.Delete(key)
			lc.mu.Lock()
			lc.size--
			lc.mu.Unlock()
			return false
		}
		return true
	}
	return false
}

// Clear 清空本地缓存
func (lc *LocalCache) Clear() {
	lc.items = sync.Map{}
	lc.mu.Lock()
	lc.size = 0
	lc.mu.Unlock()
}

// evictRandom 随机淘汰缓存项
func (lc *LocalCache) evictRandom() {
	count := 0
	lc.items.Range(func(key, value interface{}) bool {
		if count >= lc.maxSize/10 { // 淘汰10%的项目
			return false
		}
		lc.items.Delete(key)
		count++
		return true
	})
	lc.size -= count
}

// cleanup 清理过期项
func (lc *LocalCache) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		expiredKeys := make([]interface{}, 0)

		lc.items.Range(func(key, value interface{}) bool {
			item := value.(*CacheItem)
			if now.After(item.ExpiresAt) {
				expiredKeys = append(expiredKeys, key)
			}
			return true
		})

		for _, key := range expiredKeys {
			lc.items.Delete(key)
		}

		lc.mu.Lock()
		lc.size -= len(expiredKeys)
		lc.mu.Unlock()

		if len(expiredKeys) > 0 {
			zap.L().Debug("Cleaned up expired cache items",
				zap.Int("count", len(expiredKeys)),
			)
		}
	}
}

// copyValue 复制值
func copyValue(src, dest interface{}) error {
	// 使用JSON序列化/反序列化来复制值
	data, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("failed to marshal source value: %w", err)
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("failed to unmarshal to destination: %w", err)
	}

	return nil
}

// 添加Expire方法以实现CacheService接口
func (pc *PerformanceCache) Expire(ctx context.Context, key string, expiration time.Duration) error {
	fullKey := pc.buildKey(key)
	return pc.redis.Expire(ctx, fullKey, expiration)
}

// 添加Health方法以实现CacheService接口
func (pc *PerformanceCache) Health(ctx context.Context) error {
	return pc.redis.Health(ctx)
}

// 确保PerformanceCache实现CacheService接口
var _ CacheService = (*PerformanceCache)(nil)
