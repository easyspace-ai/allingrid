package application

import (
	"context"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/cache"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CacheService 统一缓存服务
// 提供多级缓存策略和智能缓存管理
type CacheService struct {
	redisCache   *cache.RedisClient
	localCache   *cache.LRUCache
	errorService *ErrorService
	config       *CacheConfig
}

// CacheConfig 缓存配置
type CacheConfig struct {
	// 多级缓存配置
	EnableLocalCache bool `json:"enable_local_cache"`
	EnableRedisCache bool `json:"enable_redis_cache"`

	// TTL配置
	DefaultTTL    time.Duration `json:"default_ttl"`
	LocalCacheTTL time.Duration `json:"local_cache_ttl"`
	RedisCacheTTL time.Duration `json:"redis_cache_ttl"`

	// 缓存策略
	CacheStrategy        string `json:"cache_strategy"`        // "write_through", "write_behind", "cache_aside"
	InvalidationStrategy string `json:"invalidation_strategy"` // "immediate", "lazy", "scheduled"

	// 性能配置
	MaxLocalCacheSize  int  `json:"max_local_cache_size"`
	CompressionEnabled bool `json:"compression_enabled"`
}

// DefaultCacheConfig 默认缓存配置
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		EnableLocalCache:     true,
		EnableRedisCache:     true,
		DefaultTTL:           10 * time.Minute,
		LocalCacheTTL:        5 * time.Minute,
		RedisCacheTTL:        1 * time.Hour,
		CacheStrategy:        "write_through",
		InvalidationStrategy: "lazy",
		MaxLocalCacheSize:    10000,
		CompressionEnabled:   true,
	}
}

// NewCacheService 创建缓存服务
func NewCacheService(
	redisCache *cache.RedisClient,
	errorService *ErrorService,
	config *CacheConfig,
) *CacheService {
	if config == nil {
		config = DefaultCacheConfig()
	}

	var localCache *cache.LRUCache
	if config.EnableLocalCache {
		localCache = cache.NewLRUCache(config.MaxLocalCacheSize, func(key string, value interface{}) {
			// 缓存淘汰回调
			logger.Debug("cache evicted", logger.String("key", key))
		})
	}

	return &CacheService{
		redisCache:   redisCache,
		localCache:   localCache,
		errorService: errorService,
		config:       config,
	}
}

// Get 获取缓存（多级缓存策略）
func (s *CacheService) Get(ctx context.Context, key string, dest interface{}) error {
	fullKey := s.buildKey(key)

	// 1. 先尝试本地缓存
	if s.config.EnableLocalCache && s.localCache != nil {
		if value, found := s.localCache.Get(fullKey); found {
			// 将值复制到dest
			if destPtr, ok := dest.(*interface{}); ok {
				*destPtr = value
			}
			s.logCacheHit("local", key)
			return nil
		}
	}

	// 2. 尝试Redis缓存
	if s.config.EnableRedisCache && s.redisCache != nil {
		if err := s.redisCache.Get(ctx, fullKey, dest); err == nil {
			// 写入本地缓存
			if s.config.EnableLocalCache && s.localCache != nil {
				s.localCache.Set(fullKey, dest, s.config.LocalCacheTTL)
			}
			s.logCacheHit("redis", key)
			return nil
		}
	}

	s.logCacheMiss(key)
	return cache.ErrCacheNotFound
}

// Set 设置缓存（多级缓存策略）
func (s *CacheService) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	fullKey := s.buildKey(key)

	// 设置TTL
	if ttl == 0 {
		ttl = s.config.DefaultTTL
	}

	// 根据缓存策略设置缓存
	switch s.config.CacheStrategy {
	case "write_through":
		return s.writeThrough(ctx, fullKey, value, ttl)
	case "write_behind":
		return s.writeBehind(ctx, fullKey, value, ttl)
	case "cache_aside":
		return s.cacheAside(ctx, fullKey, value, ttl)
	default:
		return s.writeThrough(ctx, fullKey, value, ttl)
	}
}

// Delete 删除缓存（多级缓存）
func (s *CacheService) Delete(ctx context.Context, keys ...string) error {
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = s.buildKey(key)
	}

	// 删除本地缓存
	if s.config.EnableLocalCache && s.localCache != nil {
		for _, key := range fullKeys {
			s.localCache.Delete(key)
		}
	}

	// 删除Redis缓存
	if s.config.EnableRedisCache && s.redisCache != nil {
		if err := s.redisCache.Delete(ctx, fullKeys...); err != nil {
			return s.errorService.HandleDatabaseError(ctx, "CacheDelete", err)
		}
	}

	s.logCacheDelete(keys...)
	return nil
}

// InvalidatePattern 按模式删除缓存
func (s *CacheService) InvalidatePattern(ctx context.Context, pattern string) error {
	// 删除Redis缓存
	if s.config.EnableRedisCache && s.redisCache != nil {
		if err := s.redisCache.DeletePattern(ctx, pattern); err != nil {
			return s.errorService.HandleDatabaseError(ctx, "CacheInvalidatePattern", err)
		}
	}

	// 本地缓存不支持模式删除，需要遍历
	if s.config.EnableLocalCache && s.localCache != nil {
		// 这里需要实现本地缓存的模式删除逻辑
		// 由于LRUCache可能不支持模式删除，这里暂时跳过
		logger.Warn("local cache pattern deletion not supported",
			logger.String("pattern", pattern))
	}

	s.logCacheInvalidate(pattern)
	return nil
}

// InvalidateTableCache 使表格相关缓存失效
func (s *CacheService) InvalidateTableCache(ctx context.Context, tableID string) error {
	patterns := []string{
		fmt.Sprintf("table:%s:*", tableID),
		fmt.Sprintf("fields:%s:*", tableID),
		fmt.Sprintf("records:%s:*", tableID),
		fmt.Sprintf("views:%s:*", tableID),
	}

	for _, pattern := range patterns {
		if err := s.InvalidatePattern(ctx, pattern); err != nil {
			logger.Warn("failed to invalidate cache pattern",
				logger.String("pattern", pattern),
				logger.ErrorField(err))
		}
	}

	logger.Info("table cache invalidated",
		logger.String("table_id", tableID))

	return nil
}

// InvalidateFieldCache 使字段相关缓存失效
func (s *CacheService) InvalidateFieldCache(ctx context.Context, fieldID string) error {
	patterns := []string{
		fmt.Sprintf("field:%s:*", fieldID),
		fmt.Sprintf("fields:*:%s", fieldID),
	}

	for _, pattern := range patterns {
		if err := s.InvalidatePattern(ctx, pattern); err != nil {
			logger.Warn("failed to invalidate field cache pattern",
				logger.String("pattern", pattern),
				logger.ErrorField(err))
		}
	}

	logger.Info("field cache invalidated",
		logger.String("field_id", fieldID))

	return nil
}

// writeThrough 写透策略
func (s *CacheService) writeThrough(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// 同时写入本地缓存和Redis缓存
	var lastError error

	// 写入本地缓存
	if s.config.EnableLocalCache && s.localCache != nil {
		localTTL := ttl
		if localTTL > s.config.LocalCacheTTL {
			localTTL = s.config.LocalCacheTTL
		}
		s.localCache.Set(key, value, localTTL)
	}

	// 写入Redis缓存
	if s.config.EnableRedisCache && s.redisCache != nil {
		if err := s.redisCache.Set(ctx, key, value, ttl); err != nil {
			lastError = s.errorService.HandleDatabaseError(ctx, "RedisSet", err)
		}
	}

	return lastError
}

// writeBehind 写回策略
func (s *CacheService) writeBehind(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// 先写入本地缓存，异步写入Redis
	if s.config.EnableLocalCache && s.localCache != nil {
		s.localCache.Set(key, value, ttl)
	}

	// 异步写入Redis
	if s.config.EnableRedisCache && s.redisCache != nil {
		go func() {
			asyncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := s.redisCache.Set(asyncCtx, key, value, ttl); err != nil {
				logger.Error("async redis set failed",
					logger.String("key", key),
					logger.ErrorField(err))
			}
		}()
	}

	return nil
}

// cacheAside 缓存旁路策略
func (s *CacheService) cacheAside(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// 只写入Redis缓存，本地缓存由读取时填充
	if s.config.EnableRedisCache && s.redisCache != nil {
		if err := s.redisCache.Set(ctx, key, value, ttl); err != nil {
			return s.errorService.HandleDatabaseError(ctx, "RedisSet", err)
		}
	}

	return nil
}

// buildKey 构建缓存键
func (s *CacheService) buildKey(key string) string {
	return fmt.Sprintf("luckdb:%s", key)
}

// logCacheHit 记录缓存命中
func (s *CacheService) logCacheHit(cacheType, key string) {
	logger.Debug("cache hit",
		logger.String("cache_type", cacheType),
		logger.String("key", key))
}

// logCacheMiss 记录缓存未命中
func (s *CacheService) logCacheMiss(key string) {
	logger.Debug("cache miss",
		logger.String("key", key))
}

// logCacheDelete 记录缓存删除
func (s *CacheService) logCacheDelete(keys ...string) {
	logger.Debug("cache deleted",
		logger.Strings("keys", keys))
}

// logCacheInvalidate 记录缓存失效
func (s *CacheService) logCacheInvalidate(pattern string) {
	logger.Debug("cache invalidated",
		logger.String("pattern", pattern))
}

// GetStats 获取缓存统计信息
func (s *CacheService) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// 本地缓存统计
	if s.localCache != nil {
		stats["local_cache"] = map[string]interface{}{
			"enabled":  true,
			"max_size": s.config.MaxLocalCacheSize,
		}
	} else {
		stats["local_cache"] = map[string]interface{}{
			"enabled": false,
		}
	}

	// Redis缓存统计
	if s.redisCache != nil {
		stats["redis_cache"] = map[string]interface{}{
			"enabled": true,
		}
	} else {
		stats["redis_cache"] = map[string]interface{}{
			"enabled": false,
		}
	}

	// 配置信息
	stats["config"] = map[string]interface{}{
		"strategy":              s.config.CacheStrategy,
		"invalidation_strategy": s.config.InvalidationStrategy,
		"default_ttl":           s.config.DefaultTTL.String(),
		"local_ttl":             s.config.LocalCacheTTL.String(),
		"redis_ttl":             s.config.RedisCacheTTL.String(),
	}

	return stats
}

// Health 健康检查
func (s *CacheService) Health(ctx context.Context) error {
	// 检查Redis连接
	if s.config.EnableRedisCache && s.redisCache != nil {
		if err := s.redisCache.Health(ctx); err != nil {
			return fmt.Errorf("redis cache health check failed: %w", err)
		}
	}

	// 本地缓存不需要健康检查
	return nil
}
