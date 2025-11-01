package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// LRUCache 线程安全的 LRU 缓存实现
type LRUCache struct {
	capacity  int
	evictList *list.List
	items     map[string]*list.Element
	mu        sync.RWMutex
	onEvicted func(key string, value interface{})
	stats     *CacheStats
	logger    *zap.Logger
}

// CacheStats 缓存统计信息
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Sets      uint64
	Deletes   uint64
	Size      int
	mu        sync.RWMutex
}

// cacheEntry 缓存条目
type cacheEntry struct {
	key       string
	value     interface{}
	size      int64
	expiresAt time.Time
}

// NewLRUCache 创建新的 LRU 缓存
func NewLRUCache(capacity int, onEvicted func(key string, value interface{})) *LRUCache {
	return &LRUCache{
		capacity:  capacity,
		evictList: list.New(),
		items:     make(map[string]*list.Element),
		onEvicted: onEvicted,
		stats:     &CacheStats{},
		logger:    zap.L().Named("lru_cache"),
	}
}

// Get 获取缓存值
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry)

		// 检查是否过期
		if time.Now().After(entry.expiresAt) {
			c.mu.RUnlock()
			c.Delete(key)
			c.incrementMisses()
			return nil, false
		}

		c.mu.RUnlock()

		// 移动到队列前面（最近使用）
		c.mu.Lock()
		c.evictList.MoveToFront(elem)
		c.mu.Unlock()

		c.incrementHits()
		return entry.value, true
	}
	c.mu.RUnlock()

	c.incrementMisses()
	return nil, false
}

// Set 设置缓存值
func (c *LRUCache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := time.Now().Add(ttl)

	// 如果键已存在，更新值并移到前面
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.value = value
		entry.expiresAt = expiresAt
		c.incrementSets()
		return
	}

	// 添加新条目
	entry := &cacheEntry{
		key:       key,
		value:     value,
		expiresAt: expiresAt,
	}

	elem := c.evictList.PushFront(entry)
	c.items[key] = elem

	// 如果超过容量，移除最久未使用的条目
	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}

	c.incrementSets()
	c.updateSize()
}

// Delete 删除缓存条目
func (c *LRUCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		c.incrementDeletes()
		c.updateSize()
		return true
	}

	return false
}

// Clear 清空缓存
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*cacheEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	c.evictList.Init()
	c.items = make(map[string]*list.Element)
	c.resetStats()
}

// Len 返回缓存大小
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

// removeOldest 移除最久未使用的条目
func (c *LRUCache) removeOldest() {
	elem := c.evictList.Back()
	if elem != nil {
		c.removeElement(elem)
		c.incrementEvictions()
	}
}

// removeElement 移除指定元素
func (c *LRUCache) removeElement(elem *list.Element) {
	c.evictList.Remove(elem)
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)

	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

// Cleanup 清理过期条目
func (c *LRUCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, elem := range c.items {
		entry := elem.Value.(*cacheEntry)
		if now.After(entry.expiresAt) {
			c.removeElement(elem)
			c.logger.Debug("Removed expired entry", zap.String("key", key))
		}
	}

	c.updateSize()
}

// StartCleanupTimer 启动定期清理
func (c *LRUCache) StartCleanupTimer(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.Cleanup()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// GetStats 获取缓存统计信息
func (c *LRUCache) GetStats() CacheStats {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	return CacheStats{
		Hits:      c.stats.Hits,
		Misses:    c.stats.Misses,
		Evictions: c.stats.Evictions,
		Sets:      c.stats.Sets,
		Deletes:   c.stats.Deletes,
		Size:      c.Len(),
	}
}

// 统计方法
func (c *LRUCache) incrementHits() {
	c.stats.mu.Lock()
	c.stats.Hits++
	c.stats.mu.Unlock()
}

func (c *LRUCache) incrementMisses() {
	c.stats.mu.Lock()
	c.stats.Misses++
	c.stats.mu.Unlock()
}

func (c *LRUCache) incrementEvictions() {
	c.stats.mu.Lock()
	c.stats.Evictions++
	c.stats.mu.Unlock()
}

func (c *LRUCache) incrementSets() {
	c.stats.mu.Lock()
	c.stats.Sets++
	c.stats.mu.Unlock()
}

func (c *LRUCache) incrementDeletes() {
	c.stats.mu.Lock()
	c.stats.Deletes++
	c.stats.mu.Unlock()
}

func (c *LRUCache) updateSize() {
	c.stats.mu.Lock()
	c.stats.Size = len(c.items)
	c.stats.mu.Unlock()
}

func (c *LRUCache) resetStats() {
	c.stats.mu.Lock()
	c.stats.Hits = 0
	c.stats.Misses = 0
	c.stats.Evictions = 0
	c.stats.Sets = 0
	c.stats.Deletes = 0
	c.stats.Size = 0
	c.stats.mu.Unlock()
}

// HitRate 计算命中率
func (s *CacheStats) HitRate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// LRUCacheAdapter Redis 风格的适配器
type LRUCacheAdapter struct {
	cache *LRUCache
}

// NewLRUCacheAdapter 创建 LRU 缓存适配器
func NewLRUCacheAdapter(capacity int) CacheService {
	cache := NewLRUCache(capacity, nil)
	return &LRUCacheAdapter{cache: cache}
}

// Set 实现 CacheService 接口
func (a *LRUCacheAdapter) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	a.cache.Set(key, value, expiration)
	return nil
}

// Get 实现 CacheService 接口
func (a *LRUCacheAdapter) Get(ctx context.Context, key string, dest interface{}) error {
	value, ok := a.cache.Get(key)
	if !ok {
		return ErrCacheNotFound
	}

	// 简单的类型断言，实际使用中可能需要更复杂的反序列化
	switch v := dest.(type) {
	case *string:
		*v = value.(string)
	case *int:
		*v = value.(int)
	case *bool:
		*v = value.(bool)
	default:
		// 对于复杂类型，可能需要使用反射或JSON序列化
		return fmt.Errorf("unsupported destination type")
	}

	return nil
}

// Delete 实现 CacheService 接口
func (a *LRUCacheAdapter) Delete(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		a.cache.Delete(key)
	}
	return nil
}

// DeletePattern 实现 CacheService 接口
func (a *LRUCacheAdapter) DeletePattern(ctx context.Context, pattern string) error {
	// LRU 缓存不支持模式删除，需要遍历所有键
	// 这是一个简化实现
	return fmt.Errorf("pattern deletion not supported in LRU cache")
}

// Exists 实现 CacheService 接口
func (a *LRUCacheAdapter) Exists(ctx context.Context, key string) (bool, error) {
	_, exists := a.cache.Get(key)
	return exists, nil
}

// Expire 实现 CacheService 接口
func (a *LRUCacheAdapter) Expire(ctx context.Context, key string, expiration time.Duration) error {
	// LRU 缓存不支持更新过期时间，需要重新设置
	value, ok := a.cache.Get(key)
	if !ok {
		return ErrCacheNotFound
	}
	a.cache.Set(key, value, expiration)
	return nil
}

// TTL 实现 CacheService 接口
func (a *LRUCacheAdapter) TTL(ctx context.Context, key string) (time.Duration, error) {
	// LRU 缓存不跟踪 TTL
	return 0, fmt.Errorf("TTL not supported in LRU cache")
}

// SetNX 实现 CacheService 接口
func (a *LRUCacheAdapter) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	_, exists := a.cache.Get(key)
	if exists {
		return false, nil
	}
	a.cache.Set(key, value, expiration)
	return true, nil
}

// Health 实现 CacheService 接口
func (a *LRUCacheAdapter) Health(ctx context.Context) error {
	// LRU 缓存始终健康
	return nil
}
