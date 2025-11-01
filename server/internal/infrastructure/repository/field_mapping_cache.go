package repository

import (
	"sync"
	"time"
)

// FieldMapping 字段映射（field_id <-> db_field_name）
type FieldMapping struct {
	FieldID     string
	DBFieldName string
	FieldType   string
	CachedAt    time.Time
}

// FieldMappingCache 字段映射缓存
// 用于减少重复查询字段列表，提升性能
type FieldMappingCache struct {
	cache map[string][]*FieldMapping // key: table_id, value: field mappings
	mu    sync.RWMutex
	ttl   time.Duration
}

// NewFieldMappingCache 创建字段映射缓存
func NewFieldMappingCache() *FieldMappingCache {
	return &FieldMappingCache{
		cache: make(map[string][]*FieldMapping),
		ttl:   5 * time.Minute, // 缓存5分钟
	}
}

// Get 获取表的字段映射
func (c *FieldMappingCache) Get(tableID string) ([]*FieldMapping, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mappings, exists := c.cache[tableID]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if len(mappings) > 0 && time.Since(mappings[0].CachedAt) > c.ttl {
		return nil, false
	}

	return mappings, true
}

// Set 设置表的字段映射
func (c *FieldMappingCache) Set(tableID string, mappings []*FieldMapping) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[tableID] = mappings
}

// Invalidate 使缓存失效
func (c *FieldMappingCache) Invalidate(tableID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.cache, tableID)
}

// Clear 清空所有缓存
func (c *FieldMappingCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string][]*FieldMapping)
}
