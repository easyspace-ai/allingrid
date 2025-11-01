package dependency

import (
	"context"
	"fmt"
	"time"
)

// CacheRepository 缓存仓储接口
type CacheRepository interface {
	// Get 从缓存获取数据
	Get(ctx context.Context, key string) (string, error)
	// Set 设置缓存数据
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	// Delete 删除缓存数据
	Delete(ctx context.Context, key string) error
	// Exists 检查缓存是否存在
	Exists(ctx context.Context, key string) (bool, error)
}

// DependencyGraphRepository 依赖图仓储
// 负责依赖图的缓存管理
type DependencyGraphRepository struct {
	cache   CacheRepository
	builder *DependencyGraphBuilder
	ttl     time.Duration
}

// NewDependencyGraphRepository 创建依赖图仓储
func NewDependencyGraphRepository(
	cache CacheRepository,
	builder *DependencyGraphBuilder,
	ttl time.Duration,
) *DependencyGraphRepository {
	return &DependencyGraphRepository{
		cache:   cache,
		builder: builder,
		ttl:     ttl,
	}
}

// GetDependencyGraph 获取表的依赖图（带缓存）
// 参考 teable-develop 的缓存策略
func (r *DependencyGraphRepository) GetDependencyGraph(ctx context.Context, tableID string) ([]GraphItem, error) {
	// 尝试从缓存读取
	cacheKey := r.getCacheKey(tableID)
	cachedData, err := r.cache.Get(ctx, cacheKey)

	if err == nil && cachedData != "" {
		// 缓存命中，反序列化
		graph, err := r.builder.DeserializeGraph(cachedData)
		if err == nil {
			return graph, nil
		}
		// 反序列化失败，继续构建
	}

	// 缓存未命中或失效，重新构建
	graph, err := r.builder.BuildDependencyGraph(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// 序列化并缓存
	serialized, err := r.builder.SerializeGraph(graph)
	if err == nil {
		// 忽略缓存设置错误，不影响主流程
		_ = r.cache.Set(ctx, cacheKey, serialized, r.ttl)
	}

	return graph, nil
}

// InvalidateCache 清除指定表的依赖图缓存
// 在字段结构变更时调用
func (r *DependencyGraphRepository) InvalidateCache(ctx context.Context, tableID string) error {
	cacheKey := r.getCacheKey(tableID)
	return r.cache.Delete(ctx, cacheKey)
}

// InvalidateCacheBatch 批量清除多个表的依赖图缓存
func (r *DependencyGraphRepository) InvalidateCacheBatch(ctx context.Context, tableIDs []string) error {
	for _, tableID := range tableIDs {
		if err := r.InvalidateCache(ctx, tableID); err != nil {
			// 记录错误但继续处理其他表
			// TODO: 添加日志记录
			continue
		}
	}
	return nil
}

// RefreshCache 刷新指定表的依赖图缓存
// 强制重新构建并更新缓存
func (r *DependencyGraphRepository) RefreshCache(ctx context.Context, tableID string) error {
	// 重新构建依赖图
	graph, err := r.builder.BuildDependencyGraph(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to rebuild dependency graph: %w", err)
	}

	// 序列化
	serialized, err := r.builder.SerializeGraph(graph)
	if err != nil {
		return fmt.Errorf("failed to serialize graph: %w", err)
	}

	// 更新缓存
	cacheKey := r.getCacheKey(tableID)
	if err := r.cache.Set(ctx, cacheKey, serialized, r.ttl); err != nil {
		return fmt.Errorf("failed to update cache: %w", err)
	}

	return nil
}

// GetTopologicalOrder 获取表的拓扑排序（带缓存）
func (r *DependencyGraphRepository) GetTopologicalOrder(ctx context.Context, tableID string) ([]TopoItem, error) {
	// 获取依赖图
	graph, err := r.GetDependencyGraph(ctx, tableID)
	if err != nil {
		return nil, err
	}

	// 执行拓扑排序
	topoOrder, err := GetTopoOrders(graph)
	if err != nil {
		return nil, fmt.Errorf("failed to get topological order: %w", err)
	}

	return topoOrder, nil
}

// GetAffectedFields 获取受影响的字段（带缓存）
func (r *DependencyGraphRepository) GetAffectedFields(ctx context.Context, tableID string, changedFieldIDs []string) ([]string, error) {
	return r.builder.GetAffectedFields(ctx, tableID, changedFieldIDs)
}

// getCacheKey 生成缓存键
func (r *DependencyGraphRepository) getCacheKey(tableID string) string {
	return fmt.Sprintf("dependency:graph:%s", tableID)
}

// PreWarmCache 预热缓存
// 用于系统启动或定期维护时预先构建常用表的依赖图
func (r *DependencyGraphRepository) PreWarmCache(ctx context.Context, tableIDs []string) error {
	for _, tableID := range tableIDs {
		if err := r.RefreshCache(ctx, tableID); err != nil {
			// 记录错误但继续处理其他表
			// TODO: 添加日志记录
			continue
		}
	}
	return nil
}

// GetCacheStats 获取缓存统计信息
func (r *DependencyGraphRepository) GetCacheStats(ctx context.Context, tableIDs []string) (map[string]bool, error) {
	stats := make(map[string]bool)

	for _, tableID := range tableIDs {
		cacheKey := r.getCacheKey(tableID)
		exists, err := r.cache.Exists(ctx, cacheKey)
		if err != nil {
			stats[tableID] = false
			continue
		}
		stats[tableID] = exists
	}

	return stats, nil
}
