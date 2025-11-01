package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
)

// DependencyCacheService 依赖图缓存服务
// 使用Redis缓存依赖图，避免重复构建
type DependencyCacheService struct {
	redis   *redis.Client
	ttl     time.Duration
	builder *dependency.DependencyGraphBuilder
	logger  *zap.Logger
}

// NewDependencyCacheService 创建依赖图缓存服务
func NewDependencyCacheService(
	redis *redis.Client,
	builder *dependency.DependencyGraphBuilder,
	logger *zap.Logger,
) *DependencyCacheService {
	return &DependencyCacheService{
		redis:   redis,
		ttl:     time.Hour, // 默认1小时TTL
		builder: builder,
		logger:  logger,
	}
}

// GetDependencyGraph 获取依赖图（优先从缓存）
func (s *DependencyCacheService) GetDependencyGraph(ctx context.Context, tableID string) ([]dependency.GraphItem, error) {
	key := s.getCacheKey(tableID)

	// 1. 尝试从缓存读取
	data, err := s.redis.Get(ctx, key).Result()
	if err == nil {
		// 缓存命中
		var graph []dependency.GraphItem
		if err := json.Unmarshal([]byte(data), &graph); err == nil {
			s.logger.Debug("Dependency graph cache hit",
				zap.String("table_id", tableID),
			)
			return graph, nil
		}
		// 反序列化失败，继续构建
		s.logger.Warn("Failed to unmarshal cached graph, rebuilding",
			zap.String("table_id", tableID),
			zap.Error(err),
		)
	} else if err != redis.Nil {
		// Redis错误（非缓存未命中），记录日志但继续
		s.logger.Warn("Redis get failed, building graph",
			zap.String("table_id", tableID),
			zap.Error(err),
		)
	}

	// 2. 缓存未命中或失败，构建依赖图
	s.logger.Debug("Dependency graph cache miss, building",
		zap.String("table_id", tableID),
	)

	graph, err := s.builder.BuildDependencyGraph(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// 3. 写入缓存（异步，不影响返回）
	go s.cacheGraph(context.Background(), tableID, graph)

	return graph, nil
}

// InvalidateCache 清除缓存
// 在字段变更时调用
func (s *DependencyCacheService) InvalidateCache(ctx context.Context, tableID string) error {
	key := s.getCacheKey(tableID)

	err := s.redis.Del(ctx, key).Err()
	if err != nil {
		s.logger.Error("Failed to invalidate dependency graph cache",
			zap.String("table_id", tableID),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info("Dependency graph cache invalidated",
		zap.String("table_id", tableID),
	)
	return nil
}

// cacheGraph 缓存依赖图
func (s *DependencyCacheService) cacheGraph(ctx context.Context, tableID string, graph []dependency.GraphItem) {
	key := s.getCacheKey(tableID)

	data, err := json.Marshal(graph)
	if err != nil {
		s.logger.Error("Failed to marshal dependency graph",
			zap.String("table_id", tableID),
			zap.Error(err),
		)
		return
	}

	err = s.redis.Set(ctx, key, data, s.ttl).Err()
	if err != nil {
		s.logger.Error("Failed to cache dependency graph",
			zap.String("table_id", tableID),
			zap.Error(err),
		)
		return
	}

	s.logger.Debug("Dependency graph cached",
		zap.String("table_id", tableID),
		zap.Duration("ttl", s.ttl),
	)
}

// getCacheKey 生成缓存键
func (s *DependencyCacheService) getCacheKey(tableID string) string {
	return fmt.Sprintf("dependency:graph:%s", tableID)
}

// SetTTL 设置TTL
func (s *DependencyCacheService) SetTTL(ttl time.Duration) {
	s.ttl = ttl
}
