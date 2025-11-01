package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// RedisClient Redis客户端结构
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient 创建新的Redis客户端
func NewRedisClient(cfg config.RedisConfig) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        cfg.GetRedisAddr(),
		Password:    cfg.Password,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize,
		DialTimeout: cfg.DialTimeout,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info("Redis connected successfully",
		logger.String("addr", cfg.GetRedisAddr()),
		logger.Int("db", cfg.DB),
	)

	return &RedisClient{client: rdb}, nil
}

// Set 设置缓存
func (r *RedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	return r.client.Set(ctx, key, data, expiration).Err()
}

// Get 获取缓存
func (r *RedisClient) Get(ctx context.Context, key string, dest interface{}) error {
	data, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheNotFound
		}
		return fmt.Errorf("failed to get cache: %w", err)
	}

	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return nil
}

// Delete 删除缓存
func (r *RedisClient) Delete(ctx context.Context, keys ...string) error {
	return r.client.Del(ctx, keys...).Err()
}

// DeletePattern 按模式删除缓存
func (r *RedisClient) DeletePattern(ctx context.Context, pattern string) error {
	keys, err := r.client.Keys(ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get keys by pattern: %w", err)
	}

	if len(keys) > 0 {
		return r.client.Del(ctx, keys...).Err()
	}

	return nil
}

// Exists 检查键是否存在
func (r *RedisClient) Exists(ctx context.Context, key string) (bool, error) {
	count, err := r.client.Exists(ctx, key).Result()
	return count > 0, err
}

// Expire 设置过期时间
func (r *RedisClient) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.client.Expire(ctx, key, expiration).Err()
}

// TTL 获取剩余过期时间
func (r *RedisClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	return r.client.TTL(ctx, key).Result()
}

// Increment 递增
func (r *RedisClient) Increment(ctx context.Context, key string, value int64) (int64, error) {
	return r.client.IncrBy(ctx, key, value).Result()
}

// Decrement 递减
func (r *RedisClient) Decrement(ctx context.Context, key string, value int64) (int64, error) {
	return r.client.DecrBy(ctx, key, value).Result()
}

// SetNX 仅当键不存在时设置
func (r *RedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("failed to marshal value: %w", err)
	}

	return r.client.SetNX(ctx, key, data, expiration).Result()
}

// HSet 设置哈希字段
func (r *RedisClient) HSet(ctx context.Context, key string, field string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	return r.client.HSet(ctx, key, field, data).Err()
}

// HGet 获取哈希字段
func (r *RedisClient) HGet(ctx context.Context, key string, field string, dest interface{}) error {
	data, err := r.client.HGet(ctx, key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheNotFound
		}
		return fmt.Errorf("failed to get hash field: %w", err)
	}

	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return nil
}

// HDel 删除哈希字段
func (r *RedisClient) HDel(ctx context.Context, key string, fields ...string) error {
	return r.client.HDel(ctx, key, fields...).Err()
}

// HExists 检查哈希字段是否存在
func (r *RedisClient) HExists(ctx context.Context, key string, field string) (bool, error) {
	return r.client.HExists(ctx, key, field).Result()
}

// HGetAll 获取所有哈希字段
func (r *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.client.HGetAll(ctx, key).Result()
}

// LPush 向列表左侧推入元素
func (r *RedisClient) LPush(ctx context.Context, key string, values ...interface{}) error {
	serializedValues := make([]interface{}, len(values))
	for i, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
		serializedValues[i] = data
	}

	return r.client.LPush(ctx, key, serializedValues...).Err()
}

// RPop 从列表右侧弹出元素
func (r *RedisClient) RPop(ctx context.Context, key string, dest interface{}) error {
	data, err := r.client.RPop(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheNotFound
		}
		return fmt.Errorf("failed to pop from list: %w", err)
	}

	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return nil
}

// LLen 获取列表长度
func (r *RedisClient) LLen(ctx context.Context, key string) (int64, error) {
	return r.client.LLen(ctx, key).Result()
}

// SAdd 向集合添加成员
func (r *RedisClient) SAdd(ctx context.Context, key string, members ...interface{}) error {
	serializedMembers := make([]interface{}, len(members))
	for i, member := range members {
		data, err := json.Marshal(member)
		if err != nil {
			return fmt.Errorf("failed to marshal member: %w", err)
		}
		serializedMembers[i] = data
	}

	return r.client.SAdd(ctx, key, serializedMembers...).Err()
}

// SRem 从集合移除成员
func (r *RedisClient) SRem(ctx context.Context, key string, members ...interface{}) error {
	serializedMembers := make([]interface{}, len(members))
	for i, member := range members {
		data, err := json.Marshal(member)
		if err != nil {
			return fmt.Errorf("failed to marshal member: %w", err)
		}
		serializedMembers[i] = data
	}

	return r.client.SRem(ctx, key, serializedMembers...).Err()
}

// SIsMember 检查是否为集合成员
func (r *RedisClient) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	data, err := json.Marshal(member)
	if err != nil {
		return false, fmt.Errorf("failed to marshal member: %w", err)
	}

	return r.client.SIsMember(ctx, key, data).Result()
}

// SMembers 获取集合所有成员
func (r *RedisClient) SMembers(ctx context.Context, key string) ([]string, error) {
	return r.client.SMembers(ctx, key).Result()
}

// Close 关闭连接
func (r *RedisClient) Close() error {
	return r.client.Close()
}

// Health 检查Redis健康状态
func (r *RedisClient) Health(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// GetClient 获取原始Redis客户端
func (r *RedisClient) GetClient() *redis.Client {
	return r.client
}

// 缓存服务接口
type CacheService interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	Get(ctx context.Context, key string, dest interface{}) error
	Delete(ctx context.Context, keys ...string) error
	DeletePattern(ctx context.Context, pattern string) error
	Exists(ctx context.Context, key string) (bool, error)
	Expire(ctx context.Context, key string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	Health(ctx context.Context) error
}

// 确保RedisClient实现CacheService接口
var _ CacheService = (*RedisClient)(nil)

// 缓存错误
var (
	ErrCacheNotFound = fmt.Errorf("cache not found")
)

// 常用缓存键前缀
const (
	UserCachePrefix       = "user:"
	SessionCachePrefix    = "session:"
	TokenCachePrefix      = "token:"
	SpaceCachePrefix      = "space:"
	BaseCachePrefix       = "base:"
	TableCachePrefix      = "table:"
	PermissionCachePrefix = "permission:"
)

// 默认缓存过期时间
const (
	DefaultTTL = 24 * time.Hour
)

// BuildCacheKey 构建缓存键
func BuildCacheKey(prefix, id string) string {
	return fmt.Sprintf("%s%s", prefix, id)
}
