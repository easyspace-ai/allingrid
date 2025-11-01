package config

import (
	"time"
)

// PerformanceConfig 性能配置
type PerformanceConfig struct {
	// 数据库性能配置
	Database DatabasePerformanceConfig `mapstructure:"database"`

	// 缓存性能配置
	Cache CachePerformanceConfig `mapstructure:"cache"`

	// API性能配置
	API APIPerformanceConfig `mapstructure:"api"`

	// 中间件性能配置
	Middleware MiddlewarePerformanceConfig `mapstructure:"middleware"`
}

// DatabasePerformanceConfig 数据库性能配置
type DatabasePerformanceConfig struct {
	// 连接池配置
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// 查询优化配置
	QueryOptimization QueryOptimizationConfig `mapstructure:"query_optimization"`

	// 慢查询配置
	SlowQuery SlowQueryConfig `mapstructure:"slow_query"`
}

// QueryOptimizationConfig 查询优化配置
type QueryOptimizationConfig struct {
	// 是否启用查询缓存
	EnableCache bool `mapstructure:"enable_cache"`

	// 缓存过期时间
	CacheTTL time.Duration `mapstructure:"cache_ttl"`

	// 是否启用查询分析
	EnableAnalysis bool `mapstructure:"enable_analysis"`

	// 批量插入大小
	BatchInsertSize int `mapstructure:"batch_insert_size"`
}

// SlowQueryConfig 慢查询配置
type SlowQueryConfig struct {
	// 慢查询阈值
	Threshold time.Duration `mapstructure:"threshold"`

	// 是否记录慢查询
	EnableLogging bool `mapstructure:"enable_logging"`

	// 慢查询日志级别
	LogLevel string `mapstructure:"log_level"`
}

// CachePerformanceConfig 缓存性能配置
type CachePerformanceConfig struct {
	// Redis配置
	Redis RedisPerformanceConfig `mapstructure:"redis"`

	// 本地缓存配置
	Local LocalCacheConfig `mapstructure:"local"`

	// 缓存策略配置
	Strategy CacheStrategyConfig `mapstructure:"strategy"`
}

// RedisPerformanceConfig Redis性能配置
type RedisPerformanceConfig struct {
	// 连接池大小
	PoolSize int `mapstructure:"pool_size"`

	// 连接超时时间
	DialTimeout time.Duration `mapstructure:"dial_timeout"`

	// 读取超时时间
	ReadTimeout time.Duration `mapstructure:"read_timeout"`

	// 写入超时时间
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// 是否启用压缩
	EnableCompression bool `mapstructure:"enable_compression"`

	// 压缩级别
	CompressionLevel int `mapstructure:"compression_level"`
}

// LocalCacheConfig 本地缓存配置
type LocalCacheConfig struct {
	// 是否启用本地缓存
	Enabled bool `mapstructure:"enabled"`

	// 缓存大小
	Size int `mapstructure:"size"`

	// 默认过期时间
	TTL time.Duration `mapstructure:"ttl"`

	// 清理间隔
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"`
}

// CacheStrategyConfig 缓存策略配置
type CacheStrategyConfig struct {
	// 缓存预热配置
	Warmup CacheWarmupConfig `mapstructure:"warmup"`

	// 缓存失效策略
	Invalidation CacheInvalidationConfig `mapstructure:"invalidation"`
}

// CacheWarmupConfig 缓存预热配置
type CacheWarmupConfig struct {
	// 是否启用预热
	Enabled bool `mapstructure:"enabled"`

	// 预热间隔
	Interval time.Duration `mapstructure:"interval"`

	// 预热项目
	Items []CacheWarmupItem `mapstructure:"items"`
}

// CacheWarmupItem 缓存预热项
type CacheWarmupItem struct {
	Key   string        `mapstructure:"key"`
	TTL   time.Duration `mapstructure:"ttl"`
	Query string        `mapstructure:"query"`
}

// CacheInvalidationConfig 缓存失效配置
type CacheInvalidationConfig struct {
	// 失效策略类型
	Strategy string `mapstructure:"strategy"`

	// 失效延迟
	Delay time.Duration `mapstructure:"delay"`

	// 批量失效大小
	BatchSize int `mapstructure:"batch_size"`
}

// APIPerformanceConfig API性能配置
type APIPerformanceConfig struct {
	// 响应优化配置
	Response ResponseOptimizationConfig `mapstructure:"response"`

	// 限流配置
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`

	// 超时配置
	Timeout APITimeoutConfig `mapstructure:"timeout"`
}

// ResponseOptimizationConfig 响应优化配置
type ResponseOptimizationConfig struct {
	// 是否启用压缩
	EnableCompression bool `mapstructure:"enable_compression"`

	// 压缩级别
	CompressionLevel int `mapstructure:"compression_level"`

	// 最小压缩大小
	MinCompressionSize int `mapstructure:"min_compression_size"`

	// 是否启用ETag
	EnableETag bool `mapstructure:"enable_etag"`

	// 是否启用Last-Modified
	EnableLastModified bool `mapstructure:"enable_last_modified"`
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	// 是否启用限流
	Enabled bool `mapstructure:"enabled"`

	// 时间窗口
	Window time.Duration `mapstructure:"window"`

	// 最大请求数
	MaxRequests int `mapstructure:"max_requests"`

	// 是否启用IP限流
	EnableIPLimit bool `mapstructure:"enable_ip_limit"`

	// 是否启用用户限流
	EnableUserLimit bool `mapstructure:"enable_user_limit"`
}

// APITimeoutConfig API超时配置
type APITimeoutConfig struct {
	// 默认超时时间
	Default time.Duration `mapstructure:"default"`

	// 读取超时时间
	Read time.Duration `mapstructure:"read"`

	// 写入超时时间
	Write time.Duration `mapstructure:"write"`

	// 处理超时时间
	Handler time.Duration `mapstructure:"handler"`
}

// MiddlewarePerformanceConfig 中间件性能配置
type MiddlewarePerformanceConfig struct {
	// 性能监控配置
	Monitoring MonitoringConfig `mapstructure:"monitoring"`

	// 日志配置
	Logging LoggingConfig `mapstructure:"logging"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	// 是否启用性能监控
	Enabled bool `mapstructure:"enabled"`

	// 详细指标
	DetailedMetrics bool `mapstructure:"detailed_metrics"`

	// 慢请求阈值
	SlowRequestThreshold time.Duration `mapstructure:"slow_request_threshold"`

	// 内存监控
	EnableMemoryMonitoring bool `mapstructure:"enable_memory_monitoring"`

	// 指标收集间隔
	MetricsInterval time.Duration `mapstructure:"metrics_interval"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	// 是否记录请求体
	LogRequestBody bool `mapstructure:"log_request_body"`

	// 是否记录响应体
	LogResponseBody bool `mapstructure:"log_response_body"`

	// 最大记录大小
	MaxBodySize int64 `mapstructure:"max_body_size"`

	// 跳过的路径
	SkipPaths []string `mapstructure:"skip_paths"`
}

// DefaultPerformanceConfig 默认性能配置
func DefaultPerformanceConfig() PerformanceConfig {
	return PerformanceConfig{
		Database: DatabasePerformanceConfig{
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxLifetime: time.Hour,
			QueryOptimization: QueryOptimizationConfig{
				EnableCache:     true,
				CacheTTL:        10 * time.Minute,
				EnableAnalysis:  true,
				BatchInsertSize: 1000,
			},
			SlowQuery: SlowQueryConfig{
				Threshold:     100 * time.Millisecond,
				EnableLogging: true,
				LogLevel:      "warn",
			},
		},
		Cache: CachePerformanceConfig{
			Redis: RedisPerformanceConfig{
				PoolSize:          10,
				DialTimeout:       5 * time.Second,
				ReadTimeout:       3 * time.Second,
				WriteTimeout:      3 * time.Second,
				EnableCompression: true,
				CompressionLevel:  6,
			},
			Local: LocalCacheConfig{
				Enabled:         true,
				Size:            10000,
				TTL:             5 * time.Minute,
				CleanupInterval: 5 * time.Minute,
			},
			Strategy: CacheStrategyConfig{
				Warmup: CacheWarmupConfig{
					Enabled:  true,
					Interval: time.Hour,
				},
				Invalidation: CacheInvalidationConfig{
					Strategy:  "lazy",
					Delay:     1 * time.Second,
					BatchSize: 100,
				},
			},
		},
		API: APIPerformanceConfig{
			Response: ResponseOptimizationConfig{
				EnableCompression:  true,
				CompressionLevel:   6,
				MinCompressionSize: 1024,
				EnableETag:         true,
				EnableLastModified: true,
			},
			RateLimit: RateLimitConfig{
				Enabled:         true,
				Window:          time.Minute,
				MaxRequests:     100,
				EnableIPLimit:   true,
				EnableUserLimit: true,
			},
			Timeout: APITimeoutConfig{
				Default: 30 * time.Second,
				Read:    10 * time.Second,
				Write:   10 * time.Second,
				Handler: 20 * time.Second,
			},
		},
		Middleware: MiddlewarePerformanceConfig{
			Monitoring: MonitoringConfig{
				Enabled:                true,
				DetailedMetrics:        true,
				SlowRequestThreshold:   100 * time.Millisecond,
				EnableMemoryMonitoring: true,
				MetricsInterval:        30 * time.Second,
			},
			Logging: LoggingConfig{
				LogRequestBody:  false,
				LogResponseBody: false,
				MaxBodySize:     1024 * 1024, // 1MB
				SkipPaths: []string{
					"/health",
					"/ping",
					"/metrics",
				},
			},
		},
	}
}
