package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config 应用配置结构
type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Database  DatabaseConfig  `mapstructure:"database"`
	Redis     RedisConfig     `mapstructure:"redis"`
	JWT       JWTConfig       `mapstructure:"jwt"`
	Storage   StorageConfig   `mapstructure:"storage"`
	Logger    LoggerConfig    `mapstructure:"logger"`
	SQLLogger SQLLoggerConfig `mapstructure:"sql_logger"`
	Queue     QueueConfig     `mapstructure:"queue"`
	JSVM      JSVMConfig      `mapstructure:"jsvm"`
	AI        AIConfig        `mapstructure:"ai"`
	MCP       MCPConfig       `mapstructure:"mcp"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Host                string        `mapstructure:"host"`
	Port                int           `mapstructure:"port"`
	Mode                string        `mapstructure:"mode"` // debug, release, test
	ReadTimeout         time.Duration `mapstructure:"read_timeout"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout"`
	IdleTimeout         time.Duration `mapstructure:"idle_timeout"`
	MaxHeaderBytes      int           `mapstructure:"max_header_bytes"`
	ShutdownTimeout     time.Duration `mapstructure:"shutdown_timeout"`
	EnableCORS          bool          `mapstructure:"enable_cors"`
	EnableSwagger       bool          `mapstructure:"enable_swagger"`
	PermissionsDisabled bool          `mapstructure:"permissions_disabled"` // 禁用权限检查（仅用于开发）
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	Name            string        `mapstructure:"name"`
	SSLMode         string        `mapstructure:"ssl_mode"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	LogLevel        string        `mapstructure:"log_level"`
}

// RedisConfig Redis配置
type RedisConfig struct {
	Host        string        `mapstructure:"host"`
	Port        int           `mapstructure:"port"`
	Password    string        `mapstructure:"password"`
	DB          int           `mapstructure:"db"`
	PoolSize    int           `mapstructure:"pool_size"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
}

// JWTConfig JWT配置
type JWTConfig struct {
	Secret          string        `mapstructure:"secret"`
	AccessTokenTTL  time.Duration `mapstructure:"access_token_ttl"`
	RefreshTokenTTL time.Duration `mapstructure:"refresh_token_ttl"`
	Issuer          string        `mapstructure:"issuer"`
	EnableRefresh   bool          `mapstructure:"enable_refresh"`
}

// StorageConfig 存储配置
type StorageConfig struct {
	Type       string      `mapstructure:"type"` // local, s3, minio
	Local      LocalConfig `mapstructure:"local"`
	S3         S3Config    `mapstructure:"s3"`
	CDNDomain  string      `mapstructure:"cdn_domain"`
	UploadPath string      `mapstructure:"upload_path"` // 兼容性字段
}

type LocalConfig struct {
	UploadPath string `mapstructure:"upload_path"`
	URLPrefix  string `mapstructure:"url_prefix"`
}

type S3Config struct {
	Region    string `mapstructure:"region"`
	Bucket    string `mapstructure:"bucket"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Endpoint  string `mapstructure:"endpoint"`
	UseSSL    bool   `mapstructure:"use_ssl"`
}

// LoggerConfig 日志配置
type LoggerConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"` // json, console
	OutputPath string `mapstructure:"output_path"`
}

// SQLLoggerConfig SQL日志配置
type SQLLoggerConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	OutputPath string `mapstructure:"output_path"`
	MaxSize    int    `mapstructure:"max_size"`    // 单个文件最大大小(MB)
	MaxBackups int    `mapstructure:"max_backups"` // 保留的旧日志文件数量
	MaxAge     int    `mapstructure:"max_age"`     // 保留的最大天数
	Compress   bool   `mapstructure:"compress"`    // 是否压缩
}

// QueueConfig 队列配置
type QueueConfig struct {
	RedisAddr     string `mapstructure:"redis_addr"`
	RedisPassword string `mapstructure:"redis_password"`
	RedisDB       int    `mapstructure:"redis_db"`
	MaxRetries    int    `mapstructure:"max_retries"`
	QueueCritical string `mapstructure:"queue_critical"`
	QueueDefault  string `mapstructure:"queue_default"`
	QueueLow      string `mapstructure:"queue_low"`
}

// JSVMConfig JavaScript 虚拟机配置
type JSVMConfig struct {
	Enabled             bool   `mapstructure:"enabled"`
	HooksDir            string `mapstructure:"hooks_dir"`
	HooksWatch          bool   `mapstructure:"hooks_watch"`
	HooksPoolSize       int    `mapstructure:"hooks_pool_size"`
	PluginsDir          string `mapstructure:"plugins_dir"`
	HooksFilesPattern   string `mapstructure:"hooks_files_pattern"`
	PluginsFilesPattern string `mapstructure:"plugins_files_pattern"`
}

// Load 加载配置
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// 设置环境变量前缀
	viper.SetEnvPrefix("LUCKDB")
	viper.AutomaticEnv()

	// 设置环境变量键替换规则，避免系统环境变量干扰
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// 设置默认值
	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
		// 配置文件不存在时使用默认值和环境变量
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// setDefaults 设置默认配置值
func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 3000)
	viper.SetDefault("server.mode", "debug")
	viper.SetDefault("server.read_timeout", "30s")
	viper.SetDefault("server.write_timeout", "30s")
	viper.SetDefault("server.idle_timeout", "120s")
	viper.SetDefault("server.max_header_bytes", 1<<20) // 1MB
	viper.SetDefault("server.shutdown_timeout", "10s")
	viper.SetDefault("server.enable_cors", true)
	viper.SetDefault("server.enable_swagger", true)
	viper.SetDefault("server.permissions_disabled", false)

	// Database defaults
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.user", "postgres")
	viper.SetDefault("database.password", "")
	viper.SetDefault("database.name", "easytable")
	viper.SetDefault("database.ssl_mode", "disable")
	viper.SetDefault("database.max_idle_conns", 25)
	viper.SetDefault("database.max_open_conns", 200)
	viper.SetDefault("database.conn_max_lifetime", "1h")
	viper.SetDefault("database.log_level", "info")

	// Redis defaults
	viper.SetDefault("redis.host", "localhost")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.password", "")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.pool_size", 10)
	viper.SetDefault("redis.dial_timeout", "5s")

	// JWT defaults
	viper.SetDefault("jwt.secret", "your-secret-key")
	viper.SetDefault("jwt.access_token_ttl", "24h")
	viper.SetDefault("jwt.refresh_token_ttl", "168h") // 7 days
	viper.SetDefault("jwt.issuer", "luckdb-api")
	viper.SetDefault("jwt.enable_refresh", true)

	// Storage defaults
	viper.SetDefault("storage.type", "local")
	viper.SetDefault("storage.local.upload_path", "./uploads")
	viper.SetDefault("storage.local.url_prefix", "/uploads")

	// Logger defaults
	viper.SetDefault("logger.level", "info")
	viper.SetDefault("logger.format", "json")
	viper.SetDefault("logger.output_path", "stdout")

	// SQL Logger defaults
	viper.SetDefault("sql_logger.enabled", true)
	viper.SetDefault("sql_logger.output_path", "logs/sql.log")
	viper.SetDefault("sql_logger.max_size", 100)
	viper.SetDefault("sql_logger.max_backups", 5)
	viper.SetDefault("sql_logger.max_age", 30)
	viper.SetDefault("sql_logger.compress", false)

	// Queue defaults
	viper.SetDefault("queue.redis_addr", "localhost:6379")
	viper.SetDefault("queue.redis_password", "")
	viper.SetDefault("queue.redis_db", 1)
	viper.SetDefault("queue.max_retries", 3)
	viper.SetDefault("queue.queue_critical", "critical")
	viper.SetDefault("queue.queue_default", "default")
	viper.SetDefault("queue.queue_low", "low")

	// JSVM defaults
	viper.SetDefault("jsvm.enabled", true)
	viper.SetDefault("jsvm.hooks_dir", "./hooks")
	viper.SetDefault("jsvm.hooks_watch", true)
	viper.SetDefault("jsvm.hooks_pool_size", 10)
	viper.SetDefault("jsvm.plugins_dir", "./plugins")
	viper.SetDefault("jsvm.hooks_files_pattern", `^.*\.js$`)
	viper.SetDefault("jsvm.plugins_files_pattern", `^.*\.js$`)

	// MCP defaults
	viper.SetDefault("mcp.enabled", true)
	viper.SetDefault("mcp.server.host", "0.0.0.0")
	viper.SetDefault("mcp.server.port", 8081)
	viper.SetDefault("mcp.server.protocol", "http")
	viper.SetDefault("mcp.server.timeout", "30s")

	// MCP Auth defaults
	viper.SetDefault("mcp.auth.api_key.enabled", true)
	viper.SetDefault("mcp.auth.api_key.header", "X-MCP-API-Key")
	viper.SetDefault("mcp.auth.api_key.format", "key_id:key_secret")
	viper.SetDefault("mcp.auth.api_key.key_length", 32)
	viper.SetDefault("mcp.auth.api_key.secret_length", 64)
	viper.SetDefault("mcp.auth.api_key.default_ttl", "8760h")
	viper.SetDefault("mcp.auth.api_key.max_ttl", "87600h")

	viper.SetDefault("mcp.auth.jwt.enabled", true)
	viper.SetDefault("mcp.auth.jwt.header", "Authorization")
	viper.SetDefault("mcp.auth.jwt.prefix", "Bearer ")
	viper.SetDefault("mcp.auth.jwt.issuer", "luckdb-mcp")
	viper.SetDefault("mcp.auth.jwt.audience", "mcp-client")
	viper.SetDefault("mcp.auth.jwt.access_token_ttl", "1h")
	viper.SetDefault("mcp.auth.jwt.refresh_token_ttl", "24h")

	viper.SetDefault("mcp.auth.session.enabled", true)
	viper.SetDefault("mcp.auth.session.cookie_name", "mcp_session")
	viper.SetDefault("mcp.auth.session.secure", true)
	viper.SetDefault("mcp.auth.session.http_only", true)
	viper.SetDefault("mcp.auth.session.same_site", "strict")
	viper.SetDefault("mcp.auth.session.max_age", "24h")

	// MCP Tools defaults
	viper.SetDefault("mcp.tools.enabled_tools", []string{
		"query_records", "search_records", "aggregate_data",
		"create_record", "update_record", "delete_record", "bulk_operations",
		"get_table_schema", "create_field", "create_view",
		"generate_chart", "export_data",
	})

	// MCP Resources defaults
	viper.SetDefault("mcp.resources.enabled_resources", []string{
		"table_schema", "record_data", "metadata",
	})

	// MCP Prompts defaults
	viper.SetDefault("mcp.prompts.enabled_prompts", []string{
		"analyze_data", "create_summary", "generate_insights",
	})

	// MCP Rate Limit defaults
	viper.SetDefault("mcp.rate_limit.enabled", true)
	viper.SetDefault("mcp.rate_limit.redis_url", "")
	viper.SetDefault("mcp.rate_limit.strategies.user.type", "user")
	viper.SetDefault("mcp.rate_limit.strategies.user.requests_per_minute", 100)
	viper.SetDefault("mcp.rate_limit.strategies.user.burst_size", 20)
	viper.SetDefault("mcp.rate_limit.strategies.api_key.type", "api_key")
	viper.SetDefault("mcp.rate_limit.strategies.api_key.requests_per_minute", 1000)
	viper.SetDefault("mcp.rate_limit.strategies.api_key.burst_size", 100)
	viper.SetDefault("mcp.rate_limit.strategies.ip.type", "ip")
	viper.SetDefault("mcp.rate_limit.strategies.ip.requests_per_minute", 200)
	viper.SetDefault("mcp.rate_limit.strategies.ip.burst_size", 50)

	// MCP Security defaults
	viper.SetDefault("mcp.security.validation.enabled", true)
	viper.SetDefault("mcp.security.validation.max_query_length", 10000)
	viper.SetDefault("mcp.security.validation.max_parameters", 100)
	viper.SetDefault("mcp.security.validation.dangerous_keywords", []string{
		"DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE",
	})

	viper.SetDefault("mcp.security.audit.enabled", true)
	viper.SetDefault("mcp.security.audit.log_level", "info")
	viper.SetDefault("mcp.security.audit.sensitive_fields", []string{
		"password", "token", "secret",
	})
	viper.SetDefault("mcp.security.audit.retention_days", 90)

	viper.SetDefault("mcp.security.circuit_breaker.enabled", true)
	viper.SetDefault("mcp.security.circuit_breaker.failure_threshold", 5)
	viper.SetDefault("mcp.security.circuit_breaker.timeout", "30s")
	viper.SetDefault("mcp.security.circuit_breaker.max_requests", 3)

}

// GetDSN 获取数据库连接字符串
func (c *DatabaseConfig) GetDSN() string {
	// 使用URL格式，GORM的PostgreSQL驱动对此格式支持更好
	if c.Password != "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			c.User, c.Password, c.Host, c.Port, c.Name, c.SSLMode)
	}
	return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s",
		c.User, c.Host, c.Port, c.Name, c.SSLMode)
}

// GetRedisAddr 获取Redis连接地址
func (c *RedisConfig) GetRedisAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// GetServerAddr 获取服务器监听地址
func (c *ServerConfig) GetServerAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
