package config

import (
	"time"
)

// MCPConfig MCP 服务器配置
type MCPConfig struct {
	Enabled   bool               `mapstructure:"enabled"`
	Server    MCPServerConfig    `mapstructure:"server"`
	Auth      MCPAuthConfig      `mapstructure:"auth"`
	Tools     MCPToolsConfig     `mapstructure:"tools"`
	Resources MCPResourcesConfig `mapstructure:"resources"`
	Prompts   MCPPromptsConfig   `mapstructure:"prompts"`
	RateLimit MCPRateLimitConfig `mapstructure:"rate_limit"`
	Security  MCPSecurityConfig  `mapstructure:"security"`
}

// MCPServerConfig MCP 服务器配置
type MCPServerConfig struct {
	Host     string        `mapstructure:"host"`
	Port     int           `mapstructure:"port"`
	Protocol string        `mapstructure:"protocol"` // http, websocket, stdio
	Timeout  time.Duration `mapstructure:"timeout"`
}

// MCPAuthConfig 认证配置
type MCPAuthConfig struct {
	APIKey  MCPAPIKeyConfig  `mapstructure:"api_key"`
	JWT     MCPJWTConfig     `mapstructure:"jwt"`
	Session MCPSessionConfig `mapstructure:"session"`
}

// MCPAPIKeyConfig API Key 配置
type MCPAPIKeyConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	Header       string        `mapstructure:"header"`
	Format       string        `mapstructure:"format"`
	KeyLength    int           `mapstructure:"key_length"`
	SecretLength int           `mapstructure:"secret_length"`
	DefaultTTL   time.Duration `mapstructure:"default_ttl"`
	MaxTTL       time.Duration `mapstructure:"max_ttl"`
}

// MCPJWTConfig JWT 配置
type MCPJWTConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	Header          string        `mapstructure:"header"`
	Prefix          string        `mapstructure:"prefix"`
	Secret          string        `mapstructure:"secret"`
	Issuer          string        `mapstructure:"issuer"`
	Audience        string        `mapstructure:"audience"`
	AccessTokenTTL  time.Duration `mapstructure:"access_token_ttl"`
	RefreshTokenTTL time.Duration `mapstructure:"refresh_token_ttl"`
}

// MCPSessionConfig 会话配置
type MCPSessionConfig struct {
	Enabled    bool          `mapstructure:"enabled"`
	CookieName string        `mapstructure:"cookie_name"`
	Secure     bool          `mapstructure:"secure"`
	HTTPOnly   bool          `mapstructure:"http_only"`
	SameSite   string        `mapstructure:"same_site"`
	MaxAge     time.Duration `mapstructure:"max_age"`
}

// MCPToolsConfig 工具配置
type MCPToolsConfig struct {
	EnabledTools []string `mapstructure:"enabled_tools"`
}

// MCPResourcesConfig 资源配置
type MCPResourcesConfig struct {
	EnabledResources []string `mapstructure:"enabled_resources"`
}

// MCPPromptsConfig 提示配置
type MCPPromptsConfig struct {
	EnabledPrompts []string `mapstructure:"enabled_prompts"`
}

// MCPRateLimitConfig 限流配置
type MCPRateLimitConfig struct {
	Enabled    bool                        `mapstructure:"enabled"`
	RedisURL   string                      `mapstructure:"redis_url"`
	Strategies map[string]MCPLimitStrategy `mapstructure:"strategies"`
}

// MCPLimitStrategy 限流策略
type MCPLimitStrategy struct {
	Type              string `mapstructure:"type"`
	RequestsPerMinute int    `mapstructure:"requests_per_minute"`
	BurstSize         int    `mapstructure:"burst_size"`
}

// MCPSecurityConfig 安全配置
type MCPSecurityConfig struct {
	Validation     MCPValidationConfig     `mapstructure:"validation"`
	Audit          MCPAuditConfig          `mapstructure:"audit"`
	CircuitBreaker MCPCircuitBreakerConfig `mapstructure:"circuit_breaker"`
}

// MCPValidationConfig 验证配置
type MCPValidationConfig struct {
	Enabled           bool     `mapstructure:"enabled"`
	MaxQueryLength    int      `mapstructure:"max_query_length"`
	MaxParameters     int      `mapstructure:"max_parameters"`
	DangerousKeywords []string `mapstructure:"dangerous_keywords"`
}

// MCPAuditConfig 审计配置
type MCPAuditConfig struct {
	Enabled         bool     `mapstructure:"enabled"`
	LogLevel        string   `mapstructure:"log_level"`
	SensitiveFields []string `mapstructure:"sensitive_fields"`
	RetentionDays   int      `mapstructure:"retention_days"`
}

// MCPCircuitBreakerConfig 熔断配置
type MCPCircuitBreakerConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	FailureThreshold int           `mapstructure:"failure_threshold"`
	Timeout          time.Duration `mapstructure:"timeout"`
	MaxRequests      int           `mapstructure:"max_requests"`
}
