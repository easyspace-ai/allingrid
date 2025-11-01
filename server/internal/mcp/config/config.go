package config

import (
	"fmt"
	"time"
)

// MCPConfig MCP 服务器配置
type MCPConfig struct {
	Enabled   bool            `mapstructure:"enabled"`
	Server    ServerConfig    `mapstructure:"server"`
	Auth      AuthConfig      `mapstructure:"auth"`
	Tools     ToolsConfig     `mapstructure:"tools"`
	Resources ResourcesConfig `mapstructure:"resources"`
	Prompts   PromptsConfig   `mapstructure:"prompts"`
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
	Security  SecurityConfig  `mapstructure:"security"`
}

// ServerConfig MCP 服务器配置
type ServerConfig struct {
	Host     string        `mapstructure:"host"`
	Port     int           `mapstructure:"port"`
	Protocol string        `mapstructure:"protocol"` // http, websocket, stdio
	Timeout  time.Duration `mapstructure:"timeout"`
}

// AuthConfig 认证配置
type AuthConfig struct {
	APIKey  APIKeyConfig  `mapstructure:"api_key"`
	JWT     JWTConfig     `mapstructure:"jwt"`
	Session SessionConfig `mapstructure:"session"`
}

// APIKeyConfig API Key 配置
type APIKeyConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	Header       string        `mapstructure:"header"`
	Format       string        `mapstructure:"format"`
	KeyLength    int           `mapstructure:"key_length"`
	SecretLength int           `mapstructure:"secret_length"`
	DefaultTTL   time.Duration `mapstructure:"default_ttl"`
	MaxTTL       time.Duration `mapstructure:"max_ttl"`
}

// JWTConfig JWT 配置
type JWTConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	Header          string        `mapstructure:"header"`
	Prefix          string        `mapstructure:"prefix"`
	Secret          string        `mapstructure:"secret"`
	Issuer          string        `mapstructure:"issuer"`
	Audience        string        `mapstructure:"audience"`
	AccessTokenTTL  time.Duration `mapstructure:"access_token_ttl"`
	RefreshTokenTTL time.Duration `mapstructure:"refresh_token_ttl"`
}

// SessionConfig 会话配置
type SessionConfig struct {
	Enabled    bool          `mapstructure:"enabled"`
	CookieName string        `mapstructure:"cookie_name"`
	Secure     bool          `mapstructure:"secure"`
	HTTPOnly   bool          `mapstructure:"http_only"`
	SameSite   string        `mapstructure:"same_site"`
	MaxAge     time.Duration `mapstructure:"max_age"`
}

// ToolsConfig 工具配置
type ToolsConfig struct {
	EnabledTools []string `mapstructure:"enabled_tools"`
}

// ResourcesConfig 资源配置
type ResourcesConfig struct {
	EnabledResources []string `mapstructure:"enabled_resources"`
}

// PromptsConfig 提示配置
type PromptsConfig struct {
	EnabledPrompts []string `mapstructure:"enabled_prompts"`
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	Enabled    bool                     `mapstructure:"enabled"`
	RedisURL   string                   `mapstructure:"redis_url"`
	Strategies map[string]LimitStrategy `mapstructure:"strategies"`
}

// LimitStrategy 限流策略
type LimitStrategy struct {
	Type              string `mapstructure:"type"`
	RequestsPerMinute int    `mapstructure:"requests_per_minute"`
	BurstSize         int    `mapstructure:"burst_size"`
}

// SecurityConfig 安全配置
type SecurityConfig struct {
	Validation     ValidationConfig     `mapstructure:"validation"`
	Audit          AuditConfig          `mapstructure:"audit"`
	CircuitBreaker CircuitBreakerConfig `mapstructure:"circuit_breaker"`
}

// ValidationConfig 验证配置
type ValidationConfig struct {
	Enabled           bool     `mapstructure:"enabled"`
	MaxQueryLength    int      `mapstructure:"max_query_length"`
	MaxParameters     int      `mapstructure:"max_parameters"`
	DangerousKeywords []string `mapstructure:"dangerous_keywords"`
}

// AuditConfig 审计配置
type AuditConfig struct {
	Enabled         bool     `mapstructure:"enabled"`
	LogLevel        string   `mapstructure:"log_level"`
	SensitiveFields []string `mapstructure:"sensitive_fields"`
	RetentionDays   int      `mapstructure:"retention_days"`
}

// CircuitBreakerConfig 熔断配置
type CircuitBreakerConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	FailureThreshold int           `mapstructure:"failure_threshold"`
	Timeout          time.Duration `mapstructure:"timeout"`
	MaxRequests      int           `mapstructure:"max_requests"`
}

// DefaultMCPConfig 返回默认 MCP 配置
func DefaultMCPConfig() *MCPConfig {
	return &MCPConfig{
		Enabled: true,
		Server: ServerConfig{
			Host:     "0.0.0.0",
			Port:     8081,
			Protocol: "http",
			Timeout:  30 * time.Second,
		},
		Auth: AuthConfig{
			APIKey: APIKeyConfig{
				Enabled:      true,
				Header:       "X-MCP-API-Key",
				Format:       "key_id:key_secret",
				KeyLength:    32,
				SecretLength: 64,
				DefaultTTL:   8760 * time.Hour,  // 1年
				MaxTTL:       87600 * time.Hour, // 10年
			},
			JWT: JWTConfig{
				Enabled:         true,
				Header:          "Authorization",
				Prefix:          "Bearer ",
				Secret:          "",
				Issuer:          "luckdb-mcp",
				Audience:        "mcp-client",
				AccessTokenTTL:  1 * time.Hour,
				RefreshTokenTTL: 24 * time.Hour,
			},
			Session: SessionConfig{
				Enabled:    true,
				CookieName: "mcp_session",
				Secure:     true,
				HTTPOnly:   true,
				SameSite:   "strict",
				MaxAge:     24 * time.Hour,
			},
		},
		Tools: ToolsConfig{
			EnabledTools: []string{
				"query_records",
				"search_records",
				"aggregate_data",
				"create_record",
				"update_record",
				"delete_record",
				"bulk_operations",
				"get_table_schema",
				"create_field",
				"create_view",
				"generate_chart",
				"export_data",
			},
		},
		Resources: ResourcesConfig{
			EnabledResources: []string{
				"table_schema",
				"record_data",
				"metadata",
			},
		},
		Prompts: PromptsConfig{
			EnabledPrompts: []string{
				"analyze_data",
				"create_summary",
				"generate_insights",
			},
		},
		RateLimit: RateLimitConfig{
			Enabled:  true,
			RedisURL: "",
			Strategies: map[string]LimitStrategy{
				"user": {
					Type:              "user",
					RequestsPerMinute: 100,
					BurstSize:         20,
				},
				"api_key": {
					Type:              "api_key",
					RequestsPerMinute: 1000,
					BurstSize:         100,
				},
				"ip": {
					Type:              "ip",
					RequestsPerMinute: 200,
					BurstSize:         50,
				},
			},
		},
		Security: SecurityConfig{
			Validation: ValidationConfig{
				Enabled:        true,
				MaxQueryLength: 10000,
				MaxParameters:  100,
				DangerousKeywords: []string{
					"DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE",
				},
			},
			Audit: AuditConfig{
				Enabled:         true,
				LogLevel:        "info",
				SensitiveFields: []string{"password", "token", "secret"},
				RetentionDays:   90,
			},
			CircuitBreaker: CircuitBreakerConfig{
				Enabled:          true,
				FailureThreshold: 5,
				Timeout:          30 * time.Second,
				MaxRequests:      3,
			},
		},
	}
}

// GetServerAddr 获取服务器地址
func (c *ServerConfig) GetServerAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// IsToolEnabled 检查工具是否启用
func (c *ToolsConfig) IsToolEnabled(toolName string) bool {
	for _, enabled := range c.EnabledTools {
		if enabled == toolName {
			return true
		}
	}
	return false
}

// IsResourceEnabled 检查资源是否启用
func (c *ResourcesConfig) IsResourceEnabled(resourceName string) bool {
	for _, enabled := range c.EnabledResources {
		if enabled == resourceName {
			return true
		}
	}
	return false
}

// IsPromptEnabled 检查提示是否启用
func (c *PromptsConfig) IsPromptEnabled(promptName string) bool {
	for _, enabled := range c.EnabledPrompts {
		if enabled == promptName {
			return true
		}
	}
	return false
}
