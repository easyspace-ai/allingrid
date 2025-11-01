package config

// AIConfig AI provider configuration
type AIConfig struct {
	// Default provider to use
	DefaultProvider string `yaml:"default_provider" env:"AI_DEFAULT_PROVIDER" default:"openai"`

	// Provider configurations
	Providers map[string]AIProviderConfig `yaml:"providers"`
}

// AIProviderConfig individual AI provider configuration
type AIProviderConfig struct {
	// Provider type (openai, deepseek, anthropic, etc.)
	Type string `yaml:"type"`

	// API key for authentication
	APIKey string `yaml:"api_key" env:"AI_API_KEY"`

	// Base URL for API requests (optional, uses default if not specified)
	BaseURL string `yaml:"base_url" env:"AI_BASE_URL"`

	// Default model to use
	DefaultModel string `yaml:"default_model"`

	// Request timeout in seconds
	Timeout int `yaml:"timeout" default:"30"`

	// Rate limiting
	RateLimit AIRateLimitConfig `yaml:"rate_limit"`

	// Provider-specific options
	Options map[string]interface{} `yaml:"options"`
}

// AIRateLimitConfig rate limiting configuration for AI providers
type AIRateLimitConfig struct {
	// Requests per minute
	RequestsPerMinute int `yaml:"requests_per_minute" default:"60"`

	// Tokens per minute
	TokensPerMinute int `yaml:"tokens_per_minute" default:"90000"`

	// Concurrent requests
	ConcurrentRequests int `yaml:"concurrent_requests" default:"10"`
}

// DefaultAIConfig returns default AI configuration
func DefaultAIConfig() AIConfig {
	return AIConfig{
		DefaultProvider: "openai",
		Providers: map[string]AIProviderConfig{
			"openai": {
				Type:         "openai",
				DefaultModel: "gpt-3.5-turbo",
				Timeout:      30,
				RateLimit: AIRateLimitConfig{
					RequestsPerMinute:  60,
					TokensPerMinute:    90000,
					ConcurrentRequests: 10,
				},
			},
			"deepseek": {
				Type:         "deepseek",
				BaseURL:      "https://api.deepseek.com/v1",
				DefaultModel: "deepseek-chat",
				Timeout:      30,
				RateLimit: AIRateLimitConfig{
					RequestsPerMinute:  60,
					TokensPerMinute:    90000,
					ConcurrentRequests: 10,
				},
			},
			"anthropic": {
				Type:         "anthropic",
				BaseURL:      "https://api.anthropic.com/v1",
				DefaultModel: "claude-3-sonnet-20240229",
				Timeout:      30,
				RateLimit: AIRateLimitConfig{
					RequestsPerMinute:  50,
					TokensPerMinute:    100000,
					ConcurrentRequests: 5,
				},
			},
		},
	}
}
