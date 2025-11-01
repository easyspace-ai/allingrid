package auth

import "time"

// MultiAuthConfig 多重认证配置
type MultiAuthConfig struct {
	APIKey  APIKeyConfig  `json:"api_key"`
	JWT     JWTConfig     `json:"jwt"`
	Session SessionConfig `json:"session"`
}

// DefaultMultiAuthConfig 默认多重认证配置
func DefaultMultiAuthConfig() *MultiAuthConfig {
	return &MultiAuthConfig{
		APIKey: APIKeyConfig{
			Enabled:      true,
			KeyLength:    32,
			SecretLength: 64,
			DefaultTTL:   8760 * time.Hour,  // 1年
			MaxTTL:       87600 * time.Hour, // 10年
			Header:       "X-MCP-API-Key",
			Format:       "key_id:key_secret",
		},
		JWT: JWTConfig{
			Enabled:         true,
			Header:          "Authorization",
			Prefix:          "Bearer ",
			Secret:          "your-secret-key",
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
	}
}
