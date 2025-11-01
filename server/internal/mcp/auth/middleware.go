package auth

import (
	"net/http"
	"strings"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
	"github.com/gin-gonic/gin"
)

// AuthContext 认证上下文
type AuthContext struct {
	Type      string                 `json:"type"`
	UserID    string                 `json:"user_id,omitempty"`
	APIKeyID  string                 `json:"api_key_id,omitempty"`
	Scopes    []string               `json:"scopes,omitempty"`
	ExpiresAt *time.Time             `json:"expires_at,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// AuthType 认证类型常量
const (
	AuthTypeAPIKey  = "api_key"
	AuthTypeJWT     = "jwt"
	AuthTypeSession = "session"
)

// APIKeyAuthMiddleware API Key 认证中间件
func APIKeyAuthMiddleware(apiKeyService *APIKeyService, config *APIKeyConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头获取 API Key
		keyString := c.GetHeader(config.Header)
		if keyString == "" {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "API key is required", nil))
			c.Abort()
			return
		}

		// 验证 API Key
		apiKey, err := apiKeyService.ValidateAPIKey(c.Request.Context(), keyString)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Invalid API key", nil))
			c.Abort()
			return
		}

		// 设置认证上下文
		authCtx := &AuthContext{
			Type:      AuthTypeAPIKey,
			UserID:    apiKey.UserID,
			APIKeyID:  apiKey.ID,
			Scopes:    apiKey.Scopes,
			ExpiresAt: apiKey.ExpiresAt,
			Metadata: map[string]interface{}{
				"key_id":      apiKey.KeyID,
				"description": apiKey.Description,
				"created_at":  apiKey.CreatedAt,
			},
		}

		c.Set("auth_context", authCtx)
		c.Set("user_id", apiKey.UserID)
		c.Set("api_key_id", apiKey.ID)
		c.Set("scopes", apiKey.Scopes)

		c.Next()
	}
}

// JWTAuthMiddleware JWT 认证中间件
func JWTAuthMiddleware(jwtService *JWTService, config *JWTConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头获取 JWT Token
		authHeader := c.GetHeader(config.Header)
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Authorization header is required", nil))
			c.Abort()
			return
		}

		// 检查 Bearer 前缀
		if !strings.HasPrefix(authHeader, config.Prefix) {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Invalid authorization header format", nil))
			c.Abort()
			return
		}

		// 提取 Token
		token := strings.TrimPrefix(authHeader, config.Prefix)
		if token == "" {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Token is required", nil))
			c.Abort()
			return
		}

		// 验证 JWT Token
		claims, err := jwtService.ValidateToken(token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Invalid token", nil))
			c.Abort()
			return
		}

		// 设置认证上下文
		expiresAt := claims.ExpiresAt.Time
		authCtx := &AuthContext{
			Type:      AuthTypeJWT,
			UserID:    claims.Subject,
			Scopes:    claims.MCPScopes,
			ExpiresAt: &expiresAt,
			Metadata: map[string]interface{}{
				"issuer":    claims.Issuer,
				"audience":  claims.Audience,
				"issued_at": claims.IssuedAt,
			},
		}

		c.Set("auth_context", authCtx)
		c.Set("user_id", claims.Subject)
		c.Set("scopes", claims.MCPScopes)

		c.Next()
	}
}

// SessionAuthMiddleware 会话认证中间件
func SessionAuthMiddleware(sessionService *SessionService, config *SessionConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从 Cookie 获取会话 ID
		sessionID, err := c.Cookie(config.CookieName)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Session is required", nil))
			c.Abort()
			return
		}

		// 验证会话
		session, err := sessionService.ValidateSession(c.Request.Context(), sessionID)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Invalid session", nil))
			c.Abort()
			return
		}

		// 设置认证上下文
		authCtx := &AuthContext{
			Type:      AuthTypeSession,
			UserID:    session.UserID,
			ExpiresAt: &session.ExpiresAt,
			Metadata: map[string]interface{}{
				"ip_address": session.IPAddress,
				"user_agent": session.UserAgent,
				"created_at": session.CreatedAt,
			},
		}

		c.Set("auth_context", authCtx)
		c.Set("user_id", session.UserID)
		c.Set("session_id", session.ID)

		c.Next()
	}
}

// MultiAuthMiddleware 多重认证中间件
func MultiAuthMiddleware(apiKeyService *APIKeyService, jwtService *JWTService, sessionService *SessionService, config *MultiAuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		var authCtx *AuthContext

		// 尝试 API Key 认证
		if config.APIKey.Enabled {
			keyString := c.GetHeader(config.APIKey.Header)
			if keyString != "" {
				apiKey, authErr := apiKeyService.ValidateAPIKey(c.Request.Context(), keyString)
				if authErr == nil {
					authCtx = &AuthContext{
						Type:      AuthTypeAPIKey,
						UserID:    apiKey.UserID,
						APIKeyID:  apiKey.ID,
						Scopes:    apiKey.Scopes,
						ExpiresAt: apiKey.ExpiresAt,
						Metadata: map[string]interface{}{
							"key_id":      apiKey.KeyID,
							"description": apiKey.Description,
							"created_at":  apiKey.CreatedAt,
						},
					}
				}
			}
		}

		// 如果 API Key 认证失败，尝试 JWT 认证
		if authCtx == nil && config.JWT.Enabled {
			authHeader := c.GetHeader(config.JWT.Header)
			if authHeader != "" && strings.HasPrefix(authHeader, config.JWT.Prefix) {
				token := strings.TrimPrefix(authHeader, config.JWT.Prefix)
				if token != "" {
					claims, authErr := jwtService.ValidateToken(token)
					if authErr == nil {
						expiresAt := claims.ExpiresAt.Time
						authCtx = &AuthContext{
							Type:      AuthTypeJWT,
							UserID:    claims.Subject,
							Scopes:    claims.MCPScopes,
							ExpiresAt: &expiresAt,
							Metadata: map[string]interface{}{
								"issuer":    claims.Issuer,
								"audience":  claims.Audience,
								"issued_at": claims.IssuedAt,
							},
						}
					}
				}
			}
		}

		// 如果 JWT 认证失败，尝试会话认证
		if authCtx == nil && config.Session.Enabled {
			sessionID, cookieErr := c.Cookie(config.Session.CookieName)
			if cookieErr == nil {
				session, authErr := sessionService.ValidateSession(c.Request.Context(), sessionID)
				if authErr == nil {
					authCtx = &AuthContext{
						Type:      AuthTypeSession,
						UserID:    session.UserID,
						ExpiresAt: &session.ExpiresAt,
						Metadata: map[string]interface{}{
							"ip_address": session.IPAddress,
							"user_agent": session.UserAgent,
							"created_at": session.CreatedAt,
						},
					}
				}
			}
		}

		// 如果所有认证方式都失败
		if authCtx == nil {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Authentication required", nil))
			c.Abort()
			return
		}

		// 设置认证上下文
		c.Set("auth_context", authCtx)
		c.Set("user_id", authCtx.UserID)
		if authCtx.APIKeyID != "" {
			c.Set("api_key_id", authCtx.APIKeyID)
		}
		if authCtx.Scopes != nil {
			c.Set("scopes", authCtx.Scopes)
		}

		c.Next()
	}
}

// ScopeMiddleware 权限范围中间件
func ScopeMiddleware(requiredScopes ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取认证上下文
		authCtx, exists := c.Get("auth_context")
		if !exists {
			c.JSON(http.StatusUnauthorized, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthenticationFailed, "Authentication required", nil))
			c.Abort()
			return
		}

		authContext, ok := authCtx.(*AuthContext)
		if !ok {
			c.JSON(http.StatusInternalServerError, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeInternalError, "Invalid auth context", nil))
			c.Abort()
			return
		}

		// 检查权限范围
		for _, requiredScope := range requiredScopes {
			hasScope := false
			for _, scope := range authContext.Scopes {
				if scope == requiredScope {
					hasScope = true
					break
				}
			}
			if !hasScope {
				c.JSON(http.StatusForbidden, protocol.NewMCPErrorResponse(nil, protocol.ErrorCodeAuthorizationFailed, "Insufficient permissions", map[string]string{
					"required_scope": requiredScope,
				}))
				c.Abort()
				return
			}
		}

		c.Next()
	}
}

// GetAuthContext 从 Gin 上下文获取认证上下文
func GetAuthContext(c *gin.Context) (*AuthContext, bool) {
	authCtx, exists := c.Get("auth_context")
	if !exists {
		return nil, false
	}

	authContext, ok := authCtx.(*AuthContext)
	return authContext, ok
}

// GetUserID 从 Gin 上下文获取用户 ID
func GetUserID(c *gin.Context) (string, bool) {
	userID, exists := c.Get("user_id")
	if !exists {
		return "", false
	}

	userIDStr, ok := userID.(string)
	return userIDStr, ok
}

// GetAPIKeyID 从 Gin 上下文获取 API Key ID
func GetAPIKeyID(c *gin.Context) (string, bool) {
	apiKeyID, exists := c.Get("api_key_id")
	if !exists {
		return "", false
	}

	apiKeyIDStr, ok := apiKeyID.(string)
	return apiKeyIDStr, ok
}

// GetScopes 从 Gin 上下文获取权限范围
func GetScopes(c *gin.Context) ([]string, bool) {
	scopes, exists := c.Get("scopes")
	if !exists {
		return nil, false
	}

	scopesSlice, ok := scopes.([]string)
	return scopesSlice, ok
}
