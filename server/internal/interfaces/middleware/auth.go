package middleware

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/cache"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/authctx"
	appErrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// Claims JWT声明结构
type Claims struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	IsAdmin   bool   `json:"is_admin"`
	IsSystem  bool   `json:"is_system"`
	TokenType string `json:"token_type"` // access, refresh
	jwt.RegisteredClaims
}

// AuthService 认证服务接口
type AuthService interface {
	ValidateToken(tokenString string) (*Claims, error)
	GetUserFromToken(ctx context.Context, tokenString string) (*models.User, error)
}

// JWTAuthService JWT认证服务实现
type JWTAuthService struct {
	config      config.JWTConfig
	cacheClient cache.CacheService
}

// NewJWTAuthService 创建JWT认证服务
func NewJWTAuthService(jwtConfig config.JWTConfig, cacheClient cache.CacheService) *JWTAuthService {
	return &JWTAuthService{
		config:      jwtConfig,
		cacheClient: cacheClient,
	}
}

// ValidateToken 验证JWT令牌
func (s *JWTAuthService) ValidateToken(tokenString string) (*Claims, error) {

	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// 检查签名方法
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, appErrors.ErrInvalidToken
		}
		return []byte(s.config.Secret), nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, appErrors.ErrTokenExpired
		}
		return nil, appErrors.ErrInvalidToken
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, appErrors.ErrInvalidToken
	}

	// 检查令牌是否在黑名单中
	isBlacklisted, err := s.isTokenBlacklisted(context.Background(), tokenString)
	if err != nil {
		logger.Error("Failed to check token blacklist", logger.ErrorField(err))
	}
	if isBlacklisted {
		return nil, appErrors.ErrInvalidToken
	}

	return claims, nil
}

// GetUserFromToken 从令牌获取用户信息
func (s *JWTAuthService) GetUserFromToken(ctx context.Context, tokenString string) (*models.User, error) {

	claims, err := s.ValidateToken(tokenString)
	if err != nil {
		return nil, err
	}

	// 先从缓存获取用户信息
	var user models.User
	cacheKey := cache.BuildCacheKey(cache.UserCachePrefix, claims.UserID)
	err = s.cacheClient.Get(ctx, cacheKey, &user)
	if err == nil {
		return &user, nil
	}

	// 缓存未命中，这里应该从数据库获取用户信息
	// 暂时返回从JWT中的基本信息
	user = models.User{
		ID:    claims.UserID,
		Email: claims.Email,
		Name:  claims.Name,
	}

	if claims.IsAdmin {
		isAdmin := true
		user.IsAdmin = &isAdmin
	}

	if claims.IsSystem {
		isSystem := true
		user.IsSystem = &isSystem
	}

	return &user, nil
}

// isTokenBlacklisted 检查令牌是否在黑名单中
func (s *JWTAuthService) isTokenBlacklisted(ctx context.Context, tokenString string) (bool, error) {
	key := cache.BuildCacheKey("blacklist:", tokenString)
	exists, err := s.cacheClient.Exists(ctx, key)
	return exists, err
}

// AuthMiddleware 认证中间件
func AuthMiddleware(authService AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractToken(c)
		if token == "" {
			response.Error(c, appErrors.ErrUnauthorized.WithDetails("Missing authorization token"))
			c.Abort()
			return
		}

		user, err := authService.GetUserFromToken(c.Request.Context(), token)
		if err != nil {
			response.Error(c, err)
			c.Abort()
			return
		}

		// 检查用户是否被禁用或删除
		if !user.IsActive() {
			response.Error(c, appErrors.ErrForbidden.WithDetails("User account is deactivated"))
			c.Abort()
			return
		}

		// 将用户信息存储到上下文中
		c.Set("user", user)
		c.Set("user_id", user.ID)
		c.Set("token", token)

		// 将用户ID注入到请求上下文，供服务层使用
		c.Request = c.Request.WithContext(authctx.WithUser(c.Request.Context(), user.ID))

		c.Next()
	}
}

// OptionalAuthMiddleware 可选认证中间件
func OptionalAuthMiddleware(authService AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractToken(c)
		if token != "" {
			user, err := authService.GetUserFromToken(c.Request.Context(), token)
			if err == nil && user.IsActive() {
				c.Set("user", user)
				c.Set("user_id", user.ID)
				c.Set("token", token)
				// 注入用户到请求上下文
				c.Request = c.Request.WithContext(authctx.WithUser(c.Request.Context(), user.ID))
			}
		}
		c.Next()
	}
}

// AdminRequiredMiddleware 管理员权限中间件
func AdminRequiredMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, exists := c.Get("user")
		if !exists {
			response.Error(c, appErrors.ErrUnauthorized.WithDetails("Authentication required"))
			c.Abort()
			return
		}

		u, ok := user.(*models.User)
		if !ok {
			response.Error(c, appErrors.ErrInternalServer.WithDetails("Invalid user context"))
			c.Abort()
			return
		}

		if !u.IsAdminUser() && !u.IsSuperAdmin() {
			response.Error(c, appErrors.ErrForbidden.WithDetails("Admin access required"))
			c.Abort()
			return
		}

		c.Next()
	}
}

// SuperAdminRequiredMiddleware 超级管理员权限中间件
func SuperAdminRequiredMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, exists := c.Get("user")
		if !exists {
			response.Error(c, appErrors.ErrUnauthorized.WithDetails("Authentication required"))
			c.Abort()
			return
		}

		u, ok := user.(*models.User)
		if !ok {
			response.Error(c, appErrors.ErrInternalServer.WithDetails("Invalid user context"))
			c.Abort()
			return
		}

		if !u.IsSuperAdmin() {
			response.Error(c, appErrors.ErrForbidden.WithDetails("Super admin access required"))
			c.Abort()
			return
		}

		c.Next()
	}
}

// extractToken 从请求中提取令牌
func extractToken(c *gin.Context) string {
	// 从Authorization header获取
	authHeader := c.GetHeader("Authorization")
	if authHeader != "" {
		// Bearer token格式
		if strings.HasPrefix(authHeader, "Bearer ") {
			return strings.TrimPrefix(authHeader, "Bearer ")
		}
		// 直接token格式
		return authHeader
	}

	// 从query参数获取
	token := c.Query("token")
	if token != "" {
		return token
	}

	// 从cookie获取
	cookie, err := c.Cookie("access_token")
	if err == nil && cookie != "" {
		return cookie
	}

	return ""
}

// GetCurrentUser 获取当前用户
func GetCurrentUser(c *gin.Context) (*models.User, error) {
	user, exists := c.Get("user")
	if !exists {
		return nil, appErrors.ErrUnauthorized
	}

	u, ok := user.(*models.User)
	if !ok {
		return nil, appErrors.ErrInternalServer
	}

	return u, nil
}

// GetCurrentUserID 获取当前用户ID
func GetCurrentUserID(c *gin.Context) (string, error) {
	userID, exists := c.Get("user_id")
	if !exists {
		return "", appErrors.ErrUnauthorized
	}

	id, ok := userID.(string)
	if !ok {
		return "", appErrors.ErrInternalServer
	}

	return id, nil
}

// RequireAuth 确保用户已认证
func RequireAuth(c *gin.Context) (*models.User, error) {
	user, err := GetCurrentUser(c)
	if err != nil {
		response.Error(c, appErrors.ErrUnauthorized.WithDetails("Authentication required"))
		c.Abort()
		return nil, err
	}
	return user, nil
}

// GenerateToken 生成JWT令牌
func GenerateToken(user *models.User, jwtConfig config.JWTConfig, tokenType string) (string, error) {
	now := time.Now()
	var exp time.Time

	switch tokenType {
	case "access":
		exp = now.Add(jwtConfig.AccessTokenTTL)
	case "refresh":
		exp = now.Add(jwtConfig.RefreshTokenTTL)
	default:
		exp = now.Add(jwtConfig.AccessTokenTTL)
	}

	claims := Claims{
		UserID:    user.ID,
		Email:     user.Email,
		Name:      user.Name,
		IsAdmin:   user.IsAdminUser(),
		IsSystem:  user.IsSuperAdmin(),
		TokenType: tokenType,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    jwtConfig.Issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(jwtConfig.Secret))
}
