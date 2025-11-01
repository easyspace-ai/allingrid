package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
	"github.com/golang-jwt/jwt/v5"
)

// JWTService JWT 服务
type JWTService struct {
	config *JWTConfig
}

// JWTConfig JWT 配置
type JWTConfig struct {
	Enabled         bool          `json:"enabled"`
	Header          string        `json:"header"`
	Prefix          string        `json:"prefix"`
	Secret          string        `json:"secret"`
	Issuer          string        `json:"issuer"`
	Audience        string        `json:"audience"`
	AccessTokenTTL  time.Duration `json:"access_token_ttl"`
	RefreshTokenTTL time.Duration `json:"refresh_token_ttl"`
}

// MCPClaims MCP JWT Claims
type MCPClaims struct {
	UserID    string   `json:"user_id"`
	MCPScopes []string `json:"mcp_scopes"`
	MCPSpaces []string `json:"mcp_spaces,omitempty"`
	jwt.RegisteredClaims
}

// NewJWTService 创建新的 JWT 服务
func NewJWTService(config *JWTConfig) *JWTService {
	return &JWTService{
		config: config,
	}
}

// GenerateToken 生成 JWT Token
func (s *JWTService) GenerateToken(userID string, scopes []string, spaces []string) (string, error) {
	now := time.Now()
	claims := &MCPClaims{
		UserID:    userID,
		MCPScopes: scopes,
		MCPSpaces: spaces,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    s.config.Issuer,
			Audience:  []string{s.config.Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.config.AccessTokenTTL)),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.config.Secret))
}

// ValidateToken 验证 JWT Token
func (s *JWTService) ValidateToken(tokenString string) (*MCPClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &MCPClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(s.config.Secret), nil
	})

	if err != nil {
		return nil, protocol.NewAuthenticationFailedError("Invalid token")
	}

	if claims, ok := token.Claims.(*MCPClaims); ok && token.Valid {
		// 验证发行者和受众
		if claims.Issuer != s.config.Issuer {
			return nil, protocol.NewAuthenticationFailedError("Invalid token issuer")
		}

		// 检查受众
		validAudience := false
		for _, aud := range claims.Audience {
			if aud == s.config.Audience {
				validAudience = true
				break
			}
		}
		if !validAudience {
			return nil, protocol.NewAuthenticationFailedError("Invalid token audience")
		}

		return claims, nil
	}

	return nil, protocol.NewAuthenticationFailedError("Invalid token")
}

// RefreshToken 刷新令牌结构
type RefreshToken struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
	IsActive  bool      `json:"is_active"`
}

// RefreshTokenRepository 刷新令牌仓储接口
type RefreshTokenRepository interface {
	Create(ctx context.Context, token *RefreshToken) error
	GetByToken(ctx context.Context, token string) (*RefreshToken, error)
	Delete(ctx context.Context, id string) error
	DeleteByUserID(ctx context.Context, userID string) error
	CleanupExpired(ctx context.Context) error
}

// GenerateRefreshToken 生成刷新令牌
func (s *JWTService) GenerateRefreshToken(userID string) (*RefreshToken, error) {
	// 生成随机令牌
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("failed to generate refresh token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)

	refreshToken := &RefreshToken{
		ID:        fmt.Sprintf("rt_%s", token[:8]),
		UserID:    userID,
		Token:     token,
		ExpiresAt: time.Now().Add(s.config.RefreshTokenTTL),
		CreatedAt: time.Now(),
		IsActive:  true,
	}

	return refreshToken, nil
}

// RefreshAccessToken 使用刷新令牌生成新的访问令牌
func (s *JWTService) RefreshAccessToken(ctx context.Context, refreshTokenRepo RefreshTokenRepository, refreshTokenString string, scopes []string, spaces []string) (string, *RefreshToken, error) {
	// 验证刷新令牌
	refreshToken, err := refreshTokenRepo.GetByToken(ctx, refreshTokenString)
	if err != nil {
		return "", nil, protocol.NewAuthenticationFailedError("Invalid refresh token")
	}

	// 检查是否激活
	if !refreshToken.IsActive {
		return "", nil, protocol.NewAuthenticationFailedError("Refresh token is inactive")
	}

	// 检查是否过期
	if refreshToken.ExpiresAt.Before(time.Now()) {
		return "", nil, protocol.NewAuthenticationFailedError("Refresh token has expired")
	}

	// 生成新的访问令牌
	accessToken, err := s.GenerateToken(refreshToken.UserID, scopes, spaces)
	if err != nil {
		return "", nil, fmt.Errorf("failed to generate access token: %w", err)
	}

	// 生成新的刷新令牌
	newRefreshToken, err := s.GenerateRefreshToken(refreshToken.UserID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to generate new refresh token: %w", err)
	}

	// 删除旧的刷新令牌
	if err := refreshTokenRepo.Delete(ctx, refreshToken.ID); err != nil {
		// 记录错误但不影响结果
		fmt.Printf("Failed to delete old refresh token: %v\n", err)
	}

	// 保存新的刷新令牌
	if err := refreshTokenRepo.Create(ctx, newRefreshToken); err != nil {
		return "", nil, fmt.Errorf("failed to save new refresh token: %w", err)
	}

	return accessToken, newRefreshToken, nil
}

// RevokeRefreshToken 撤销刷新令牌
func (s *JWTService) RevokeRefreshToken(ctx context.Context, refreshTokenRepo RefreshTokenRepository, refreshTokenString string) error {
	refreshToken, err := refreshTokenRepo.GetByToken(ctx, refreshTokenString)
	if err != nil {
		return protocol.NewAuthenticationFailedError("Invalid refresh token")
	}

	return refreshTokenRepo.Delete(ctx, refreshToken.ID)
}

// RevokeAllUserTokens 撤销用户的所有刷新令牌
func (s *JWTService) RevokeAllUserTokens(ctx context.Context, refreshTokenRepo RefreshTokenRepository, userID string) error {
	return refreshTokenRepo.DeleteByUserID(ctx, userID)
}
