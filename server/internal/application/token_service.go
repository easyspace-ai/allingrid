package application

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// TokenService Token服务
type TokenService struct {
	jwtSecret  string
	accessTTL  time.Duration
	refreshTTL time.Duration
	issuer     string
}

// NewTokenService 创建Token服务
func NewTokenService(cfg config.JWTConfig) *TokenService {
	return &TokenService{
		jwtSecret:  cfg.Secret,
		accessTTL:  cfg.AccessTokenTTL,
		refreshTTL: cfg.RefreshTokenTTL,
		issuer:     cfg.Issuer,
	}
}

// Claims JWT声明
type Claims struct {
	UserID  string `json:"user_id"`
	Email   string `json:"email"`
	IsAdmin bool   `json:"is_admin"`
	jwt.RegisteredClaims
}

// GenerateTokens 生成访问令牌和刷新令牌
func (s *TokenService) GenerateTokens(userID, email string, isAdmin bool) (accessToken string, refreshToken string, err error) {
	// 生成访问Token
	accessToken, err = s.generateAccessToken(userID, email, isAdmin)
	if err != nil {
		return "", "", err
	}

	// 生成刷新Token
	refreshToken, err = s.generateRefreshToken(userID, email, isAdmin)
	if err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

// generateAccessToken 生成访问令牌
func (s *TokenService) generateAccessToken(userID, email string, isAdmin bool) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID:  userID,
		Email:   email,
		IsAdmin: isAdmin,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.accessTTL)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.jwtSecret))
}

// generateRefreshToken 生成刷新令牌
func (s *TokenService) generateRefreshToken(userID, email string, isAdmin bool) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID:  userID,
		Email:   email,
		IsAdmin: isAdmin,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.refreshTTL)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.jwtSecret))
}

// ValidateAccessToken 验证访问令牌
func (s *TokenService) ValidateAccessToken(tokenString string) (*Claims, error) {
	return s.parseToken(tokenString)
}

// ValidateRefreshToken 验证刷新令牌
func (s *TokenService) ValidateRefreshToken(tokenString string) (*Claims, error) {
	return s.parseToken(tokenString)
}

// parseToken 解析Token
func (s *TokenService) parseToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// 验证签名方法
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, pkgerrors.ErrUnauthorized.WithDetails(fmt.Sprintf("unexpected signing method: %v", token.Header["alg"]))
		}
		return []byte(s.jwtSecret), nil
	})

	if err != nil {
		return nil, pkgerrors.ErrUnauthorized.WithDetails(fmt.Sprintf("解析Token失败: %v", err))
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, pkgerrors.ErrUnauthorized.WithDetails("Token无效")
}

// ExtractUserID 从Token中提取用户ID
func (s *TokenService) ExtractUserID(tokenString string) (string, error) {
	claims, err := s.parseToken(tokenString)
	if err != nil {
		return "", err
	}

	return claims.UserID, nil
}

// IsTokenExpired 检查Token是否过期
func (s *TokenService) IsTokenExpired(tokenString string) bool {
	claims, err := s.parseToken(tokenString)
	if err != nil {
		return true
	}

	return claims.ExpiresAt != nil && claims.ExpiresAt.Before(time.Now())
}
