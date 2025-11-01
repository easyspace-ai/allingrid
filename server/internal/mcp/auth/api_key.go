package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// APIKey API Key 结构
type APIKey struct {
	ID          string                 `json:"id"`
	KeyID       string                 `json:"key_id"`
	Secret      string                 `json:"secret"`
	UserID      string                 `json:"user_id,omitempty"`
	Scopes      []string               `json:"scopes"`
	Description string                 `json:"description,omitempty"`
	ExpiresAt   *time.Time             `json:"expires_at,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	LastUsedAt  *time.Time             `json:"last_used_at,omitempty"`
	IsActive    bool                   `json:"is_active"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// APIKeyRepository API Key 仓储接口
type APIKeyRepository interface {
	Create(ctx context.Context, apiKey *APIKey) error
	GetByID(ctx context.Context, id string) (*APIKey, error)
	GetByKeyID(ctx context.Context, keyID string) (*APIKey, error)
	Update(ctx context.Context, apiKey *APIKey) error
	Delete(ctx context.Context, id string) error
	ListByUserID(ctx context.Context, userID string) ([]*APIKey, error)
	ListActive(ctx context.Context) ([]*APIKey, error)
}

// APIKeyService API Key 服务
type APIKeyService struct {
	repo   APIKeyRepository
	config *APIKeyConfig
}

// APIKeyConfig API Key 配置
type APIKeyConfig struct {
	Enabled      bool          `json:"enabled"`
	KeyLength    int           `json:"key_length"`
	SecretLength int           `json:"secret_length"`
	DefaultTTL   time.Duration `json:"default_ttl"`
	MaxTTL       time.Duration `json:"max_ttl"`
	Header       string        `json:"header"`
	Format       string        `json:"format"`
}

// NewAPIKeyService 创建新的 API Key 服务
func NewAPIKeyService(repo APIKeyRepository, config *APIKeyConfig) *APIKeyService {
	return &APIKeyService{
		repo:   repo,
		config: config,
	}
}

// GenerateAPIKey 生成新的 API Key
func (s *APIKeyService) GenerateAPIKey(userID string, scopes []string, description string, ttl *time.Duration) (*APIKey, error) {
	// 生成 Key ID
	keyID, err := s.generateRandomString(s.config.KeyLength)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key ID: %w", err)
	}

	// 生成 Secret
	secret, err := s.generateRandomString(s.config.SecretLength)
	if err != nil {
		return nil, fmt.Errorf("failed to generate secret: %w", err)
	}

	// 计算过期时间
	var expiresAt *time.Time
	if ttl != nil {
		if *ttl > s.config.MaxTTL {
			return nil, fmt.Errorf("TTL exceeds maximum allowed duration")
		}
		exp := time.Now().Add(*ttl)
		expiresAt = &exp
	} else {
		exp := time.Now().Add(s.config.DefaultTTL)
		expiresAt = &exp
	}

	// 创建 API Key
	apiKey := &APIKey{
		ID:          fmt.Sprintf("ak_%s", keyID),
		KeyID:       keyID,
		Secret:      secret,
		UserID:      userID,
		Scopes:      scopes,
		Description: description,
		ExpiresAt:   expiresAt,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		IsActive:    true,
	}

	return apiKey, nil
}

// CreateAPIKey 创建 API Key
func (s *APIKeyService) CreateAPIKey(ctx context.Context, userID string, scopes []string, description string, ttl *time.Duration) (*APIKey, error) {
	apiKey, err := s.GenerateAPIKey(userID, scopes, description, ttl)
	if err != nil {
		return nil, err
	}

	if err := s.repo.Create(ctx, apiKey); err != nil {
		return nil, fmt.Errorf("failed to create API key: %w", err)
	}

	return apiKey, nil
}

// ValidateAPIKey 验证 API Key
func (s *APIKeyService) ValidateAPIKey(ctx context.Context, keyString string) (*APIKey, error) {
	// 解析 Key String
	keyID, secret, err := s.parseKeyString(keyString)
	if err != nil {
		return nil, protocol.NewAuthenticationFailedError("Invalid API key format")
	}

	// 获取 API Key
	apiKey, err := s.repo.GetByKeyID(ctx, keyID)
	if err != nil {
		return nil, protocol.NewAuthenticationFailedError("API key not found")
	}

	// 检查是否激活
	if !apiKey.IsActive {
		return nil, protocol.NewAuthenticationFailedError("API key is inactive")
	}

	// 检查是否过期
	if apiKey.ExpiresAt != nil && apiKey.ExpiresAt.Before(time.Now()) {
		return nil, protocol.NewAuthenticationFailedError("API key has expired")
	}

	// 验证 Secret
	if !s.verifySecret(apiKey.Secret, secret) {
		return nil, protocol.NewAuthenticationFailedError("Invalid API key secret")
	}

	// 更新最后使用时间
	now := time.Now()
	apiKey.LastUsedAt = &now
	apiKey.UpdatedAt = now
	if err := s.repo.Update(ctx, apiKey); err != nil {
		// 记录错误但不影响验证结果
		fmt.Printf("Failed to update API key last used time: %v\n", err)
	}

	return apiKey, nil
}

// RevokeAPIKey 撤销 API Key
func (s *APIKeyService) RevokeAPIKey(ctx context.Context, id string) error {
	apiKey, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("API key not found: %w", err)
	}

	apiKey.IsActive = false
	apiKey.UpdatedAt = time.Now()

	return s.repo.Update(ctx, apiKey)
}

// ListUserAPIKeys 列出用户的 API Keys
func (s *APIKeyService) ListUserAPIKeys(ctx context.Context, userID string) ([]*APIKey, error) {
	return s.repo.ListByUserID(ctx, userID)
}

// GetAPIKey 获取 API Key 详情
func (s *APIKeyService) GetAPIKey(ctx context.Context, id string) (*APIKey, error) {
	return s.repo.GetByID(ctx, id)
}

// UpdateAPIKey 更新 API Key
func (s *APIKeyService) UpdateAPIKey(ctx context.Context, apiKey *APIKey) error {
	apiKey.UpdatedAt = time.Now()
	return s.repo.Update(ctx, apiKey)
}

// parseKeyString 解析 Key String
func (s *APIKeyService) parseKeyString(keyString string) (string, string, error) {
	// 根据配置的格式解析
	switch s.config.Format {
	case "key_id:key_secret":
		parts := strings.Split(keyString, ":")
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid key format")
		}
		return parts[0], parts[1], nil
	case "key_id.key_secret":
		parts := strings.Split(keyString, ".")
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid key format")
		}
		return parts[0], parts[1], nil
	default:
		return "", "", fmt.Errorf("unsupported key format: %s", s.config.Format)
	}
}

// verifySecret 验证 Secret
func (s *APIKeyService) verifySecret(storedSecret, providedSecret string) bool {
	// 使用 SHA256 哈希比较
	storedHash := sha256.Sum256([]byte(storedSecret))
	providedHash := sha256.Sum256([]byte(providedSecret))

	return storedHash == providedHash
}

// generateRandomString 生成随机字符串
func (s *APIKeyService) generateRandomString(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// HasScope 检查 API Key 是否有指定权限
func (apiKey *APIKey) HasScope(scope string) bool {
	for _, s := range apiKey.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// IsExpired 检查 API Key 是否过期
func (apiKey *APIKey) IsExpired() bool {
	if apiKey.ExpiresAt == nil {
		return false
	}
	return apiKey.ExpiresAt.Before(time.Now())
}

// ToPublicInfo 转换为公开信息（隐藏敏感数据）
func (apiKey *APIKey) ToPublicInfo() map[string]interface{} {
	return map[string]interface{}{
		"id":           apiKey.ID,
		"key_id":       apiKey.KeyID,
		"user_id":      apiKey.UserID,
		"scopes":       apiKey.Scopes,
		"description":  apiKey.Description,
		"expires_at":   apiKey.ExpiresAt,
		"created_at":   apiKey.CreatedAt,
		"updated_at":   apiKey.UpdatedAt,
		"last_used_at": apiKey.LastUsedAt,
		"is_active":    apiKey.IsActive,
		"metadata":     apiKey.Metadata,
	}
}
