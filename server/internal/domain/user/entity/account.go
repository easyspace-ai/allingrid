package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"

	"github.com/google/uuid"
)

// Account 第三方账户实体
type Account struct {
	id          string
	userID      valueobject.UserID
	accountType string
	provider    string
	providerID  string
	createdAt   time.Time
}

// NewAccount 创建第三方账户
func NewAccount(
	userID valueobject.UserID,
	accountType string,
	provider string,
	providerID string,
) (*Account, error) {
	// 验证
	if err := validateProvider(provider); err != nil {
		return nil, err
	}

	if providerID == "" {
		return nil, user.NewDomainError(
			"EMPTY_PROVIDER_ID",
			"provider ID cannot be empty",
			nil,
		)
	}

	return &Account{
		id:          uuid.New().String(),
		userID:      userID,
		accountType: accountType,
		provider:    provider,
		providerID:  providerID,
		createdAt:   time.Now(),
	}, nil
}

// ==================== 访问器方法 ====================

func (a *Account) ID() string                 { return a.id }
func (a *Account) UserID() valueobject.UserID { return a.userID }
func (a *Account) AccountType() string        { return a.accountType }
func (a *Account) Provider() string           { return a.provider }
func (a *Account) ProviderID() string         { return a.providerID }
func (a *Account) CreatedAt() time.Time       { return a.createdAt }

// IsOAuth 是否为OAuth账户
func (a *Account) IsOAuth() bool {
	return a.accountType == "oauth"
}

// IsLocal 是否为本地账户
func (a *Account) IsLocal() bool {
	return a.accountType == "local"
}

// validateProvider 验证提供商
func validateProvider(provider string) error {
	validProviders := map[string]bool{
		"local":  true,
		"github": true,
		"google": true,
		"oidc":   true,
	}

	if !validProviders[provider] {
		return user.ErrInvalidProvider
	}

	return nil
}
