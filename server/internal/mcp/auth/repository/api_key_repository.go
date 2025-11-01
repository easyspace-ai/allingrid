package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/auth"
	"gorm.io/gorm"
)

// APIKeyModel API Key 数据库模型
type APIKeyModel struct {
	ID          string       `gorm:"primaryKey;type:varchar(255)" json:"id"`
	KeyID       string       `gorm:"uniqueIndex;type:varchar(255);not null" json:"key_id"`
	Secret      string       `gorm:"type:varchar(255);not null" json:"secret"`
	UserID      string       `gorm:"type:varchar(255);index" json:"user_id"`
	Scopes      ScopesArray  `gorm:"type:json" json:"scopes"`
	Description string       `gorm:"type:text" json:"description"`
	ExpiresAt   *time.Time   `gorm:"type:timestamp" json:"expires_at"`
	CreatedAt   time.Time    `gorm:"type:timestamp;not null" json:"created_at"`
	UpdatedAt   time.Time    `gorm:"type:timestamp;not null" json:"updated_at"`
	LastUsedAt  *time.Time   `gorm:"type:timestamp" json:"last_used_at"`
	IsActive    bool         `gorm:"type:boolean;default:true" json:"is_active"`
	Metadata    MetadataJSON `gorm:"type:json" json:"metadata"`
}

// TableName 指定表名
func (APIKeyModel) TableName() string {
	return "mcp_api_keys"
}

// ScopesArray 权限数组类型
type ScopesArray []string

// Scan 实现 sql.Scanner 接口
func (s *ScopesArray) Scan(value interface{}) error {
	if value == nil {
		*s = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, s)
	case string:
		return json.Unmarshal([]byte(v), s)
	default:
		return fmt.Errorf("cannot scan %T into ScopesArray", value)
	}
}

// Value 实现 driver.Valuer 接口
func (s ScopesArray) Value() (driver.Value, error) {
	if s == nil {
		return nil, nil
	}
	return json.Marshal(s)
}

// MetadataJSON 元数据 JSON 类型
type MetadataJSON map[string]interface{}

// Scan 实现 sql.Scanner 接口
func (m *MetadataJSON) Scan(value interface{}) error {
	if value == nil {
		*m = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, m)
	case string:
		return json.Unmarshal([]byte(v), m)
	default:
		return fmt.Errorf("cannot scan %T into MetadataJSON", value)
	}
}

// Value 实现 driver.Valuer 接口
func (m MetadataJSON) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	return json.Marshal(m)
}

// ToAPIKey 转换为 APIKey 实体
func (m *APIKeyModel) ToAPIKey() *auth.APIKey {
	return &auth.APIKey{
		ID:          m.ID,
		KeyID:       m.KeyID,
		Secret:      m.Secret,
		UserID:      m.UserID,
		Scopes:      []string(m.Scopes),
		Description: m.Description,
		ExpiresAt:   m.ExpiresAt,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
		LastUsedAt:  m.LastUsedAt,
		IsActive:    m.IsActive,
		Metadata:    map[string]interface{}(m.Metadata),
	}
}

// FromAPIKey 从 APIKey 实体创建模型
func (m *APIKeyModel) FromAPIKey(apiKey *auth.APIKey) {
	m.ID = apiKey.ID
	m.KeyID = apiKey.KeyID
	m.Secret = apiKey.Secret
	m.UserID = apiKey.UserID
	m.Scopes = ScopesArray(apiKey.Scopes)
	m.Description = apiKey.Description
	m.ExpiresAt = apiKey.ExpiresAt
	m.CreatedAt = apiKey.CreatedAt
	m.UpdatedAt = apiKey.UpdatedAt
	m.LastUsedAt = apiKey.LastUsedAt
	m.IsActive = apiKey.IsActive
	m.Metadata = MetadataJSON(apiKey.Metadata)
}

// APIKeyRepositoryImpl API Key 仓储实现
type APIKeyRepositoryImpl struct {
	db *gorm.DB
}

// NewAPIKeyRepository 创建新的 API Key 仓储
func NewAPIKeyRepository(db *gorm.DB) auth.APIKeyRepository {
	return &APIKeyRepositoryImpl{
		db: db,
	}
}

// Create 创建 API Key
func (r *APIKeyRepositoryImpl) Create(ctx context.Context, apiKey *auth.APIKey) error {
	model := &APIKeyModel{}
	model.FromAPIKey(apiKey)

	return r.db.WithContext(ctx).Create(model).Error
}

// GetByID 根据 ID 获取 API Key
func (r *APIKeyRepositoryImpl) GetByID(ctx context.Context, id string) (*auth.APIKey, error) {
	var model APIKeyModel
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&model).Error
	if err != nil {
		return nil, err
	}

	return model.ToAPIKey(), nil
}

// GetByKeyID 根据 Key ID 获取 API Key
func (r *APIKeyRepositoryImpl) GetByKeyID(ctx context.Context, keyID string) (*auth.APIKey, error) {
	var model APIKeyModel
	err := r.db.WithContext(ctx).Where("key_id = ?", keyID).First(&model).Error
	if err != nil {
		return nil, err
	}

	return model.ToAPIKey(), nil
}

// Update 更新 API Key
func (r *APIKeyRepositoryImpl) Update(ctx context.Context, apiKey *auth.APIKey) error {
	model := &APIKeyModel{}
	model.FromAPIKey(apiKey)

	return r.db.WithContext(ctx).Save(model).Error
}

// Delete 删除 API Key
func (r *APIKeyRepositoryImpl) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&APIKeyModel{}).Error
}

// ListByUserID 根据用户 ID 列出 API Keys
func (r *APIKeyRepositoryImpl) ListByUserID(ctx context.Context, userID string) ([]*auth.APIKey, error) {
	var models []APIKeyModel
	err := r.db.WithContext(ctx).Where("user_id = ?", userID).Order("created_at DESC").Find(&models).Error
	if err != nil {
		return nil, err
	}

	apiKeys := make([]*auth.APIKey, len(models))
	for i, model := range models {
		apiKeys[i] = model.ToAPIKey()
	}

	return apiKeys, nil
}

// ListActive 列出所有激活的 API Keys
func (r *APIKeyRepositoryImpl) ListActive(ctx context.Context) ([]*auth.APIKey, error) {
	var models []APIKeyModel
	err := r.db.WithContext(ctx).Where("is_active = ?", true).Order("created_at DESC").Find(&models).Error
	if err != nil {
		return nil, err
	}

	apiKeys := make([]*auth.APIKey, len(models))
	for i, model := range models {
		apiKeys[i] = model.ToAPIKey()
	}

	return apiKeys, nil
}

// Migrate 执行数据库迁移
func (r *APIKeyRepositoryImpl) Migrate() error {
	return r.db.AutoMigrate(&APIKeyModel{})
}
