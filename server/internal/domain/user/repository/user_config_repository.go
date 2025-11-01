package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
)

// UserConfigRepository 用户配置仓储接口
type UserConfigRepository interface {
	// Create 创建用户配置
	Create(ctx context.Context, config *entity.UserConfig) error

	// GetByUserID 根据用户ID获取配置
	GetByUserID(ctx context.Context, userID string) (*entity.UserConfig, error)

	// Update 更新用户配置
	Update(ctx context.Context, config *entity.UserConfig) error

	// Delete 删除用户配置
	Delete(ctx context.Context, userID string) error

	// Exists 检查用户配置是否存在
	Exists(ctx context.Context, userID string) (bool, error)
}
