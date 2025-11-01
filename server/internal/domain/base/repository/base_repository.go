package repository

import (
	"context"
	"github.com/easyspace-ai/luckdb/server/internal/domain/base/entity"
)

// BaseRepository Base仓储接口（对齐原版）
type BaseRepository interface {
	// Create 创建Base
	Create(ctx context.Context, base *entity.Base) error

	// FindByID 根据ID查找Base
	FindByID(ctx context.Context, id string) (*entity.Base, error)

	// FindBySpaceID 根据SpaceID查找所有Base
	FindBySpaceID(ctx context.Context, spaceID string) ([]*entity.Base, error)

	// Update 更新Base
	Update(ctx context.Context, base *entity.Base) error

	// Delete 删除Base（软删除）
	Delete(ctx context.Context, id string) error

	// List 分页列表
	List(ctx context.Context, spaceID string, offset, limit int) ([]*entity.Base, int64, error)

	// Exists 检查Base是否存在
	Exists(ctx context.Context, id string) (bool, error)

	// CountBySpaceID 统计Space下的Base数量
	CountBySpaceID(ctx context.Context, spaceID string) (int64, error)
}
