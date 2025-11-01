package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space/aggregate"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/entity"
)

// SpaceRepository 空间仓储接口
type SpaceRepository interface {
	// Save 保存空间聚合根
	Save(ctx context.Context, spaceAgg *aggregate.SpaceAggregate) error

	// GetByID 根据ID获取空间
	GetByID(ctx context.Context, id string) (*aggregate.SpaceAggregate, error)

	// GetSpaceByID 根据ID获取空间实体
	GetSpaceByID(ctx context.Context, id string) (*entity.Space, error)

	// ListByUserID 获取用户的空间列表
	ListByUserID(ctx context.Context, userID string) ([]*entity.Space, error)

	// Delete 删除空间
	Delete(ctx context.Context, id string) error

	// Update 更新空间
	Update(ctx context.Context, space *entity.Space) error
}
