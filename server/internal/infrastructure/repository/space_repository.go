package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space/aggregate"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/repository"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository/mapper"
)

// SpaceRepositoryImpl 空间仓储实现
type SpaceRepositoryImpl struct {
	db *gorm.DB
}

// NewSpaceRepository 创建空间仓储
func NewSpaceRepository(db *gorm.DB) repository.SpaceRepository {
	return &SpaceRepositoryImpl{db: db}
}

// Save 保存空间聚合根
func (r *SpaceRepositoryImpl) Save(ctx context.Context, spaceAgg *aggregate.SpaceAggregate) error {
	// 获取空间实体
	space := spaceAgg.Space()
	dbSpace := mapper.ToSpaceModel(space)

	// 检查是否已存在
	var existing models.Space
	err := r.db.WithContext(ctx).Where("id = ?", dbSpace.ID).First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// 创建新空间
		return r.db.WithContext(ctx).Create(dbSpace).Error
	} else if err != nil {
		return fmt.Errorf("failed to check existing space: %w", err)
	}

	// 更新现有空间
	return r.db.WithContext(ctx).Model(&models.Space{}).
		Where("id = ?", dbSpace.ID).
		Updates(dbSpace).Error
}

// GetByID 根据ID获取空间聚合根
func (r *SpaceRepositoryImpl) GetByID(ctx context.Context, id string) (*aggregate.SpaceAggregate, error) {
	space, err := r.GetSpaceByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if space == nil {
		return nil, nil
	}

	return aggregate.NewSpaceAggregate(space), nil
}

// GetSpaceByID 根据ID获取空间实体
func (r *SpaceRepositoryImpl) GetSpaceByID(ctx context.Context, id string) (*entity.Space, error) {
	var dbSpace models.Space

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("space").
		Where("id = ?", id).
		Where("deleted_time IS NULL").
		First(&dbSpace).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find space: %w", err)
	}

	return mapper.ToSpaceEntity(&dbSpace)
}

// ListByUserID 获取用户的空间列表
func (r *SpaceRepositoryImpl) ListByUserID(ctx context.Context, userID string) ([]*entity.Space, error) {
	var dbSpaces []*models.Space

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("space").
		Where("created_by = ?", userID).
		Where("deleted_time IS NULL").
		Order("created_time DESC").
		Find(&dbSpaces).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find spaces by user: %w", err)
	}

	return mapper.ToSpaceList(dbSpaces)
}

// Delete 删除空间（软删除）
func (r *SpaceRepositoryImpl) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).
		Model(&models.Space{}).
		Where("id = ?", id).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// Update 更新空间
func (r *SpaceRepositoryImpl) Update(ctx context.Context, space *entity.Space) error {
	dbSpace := mapper.ToSpaceModel(space)

	return r.db.WithContext(ctx).Model(&models.Space{}).
		Where("id = ?", dbSpace.ID).
		Updates(dbSpace).Error
}
