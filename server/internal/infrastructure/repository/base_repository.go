package repository

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/base/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/base/repository"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"

	"gorm.io/gorm"
)

type baseRepository struct {
	db *gorm.DB
}

// NewBaseRepository 创建Base仓储（对齐原版）
func NewBaseRepository(db *gorm.DB) repository.BaseRepository {
	return &baseRepository{db: db}
}

// Create 创建Base
func (r *baseRepository) Create(ctx context.Context, base *entity.Base) error {
	model := r.toModel(base)
	return r.db.WithContext(ctx).Create(model).Error
}

// FindByID 根据ID查找Base
func (r *baseRepository) FindByID(ctx context.Context, id string) (*entity.Base, error) {
	var model models.Base
	// ✅ 显式指定 schema
	if err := r.db.WithContext(ctx).Table("base").Where("id = ?", id).First(&model).Error; err != nil {
		return nil, err
	}
	return r.toEntity(&model), nil
}

// FindBySpaceID 根据SpaceID查找所有Base
func (r *baseRepository) FindBySpaceID(ctx context.Context, spaceID string) ([]*entity.Base, error) {
	var modelList []models.Base
	// ✅ 显式指定 schema
	if err := r.db.WithContext(ctx).
		Table("base").
		Where("space_id = ?", spaceID).
		Order("created_time DESC"). // 修复：使用正确的列名 created_time
		Find(&modelList).Error; err != nil {
		return nil, err
	}

	bases := make([]*entity.Base, len(modelList))
	for i, model := range modelList {
		bases[i] = r.toEntity(&model)
	}
	return bases, nil
}

// Update 更新Base
func (r *baseRepository) Update(ctx context.Context, base *entity.Base) error {
	model := r.toModel(base)
	return r.db.WithContext(ctx).Model(&models.Base{}).
		Where("id = ?", base.ID).
		Updates(model).Error
}

// Delete 删除Base（软删除）
func (r *baseRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&models.Base{}).Error
}

// List 分页列表
func (r *baseRepository) List(ctx context.Context, spaceID string, offset, limit int) ([]*entity.Base, int64, error) {
	var modelList []models.Base
	var total int64

	// ✅ 显式指定 schema
	db := r.db.WithContext(ctx).Table("base")

	// 如果指定了spaceID，则过滤
	if spaceID != "" {
		db = db.Where("space_id = ?", spaceID)
	}

	// 统计总数
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 查询数据
	if err := db.Offset(offset).Limit(limit).
		Order("created_time DESC"). // 修复：使用正确的列名 created_time
		Find(&modelList).Error; err != nil {
		return nil, 0, err
	}

	bases := make([]*entity.Base, len(modelList))
	for i, model := range modelList {
		bases[i] = r.toEntity(&model)
	}

	return bases, total, nil
}

// Exists 检查Base是否存在
func (r *baseRepository) Exists(ctx context.Context, id string) (bool, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).Table("base").
		Where("id = ?", id).
		Count(&count).Error
	return count > 0, err
}

// CountBySpaceID 统计Space下的Base数量
func (r *baseRepository) CountBySpaceID(ctx context.Context, spaceID string) (int64, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).Table("base").
		Where("space_id = ?", spaceID).
		Count(&count).Error
	return count, err
}

// toModel 实体转模型（完美对齐）
func (r *baseRepository) toModel(base *entity.Base) *models.Base {
	// 处理可选字段
	var icon *string
	if base.Icon != "" {
		icon = &base.Icon
	}

	var desc *string // Description暂时为nil

	return &models.Base{
		ID:               base.ID,
		SpaceID:          base.SpaceID,
		Name:             base.Name,
		Description:      desc,
		Icon:             icon,
		CreatedBy:        base.CreatedBy,
		CreatedTime:      base.CreatedAt,
		DeletedTime:      gorm.DeletedAt{}, // 使用零值
		LastModifiedTime: &base.UpdatedAt,
		Order:            0, // 默认顺序
		SchemaPass:       nil,
		LastModifiedBy:   nil,
	}
}

// toEntity 模型转实体（完美对齐）
func (r *baseRepository) toEntity(model *models.Base) *entity.Base {
	// 处理可选字段
	icon := ""
	if model.Icon != nil {
		icon = *model.Icon
	}

	// 处理时间字段
	updatedAt := time.Now()
	if model.LastModifiedTime != nil {
		updatedAt = *model.LastModifiedTime
	}

	return &entity.Base{
		ID:        model.ID,
		Name:      model.Name,
		Icon:      icon,
		SpaceID:   model.SpaceID,
		CreatedBy: model.CreatedBy,
		CreatedAt: model.CreatedTime,
		UpdatedAt: updatedAt,
	}
}
