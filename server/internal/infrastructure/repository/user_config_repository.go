package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/repository"

	"gorm.io/gorm"
)

// UserConfigModel 用户配置GORM模型
type UserConfigModel struct {
	ID         string `gorm:"column:id;primaryKey"`
	UserID     string `gorm:"column:user_id;not null;uniqueIndex"`
	Timezone   string `gorm:"column:timezone;default:'UTC'"`
	Language   string `gorm:"column:language;default:'en-US'"`
	DateFormat string `gorm:"column:date_format;default:'YYYY-MM-DD'"`
	TimeFormat string `gorm:"column:time_format;default:'24h'"`
	CreatedAt  int64  `gorm:"column:created_at;<-:create"`
	UpdatedAt  int64  `gorm:"column:updated_at"`
	DeletedAt  *int64 `gorm:"column:deleted_at;index"`
}

// TableName 指定表名
func (UserConfigModel) TableName() string {
	return "user_configs"
}

// userConfigRepositoryImpl UserConfig仓储实现
type userConfigRepositoryImpl struct {
	db *gorm.DB
}

// NewGormUserConfigRepository 创建GORM实现的UserConfig仓储
func NewGormUserConfigRepository(db *gorm.DB) repository.UserConfigRepository {
	return &userConfigRepositoryImpl{
		db: db,
	}
}

// Create 创建用户配置
func (r *userConfigRepositoryImpl) Create(ctx context.Context, config *entity.UserConfig) error {
	model := &UserConfigModel{
		ID:         config.ID(),
		UserID:     config.UserID(),
		Timezone:   config.Timezone(),
		Language:   config.Language(),
		DateFormat: config.DateFormat(),
		TimeFormat: config.TimeFormat(),
		CreatedAt:  config.CreatedAt().Unix(),
		UpdatedAt:  config.UpdatedAt().Unix(),
	}

	return r.db.WithContext(ctx).Create(model).Error
}

// GetByUserID 根据用户ID获取配置
func (r *userConfigRepositoryImpl) GetByUserID(ctx context.Context, userID string) (*entity.UserConfig, error) {
	var model UserConfigModel
	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		First(&model).Error

	if err != nil {
		return nil, err
	}

	return r.toEntity(&model)
}

// Update 更新用户配置
func (r *userConfigRepositoryImpl) Update(ctx context.Context, config *entity.UserConfig) error {
	updates := map[string]interface{}{
		"timezone":    config.Timezone(),
		"language":    config.Language(),
		"date_format": config.DateFormat(),
		"time_format": config.TimeFormat(),
		"updated_at":  config.UpdatedAt().Unix(),
	}

	return r.db.WithContext(ctx).
		Model(&UserConfigModel{}).
		Where("user_id = ?", config.UserID()).
		Updates(updates).Error
}

// Delete 删除用户配置
func (r *userConfigRepositoryImpl) Delete(ctx context.Context, userID string) error {
	return r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Delete(&UserConfigModel{}).Error
}

// Exists 检查用户配置是否存在
func (r *userConfigRepositoryImpl) Exists(ctx context.Context, userID string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&UserConfigModel{}).
		Where("user_id = ?", userID).
		Count(&count).Error

	return count > 0, err
}

// toEntity 将模型转换为实体
func (r *userConfigRepositoryImpl) toEntity(model *UserConfigModel) (*entity.UserConfig, error) {
	// 使用反射或重新构造实体
	// 这里简化处理，直接创建新实体并设置字段
	config, err := entity.NewUserConfig(model.UserID)
	if err != nil {
		return nil, err
	}

	// 通过Update方法设置配置
	err = config.Update(
		model.Timezone,
		model.Language,
		model.DateFormat,
		model.TimeFormat,
	)
	if err != nil {
		return nil, err
	}

	return config, nil
}
