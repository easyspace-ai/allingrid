package application

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"

	"gorm.io/gorm"
)

// UserConfigService 用户配置服务
type UserConfigService struct {
	repo repository.UserConfigRepository
}

// NewUserConfigService 创建用户配置服务
func NewUserConfigService(repo repository.UserConfigRepository) *UserConfigService {
	return &UserConfigService{
		repo: repo,
	}
}

// GetUserConfig 获取用户配置（如果不存在则创建默认配置）
func (s *UserConfigService) GetUserConfig(ctx context.Context, userID string) (*dto.UserConfigResponse, error) {
	config, err := s.repo.GetByUserID(ctx, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 创建默认配置
			config, err = entity.NewUserConfig(userID)
			if err != nil {
				return nil, errors.ErrInternalServer.WithDetails(err.Error())
			}

			if err := s.repo.Create(ctx, config); err != nil {
				return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
			}
		} else {
			return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	}

	return s.toDTO(config), nil
}

// UpdateUserConfig 更新用户配置
func (s *UserConfigService) UpdateUserConfig(ctx context.Context, userID string, req dto.UpdateUserConfigRequest) (*dto.UserConfigResponse, error) {
	// 获取现有配置
	config, err := s.repo.GetByUserID(ctx, userID)
	isNew := false

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 如果不存在，创建新配置
			config, err = entity.NewUserConfig(userID)
			if err != nil {
				return nil, errors.ErrInternalServer.WithDetails(err.Error())
			}
			isNew = true
		} else {
			return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	}

	// 更新配置
	if err := config.Update(req.Timezone, req.Language, req.DateFormat, req.TimeFormat); err != nil {
		return nil, errors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 保存配置：如果是新创建的则Create，否则Update
	if isNew {
		if err := s.repo.Create(ctx, config); err != nil {
			return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	} else {
		if err := s.repo.Update(ctx, config); err != nil {
			return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	}

	return s.toDTO(config), nil
}

// toDTO 转换实体到DTO
func (s *UserConfigService) toDTO(config *entity.UserConfig) *dto.UserConfigResponse {
	return &dto.UserConfigResponse{
		ID:         config.ID(),
		UserID:     config.UserID(),
		Timezone:   config.Timezone(),
		Language:   config.Language(),
		DateFormat: config.DateFormat(),
		TimeFormat: config.TimeFormat(),
		UpdatedAt:  config.UpdatedAt().Format("2006-01-02T15:04:05Z07:00"),
	}
}
