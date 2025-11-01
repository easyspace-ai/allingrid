package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/aggregate"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/valueobject"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// SpaceService 空间应用服务
type SpaceService struct {
	spaceRepo repository.SpaceRepository
}

// NewSpaceService 创建空间服务
func NewSpaceService(spaceRepo repository.SpaceRepository) *SpaceService {
	return &SpaceService{
		spaceRepo: spaceRepo,
	}
}

// CreateSpace 创建空间
func (s *SpaceService) CreateSpace(ctx context.Context, req dto.CreateSpaceRequest, userID string) (*dto.SpaceResponse, error) {
	// 1. 验证名称
	spaceName, err := valueobject.NewSpaceName(req.Name)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("空间名称无效: %v", err))
	}

	// 2. 创建空间实体
	space, err := entity.NewSpace(spaceName, userID)
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("创建空间实体失败: %v", err))
	}

	// 3. 设置可选属性
	if req.Description != "" {
		space.UpdateDescription(req.Description)
	}

	// 4. 创建聚合根并保存
	spaceAgg := aggregate.NewSpaceAggregate(space)
	if err := s.spaceRepo.Save(ctx, spaceAgg); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存空间失败: %v", err))
	}

	logger.Info("空间创建成功",
		logger.String("space_id", space.ID().String()),
		logger.String("name", spaceName.String()),
		logger.String("created_by", userID),
	)

	return dto.FromSpaceEntity(space), nil
}

// GetSpace 获取空间详情
func (s *SpaceService) GetSpace(ctx context.Context, spaceID string) (*dto.SpaceResponse, error) {
	space, err := s.spaceRepo.GetSpaceByID(ctx, spaceID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找空间失败: %v", err))
	}
	if space == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("空间不存在")
	}

	return dto.FromSpaceEntity(space), nil
}

// UpdateSpace 更新空间
func (s *SpaceService) UpdateSpace(ctx context.Context, spaceID string, req dto.UpdateSpaceRequest) (*dto.SpaceResponse, error) {
	// 1. 查找空间
	space, err := s.spaceRepo.GetSpaceByID(ctx, spaceID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找空间失败: %v", err))
	}
	if space == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("空间不存在")
	}

	// 2. 更新名称
	if req.Name != nil && *req.Name != "" {
		spaceName, err := valueobject.NewSpaceName(*req.Name)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("空间名称无效: %v", err))
		}
		if err := space.Rename(spaceName); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("重命名失败: %v", err))
		}
	}

	// 3. 更新描述
	if req.Description != nil {
		space.UpdateDescription(*req.Description)
	}

	// 4. 更新图标
	if req.Icon != nil {
		space.UpdateIcon(*req.Icon)
	}

	// 5. 保存
	if err := s.spaceRepo.Update(ctx, space); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存空间失败: %v", err))
	}

	logger.Info("空间更新成功",
		logger.String("space_id", spaceID),
	)

	return dto.FromSpaceEntity(space), nil
}

// DeleteSpace 删除空间
func (s *SpaceService) DeleteSpace(ctx context.Context, spaceID string) error {
	// 1. 检查空间是否存在
	space, err := s.spaceRepo.GetSpaceByID(ctx, spaceID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找空间失败: %v", err))
	}
	if space == nil {
		return pkgerrors.ErrSpaceNotFound.WithDetails(fmt.Sprintf("空间不存在: %s", spaceID))
	}

	// 2. 删除空间
	if err := s.spaceRepo.Delete(ctx, spaceID); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("删除空间失败: %v", err))
	}

	logger.Info("空间删除成功",
		logger.String("space_id", spaceID),
	)

	return nil
}

// ListSpaces 列出空间
func (s *SpaceService) ListSpaces(ctx context.Context, userID string) ([]*dto.SpaceResponse, error) {
	spaces, err := s.spaceRepo.ListByUserID(ctx, userID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查询用户空间失败: %v", err))
	}

	// 转换为 DTO
	spaceList := make([]*dto.SpaceResponse, 0, len(spaces))
	for _, space := range spaces {
		spaceList = append(spaceList, dto.FromSpaceEntity(space))
	}

	return spaceList, nil
}
