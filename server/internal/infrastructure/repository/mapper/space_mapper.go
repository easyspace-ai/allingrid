package mapper

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
)

// ToSpaceEntity 将数据库模型转换为领域实体
func ToSpaceEntity(dbSpace *models.Space) (*entity.Space, error) {
	if dbSpace == nil {
		return nil, nil
	}

	// 创建 SpaceID
	spaceID := valueobject.NewSpaceID(dbSpace.ID)

	// 创建 SpaceName
	spaceName, err := valueobject.NewSpaceName(dbSpace.Name)
	if err != nil {
		return nil, err
	}

	// 处理 LastModifiedTime
	updatedAt := dbSpace.CreatedTime
	if dbSpace.LastModifiedTime != nil {
		updatedAt = *dbSpace.LastModifiedTime
	}

	// 处理 DeletedTime
	var deletedAt *time.Time
	if dbSpace.DeletedTime.Valid {
		deletedAt = &dbSpace.DeletedTime.Time
	}

	// 处理 LastModifiedBy
	lastModifiedBy := dbSpace.CreatedBy
	if dbSpace.LastModifiedBy != nil {
		lastModifiedBy = *dbSpace.LastModifiedBy
	}

	// 重建实体
	space := entity.ReconstructSpace(
		spaceID,
		spaceName,
		dbSpace.Description,
		dbSpace.Icon,
		dbSpace.CreatedBy,
		lastModifiedBy,
		dbSpace.CreatedTime,
		updatedAt,
		deletedAt,
		1, // version - 数据库模型中没有版本字段
	)

	return space, nil
}

// ToSpaceModel 将领域实体转换为数据库模型
func ToSpaceModel(space *entity.Space) *models.Space {
	if space == nil {
		return nil
	}

	updatedAt := space.UpdatedAt()

	return &models.Space{
		ID:               space.ID().String(),
		Name:             space.Name().String(),
		Description:      space.Description(),
		Icon:             space.Icon(),
		CreatedBy:        space.CreatedBy(),
		CreatedTime:      space.CreatedAt(),
		LastModifiedTime: &updatedAt,
	}
}

// ToSpaceList 批量转换
func ToSpaceList(dbSpaces []*models.Space) ([]*entity.Space, error) {
	spaces := make([]*entity.Space, 0, len(dbSpaces))
	for _, dbSpace := range dbSpaces {
		space, err := ToSpaceEntity(dbSpace)
		if err != nil {
			return nil, err
		}
		if space != nil {
			spaces = append(spaces, space)
		}
	}
	return spaces, nil
}
