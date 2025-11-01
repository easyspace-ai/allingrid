package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"
)

// CollaboratorRepository 协作者仓储接口
type CollaboratorRepository interface {
	// Create 创建协作者
	Create(ctx context.Context, collaborator *entity.Collaborator) error

	// GetByID 根据ID获取协作者
	GetByID(ctx context.Context, id string) (*entity.Collaborator, error)

	// ListByResource 列出资源的所有协作者
	ListByResource(ctx context.Context, resourceID string, resourceType entity.ResourceType) ([]*entity.Collaborator, error)

	// ListByPrincipal 列出主体的所有协作关系
	ListByPrincipal(ctx context.Context, principalID string, principalType entity.PrincipalType) ([]*entity.Collaborator, error)

	// FindByResourceAndPrincipal 查找特定资源和主体的协作关系
	FindByResourceAndPrincipal(ctx context.Context, resourceID, principalID string) (*entity.Collaborator, error)

	// Update 更新协作者
	Update(ctx context.Context, collaborator *entity.Collaborator) error

	// Delete 删除协作者
	Delete(ctx context.Context, id string) error

	// DeleteByResourceAndPrincipal 删除特定资源和主体的协作关系
	DeleteByResourceAndPrincipal(ctx context.Context, resourceID, principalID string) error
}
