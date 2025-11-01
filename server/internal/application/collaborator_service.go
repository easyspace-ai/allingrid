package application

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// CollaboratorService 协作者服务
type CollaboratorService struct {
	repo repository.CollaboratorRepository
}

// NewCollaboratorService 创建协作者服务
func NewCollaboratorService(repo repository.CollaboratorRepository) *CollaboratorService {
	return &CollaboratorService{
		repo: repo,
	}
}

// AddCollaborator 添加协作者
func (s *CollaboratorService) AddCollaborator(
	ctx context.Context,
	resourceID string,
	resourceType entity.ResourceType,
	req dto.AddCollaboratorRequest,
	createdBy string,
) (*dto.CollaboratorResponse, error) {
	// 检查是否已存在
	existing, err := s.repo.FindByResourceAndPrincipal(ctx, resourceID, req.PrincipalID)
	if err == nil && existing != nil {
		return nil, errors.ErrConflict.WithDetails("协作者已存在")
	}

	// 创建协作者
	collaborator, err := entity.NewCollaborator(
		resourceID,
		resourceType,
		req.PrincipalID,
		entity.PrincipalType(req.PrincipalType),
		entity.RoleName(req.Role),
		createdBy,
	)
	if err != nil {
		return nil, errors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 保存
	if err := s.repo.Create(ctx, collaborator); err != nil {
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	return s.toDTO(collaborator), nil
}

// ListCollaborators 列出协作者
func (s *CollaboratorService) ListCollaborators(
	ctx context.Context,
	resourceID string,
	resourceType entity.ResourceType,
) (*dto.ListCollaboratorsResponse, error) {
	collaborators, err := s.repo.ListByResource(ctx, resourceID, resourceType)
	if err != nil {
		return nil, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	// 转换为DTO
	dtos := make([]dto.CollaboratorResponse, 0, len(collaborators))
	for _, c := range collaborators {
		dtos = append(dtos, *s.toDTO(c))
	}

	return &dto.ListCollaboratorsResponse{
		Collaborators: dtos,
		Total:         len(dtos),
	}, nil
}

// UpdateCollaborator 更新协作者角色
func (s *CollaboratorService) UpdateCollaborator(
	ctx context.Context,
	collaboratorID string,
	req dto.UpdateCollaboratorRequest,
) (*dto.CollaboratorResponse, error) {
	// 获取协作者
	collaborator, err := s.repo.GetByID(ctx, collaboratorID)
	if err != nil {
		return nil, errors.ErrNotFound.WithDetails("协作者不存在")
	}

	// 更新角色
	if err := collaborator.UpdateRole(entity.RoleName(req.Role)); err != nil {
		return nil, errors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 保存
	if err := s.repo.Update(ctx, collaborator); err != nil {
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	return s.toDTO(collaborator), nil
}

// RemoveCollaborator 移除协作者
func (s *CollaboratorService) RemoveCollaborator(ctx context.Context, collaboratorID string) error {
	if err := s.repo.Delete(ctx, collaboratorID); err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	return nil
}

// toDTO 转换实体到DTO
func (s *CollaboratorService) toDTO(collaborator *entity.Collaborator) *dto.CollaboratorResponse {
	return &dto.CollaboratorResponse{
		ID:            collaborator.ID(),
		ResourceID:    collaborator.ResourceID(),
		ResourceType:  string(collaborator.ResourceType()),
		PrincipalID:   collaborator.PrincipalID(),
		PrincipalType: string(collaborator.PrincipalType()),
		Role:          string(collaborator.Role()),
		CreatedBy:     collaborator.CreatedBy(),
		CreatedAt:     collaborator.CreatedAt().Format(time.RFC3339),
		UpdatedAt:     collaborator.UpdatedAt().Format(time.RFC3339),
	}
}
