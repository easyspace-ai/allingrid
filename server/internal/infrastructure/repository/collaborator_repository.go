package repository

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"

	"gorm.io/gorm"
)

// CollaboratorModel 协作者GORM模型
type CollaboratorModel struct {
	ID               string     `gorm:"column:id;primaryKey"`
	ResourceID       string     `gorm:"column:resource_id;not null;index:idx_resource"`
	ResourceType     string     `gorm:"column:resource_type;not null;index:idx_resource"`
	PrincipalID      string     `gorm:"column:principal_id;not null;index:idx_principal"`
	PrincipalType    string     `gorm:"column:principal_type;not null;index:idx_principal"`
	RoleName         string     `gorm:"column:role_name;not null"`
	CreatedBy        string     `gorm:"column:created_by;not null"`
	CreatedTime      time.Time  `gorm:"column:created_time;<-:create"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time"`
	LastModifiedBy   *string    `gorm:"column:last_modified_by"`
}

// TableName 指定表名
func (CollaboratorModel) TableName() string {
	return "collaborators"
}

// collaboratorRepositoryImpl 协作者仓储实现
type collaboratorRepositoryImpl struct {
	db *gorm.DB
}

// NewCollaboratorRepository 创建协作者仓储
func NewCollaboratorRepository(db *gorm.DB) repository.CollaboratorRepository {
	return &collaboratorRepositoryImpl{
		db: db,
	}
}

// Create 创建协作者
func (r *collaboratorRepositoryImpl) Create(ctx context.Context, collaborator *entity.Collaborator) error {
	model := &CollaboratorModel{
		ID:               collaborator.ID(),
		ResourceID:       collaborator.ResourceID(),
		ResourceType:     string(collaborator.ResourceType()),
		PrincipalID:      collaborator.PrincipalID(),
		PrincipalType:    string(collaborator.PrincipalType()),
		RoleName:         string(collaborator.Role()),
		CreatedBy:        collaborator.CreatedBy(),
		CreatedTime:      collaborator.CreatedAt(),
		LastModifiedTime: func() *time.Time { t := collaborator.UpdatedAt(); return &t }(),
	}

	if err := r.db.WithContext(ctx).Create(model).Error; err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	return nil
}

// GetByID 根据ID获取协作者
func (r *collaboratorRepositoryImpl) GetByID(ctx context.Context, id string) (*entity.Collaborator, error) {
	var model CollaboratorModel
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&model).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrNotFound.WithDetails("collaborator not found")
		}
		return nil, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	return r.toEntity(&model)
}

// ListByResource 列出资源的所有协作者
func (r *collaboratorRepositoryImpl) ListByResource(ctx context.Context, resourceID string, resourceType entity.ResourceType) ([]*entity.Collaborator, error) {
	var models []CollaboratorModel
	err := r.db.WithContext(ctx).
		Where("resource_id = ? AND resource_type = ?", resourceID, string(resourceType)).
		Find(&models).Error

	if err != nil {
		return nil, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	collaborators := make([]*entity.Collaborator, 0, len(models))
	for i := range models {
		collab, err := r.toEntity(&models[i])
		if err != nil {
			return nil, err
		}
		collaborators = append(collaborators, collab)
	}

	return collaborators, nil
}

// ListByPrincipal 列出主体的所有协作关系
func (r *collaboratorRepositoryImpl) ListByPrincipal(ctx context.Context, principalID string, principalType entity.PrincipalType) ([]*entity.Collaborator, error) {
	var models []CollaboratorModel
	err := r.db.WithContext(ctx).
		Where("principal_id = ? AND principal_type = ?", principalID, string(principalType)).
		Find(&models).Error

	if err != nil {
		return nil, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	collaborators := make([]*entity.Collaborator, 0, len(models))
	for i := range models {
		collab, err := r.toEntity(&models[i])
		if err != nil {
			return nil, err
		}
		collaborators = append(collaborators, collab)
	}

	return collaborators, nil
}

// FindByResourceAndPrincipal 查找特定资源和主体的协作关系
func (r *collaboratorRepositoryImpl) FindByResourceAndPrincipal(ctx context.Context, resourceID, principalID string) (*entity.Collaborator, error) {
	var model CollaboratorModel
	err := r.db.WithContext(ctx).
		Where("resource_id = ? AND principal_id = ?", resourceID, principalID).
		First(&model).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrNotFound.WithDetails("collaborator not found")
		}
		return nil, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	return r.toEntity(&model)
}

// Update 更新协作者
func (r *collaboratorRepositoryImpl) Update(ctx context.Context, collaborator *entity.Collaborator) error {
	updates := map[string]interface{}{
		"role_name":          string(collaborator.Role()),
		"last_modified_time": collaborator.UpdatedAt(),
	}

	err := r.db.WithContext(ctx).
		Model(&CollaboratorModel{}).
		Where("id = ?", collaborator.ID()).
		Updates(updates).Error

	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	return nil
}

// Delete 删除协作者
func (r *collaboratorRepositoryImpl) Delete(ctx context.Context, id string) error {
	err := r.db.WithContext(ctx).
		Where("id = ?", id).
		Delete(&CollaboratorModel{}).Error

	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	return nil
}

// DeleteByResourceAndPrincipal 删除特定资源和主体的协作关系
func (r *collaboratorRepositoryImpl) DeleteByResourceAndPrincipal(ctx context.Context, resourceID, principalID string) error {
	err := r.db.WithContext(ctx).
		Where("resource_id = ? AND principal_id = ?", resourceID, principalID).
		Delete(&CollaboratorModel{}).Error

	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	return nil
}

// toEntity 将模型转换为实体
func (r *collaboratorRepositoryImpl) toEntity(model *CollaboratorModel) (*entity.Collaborator, error) {
	collab, err := entity.NewCollaborator(
		model.ResourceID,
		entity.ResourceType(model.ResourceType),
		model.PrincipalID,
		entity.PrincipalType(model.PrincipalType),
		entity.RoleName(model.RoleName),
		model.CreatedBy,
	)
	if err != nil {
		return nil, errors.ErrInternalServer.WithDetails(err.Error())
	}

	return collab, nil
}
