package permission

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// Service 权限服务接口
type Service interface {
	// 权限管理
	GrantPermission(ctx context.Context, userID, resourceType, resourceID string, role Role, grantedBy string) error
	RevokePermission(ctx context.Context, userID, resourceType, resourceID string) error
	UpdatePermission(ctx context.Context, userID, resourceType, resourceID string, role Role, updatedBy string) error
	GetUserPermissions(ctx context.Context, userID string) ([]*Permission, error)
	GetResourcePermissions(ctx context.Context, resourceType, resourceID string) ([]*Permission, error)

	// 空间协作者管理
	AddSpaceCollaborator(ctx context.Context, spaceID, userID string, role Role, invitedBy string, email *string) error
	RemoveSpaceCollaborator(ctx context.Context, spaceID, userID string) error
	UpdateSpaceCollaboratorRole(ctx context.Context, spaceID, userID string, role Role, updatedBy string) error
	GetSpaceCollaborators(ctx context.Context, spaceID string) ([]*SpaceCollaborator, error)
	GetUserSpaceRole(ctx context.Context, userID, spaceID string) (Role, error)

	// 基础表协作者管理
	AddBaseCollaborator(ctx context.Context, baseID, userID string, role Role, invitedBy string, email *string) error
	RemoveBaseCollaborator(ctx context.Context, baseID, userID string) error
	UpdateBaseCollaboratorRole(ctx context.Context, baseID, userID string, role Role, updatedBy string) error
	GetBaseCollaborators(ctx context.Context, baseID string) ([]*BaseCollaborator, error)
	GetUserBaseRole(ctx context.Context, userID, baseID string) (Role, error)

	// 权限检查
	CheckPermission(ctx context.Context, userID, resourceType, resourceID string, action Action) (bool, error)
	CheckMultiplePermissions(ctx context.Context, userID, resourceType, resourceID string, actions []Action) (bool, error)
	GetUserEffectiveRole(ctx context.Context, userID, resourceType, resourceID string) (Role, error)
	GetUserEffectivePermissions(ctx context.Context, userID, resourceType, resourceID string) ([]Action, error)

	// 批量操作
	GetUserResources(ctx context.Context, userID, resourceType string) ([]string, error)
	GetResourceCollaborators(ctx context.Context, resourceType, resourceID string) ([]*CollaboratorInfo, error)
	TransferOwnership(ctx context.Context, resourceType, resourceID, fromUserID, toUserID string) error

	// 统计
	GetPermissionStats(ctx context.Context) (*PermissionStats, error)
}

// service 权限服务实现
type service struct {
	repo   Repository
	logger *zap.Logger
}

// NewService 创建权限服务
func NewService(repo Repository, logger *zap.Logger) Service {
	return &service{
		repo:   repo,
		logger: logger,
	}
}

// GrantPermission 授予权限
func (s *service) GrantPermission(ctx context.Context, userID, resourceType, resourceID string, role Role, grantedBy string) error {
	// 检查是否已有权限
	existingPermission, err := s.repo.GetUserPermission(ctx, userID, resourceType, resourceID)
	if err != nil && !errors.Is(err, ErrPermissionNotFound) {
		return fmt.Errorf("failed to check existing permission: %w", err)
	}

	if existingPermission != nil {
		// 更新现有权限
		existingPermission.Role = role
		existingPermission.GrantedBy = grantedBy
		existingPermission.GrantedAt = time.Now()
		existingPermission.IsActive = true
		existingPermission.UpdatedAt = time.Now()

		if err := s.repo.UpdatePermission(ctx, existingPermission); err != nil {
			return fmt.Errorf("failed to update permission: %w", err)
		}

		s.logger.Info("Permission updated",
			logger.String("user_id", userID),
			logger.String("resource_type", resourceType),
			logger.String("resource_id", resourceID),
			logger.String("role", string(role)),
			logger.String("granted_by", grantedBy),
		)
	} else {
		// 创建新权限
		permission := NewPermission(userID, resourceType, resourceID, role, grantedBy)
		if err := s.repo.CreatePermission(ctx, permission); err != nil {
			return fmt.Errorf("failed to create permission: %w", err)
		}

		s.logger.Info("Permission granted",
			logger.String("user_id", userID),
			logger.String("resource_type", resourceType),
			logger.String("resource_id", resourceID),
			logger.String("role", string(role)),
			logger.String("granted_by", grantedBy),
		)
	}

	return nil
}

// RevokePermission 撤销权限
func (s *service) RevokePermission(ctx context.Context, userID, resourceType, resourceID string) error {
	if err := s.repo.DeleteUserPermissions(ctx, userID, resourceType, resourceID); err != nil {
		return fmt.Errorf("failed to revoke permission: %w", err)
	}

	s.logger.Info("Permission revoked",
		logger.String("user_id", userID),
		logger.String("resource_type", resourceType),
		logger.String("resource_id", resourceID),
	)

	return nil
}

// UpdatePermission 更新权限
func (s *service) UpdatePermission(ctx context.Context, userID, resourceType, resourceID string, role Role, updatedBy string) error {
	permission, err := s.repo.GetUserPermission(ctx, userID, resourceType, resourceID)
	if err != nil {
		if errors.Is(err, ErrPermissionNotFound) {
			return ErrPermissionNotFound
		}
		return fmt.Errorf("failed to get permission: %w", err)
	}

	permission.Role = role
	permission.GrantedBy = updatedBy
	permission.GrantedAt = time.Now()
	permission.UpdatedAt = time.Now()

	if err := s.repo.UpdatePermission(ctx, permission); err != nil {
		return fmt.Errorf("failed to update permission: %w", err)
	}

	s.logger.Info("Permission updated",
		logger.String("user_id", userID),
		logger.String("resource_type", resourceType),
		logger.String("resource_id", resourceID),
		logger.String("role", string(role)),
		logger.String("updated_by", updatedBy),
	)

	return nil
}

// GetUserPermissions 获取用户权限
func (s *service) GetUserPermissions(ctx context.Context, userID string) ([]*Permission, error) {
	permissions, err := s.repo.GetPermissionsByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user permissions: %w", err)
	}

	// 过滤有效权限
	var validPermissions []*Permission
	for _, permission := range permissions {
		if permission.IsValid() {
			validPermissions = append(validPermissions, permission)
		}
	}

	return validPermissions, nil
}

// GetResourcePermissions 获取资源权限
func (s *service) GetResourcePermissions(ctx context.Context, resourceType, resourceID string) ([]*Permission, error) {
	permissions, err := s.repo.GetPermissionsByResource(ctx, resourceType, resourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource permissions: %w", err)
	}

	// 过滤有效权限
	var validPermissions []*Permission
	for _, permission := range permissions {
		if permission.IsValid() {
			validPermissions = append(validPermissions, permission)
		}
	}

	return validPermissions, nil
}

// AddSpaceCollaborator 添加空间协作者
func (s *service) AddSpaceCollaborator(ctx context.Context, spaceID, userID string, role Role, invitedBy string, email *string) error {
	// 检查是否已是协作者
	existing, err := s.repo.GetUserSpaceCollaborator(ctx, userID, spaceID)
	if err != nil && !errors.Is(err, ErrCollaboratorNotFound) {
		return fmt.Errorf("failed to check existing collaborator: %w", err)
	}

	if existing != nil {
		return ErrCollaboratorExists
	}

	collaborator := NewSpaceCollaborator(spaceID, userID, role, invitedBy, email)
	if err := s.repo.CreateSpaceCollaborator(ctx, collaborator); err != nil {
		return fmt.Errorf("failed to create space collaborator: %w", err)
	}

	// 同时创建权限记录
	if err := s.GrantPermission(ctx, userID, "space", spaceID, role, invitedBy); err != nil {
		s.logger.Error("Failed to grant permission for space collaborator",
			logger.String("space_id", spaceID),
			logger.String("user_id", userID),
			logger.ErrorField(err),
		)
	}

	s.logger.Info("Space collaborator added",
		logger.String("space_id", spaceID),
		logger.String("user_id", userID),
		logger.String("role", string(role)),
		logger.String("invited_by", invitedBy),
	)

	return nil
}

// RemoveSpaceCollaborator 移除空间协作者
func (s *service) RemoveSpaceCollaborator(ctx context.Context, spaceID, userID string) error {
	// 检查协作者是否存在
	collaborator, err := s.repo.GetUserSpaceCollaborator(ctx, userID, spaceID)
	if err != nil {
		if errors.Is(err, ErrCollaboratorNotFound) {
			return ErrCollaboratorNotFound
		}
		return fmt.Errorf("failed to get space collaborator: %w", err)
	}

	// 不能移除所有者
	if collaborator.Role == RoleOwner {
		return ErrCannotRemoveOwner
	}

	// 删除协作者记录
	if err := s.repo.DeleteUserSpaceCollaborator(ctx, userID, spaceID); err != nil {
		return fmt.Errorf("failed to delete space collaborator: %w", err)
	}

	// 同时撤销权限
	if err := s.RevokePermission(ctx, userID, "space", spaceID); err != nil {
		s.logger.Error("Failed to revoke permission for space collaborator",
			logger.String("space_id", spaceID),
			logger.String("user_id", userID),
			logger.ErrorField(err),
		)
	}

	s.logger.Info("Space collaborator removed",
		logger.String("space_id", spaceID),
		logger.String("user_id", userID),
	)

	return nil
}

// UpdateSpaceCollaboratorRole 更新空间协作者角色
func (s *service) UpdateSpaceCollaboratorRole(ctx context.Context, spaceID, userID string, role Role, updatedBy string) error {
	// 检查协作者是否存在
	collaborator, err := s.repo.GetUserSpaceCollaborator(ctx, userID, spaceID)
	if err != nil {
		if errors.Is(err, ErrCollaboratorNotFound) {
			return ErrCollaboratorNotFound
		}
		return fmt.Errorf("failed to get space collaborator: %w", err)
	}

	// 不能更改所有者角色
	if collaborator.Role == RoleOwner {
		return ErrCannotChangeOwnerRole
	}

	// 更新协作者角色
	collaborator.UpdateRole(role)
	if err := s.repo.UpdateSpaceCollaborator(ctx, collaborator); err != nil {
		return fmt.Errorf("failed to update space collaborator: %w", err)
	}

	// 同时更新权限
	if err := s.UpdatePermission(ctx, userID, "space", spaceID, role, updatedBy); err != nil {
		s.logger.Error("Failed to update permission for space collaborator",
			logger.String("space_id", spaceID),
			logger.String("user_id", userID),
			logger.ErrorField(err),
		)
	}

	s.logger.Info("Space collaborator role updated",
		logger.String("space_id", spaceID),
		logger.String("user_id", userID),
		logger.String("role", string(role)),
		logger.String("updated_by", updatedBy),
	)

	return nil
}

// GetSpaceCollaborators 获取空间协作者
func (s *service) GetSpaceCollaborators(ctx context.Context, spaceID string) ([]*SpaceCollaborator, error) {
	collaborators, err := s.repo.GetSpaceCollaborators(ctx, spaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get space collaborators: %w", err)
	}

	// 过滤活跃协作者
	var activeCollaborators []*SpaceCollaborator
	for _, collaborator := range collaborators {
		if collaborator.IsActive {
			activeCollaborators = append(activeCollaborators, collaborator)
		}
	}

	return activeCollaborators, nil
}

// GetUserSpaceRole 获取用户空间角色
func (s *service) GetUserSpaceRole(ctx context.Context, userID, spaceID string) (Role, error) {
	role, err := s.repo.GetUserEffectiveRole(ctx, userID, "space", spaceID)
	if err != nil {
		return "", fmt.Errorf("failed to get user space role: %w", err)
	}
	return role, nil
}

// AddBaseCollaborator 添加基础表协作者
func (s *service) AddBaseCollaborator(ctx context.Context, baseID, userID string, role Role, invitedBy string, email *string) error {
	// 检查是否已是协作者
	existing, err := s.repo.GetUserBaseCollaborator(ctx, userID, baseID)
	if err != nil && !errors.Is(err, ErrCollaboratorNotFound) {
		return fmt.Errorf("failed to check existing collaborator: %w", err)
	}

	if existing != nil {
		return ErrCollaboratorExists
	}

	collaborator := NewBaseCollaborator(baseID, userID, role, invitedBy, email)
	if err := s.repo.CreateBaseCollaborator(ctx, collaborator); err != nil {
		return fmt.Errorf("failed to create base collaborator: %w", err)
	}

	// 同时创建权限记录
	if err := s.GrantPermission(ctx, userID, "base", baseID, role, invitedBy); err != nil {
		s.logger.Error("Failed to grant permission for base collaborator",
			logger.String("base_id", baseID),
			logger.String("user_id", userID),
			logger.ErrorField(err),
		)
	}

	s.logger.Info("Base collaborator added",
		logger.String("base_id", baseID),
		logger.String("user_id", userID),
		logger.String("role", string(role)),
		logger.String("invited_by", invitedBy),
	)

	return nil
}

// RemoveBaseCollaborator 移除基础表协作者
func (s *service) RemoveBaseCollaborator(ctx context.Context, baseID, userID string) error {
	// 检查协作者是否存在
	collaborator, err := s.repo.GetUserBaseCollaborator(ctx, userID, baseID)
	if err != nil {
		if errors.Is(err, ErrCollaboratorNotFound) {
			return ErrCollaboratorNotFound
		}
		return fmt.Errorf("failed to get base collaborator: %w", err)
	}

	// 不能移除所有者
	if collaborator.Role == RoleBaseOwner {
		return ErrCannotRemoveOwner
	}

	// 删除协作者记录
	if err := s.repo.DeleteUserBaseCollaborator(ctx, userID, baseID); err != nil {
		return fmt.Errorf("failed to delete base collaborator: %w", err)
	}

	// 同时撤销权限
	if err := s.RevokePermission(ctx, userID, "base", baseID); err != nil {
		s.logger.Error("Failed to revoke permission for base collaborator",
			logger.String("base_id", baseID),
			logger.String("user_id", userID),
			logger.ErrorField(err),
		)
	}

	s.logger.Info("Base collaborator removed",
		logger.String("base_id", baseID),
		logger.String("user_id", userID),
	)

	return nil
}

// UpdateBaseCollaboratorRole 更新基础表协作者角色
func (s *service) UpdateBaseCollaboratorRole(ctx context.Context, baseID, userID string, role Role, updatedBy string) error {
	// 检查协作者是否存在
	collaborator, err := s.repo.GetUserBaseCollaborator(ctx, userID, baseID)
	if err != nil {
		if errors.Is(err, ErrCollaboratorNotFound) {
			return ErrCollaboratorNotFound
		}
		return fmt.Errorf("failed to get base collaborator: %w", err)
	}

	// 不能更改所有者角色
	if collaborator.Role == RoleBaseOwner {
		return ErrCannotChangeOwnerRole
	}

	// 更新协作者角色
	collaborator.UpdateRole(role)
	if err := s.repo.UpdateBaseCollaborator(ctx, collaborator); err != nil {
		return fmt.Errorf("failed to update base collaborator: %w", err)
	}

	// 同时更新权限
	if err := s.UpdatePermission(ctx, userID, "base", baseID, role, updatedBy); err != nil {
		s.logger.Error("Failed to update permission for base collaborator",
			logger.String("base_id", baseID),
			logger.String("user_id", userID),
			logger.ErrorField(err),
		)
	}

	s.logger.Info("Base collaborator role updated",
		logger.String("base_id", baseID),
		logger.String("user_id", userID),
		logger.String("role", string(role)),
		logger.String("updated_by", updatedBy),
	)

	return nil
}

// GetBaseCollaborators 获取基础表协作者
func (s *service) GetBaseCollaborators(ctx context.Context, baseID string) ([]*BaseCollaborator, error) {
	collaborators, err := s.repo.GetBaseCollaborators(ctx, baseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get base collaborators: %w", err)
	}

	// 过滤活跃协作者
	var activeCollaborators []*BaseCollaborator
	for _, collaborator := range collaborators {
		if collaborator.IsActive {
			activeCollaborators = append(activeCollaborators, collaborator)
		}
	}

	return activeCollaborators, nil
}

// GetUserBaseRole 获取用户基础表角色
func (s *service) GetUserBaseRole(ctx context.Context, userID, baseID string) (Role, error) {
	role, err := s.repo.GetUserEffectiveRole(ctx, userID, "base", baseID)
	if err != nil {
		return "", fmt.Errorf("failed to get user base role: %w", err)
	}
	return role, nil
}

// CheckPermission 检查权限
func (s *service) CheckPermission(ctx context.Context, userID, resourceType, resourceID string, action Action) (bool, error) {
	hasPermission, err := s.repo.CheckUserPermission(ctx, userID, resourceType, resourceID, action)
	if err != nil {
		return false, fmt.Errorf("failed to check permission: %w", err)
	}
	return hasPermission, nil
}

// CheckMultiplePermissions 检查多个权限
func (s *service) CheckMultiplePermissions(ctx context.Context, userID, resourceType, resourceID string, actions []Action) (bool, error) {
	for _, action := range actions {
		hasPermission, err := s.CheckPermission(ctx, userID, resourceType, resourceID, action)
		if err != nil {
			return false, err
		}
		if !hasPermission {
			return false, nil
		}
	}
	return true, nil
}

// GetUserEffectiveRole 获取用户有效角色
func (s *service) GetUserEffectiveRole(ctx context.Context, userID, resourceType, resourceID string) (Role, error) {
	role, err := s.repo.GetUserEffectiveRole(ctx, userID, resourceType, resourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get user effective role: %w", err)
	}
	return role, nil
}

// GetUserEffectivePermissions 获取用户有效权限
func (s *service) GetUserEffectivePermissions(ctx context.Context, userID, resourceType, resourceID string) ([]Action, error) {
	role, err := s.GetUserEffectiveRole(ctx, userID, resourceType, resourceID)
	if err != nil {
		return nil, err
	}

	permissions := GetRolePermissions(role)
	return permissions, nil
}

// GetUserResources 获取用户资源
func (s *service) GetUserResources(ctx context.Context, userID, resourceType string) ([]string, error) {
	resources, err := s.repo.GetUserResources(ctx, userID, resourceType)
	if err != nil {
		return nil, fmt.Errorf("failed to get user resources: %w", err)
	}
	return resources, nil
}

// GetResourceCollaborators 获取资源协作者
func (s *service) GetResourceCollaborators(ctx context.Context, resourceType, resourceID string) ([]*CollaboratorInfo, error) {
	collaborators, err := s.repo.GetResourceCollaborators(ctx, resourceType, resourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource collaborators: %w", err)
	}
	return collaborators, nil
}

// TransferOwnership 转移所有权
func (s *service) TransferOwnership(ctx context.Context, resourceType, resourceID, fromUserID, toUserID string) error {
	// 获取当前所有者角色
	var ownerRole Role
	switch resourceType {
	case "space":
		ownerRole = RoleOwner
	case "base":
		ownerRole = RoleBaseOwner
	default:
		return fmt.Errorf("unsupported resource type: %s", resourceType)
	}

	// 检查当前用户是否有转移权限
	hasPermission, err := s.CheckPermission(ctx, fromUserID, resourceType, resourceID, ActionSpaceGrantRole)
	if err != nil {
		return fmt.Errorf("failed to check transfer permission: %w", err)
	}
	if !hasPermission {
		return ErrInsufficientPermission
	}

	// 更新新所有者的角色
	if err := s.UpdatePermission(ctx, toUserID, resourceType, resourceID, ownerRole, fromUserID); err != nil {
		return fmt.Errorf("failed to update new owner permission: %w", err)
	}

	// 更新原所有者的角色为创建者
	var creatorRole Role
	switch resourceType {
	case "space":
		creatorRole = RoleCreator
	case "base":
		creatorRole = RoleBaseCreator
	}

	if err := s.UpdatePermission(ctx, fromUserID, resourceType, resourceID, creatorRole, fromUserID); err != nil {
		return fmt.Errorf("failed to update former owner permission: %w", err)
	}

	s.logger.Info("Ownership transferred",
		logger.String("resource_type", resourceType),
		logger.String("resource_id", resourceID),
		logger.String("from_user_id", fromUserID),
		logger.String("to_user_id", toUserID),
	)

	return nil
}

// GetPermissionStats 获取权限统计
func (s *service) GetPermissionStats(ctx context.Context) (*PermissionStats, error) {
	stats, err := s.repo.GetPermissionStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get permission stats: %w", err)
	}
	return stats, nil
}
