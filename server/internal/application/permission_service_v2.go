package application

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/application/permission"
	baseRepo "github.com/easyspace-ai/luckdb/server/internal/domain/base/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/repository"
	fieldRepo "github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	spaceRepo "github.com/easyspace-ai/luckdb/server/internal/domain/space/repository"
	tableRepo "github.com/easyspace-ai/luckdb/server/internal/domain/table/repository"
	viewRepo "github.com/easyspace-ai/luckdb/server/internal/domain/view/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"

	"go.uber.org/zap"
)

// PermissionServiceV2 权限服务v2（Action-based模型）
// 参考原 Teable 项目的权限设计：
// - 基于Action的权限检查
// - 角色权限矩阵
// - Collaborator模型管理用户-资源-角色关系
type PermissionServiceV2 struct {
	collaboratorRepo repository.CollaboratorRepository
	spaceRepo        spaceRepo.SpaceRepository
	baseRepo         baseRepo.BaseRepository
	tableRepo        tableRepo.TableRepository
	fieldRepo        fieldRepo.FieldRepository
	viewRepo         viewRepo.ViewRepository
}

// NewPermissionServiceV2 创建权限服务v2
func NewPermissionServiceV2(
	collaboratorRepo repository.CollaboratorRepository,
	spaceRepo spaceRepo.SpaceRepository,
	baseRepo baseRepo.BaseRepository,
	tableRepo tableRepo.TableRepository,
	fieldRepo fieldRepo.FieldRepository,
	viewRepo viewRepo.ViewRepository,
) *PermissionServiceV2 {
	return &PermissionServiceV2{
		collaboratorRepo: collaboratorRepo,
		spaceRepo:        spaceRepo,
		baseRepo:         baseRepo,
		tableRepo:        tableRepo,
		fieldRepo:        fieldRepo,
		viewRepo:         viewRepo,
	}
}

// ==================== 核心权限检查方法 ====================

// Can 检查用户是否可以对资源执行某个动作
// 这是核心权限检查方法，所有其他方法都基于此
func (s *PermissionServiceV2) Can(ctx context.Context, userID, resourceID string, resourceType entity.ResourceType, action permission.Action) bool {
	// 1. 查找用户在该资源上的协作者记录
	collaborator, err := s.collaboratorRepo.FindByResourceAndPrincipal(ctx, resourceID, userID)
	if err != nil {
		logger.Debug("No collaborator found",
			zap.String("user_id", userID),
			zap.String("resource_id", resourceID),
			zap.String("resource_type", string(resourceType)),
			zap.Error(err),
		)
		return false
	}

	// 2. 根据角色权限矩阵检查是否有权限
	hasPermission := permission.HasPermission(collaborator.Role(), action)

	logger.Debug("Permission check",
		zap.String("user_id", userID),
		zap.String("resource_id", resourceID),
		zap.String("role", string(collaborator.Role())),
		zap.String("action", string(action)),
		zap.Bool("granted", hasPermission),
	)

	return hasPermission
}

// ==================== Space权限 ====================

// CanAccessSpace 检查用户是否可以访问Space
func (s *PermissionServiceV2) CanAccessSpace(ctx context.Context, userID, spaceID string) bool {
	return s.Can(ctx, userID, spaceID, entity.ResourceTypeSpace, permission.ActionSpaceRead)
}

// CanUpdateSpace 检查用户是否可以更新Space
func (s *PermissionServiceV2) CanUpdateSpace(ctx context.Context, userID, spaceID string) bool {
	return s.Can(ctx, userID, spaceID, entity.ResourceTypeSpace, permission.ActionSpaceUpdate)
}

// CanDeleteSpace 检查用户是否可以删除Space
func (s *PermissionServiceV2) CanDeleteSpace(ctx context.Context, userID, spaceID string) bool {
	return s.Can(ctx, userID, spaceID, entity.ResourceTypeSpace, permission.ActionSpaceDelete)
}

// CanManageSpaceCollaborators 检查用户是否可以管理Space协作者
func (s *PermissionServiceV2) CanManageSpaceCollaborators(ctx context.Context, userID, spaceID string) bool {
	return s.Can(ctx, userID, spaceID, entity.ResourceTypeSpace, permission.ActionSpaceManageCollaborator)
}

// CanCreateBaseInSpace 检查用户是否可以在Space中创建Base
func (s *PermissionServiceV2) CanCreateBaseInSpace(ctx context.Context, userID, spaceID string) bool {
	// 在Space中有读权限即可创建Base（业务规则）
	return s.Can(ctx, userID, spaceID, entity.ResourceTypeSpace, permission.ActionSpaceRead)
}

// ==================== Base权限 ====================

// CanAccessBase 检查用户是否可以访问Base
func (s *PermissionServiceV2) CanAccessBase(ctx context.Context, userID, baseID string) bool {
	return s.Can(ctx, userID, baseID, entity.ResourceTypeBase, permission.ActionBaseRead)
}

// CanUpdateBase 检查用户是否可以更新Base
func (s *PermissionServiceV2) CanUpdateBase(ctx context.Context, userID, baseID string) bool {
	return s.Can(ctx, userID, baseID, entity.ResourceTypeBase, permission.ActionBaseUpdate)
}

// CanDeleteBase 检查用户是否可以删除Base
func (s *PermissionServiceV2) CanDeleteBase(ctx context.Context, userID, baseID string) bool {
	return s.Can(ctx, userID, baseID, entity.ResourceTypeBase, permission.ActionBaseDelete)
}

// CanDuplicateBase 检查用户是否可以复制Base
func (s *PermissionServiceV2) CanDuplicateBase(ctx context.Context, userID, baseID string) bool {
	return s.Can(ctx, userID, baseID, entity.ResourceTypeBase, permission.ActionBaseDuplicate)
}

// CanManageBaseCollaborators 检查用户是否可以管理Base协作者
func (s *PermissionServiceV2) CanManageBaseCollaborators(ctx context.Context, userID, baseID string) bool {
	return s.Can(ctx, userID, baseID, entity.ResourceTypeBase, permission.ActionBaseManageCollaborator)
}

// CanCreateTablesInBase 检查用户是否可以在Base中创建Table
func (s *PermissionServiceV2) CanCreateTablesInBase(ctx context.Context, userID, baseID string) bool {
	return s.Can(ctx, userID, baseID, entity.ResourceTypeBase, permission.ActionBaseTableCreate)
}

// ==================== Table权限 ====================

// CanAccessTable 检查用户是否可以访问Table
// Table继承Base的权限
func (s *PermissionServiceV2) CanAccessTable(ctx context.Context, userID, tableID string) bool {
	// 1. 获取Table所属的Base
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	// 2. 检查Base权限
	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableRead)
}

// CanManageTableSchema 检查用户是否可以管理Table结构（字段、视图）
func (s *PermissionServiceV2) CanManageTableSchema(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableFieldCreate)
}

// CanDeleteTable 检查用户是否可以删除Table
func (s *PermissionServiceV2) CanDeleteTable(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableDelete)
}

// ==================== Record权限 ====================

// CanAccessRecord 检查用户是否可以访问Record
func (s *PermissionServiceV2) CanAccessRecord(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionRecordRead)
}

// CanCreateRecordsInTable 检查用户是否可以在Table中创建Record
func (s *PermissionServiceV2) CanCreateRecordsInTable(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionRecordCreate)
}

// CanUpdateRecordsInTable 检查用户是否可以更新Record
func (s *PermissionServiceV2) CanUpdateRecordsInTable(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionRecordUpdate)
}

// CanDeleteRecordsInTable 检查用户是否可以删除Record
func (s *PermissionServiceV2) CanDeleteRecordsInTable(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionRecordDelete)
}

// FilterAccessibleRecords 过滤用户可访问的Record列表
func (s *PermissionServiceV2) FilterAccessibleRecords(ctx context.Context, userID, tableID string, recordIDs []string) []string {
	// 简化实现：如果用户有读权限，则返回所有recordIDs
	// 实际应用中可能需要更细粒度的记录级权限
	if s.CanAccessRecord(ctx, userID, tableID) {
		return recordIDs
	}
	return []string{}
}

// ==================== Field权限 ====================

// CanReadField 检查用户是否可以读取Field
// Field继承Table的权限
func (s *PermissionServiceV2) CanReadField(ctx context.Context, userID, fieldID string) bool {
	// 1. 获取Field所属的Table
	field, err := s.fieldRepo.FindByID(ctx, valueobject.NewFieldID(fieldID))
	if err != nil || field == nil {
		logger.Debug("Field not found",
			zap.String("user_id", userID),
			zap.String("field_id", fieldID),
			zap.Error(err),
		)
		return false
	}

	// 2. 检查Table权限（Field继承Table的读权限）
	return s.CanAccessTable(ctx, userID, field.TableID())
}

// CanUpdateField 检查用户是否可以更新Field
// Field继承Table的字段管理权限
func (s *PermissionServiceV2) CanUpdateField(ctx context.Context, userID, fieldID string) bool {
	// 1. 获取Field所属的Table
	field, err := s.fieldRepo.FindByID(ctx, valueobject.NewFieldID(fieldID))
	if err != nil || field == nil {
		logger.Debug("Field not found",
			zap.String("user_id", userID),
			zap.String("field_id", fieldID),
			zap.Error(err),
		)
		return false
	}

	// 2. 检查Table的字段更新权限
	table, err := s.tableRepo.GetByID(ctx, field.TableID())
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableFieldUpdate)
}

// CanDeleteField 检查用户是否可以删除Field
// Field继承Table的字段管理权限
func (s *PermissionServiceV2) CanDeleteField(ctx context.Context, userID, fieldID string) bool {
	// 1. 获取Field所属的Table
	field, err := s.fieldRepo.FindByID(ctx, valueobject.NewFieldID(fieldID))
	if err != nil || field == nil {
		logger.Debug("Field not found",
			zap.String("user_id", userID),
			zap.String("field_id", fieldID),
			zap.Error(err),
		)
		return false
	}

	// 2. 检查Table的字段删除权限
	table, err := s.tableRepo.GetByID(ctx, field.TableID())
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableFieldDelete)
}

// CanCreateField 检查用户是否可以在Table中创建Field
func (s *PermissionServiceV2) CanCreateField(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableFieldCreate)
}

// ==================== View权限 ====================

// CanReadView 检查用户是否可以读取View
// View继承Table的权限
func (s *PermissionServiceV2) CanReadView(ctx context.Context, userID, viewID string) bool {
	// 1. 获取View所属的Table
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil || view == nil {
		logger.Debug("View not found",
			zap.String("user_id", userID),
			zap.String("view_id", viewID),
			zap.Error(err),
		)
		return false
	}

	// 2. 检查Table权限（View继承Table的读权限）
	return s.CanAccessTable(ctx, userID, view.TableID())
}

// CanUpdateView 检查用户是否可以更新View
// View继承Table的视图管理权限
func (s *PermissionServiceV2) CanUpdateView(ctx context.Context, userID, viewID string) bool {
	// 1. 获取View所属的Table
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil || view == nil {
		logger.Debug("View not found",
			zap.String("user_id", userID),
			zap.String("view_id", viewID),
			zap.Error(err),
		)
		return false
	}

	// 2. 检查Table的视图更新权限
	table, err := s.tableRepo.GetByID(ctx, view.TableID())
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableViewUpdate)
}

// CanDeleteView 检查用户是否可以删除View
// View继承Table的视图管理权限
func (s *PermissionServiceV2) CanDeleteView(ctx context.Context, userID, viewID string) bool {
	// 1. 获取View所属的Table
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil || view == nil {
		logger.Debug("View not found",
			zap.String("user_id", userID),
			zap.String("view_id", viewID),
			zap.Error(err),
		)
		return false
	}

	// 2. 检查Table的视图删除权限
	table, err := s.tableRepo.GetByID(ctx, view.TableID())
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableViewDelete)
}

// CanCreateView 检查用户是否可以在Table中创建View
func (s *PermissionServiceV2) CanCreateView(ctx context.Context, userID, tableID string) bool {
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	return s.Can(ctx, userID, table.BaseID(), entity.ResourceTypeBase, permission.ActionTableViewCreate)
}

// ==================== 辅助方法 ====================

// GetUserRole 获取用户在资源上的角色
func (s *PermissionServiceV2) GetUserRole(ctx context.Context, userID, resourceID string) (entity.RoleName, error) {
	collaborator, err := s.collaboratorRepo.FindByResourceAndPrincipal(ctx, resourceID, userID)
	if err != nil {
		return "", err
	}
	return collaborator.Role(), nil
}

// GetUserPermissions 获取用户在资源上的所有权限
func (s *PermissionServiceV2) GetUserPermissions(ctx context.Context, userID, resourceID string) ([]permission.Action, error) {
	role, err := s.GetUserRole(ctx, userID, resourceID)
	if err != nil {
		return nil, err
	}
	return permission.GetRoleActions(role), nil
}
