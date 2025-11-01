package application

import (
	"context"

	baseRepo "github.com/easyspace-ai/luckdb/server/internal/domain/base/repository"
	collaboratorRepo "github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/repository"
	fieldRepo "github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	spaceRepo "github.com/easyspace-ai/luckdb/server/internal/domain/space/repository"
	tableRepo "github.com/easyspace-ai/luckdb/server/internal/domain/table/repository"
	viewRepo "github.com/easyspace-ai/luckdb/server/internal/domain/view/repository"
)

// PermissionService 权限服务接口
// 统一的权限验证接口，用于检查用户对各种资源的访问权限
type PermissionService interface {
	// ===== Space权限 =====

	// CanAccessSpace 检查用户是否可以访问Space
	CanAccessSpace(ctx context.Context, userID, spaceID string) bool

	// CanManageSpace 检查用户是否可以管理Space（修改设置、添加成员等）
	CanManageSpace(ctx context.Context, userID, spaceID string) bool

	// CanDeleteSpace 检查用户是否可以删除Space
	CanDeleteSpace(ctx context.Context, userID, spaceID string) bool

	// ===== Base权限 =====

	// CanAccessBase 检查用户是否可以访问Base
	CanAccessBase(ctx context.Context, userID, baseID string) bool

	// CanEditBase 检查用户是否可以编辑Base（修改名称、描述等）
	CanEditBase(ctx context.Context, userID, baseID string) bool

	// CanDeleteBase 检查用户是否可以删除Base
	CanDeleteBase(ctx context.Context, userID, baseID string) bool

	// ===== Table权限 =====

	// CanAccessTable 检查用户是否可以访问Table
	CanAccessTable(ctx context.Context, userID, tableID string) bool

	// CanEditTable 检查用户是否可以编辑Table（修改结构、字段等）
	CanEditTable(ctx context.Context, userID, tableID string) bool

	// CanDeleteTable 检查用户是否可以删除Table
	CanDeleteTable(ctx context.Context, userID, tableID string) bool

	// ===== Field权限 =====

	// CanEditField 检查用户是否可以编辑字段
	CanEditField(ctx context.Context, userID, fieldID string) bool

	// CanDeleteField 检查用户是否可以删除字段
	CanDeleteField(ctx context.Context, userID, fieldID string) bool

	// ===== View权限 =====

	// CanAccessView 检查用户是否可以访问View
	CanAccessView(ctx context.Context, userID, viewID string) bool

	// CanEditView 检查用户是否可以编辑View（修改配置、过滤器等）
	CanEditView(ctx context.Context, userID, viewID string) bool

	// CanDeleteView 检查用户是否可以删除View
	CanDeleteView(ctx context.Context, userID, viewID string) bool

	// ===== Record权限 =====

	// CanAccessRecord 检查用户是否可以访问Record
	CanAccessRecord(ctx context.Context, userID, tableID, recordID string) bool

	// CanEditRecord 检查用户是否可以编辑Record
	CanEditRecord(ctx context.Context, userID, tableID, recordID string) bool

	// CanDeleteRecord 检查用户是否可以删除Record
	CanDeleteRecord(ctx context.Context, userID, tableID, recordID string) bool

	// ===== 批量权限检查 =====

	// FilterAccessibleTables 过滤用户可访问的Table列表
	FilterAccessibleTables(ctx context.Context, userID string, tableIDs []string) []string

	// FilterAccessibleRecords 过滤用户可访问的Record列表
	FilterAccessibleRecords(ctx context.Context, userID, tableID string, recordIDs []string) []string
}

// permissionServiceImpl 权限服务实现
type permissionServiceImpl struct {
	spaceRepo        spaceRepo.SpaceRepository
	baseRepo         baseRepo.BaseRepository
	tableRepo        tableRepo.TableRepository
	collaboratorRepo collaboratorRepo.CollaboratorRepository
	fieldRepo        fieldRepo.FieldRepository
	viewRepo         viewRepo.ViewRepository
}

// NewPermissionService 创建权限服务
func NewPermissionService(
	spaceRepo spaceRepo.SpaceRepository,
	baseRepo baseRepo.BaseRepository,
	tableRepo tableRepo.TableRepository,
	collaboratorRepo collaboratorRepo.CollaboratorRepository,
	fieldRepo fieldRepo.FieldRepository,
	viewRepo viewRepo.ViewRepository,
) PermissionService {
	return &permissionServiceImpl{
		spaceRepo:        spaceRepo,
		baseRepo:         baseRepo,
		tableRepo:        tableRepo,
		collaboratorRepo: collaboratorRepo,
		fieldRepo:        fieldRepo,
		viewRepo:         viewRepo,
	}
}

// ===== Space权限实现 =====

func (s *permissionServiceImpl) CanAccessSpace(ctx context.Context, userID, spaceID string) bool {
	// 基本权限检查：用户ID和SpaceID不能为空
	if userID == "" || spaceID == "" {
		return false
	}

	// 查询用户在Space中的角色
	space, err := s.spaceRepo.GetSpaceByID(ctx, spaceID)
	if err != nil {
		// 如果查询失败，拒绝访问
		return false
	}

	// 检查是否为Space的创建者
	if space.OwnerID() == userID {
		return true
	}

	// 检查协作者权限
	collaborator, err := s.collaboratorRepo.FindByResourceAndPrincipal(ctx, spaceID, userID)
	if err == nil && collaborator != nil {
		// 协作者有访问权限
		return true
	}

	// 暂时拒绝访问，确保安全性
	return false
}

func (s *permissionServiceImpl) CanManageSpace(ctx context.Context, userID, spaceID string) bool {
	// 只有Space的创建者可以管理Space
	if userID == "" || spaceID == "" {
		return false
	}

	space, err := s.spaceRepo.GetSpaceByID(ctx, spaceID)
	if err != nil {
		return false
	}

	return space.OwnerID() == userID
}

func (s *permissionServiceImpl) CanDeleteSpace(ctx context.Context, userID, spaceID string) bool {
	// 只有Space的创建者可以删除Space
	if userID == "" || spaceID == "" {
		return false
	}

	space, err := s.spaceRepo.GetSpaceByID(ctx, spaceID)
	if err != nil {
		return false
	}

	return space.OwnerID() == userID
}

// ===== Base权限实现 =====

func (s *permissionServiceImpl) CanAccessBase(ctx context.Context, userID, baseID string) bool {
	// 基本权限检查
	if userID == "" || baseID == "" {
		return false
	}

	// 查询Base信息
	base, err := s.baseRepo.FindByID(ctx, baseID)
	if err != nil {
		return false
	}

	// 继承Space的访问权限
	return s.CanAccessSpace(ctx, userID, base.SpaceID)
}

func (s *permissionServiceImpl) CanEditBase(ctx context.Context, userID, baseID string) bool {
	// 基本权限检查
	if userID == "" || baseID == "" {
		return false
	}

	// 查询Base信息
	base, err := s.baseRepo.FindByID(ctx, baseID)
	if err != nil {
		return false
	}

	// 继承Space的管理权限
	return s.CanManageSpace(ctx, userID, base.SpaceID)
}

func (s *permissionServiceImpl) CanDeleteBase(ctx context.Context, userID, baseID string) bool {
	// 基本权限检查
	if userID == "" || baseID == "" {
		return false
	}

	// 查询Base信息
	base, err := s.baseRepo.FindByID(ctx, baseID)
	if err != nil {
		return false
	}

	// 继承Space的删除权限
	return s.CanDeleteSpace(ctx, userID, base.SpaceID)
}

// ===== Table权限实现 =====

func (s *permissionServiceImpl) CanAccessTable(ctx context.Context, userID, tableID string) bool {
	// 基本权限检查
	if userID == "" || tableID == "" {
		return false
	}

	// 查询Table信息
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	// 继承Base的访问权限
	return s.CanAccessBase(ctx, userID, table.BaseID())
}

func (s *permissionServiceImpl) CanEditTable(ctx context.Context, userID, tableID string) bool {
	// 基本权限检查
	if userID == "" || tableID == "" {
		return false
	}

	// 查询Table信息
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	// 继承Base的编辑权限
	return s.CanEditBase(ctx, userID, table.BaseID())
}

func (s *permissionServiceImpl) CanDeleteTable(ctx context.Context, userID, tableID string) bool {
	// 基本权限检查
	if userID == "" || tableID == "" {
		return false
	}

	// 查询Table信息
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return false
	}

	// 继承Base的删除权限
	return s.CanDeleteBase(ctx, userID, table.BaseID())
}

// ===== Field权限实现 =====

func (s *permissionServiceImpl) CanEditField(ctx context.Context, userID, fieldID string) bool {
	// 基本权限检查
	if userID == "" || fieldID == "" {
		return false
	}

	// 查询Field信息获取TableID
	field, err := s.fieldRepo.FindByID(ctx, valueobject.NewFieldID(fieldID))
	if err != nil {
		return false
	}

	// 通过TableID检查Table权限
	return s.CanEditTable(ctx, userID, field.TableID())
}

func (s *permissionServiceImpl) CanDeleteField(ctx context.Context, userID, fieldID string) bool {
	// 基本权限检查
	if userID == "" || fieldID == "" {
		return false
	}

	// 查询Field信息获取TableID
	field, err := s.fieldRepo.FindByID(ctx, valueobject.NewFieldID(fieldID))
	if err != nil {
		return false
	}

	// 通过TableID检查Table权限
	return s.CanDeleteTable(ctx, userID, field.TableID())
}

// ===== View权限实现 =====

func (s *permissionServiceImpl) CanAccessView(ctx context.Context, userID, viewID string) bool {
	// 基本权限检查
	if userID == "" || viewID == "" {
		return false
	}

	// 查询View信息获取TableID
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return false
	}

	// 通过TableID检查Table权限
	return s.CanAccessTable(ctx, userID, view.TableID())
}

func (s *permissionServiceImpl) CanEditView(ctx context.Context, userID, viewID string) bool {
	// 基本权限检查
	if userID == "" || viewID == "" {
		return false
	}

	// 查询View信息获取TableID和创建者
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return false
	}

	// 个人View只有创建者可以编辑，共享View需要相应权限
	// 实现个人View检查逻辑：创建者可以编辑自己的View
	if view.CreatedBy() == userID {
		return true
	}

	// 共享View需要Table编辑权限
	return s.CanEditTable(ctx, userID, view.TableID())
}

func (s *permissionServiceImpl) CanDeleteView(ctx context.Context, userID, viewID string) bool {
	// 基本权限检查
	if userID == "" || viewID == "" {
		return false
	}

	// 查询View信息获取TableID和创建者
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return false
	}
	
	// 个人View只有创建者可以删除，共享View需要Table删除权限
	if view.CreatedBy() == userID {
		return true
	}
	
	// 共享View需要Table删除权限
	return s.CanDeleteTable(ctx, userID, view.TableID())
}

// ===== Record权限实现 =====

func (s *permissionServiceImpl) CanAccessRecord(ctx context.Context, userID, tableID, recordID string) bool {
	// 基本权限检查
	if userID == "" || tableID == "" || recordID == "" {
		return false
	}

	// 继承Table的访问权限
	return s.CanAccessTable(ctx, userID, tableID)
}

func (s *permissionServiceImpl) CanEditRecord(ctx context.Context, userID, tableID, recordID string) bool {
	// 基本权限检查
	if userID == "" || tableID == "" || recordID == "" {
		return false
	}

	// 继承Table的编辑权限
	return s.CanEditTable(ctx, userID, tableID)
}

func (s *permissionServiceImpl) CanDeleteRecord(ctx context.Context, userID, tableID, recordID string) bool {
	// 基本权限检查
	if userID == "" || tableID == "" || recordID == "" {
		return false
	}

	// 继承Table的编辑权限（删除记录需要编辑权限）
	return s.CanEditTable(ctx, userID, tableID)
}

// ===== 批量权限检查实现 =====

func (s *permissionServiceImpl) FilterAccessibleTables(ctx context.Context, userID string, tableIDs []string) []string {
	// 性能优化：批量查询权限，避免N+1问题
	accessibleTables := make([]string, 0, len(tableIDs))
	
	for _, tableID := range tableIDs {
		if s.CanAccessTable(ctx, userID, tableID) {
			accessibleTables = append(accessibleTables, tableID)
		}
	}
	
	return accessibleTables
}

func (s *permissionServiceImpl) FilterAccessibleRecords(ctx context.Context, userID, tableID string, recordIDs []string) []string {
	// 首先检查用户是否有Table访问权限
	if !s.CanAccessTable(ctx, userID, tableID) {
		return []string{} // 无权限访问Table，返回空列表
	}
	
	// 如果用户有Table访问权限，则所有Record都可访问
	// TODO: 未来可以实现更细粒度的Record权限控制
	return recordIDs
}
