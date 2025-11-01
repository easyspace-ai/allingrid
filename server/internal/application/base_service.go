package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/base/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/base/repository"
	spaceRepository "github.com/easyspace-ai/luckdb/server/internal/domain/space/repository"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database"
	"github.com/easyspace-ai/luckdb/server/pkg/authctx"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"

	"gorm.io/gorm"
)

// BaseService Base应用服务（对齐原版BaseService）
// 集成完全动态表架构：每个Base独立Schema
type BaseService struct {
	repo       repository.BaseRepository
	spaceRepo  spaceRepository.SpaceRepository // 用于检查父空间是否存在
	dbProvider database.DBProvider             // ✅ 数据库提供者（Schema管理）
}

// NewBaseService 创建Base服务
func NewBaseService(repo repository.BaseRepository, spaceRepo spaceRepository.SpaceRepository, dbProvider database.DBProvider) *BaseService {
	return &BaseService{
		repo:       repo,
		spaceRepo:  spaceRepo,
		dbProvider: dbProvider,
	}
}

// CreateBase 创建Base（严格遵守：返回AppError）
// ✅ 完全动态表架构：创建Base时创建独立Schema
// 严格按照旧系统实现：teable-develop/apps/nestjs-backend/src/features/base/base.service.ts
func (s *BaseService) CreateBase(ctx context.Context, req dto.CreateBaseRequest, userID string) (*dto.BaseResponse, error) {
	// 1. 参数验证
	if req.Name == "" {
		return nil, errors.ErrRequiredField.WithDetails(map[string]interface{}{
			"field":   "name",
			"message": "Base名称不能为空",
		})
	}

	if req.SpaceID == "" {
		return nil, errors.ErrRequiredField.WithDetails(map[string]interface{}{
			"field":   "spaceId",
			"message": "Space ID不能为空",
		})
	}

	// 2. 检查父空间是否存在
	space, err := s.spaceRepo.GetSpaceByID(ctx, req.SpaceID)
	if err != nil {
		return nil, errors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找父空间失败: %v", err))
	}
	if space == nil {
		return nil, errors.ErrSpaceNotFound.WithDetails(fmt.Sprintf("父空间不存在: %s", req.SpaceID))
	}

	// 3. 权限检查
	// 检查用户是否有权限在Space中创建Base
	userID, exists := authctx.UserFrom(ctx)
	if !exists {
		return nil, errors.ErrUnauthorized.WithDetails("用户未认证")
	}

	// 基本权限检查：用户必须是Space的成员
	// TODO: 集成PermissionService进行更细粒度的权限检查
	// 当前实现：允许所有认证用户在Space中创建Base

	// 4. 创建Base实体
	base, err := entity.NewBase(req.Name, req.Icon, req.SpaceID, userID)
	if err != nil {
		return nil, errors.ErrBadRequest.WithDetails(err.Error())
	}

	// 5. ✅ 创建独立的PostgreSQL Schema（完全动态表架构）
	// 参考旧系统：const sqlList = this.dbProvider.createSchema(base.id);
	if s.dbProvider.SupportsSchema() {
		logger.Info("正在为Base创建独立Schema",
			logger.String("base_id", base.ID),
			logger.String("schema_name", base.ID))

		if err := s.dbProvider.CreateSchema(ctx, base.ID); err != nil {
			logger.Error("创建Schema失败",
				logger.String("base_id", base.ID),
				logger.ErrorField(err))
			return nil, errors.ErrDatabaseOperation.WithDetails(
				fmt.Sprintf("创建Schema失败: %v", err))
		}

		logger.Info("✅ Base Schema创建成功",
			logger.String("base_id", base.ID),
			logger.String("schema_name", base.ID))
	}

	// 6. 持久化Base元数据
	if err := s.repo.Create(ctx, base); err != nil {
		// ❌ 回滚：删除已创建的Schema
		if s.dbProvider.SupportsSchema() {
			if rollbackErr := s.dbProvider.DropSchema(ctx, base.ID); rollbackErr != nil {
				logger.Error("回滚删除Schema失败",
					logger.String("base_id", base.ID),
					logger.ErrorField(rollbackErr))
			}
		}
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	logger.Info("✅ Base创建成功（含独立Schema）",
		logger.String("base_id", base.ID),
		logger.String("base_name", base.Name),
		logger.String("schema_name", base.ID))

	// 7. 返回DTO
	return s.toDTO(base), nil
}

// GetBase 获取Base详情（严格遵守：返回AppError）
func (s *BaseService) GetBase(ctx context.Context, baseID string) (*dto.BaseResponse, error) {
	base, err := s.repo.FindByID(ctx, baseID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrNotFound.WithDetails(map[string]interface{}{
				"resource": "base",
				"id":       baseID,
			})
		}
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	return s.toDTO(base), nil
}

// UpdateBase 更新Base（严格遵守：返回AppError）
func (s *BaseService) UpdateBase(ctx context.Context, baseID string, req dto.UpdateBaseRequest) (*dto.BaseResponse, error) {
	// 1. 查找Base
	base, err := s.repo.FindByID(ctx, baseID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrNotFound.WithDetails(map[string]interface{}{
				"resource": "base",
				"id":       baseID,
			})
		}
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	// 2. 权限检查
	// 检查用户是否有权限更新Base
	_, exists := authctx.UserFrom(ctx)
	if !exists {
		return nil, errors.ErrUnauthorized.WithDetails("用户未认证")
	}

	// 基本权限检查：用户必须是Base的创建者或Space的管理员
	// TODO: 集成PermissionService进行更细粒度的权限检查
	// 当前实现：允许所有认证用户更新Base

	// 3. 更新字段
	if req.Name != "" {
		if err := base.UpdateName(req.Name); err != nil {
			return nil, errors.ErrBadRequest.WithDetails(err.Error())
		}
	}

	if req.Icon != "" {
		base.UpdateIcon(req.Icon)
	}

	// 4. 持久化
	if err := s.repo.Update(ctx, base); err != nil {
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	// 5. 返回DTO
	return s.toDTO(base), nil
}

// DeleteBase 删除Base（严格遵守：返回AppError）
// ✅ 完全动态表架构：删除Base时删除Schema及其所有物理表
// 严格按照旧系统实现
func (s *BaseService) DeleteBase(ctx context.Context, baseID string) error {
	// 1. 检查Base是否存在
	exists, err := s.repo.Exists(ctx, baseID)
	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	if !exists {
		return errors.ErrNotFound.WithDetails(map[string]interface{}{
			"resource": "base",
			"id":       baseID,
		})
	}

	// 2. 权限检查
	// 检查用户是否有权限删除Base
	_, userExists := authctx.UserFrom(ctx)
	if !userExists {
		return errors.ErrUnauthorized.WithDetails("用户未认证")
	}

	// 基本权限检查：用户必须是Base的创建者或Space的owner
	// TODO: 集成PermissionService进行更细粒度的权限检查
	// 当前实现：允许所有认证用户删除Base

	logger.Info("正在删除Base及其Schema",
		logger.String("base_id", baseID))

	// 3. ✅ 删除Schema（CASCADE会自动删除其中所有的物理表）
	// 参考旧系统：DROP SCHEMA IF EXISTS base_id CASCADE
	if s.dbProvider.SupportsSchema() {
		logger.Info("正在删除Base的Schema",
			logger.String("base_id", baseID),
			logger.String("schema_name", baseID))

		if err := s.dbProvider.DropSchema(ctx, baseID); err != nil {
			logger.Error("删除Schema失败",
				logger.String("base_id", baseID),
				logger.ErrorField(err))
			return errors.ErrDatabaseOperation.WithDetails(
				fmt.Sprintf("删除Schema失败: %v", err))
		}

		logger.Info("✅ Schema删除成功（包含所有物理表）",
			logger.String("base_id", baseID))
	}

	// 4. 删除Base元数据
	if err := s.repo.Delete(ctx, baseID); err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	logger.Info("✅ Base删除成功（含Schema和所有物理表）",
		logger.String("base_id", baseID))

	return nil
}

// DuplicateBase 复制Base
func (s *BaseService) DuplicateBase(ctx context.Context, baseID string, req *dto.DuplicateBaseRequest) (*dto.BaseResponse, error) {
	// 1. 获取原始Base
	originalBase, err := s.repo.FindByID(ctx, baseID)
	if err != nil {
		return nil, errors.ErrBaseNotFound.WithDetails(fmt.Sprintf("Base不存在: %s", baseID))
	}

	// 2. 权限检查
	// 检查用户是否有权限复制Base
	userID, exists := authctx.UserFrom(ctx)
	if !exists {
		return nil, errors.ErrUnauthorized.WithDetails("用户未认证")
	}

	// 基本权限检查：用户必须有Base的读取权限
	// TODO: 集成PermissionService进行更细粒度的权限检查
	// 当前实现：允许所有认证用户复制Base

	// 3. 创建新的Base
	newBaseReq := &dto.CreateBaseRequest{
		SpaceID: originalBase.SpaceID,
		Name:    req.Name,
		Icon:    originalBase.Icon,
	}

	newBase, err := s.CreateBase(ctx, *newBaseReq, userID)
	if err != nil {
		return nil, err
	}

	// 4. 复制Tables
	// TODO: 实现Table复制功能
	// 需要添加TableService依赖来复制Tables和Fields

	logger.Info("✅ Base复制成功",
		logger.String("original_base_id", baseID),
		logger.String("new_base_id", newBase.ID))

	return newBase, nil
}

// ListBases 获取Base列表（严格遵守：返回AppError）
func (s *BaseService) ListBases(ctx context.Context, spaceID string) ([]*dto.BaseResponse, error) {
	// 查询指定空间下的所有 Base（不分页）
	bases, err := s.repo.FindBySpaceID(ctx, spaceID)
	if err != nil {
		return nil, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	// 转换为 DTO
	items := make([]*dto.BaseResponse, len(bases))
	for i, base := range bases {
		items[i] = s.toDTO(base)
	}

	return items, nil
}

// toDTO 实体转DTO（私有方法）
func (s *BaseService) toDTO(base *entity.Base) *dto.BaseResponse {
	return &dto.BaseResponse{
		ID:        base.ID,
		Name:      base.Name,
		Icon:      base.Icon,
		SpaceID:   base.SpaceID,
		CreatedBy: base.CreatedBy,
		CreatedAt: base.CreatedAt,
		UpdatedAt: base.UpdatedAt,
	}
}
