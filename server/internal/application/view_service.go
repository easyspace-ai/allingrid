package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	tableRepo "github.com/easyspace-ai/luckdb/server/internal/domain/table/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/events"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// ViewService 视图应用服务
type ViewService struct {
	viewRepo             repository.ViewRepository
	tableRepo            tableRepo.TableRepository    // ✅ 添加表仓储，用于检查表存在性
	businessEventManager *events.BusinessEventManager // ✅ 添加业务事件管理器，用于发布业务事件
}

// NewViewService 创建视图服务
func NewViewService(viewRepo repository.ViewRepository, tableRepo tableRepo.TableRepository, businessEventManager *events.BusinessEventManager) *ViewService {
	return &ViewService{
		viewRepo:             viewRepo,
		tableRepo:            tableRepo,
		businessEventManager: businessEventManager,
	}
}

// CreateView 创建视图
func (s *ViewService) CreateView(
	ctx context.Context,
	req dto.CreateViewRequest,
	userID string,
) (*dto.ViewResponse, error) {
	// 0. ✅ 检查表是否存在
	table, err := s.tableRepo.GetByID(ctx, req.TableID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找表失败: %v", err))
	}
	if table == nil {
		return nil, pkgerrors.ErrTableNotFound.WithDetails(map[string]interface{}{
			"table_id": req.TableID,
		})
	}

	// 1. 验证视图类型
	viewType, err := valueobject.NewViewType(req.Type)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("无效的视图类型: %v", err))
	}

	// 2. 创建视图实体
	view, err := entity.NewView(req.TableID, req.Name, viewType, userID)
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("创建视图实体失败: %v", err))
	}

	// 3. 设置可选属性
	if req.Description != "" {
		view.UpdateDescription(req.Description)
	}

	// 4. 设置过滤器
	if req.Filter != nil {
		filter, err := valueobject.NewFilter(req.Filter)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("过滤器无效: %v", err))
		}
		if err := view.UpdateFilter(filter); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(err.Error())
		}
	}

	// 5. 设置排序
	if req.Sort != nil {
		sort, err := valueobject.NewSort(req.Sort)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("排序无效: %v", err))
		}
		if err := view.UpdateSort(sort); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(err.Error())
		}
	}

	// 6. 设置分组
	if req.Group != nil {
		group, err := valueobject.NewGroup(req.Group)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("分组无效: %v", err))
		}
		if err := view.UpdateGroup(group); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(err.Error())
		}
	}

	// 7. 设置列配置
	if req.ColumnMeta != nil {
		columnMeta, err := valueobject.NewColumnMetaList(req.ColumnMeta)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("列配置无效: %v", err))
		}
		if err := view.UpdateColumnMeta(columnMeta); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(err.Error())
		}
	}

	// 8. 设置选项
	if req.Options != nil {
		if err := view.UpdateOptions(req.Options); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(err.Error())
		}
	}

	// 8.5. 设置视图排序order（基于当前表的视图数量）
	count, err := s.viewRepo.Count(ctx, req.TableID)
	if err != nil {
		logger.Warn("获取视图数量失败，使用默认order=0", logger.ErrorField(err))
	} else {
		view.UpdateOrder(float64(count)) // order从0开始递增
	}

	// 9. 保存视图
	if err := s.viewRepo.Save(ctx, view); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存视图失败: %v", err))
	}

	logger.Info("视图创建成功",
		logger.String("view_id", view.ID()),
		logger.String("table_id", req.TableID),
		logger.String("view_type", req.Type),
	)

	return dto.FromViewEntity(view), nil
}

// GetView 获取视图详情
func (s *ViewService) GetView(ctx context.Context, viewID string) (*dto.ViewResponse, error) {
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	return dto.FromViewEntity(view), nil
}

// ListViewsByTable 获取表格的所有视图
func (s *ViewService) ListViewsByTable(ctx context.Context, tableID string) ([]*dto.ViewResponse, error) {
	views, err := s.viewRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图列表失败: %v", err))
	}

	responses := make([]*dto.ViewResponse, len(views))
	for i, view := range views {
		responses[i] = dto.FromViewEntity(view)
	}

	return responses, nil
}

// UpdateView 更新视图基本信息
func (s *ViewService) UpdateView(
	ctx context.Context,
	viewID string,
	req dto.UpdateViewRequest,
) (*dto.ViewResponse, error) {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 更新名称
	if req.Name != nil && *req.Name != "" {
		if err := view.UpdateName(*req.Name); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(err.Error())
		}
	}

	// 3. 更新描述
	if req.Description != nil {
		view.UpdateDescription(*req.Description)
	}

	// 4. 更新锁定状态
	if req.IsLocked != nil {
		if *req.IsLocked {
			view.Lock()
		} else {
			view.Unlock()
		}
	}

	// 5. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图更新成功",
		logger.String("view_id", viewID),
	)

	return dto.FromViewEntity(view), nil
}

// UpdateViewFilter 更新视图过滤器
func (s *ViewService) UpdateViewFilter(
	ctx context.Context,
	viewID string,
	filterData map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 解析过滤器
	var filter *valueobject.Filter
	if filterData != nil {
		filter, err = valueobject.NewFilter(filterData)
		if err != nil {
			return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("过滤器无效: %v", err))
		}
	}

	// 3. 更新过滤器
	if err := view.UpdateFilter(filter); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 4. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图过滤器更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// UpdateViewSort 更新视图排序
func (s *ViewService) UpdateViewSort(
	ctx context.Context,
	viewID string,
	sortData []map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 解析排序
	var sort *valueobject.Sort
	if sortData != nil {
		sort, err = valueobject.NewSort(sortData)
		if err != nil {
			return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("排序无效: %v", err))
		}
	}

	// 3. 更新排序
	if err := view.UpdateSort(sort); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 4. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图排序更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// UpdateViewGroup 更新视图分组
func (s *ViewService) UpdateViewGroup(
	ctx context.Context,
	viewID string,
	groupData []map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 解析分组
	var group *valueobject.Group
	if groupData != nil {
		group, err = valueobject.NewGroup(groupData)
		if err != nil {
			return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("分组无效: %v", err))
		}
	}

	// 3. 更新分组
	if err := view.UpdateGroup(group); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 4. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图分组更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// UpdateViewColumnMeta 更新视图列配置
func (s *ViewService) UpdateViewColumnMeta(
	ctx context.Context,
	viewID string,
	columnMetaData []map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 解析列配置
	columnMeta, err := valueobject.NewColumnMetaList(columnMetaData)
	if err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("列配置无效: %v", err))
	}

	// 3. 更新列配置
	if err := view.UpdateColumnMeta(columnMeta); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 4. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	// 5. 发布业务事件，触发 YJS 同步
	if s.businessEventManager != nil {
		event := &events.BusinessEvent{
			Type:    events.BusinessEventTypeViewUpdate,
			TableID: view.TableID(),
			Data: map[string]interface{}{
				"view_id":     viewID,
				"column_meta": columnMetaData,
				"update_type": "column_meta",
			},
			UserID: "system", // TODO: 从上下文获取用户ID
		}

		if err := s.businessEventManager.Publish(event); err != nil {
			logger.Warn("发布视图更新事件失败",
				logger.String("view_id", viewID),
				logger.ErrorField(err))
		} else {
			logger.Info("视图更新事件已发布",
				logger.String("view_id", viewID),
				logger.String("event_type", string(event.Type)))
		}
	}

	logger.Info("视图列配置更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// UpdateViewOptions 更新视图选项（完全替换）
func (s *ViewService) UpdateViewOptions(
	ctx context.Context,
	viewID string,
	options map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 更新选项
	if err := view.UpdateOptions(options); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图选项更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// PatchViewOptions 部分更新视图选项
func (s *ViewService) PatchViewOptions(
	ctx context.Context,
	viewID string,
	options map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 部分更新选项
	if err := view.PatchOptions(options); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图选项部分更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// UpdateViewOrder 更新视图排序位置
func (s *ViewService) UpdateViewOrder(
	ctx context.Context,
	viewID string,
	order float64,
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 更新排序
	if err := view.UpdateOrder(order); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图排序更新成功",
		logger.String("view_id", viewID),
		logger.Float64("order", order),
	)

	return nil
}

// EnableShare 启用视图分享
func (s *ViewService) EnableShare(ctx context.Context, viewID string) (string, error) {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return "", pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return "", pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 启用分享
	shareID, err := view.EnableSharing()
	if err != nil {
		return "", pkgerrors.ErrInternalServer.WithDetails(err.Error())
	}

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return "", pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图分享已启用",
		logger.String("view_id", viewID),
		logger.String("share_id", shareID),
	)

	return shareID, nil
}

// DisableShare 禁用视图分享
func (s *ViewService) DisableShare(ctx context.Context, viewID string) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 禁用分享
	view.DisableSharing()

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图分享已禁用",
		logger.String("view_id", viewID),
	)

	return nil
}

// RefreshShareID 刷新分享ID
func (s *ViewService) RefreshShareID(ctx context.Context, viewID string) (string, error) {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return "", pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return "", pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 刷新分享ID
	shareID, err := view.RefreshShareID()
	if err != nil {
		return "", pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return "", pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图分享ID已刷新",
		logger.String("view_id", viewID),
		logger.String("new_share_id", shareID),
	)

	return shareID, nil
}

// UpdateShareMeta 更新分享元数据
func (s *ViewService) UpdateShareMeta(
	ctx context.Context,
	viewID string,
	shareMeta map[string]interface{},
) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 更新分享元数据
	if err := view.UpdateShareMeta(shareMeta); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(err.Error())
	}

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图分享元数据更新成功",
		logger.String("view_id", viewID),
	)

	return nil
}

// LockView 锁定视图
func (s *ViewService) LockView(ctx context.Context, viewID string) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 锁定视图
	view.Lock()

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图已锁定",
		logger.String("view_id", viewID),
	)

	return nil
}

// UnlockView 解锁视图
func (s *ViewService) UnlockView(ctx context.Context, viewID string) error {
	// 1. 查找视图
	view, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 解锁视图
	view.Unlock()

	// 3. 保存更新
	if err := s.viewRepo.Update(ctx, view); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("更新视图失败: %v", err))
	}

	logger.Info("视图已解锁",
		logger.String("view_id", viewID),
	)

	return nil
}

// DeleteView 删除视图
func (s *ViewService) DeleteView(ctx context.Context, viewID string) error {
	// 1. 检查视图是否存在
	exists, err := s.viewRepo.Exists(ctx, viewID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查视图失败: %v", err))
	}
	if !exists {
		return pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 删除视图
	if err := s.viewRepo.Delete(ctx, viewID); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("删除视图失败: %v", err))
	}

	logger.Info("视图已删除",
		logger.String("view_id", viewID),
	)

	return nil
}

// DuplicateView 复制视图
func (s *ViewService) DuplicateView(
	ctx context.Context,
	viewID string,
	newName string,
	userID string,
) (*dto.ViewResponse, error) {
	// 1. 查找原视图
	originalView, err := s.viewRepo.FindByID(ctx, viewID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if originalView == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("视图不存在")
	}

	// 2. 克隆视图
	newView, err := originalView.Clone(newName, userID)
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("克隆视图失败: %v", err))
	}

	// 3. 保存新视图
	if err := s.viewRepo.Save(ctx, newView); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存视图失败: %v", err))
	}

	logger.Info("视图复制成功",
		logger.String("original_view_id", viewID),
		logger.String("new_view_id", newView.ID()),
		logger.String("new_name", newName),
	)

	return dto.FromViewEntity(newView), nil
}

// GetViewByShareID 通过分享ID获取视图
func (s *ViewService) GetViewByShareID(ctx context.Context, shareID string) (*dto.ViewResponse, error) {
	view, err := s.viewRepo.FindByShareID(ctx, shareID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找视图失败: %v", err))
	}
	if view == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("分享链接无效或已失效")
	}

	return dto.FromViewEntity(view), nil
}

// CountViews 统计表格的视图数量
func (s *ViewService) CountViews(ctx context.Context, tableID string) (int64, error) {
	count, err := s.viewRepo.Count(ctx, tableID)
	if err != nil {
		return 0, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("统计视图失败: %v", err))
	}

	return count, nil
}
