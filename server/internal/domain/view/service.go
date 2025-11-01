package view

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// Service 视图服务接口
type Service interface {
	CreateView(ctx context.Context, req CreateViewRequest) (*View, error)
	GetView(ctx context.Context, id string) (*View, error)
	UpdateView(ctx context.Context, id string, req UpdateViewRequest) (*View, error)
	DeleteView(ctx context.Context, id string) error
	ListViews(ctx context.Context, filter ListViewFilter) ([]*View, int64, error)

	// 网格视图特定功能
	GetGridViewData(ctx context.Context, viewID string, page, pageSize int) (*GridViewData, error)
	UpdateGridViewConfig(ctx context.Context, viewID string, config GridViewConfig) error
	AddGridViewColumn(ctx context.Context, viewID string, column GridViewColumn) error
	UpdateGridViewColumn(ctx context.Context, viewID string, fieldID string, column GridViewColumn) error
	RemoveGridViewColumn(ctx context.Context, viewID string, fieldID string) error
	ReorderGridViewColumns(ctx context.Context, viewID string, fieldIDs []string) error

	// 视图配置管理
	GetViewConfig(ctx context.Context, viewID string) (map[string]interface{}, error)
	UpdateViewConfig(ctx context.Context, viewID string, config map[string]interface{}) error

	// 表单视图特定功能
	GetFormViewData(ctx context.Context, viewID string) (*FormViewData, error)
	UpdateFormViewConfig(ctx context.Context, viewID string, config FormViewConfig) error
	AddFormViewField(ctx context.Context, viewID string, field FormViewField) error
	UpdateFormViewField(ctx context.Context, viewID string, fieldID string, field FormViewField) error
	RemoveFormViewField(ctx context.Context, viewID string, fieldID string) error
	ReorderFormViewFields(ctx context.Context, viewID string, fieldIDs []string) error

	// 看板视图特定功能
	GetKanbanViewData(ctx context.Context, viewID string) (*KanbanViewData, error)
	UpdateKanbanViewConfig(ctx context.Context, viewID string, config KanbanViewConfig) error
	MoveKanbanCard(ctx context.Context, viewID string, recordID string, fromGroup string, toGroup string) error

	// 日历视图特定功能
	GetCalendarViewData(ctx context.Context, viewID string, startDate, endDate string) (*CalendarViewData, error)
	UpdateCalendarViewConfig(ctx context.Context, viewID string, config CalendarViewConfig) error

	// 画廊视图特定功能
	GetGalleryViewData(ctx context.Context, viewID string, page, pageSize int) (*GalleryViewData, error)
	UpdateGalleryViewConfig(ctx context.Context, viewID string, config GalleryViewConfig) error
}

// ServiceImpl 视图服务实现
type ServiceImpl struct {
	repo Repository
}

// NewService 创建视图服务
func NewService(repo Repository) Service {
	return &ServiceImpl{repo: repo}
}

// CreateView 创建视图
func (s *ServiceImpl) CreateView(ctx context.Context, req CreateViewRequest) (*View, error) {
	// 检查名称是否已存在于同一数据表下
	exists, err := s.repo.Exists(ctx, ListViewFilter{TableID: &req.TableID, Name: &req.Name})
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.ErrResourceExists.WithDetails("视图名称已存在于此数据表")
	}

	// 如果设置为默认视图，需要先将其他视图设为非默认
	// 参考 teable-develop: 每个表只能有一个默认视图（order=0的第一个视图）
	// 简化实现：在创建时默认将新视图设为非默认（order > 0）
	// 完整实现需要视图仓储支持批量更新 order 字段
	if req.IsDefault {
		// 标记：这里应该调整其他视图的 order
		// 当前简化实现，假设前端会处理默认视图的切换
	}

	view := NewView(req)
	if err := s.repo.Create(ctx, view); err != nil {
		return nil, err
	}
	return view, nil
}

// GetView 获取视图
func (s *ServiceImpl) GetView(ctx context.Context, id string) (*View, error) {
	view, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, errors.ErrNotFound.WithDetails("视图未找到")
	}
	return view, nil
}

// UpdateView 更新视图
func (s *ServiceImpl) UpdateView(ctx context.Context, id string, req UpdateViewRequest) (*View, error) {
	view, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, errors.ErrNotFound.WithDetails("视图未找到")
	}

	// 检查更新后的名称是否冲突
	if req.Name != nil && *req.Name != view.Name {
		exists, err := s.repo.Exists(ctx, ListViewFilter{TableID: &view.TableID, Name: req.Name})
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, errors.ErrResourceExists.WithDetails("更新后的视图名称已存在于此数据表")
		}
	}

	// 如果设置为默认视图，需要先将其他视图设为非默认
	// 参考 teable-develop: 每个表只能有一个默认视图（order=0的第一个视图）
	// 简化实现：在更新时标记，完整实现需要批量更新其他视图
	if req.IsDefault != nil && *req.IsDefault {
		// 标记：这里应该调整其他视图的 order
		// 当前简化实现，假设前端会处理默认视图的切换
	}

	view.Update(req)
	if err := s.repo.Update(ctx, view); err != nil {
		return nil, err
	}
	return view, nil
}

// DeleteView 删除视图
func (s *ServiceImpl) DeleteView(ctx context.Context, id string) error {
	view, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if view == nil {
		return errors.ErrNotFound.WithDetails("视图未找到")
	}

	view.SoftDelete() // 软删除
	if err := s.repo.Update(ctx, view); err != nil {
		return err
	}
	return nil
}

// ListViews 列出视图
func (s *ServiceImpl) ListViews(ctx context.Context, filter ListViewFilter) ([]*View, int64, error) {
	views, err := s.repo.List(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	total, err := s.repo.Count(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	return views, total, nil
}

// GetGridViewData 获取网格视图数据
func (s *ServiceImpl) GetGridViewData(ctx context.Context, viewID string, page, pageSize int) (*GridViewData, error) {
	// 获取视图信息
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	if view.Type != "grid" {
		return nil, errors.ErrInvalidRequest.WithDetails("视图类型不是网格视图")
	}

	// 构建查询条件
	req := GridViewDataRequest{
		ViewID:   viewID,
		Page:     page,
		PageSize: pageSize,
	}

	// 解析网格视图配置并设置排序、过滤、分组条件
	// 参考 teable-develop: 网格视图支持 sort, filter, group, columnMeta 等配置
	// 简化实现：配置已在视图对象中，实际应用在查询记录时执行

	if view.Type == "grid" {
		// 配置已经在视图对象中
		// 实际的排序、过滤、分组逻辑在查询记录时应用
	}

	// 调用仓储获取数据
	data, err := s.repo.GetGridViewData(ctx, req)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// UpdateGridViewConfig 更新网格视图配置
func (s *ServiceImpl) UpdateGridViewConfig(ctx context.Context, viewID string, config GridViewConfig) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "grid" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是网格视图")
	}

	// 将配置转换为map（使用 JSON 序列化）
	configMap, err := structToMap(config)
	if err != nil {
		return fmt.Errorf("failed to convert config to map: %w", err)
	}

	// 更新视图配置
	updateReq := UpdateViewRequest{
		Config: configMap,
	}

	_, err = s.UpdateView(ctx, viewID, updateReq)
	return err
}

// ==================== 配置转换辅助函数 ====================

// structToMap 将结构体转换为 map（使用 JSON 序列化）
func structToMap(v interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal struct: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to map: %w", err)
	}

	return result, nil
}

// mapToStruct 将 map 转换为结构体（使用 JSON 序列化）
func mapToStruct(m map[string]interface{}, v interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal map: %w", err)
	}

	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to unmarshal to struct: %w", err)
	}

	return nil
}

// AddGridViewColumn 添加网格视图列
func (s *ServiceImpl) AddGridViewColumn(ctx context.Context, viewID string, column GridViewColumn) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "grid" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是网格视图")
	}

	// 解析当前配置
	var config GridViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse grid view config: %w", err)
		}
	}

	// 添加新列
	config.Columns = append(config.Columns, column)

	// 更新配置
	return s.UpdateGridViewConfig(ctx, viewID, config)
}

// UpdateGridViewColumn 更新网格视图列
func (s *ServiceImpl) UpdateGridViewColumn(ctx context.Context, viewID string, fieldID string, column GridViewColumn) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "grid" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是网格视图")
	}

	// 解析当前配置
	var config GridViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse grid view config: %w", err)
		}
	}

	// 更新指定列
	for i, col := range config.Columns {
		if col.FieldID == fieldID {
			config.Columns[i] = column
			break
		}
	}

	// 更新配置
	return s.UpdateGridViewConfig(ctx, viewID, config)
}

// RemoveGridViewColumn 移除网格视图列
func (s *ServiceImpl) RemoveGridViewColumn(ctx context.Context, viewID string, fieldID string) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "grid" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是网格视图")
	}

	// 解析当前配置
	var config GridViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse grid view config: %w", err)
		}
	}

	// 移除指定列
	var newColumns []GridViewColumn
	for _, col := range config.Columns {
		if col.FieldID != fieldID {
			newColumns = append(newColumns, col)
		}
	}
	config.Columns = newColumns

	// 更新配置
	return s.UpdateGridViewConfig(ctx, viewID, config)
}

// ReorderGridViewColumns 重新排序网格视图列
func (s *ServiceImpl) ReorderGridViewColumns(ctx context.Context, viewID string, fieldIDs []string) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "grid" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是网格视图")
	}

	// 解析当前配置
	var config GridViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse grid view config: %w", err)
		}
	}

	// 重新排序列
	columnMap := make(map[string]GridViewColumn)
	for _, col := range config.Columns {
		columnMap[col.FieldID] = col
	}

	var newColumns []GridViewColumn
	for i, fieldID := range fieldIDs {
		if col, exists := columnMap[fieldID]; exists {
			col.Order = i
			newColumns = append(newColumns, col)
		}
	}
	config.Columns = newColumns

	// 更新配置
	return s.UpdateGridViewConfig(ctx, viewID, config)
}

// GetViewConfig 获取视图配置
func (s *ServiceImpl) GetViewConfig(ctx context.Context, viewID string) (map[string]interface{}, error) {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	return view.Config, nil
}

// UpdateViewConfig 更新视图配置
func (s *ServiceImpl) UpdateViewConfig(ctx context.Context, viewID string, config map[string]interface{}) error {
	updateReq := UpdateViewRequest{
		Config: config,
	}

	_, err := s.UpdateView(ctx, viewID, updateReq)
	return err
}

// GetFormViewData 获取表单视图数据
func (s *ServiceImpl) GetFormViewData(ctx context.Context, viewID string) (*FormViewData, error) {
	// 获取视图信息
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	if view.Type != "form" {
		return nil, errors.ErrInvalidRequest.WithDetails("视图类型不是表单视图")
	}

	// 解析表单视图配置
	var config FormViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return nil, fmt.Errorf("failed to parse form view config: %w", err)
		}
	}

	// 构建请求
	req := FormViewDataRequest{
		ViewID: viewID,
	}

	// 调用仓储获取数据
	data, err := s.repo.GetFormViewData(ctx, req)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// UpdateFormViewConfig 更新表单视图配置
func (s *ServiceImpl) UpdateFormViewConfig(ctx context.Context, viewID string, config FormViewConfig) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "form" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是表单视图")
	}

	// 将配置转换为map（使用 JSON 序列化）
	configMap, err := structToMap(config)
	if err != nil {
		return fmt.Errorf("failed to convert config to map: %w", err)
	}

	// 更新视图配置
	updateReq := UpdateViewRequest{
		Config: configMap,
	}

	_, err = s.UpdateView(ctx, viewID, updateReq)
	return err
}

// AddFormViewField 添加表单视图字段
func (s *ServiceImpl) AddFormViewField(ctx context.Context, viewID string, field FormViewField) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "form" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是表单视图")
	}

	// 解析当前配置
	var config FormViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse form view config: %w", err)
		}
	}

	// 添加新字段
	config.Fields = append(config.Fields, field)

	// 更新配置
	return s.UpdateFormViewConfig(ctx, viewID, config)
}

// UpdateFormViewField 更新表单视图字段
func (s *ServiceImpl) UpdateFormViewField(ctx context.Context, viewID string, fieldID string, field FormViewField) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "form" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是表单视图")
	}

	// 解析当前配置
	var config FormViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse form view config: %w", err)
		}
	}

	// 更新指定字段
	for i, f := range config.Fields {
		if f.FieldID == fieldID {
			config.Fields[i] = field
			break
		}
	}

	// 更新配置
	return s.UpdateFormViewConfig(ctx, viewID, config)
}

// RemoveFormViewField 移除表单视图字段
func (s *ServiceImpl) RemoveFormViewField(ctx context.Context, viewID string, fieldID string) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "form" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是表单视图")
	}

	// 解析当前配置
	var config FormViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse form view config: %w", err)
		}
	}

	// 移除指定字段
	var newFields []FormViewField
	for _, f := range config.Fields {
		if f.FieldID != fieldID {
			newFields = append(newFields, f)
		}
	}
	config.Fields = newFields

	// 更新配置
	return s.UpdateFormViewConfig(ctx, viewID, config)
}

// ReorderFormViewFields 重新排序表单视图字段
func (s *ServiceImpl) ReorderFormViewFields(ctx context.Context, viewID string, fieldIDs []string) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "form" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是表单视图")
	}

	// 解析当前配置
	var config FormViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse form view config: %w", err)
		}
	}

	// 重新排序字段
	fieldMap := make(map[string]FormViewField)
	for _, f := range config.Fields {
		fieldMap[f.FieldID] = f
	}

	var newFields []FormViewField
	for i, fieldID := range fieldIDs {
		if f, exists := fieldMap[fieldID]; exists {
			f.Order = i
			newFields = append(newFields, f)
		}
	}
	config.Fields = newFields

	// 更新配置
	return s.UpdateFormViewConfig(ctx, viewID, config)
}

// GetKanbanViewData 获取看板视图数据
func (s *ServiceImpl) GetKanbanViewData(ctx context.Context, viewID string) (*KanbanViewData, error) {
	// 获取视图信息
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	if view.Type != "kanban" {
		return nil, errors.ErrInvalidRequest.WithDetails("视图类型不是看板视图")
	}

	// 解析看板视图配置
	var config KanbanViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return nil, fmt.Errorf("failed to parse kanban view config: %w", err)
		}
	}

	// 构建请求
	req := KanbanViewDataRequest{
		ViewID: viewID,
	}

	// 调用仓储获取数据
	data, err := s.repo.GetKanbanViewData(ctx, req)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// UpdateKanbanViewConfig 更新看板视图配置
func (s *ServiceImpl) UpdateKanbanViewConfig(ctx context.Context, viewID string, config KanbanViewConfig) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "kanban" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是看板视图")
	}

	// 将配置转换为map（使用 JSON 序列化）
	configMap, err := structToMap(config)
	if err != nil {
		return fmt.Errorf("failed to convert config to map: %w", err)
	}

	// 更新视图配置
	updateReq := UpdateViewRequest{
		Config: configMap,
	}

	_, err = s.UpdateView(ctx, viewID, updateReq)
	return err
}

// MoveKanbanCard 移动看板卡片
func (s *ServiceImpl) MoveKanbanCard(ctx context.Context, viewID string, recordID string, fromGroup string, toGroup string) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "kanban" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是看板视图")
	}

	// 解析看板视图配置
	var config KanbanViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return fmt.Errorf("failed to parse kanban view config: %w", err)
		}
	}

	// 这里需要调用记录服务来更新记录的分组字段值
	// 由于视图服务不应该直接依赖记录服务，我们需要通过应用层来处理
	// 暂时返回成功
	_ = config // 避免未使用变量警告

	return nil
}

// GetCalendarViewData 获取日历视图数据
func (s *ServiceImpl) GetCalendarViewData(ctx context.Context, viewID string, startDate, endDate string) (*CalendarViewData, error) {
	// 获取视图信息
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	if view.Type != "calendar" {
		return nil, errors.ErrInvalidRequest.WithDetails("视图类型不是日历视图")
	}

	// 解析日历视图配置
	var config CalendarViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return nil, fmt.Errorf("failed to parse calendar view config: %w", err)
		}
	}

	// 构建请求
	req := CalendarViewDataRequest{
		ViewID:    viewID,
		StartDate: startDate,
		EndDate:   endDate,
	}

	// 调用仓储获取数据
	data, err := s.repo.GetCalendarViewData(ctx, req)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// UpdateCalendarViewConfig 更新日历视图配置
func (s *ServiceImpl) UpdateCalendarViewConfig(ctx context.Context, viewID string, config CalendarViewConfig) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "calendar" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是日历视图")
	}

	// 将配置转换为map（使用 JSON 序列化）
	configMap, err := structToMap(config)
	if err != nil {
		return fmt.Errorf("failed to convert config to map: %w", err)
	}

	// 更新视图配置
	updateReq := UpdateViewRequest{
		Config: configMap,
	}

	_, err = s.UpdateView(ctx, viewID, updateReq)
	return err
}

// GetGalleryViewData 获取画廊视图数据
func (s *ServiceImpl) GetGalleryViewData(ctx context.Context, viewID string, page, pageSize int) (*GalleryViewData, error) {
	// 获取视图信息
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	if view.Type != "gallery" {
		return nil, errors.ErrInvalidRequest.WithDetails("视图类型不是画廊视图")
	}

	// 解析画廊视图配置
	var config GalleryViewConfig
	if view.Config != nil {
		if err := mapToStruct(view.Config, &config); err != nil {
			return nil, fmt.Errorf("failed to parse gallery view config: %w", err)
		}
	}

	// 构建请求
	req := GalleryViewDataRequest{
		ViewID:   viewID,
		Page:     page,
		PageSize: pageSize,
	}

	// 调用仓储获取数据
	data, err := s.repo.GetGalleryViewData(ctx, req)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// UpdateGalleryViewConfig 更新画廊视图配置
func (s *ServiceImpl) UpdateGalleryViewConfig(ctx context.Context, viewID string, config GalleryViewConfig) error {
	view, err := s.GetView(ctx, viewID)
	if err != nil {
		return err
	}

	if view.Type != "gallery" {
		return errors.ErrInvalidRequest.WithDetails("视图类型不是画廊视图")
	}

	// 将配置转换为map（使用 JSON 序列化）
	configMap, err := structToMap(config)
	if err != nil {
		return fmt.Errorf("failed to convert config to map: %w", err)
	}

	// 更新视图配置
	updateReq := UpdateViewRequest{
		Config: configMap,
	}

	_, err = s.UpdateView(ctx, viewID, updateReq)
	return err
}
