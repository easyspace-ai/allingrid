package view

import (
	"context"
	"fmt"
)

// ViewDataProcessor 视图数据处理器
type ViewDataProcessor struct {
	registry *ViewTypeRegistry
}

// NewViewDataProcessor 创建视图数据处理器
func NewViewDataProcessor() *ViewDataProcessor {
	return &ViewDataProcessor{
		registry: GetGlobalViewTypeRegistry(),
	}
}

// ProcessViewData 处理视图数据
func (p *ViewDataProcessor) ProcessViewData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	// 验证视图类型
	if !p.registry.IsSupported(view.Type) {
		return nil, fmt.Errorf("不支持的视图类型: %s", view.Type)
	}

	// 获取处理器
	handler, err := p.registry.GetHandler(view.Type)
	if err != nil {
		return nil, err
	}

	// 处理数据
	return handler.ProcessData(ctx, view, request)
}

// ValidateViewConfig 验证视图配置
func (p *ViewDataProcessor) ValidateViewConfig(viewType ViewType, config ViewConfig) error {
	handler, err := p.registry.GetHandler(viewType)
	if err != nil {
		return err
	}

	return handler.ValidateConfig(config)
}

// CreateDefaultConfig 创建默认配置
func (p *ViewDataProcessor) CreateDefaultConfig(viewType ViewType) (ViewConfig, error) {
	handler, err := p.registry.GetHandler(viewType)
	if err != nil {
		return nil, err
	}

	return handler.CreateDefaultConfig(), nil
}

// TransformView 转换视图类型
func (p *ViewDataProcessor) TransformView(view *View, targetType ViewType) (*View, error) {
	// 检查是否可以转换
	handler, err := p.registry.GetHandler(view.Type)
	if err != nil {
		return nil, err
	}

	if !handler.CanTransformTo(targetType) {
		return nil, fmt.Errorf("不支持从 %s 视图转换为 %s 视图", view.Type, targetType)
	}

	// 转换配置
	newConfig, err := handler.TransformConfig(view.GetParsedConfig(), targetType)
	if err != nil {
		return nil, err
	}

	// 创建新视图
	newView := view.Clone(view.Name+" (转换)", view.CreatedBy)
	newView.Type = targetType
	newView.Config = newConfig.ToMap()
	newView.parsedConfig = newConfig

	return newView, nil
}

// GetViewTypeInfo 获取视图类型信息
func (p *ViewDataProcessor) GetViewTypeInfo(viewType ViewType) (ViewTypeInfo, error) {
	handler, err := p.registry.GetHandler(viewType)
	if err != nil {
		return ViewTypeInfo{}, err
	}

	return handler.GetInfo(), nil
}

// GetAllViewTypeInfos 获取所有视图类型信息
func (p *ViewDataProcessor) GetAllViewTypeInfos() []ViewTypeInfo {
	return p.registry.GetAllViewTypes()
}

// CheckFeatureSupport 检查视图类型是否支持特定功能
func (p *ViewDataProcessor) CheckFeatureSupport(viewType ViewType, feature string) (bool, error) {
	handler, err := p.registry.GetHandler(viewType)
	if err != nil {
		return false, err
	}

	return handler.SupportsFeature(feature), nil
}

// GetSupportedFeatures 获取视图类型支持的功能列表
func (p *ViewDataProcessor) GetSupportedFeatures(viewType ViewType) ([]string, error) {
	handler, err := p.registry.GetHandler(viewType)
	if err != nil {
		return nil, err
	}

	return handler.GetSupportedFeatures(), nil
}

// ViewDataCache 视图数据缓存
type ViewDataCache struct {
	cache map[string]ViewDataResponse
}

// NewViewDataCache 创建视图数据缓存
func NewViewDataCache() *ViewDataCache {
	return &ViewDataCache{
		cache: make(map[string]ViewDataResponse),
	}
}

// Get 获取缓存数据
func (c *ViewDataCache) Get(key string) (ViewDataResponse, bool) {
	data, exists := c.cache[key]
	return data, exists
}

// Set 设置缓存数据
func (c *ViewDataCache) Set(key string, data ViewDataResponse) {
	c.cache[key] = data
}

// Delete 删除缓存数据
func (c *ViewDataCache) Delete(key string) {
	delete(c.cache, key)
}

// Clear 清空缓存
func (c *ViewDataCache) Clear() {
	c.cache = make(map[string]ViewDataResponse)
}

// CachedViewDataProcessor 带缓存的视图数据处理器
type CachedViewDataProcessor struct {
	processor *ViewDataProcessor
	cache     *ViewDataCache
}

// NewCachedViewDataProcessor 创建带缓存的视图数据处理器
func NewCachedViewDataProcessor() *CachedViewDataProcessor {
	return &CachedViewDataProcessor{
		processor: NewViewDataProcessor(),
		cache:     NewViewDataCache(),
	}
}

// ProcessViewData 处理视图数据（带缓存）
func (p *CachedViewDataProcessor) ProcessViewData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	// 生成缓存键
	cacheKey := fmt.Sprintf("%s:%s:%v", view.ID, view.Type, request)

	// 尝试从缓存获取
	if data, exists := p.cache.Get(cacheKey); exists {
		return data, nil
	}

	// 处理数据
	data, err := p.processor.ProcessViewData(ctx, view, request)
	if err != nil {
		return nil, err
	}

	// 缓存结果
	p.cache.Set(cacheKey, data)

	return data, nil
}

// InvalidateCache 使缓存失效
func (p *CachedViewDataProcessor) InvalidateCache(viewID string) {
	// 简化实现：清空所有缓存
	// 实际应用中应该只清空相关的缓存项
	p.cache.Clear()
}

// ViewDataSynchronizer 视图数据同步器
type ViewDataSynchronizer struct {
	processor *ViewDataProcessor
}

// NewViewDataSynchronizer 创建视图数据同步器
func NewViewDataSynchronizer() *ViewDataSynchronizer {
	return &ViewDataSynchronizer{
		processor: NewViewDataProcessor(),
	}
}

// SyncViewData 同步视图数据
func (s *ViewDataSynchronizer) SyncViewData(ctx context.Context, views []*View) error {
	for _, view := range views {
		// 这里应该实现视图数据的同步逻辑
		// 例如：更新缓存、通知客户端等
		_ = view
	}
	return nil
}

// SyncViewConfig 同步视图配置
func (s *ViewDataSynchronizer) SyncViewConfig(ctx context.Context, view *View) error {
	// 验证配置
	if err := s.processor.ValidateViewConfig(view.Type, view.GetParsedConfig()); err != nil {
		return fmt.Errorf("视图配置验证失败: %v", err)
	}

	// 这里应该实现配置同步逻辑
	// 例如：更新数据库、通知相关服务等

	return nil
}

// ViewDataOptimizer 视图数据优化器
type ViewDataOptimizer struct {
	processor *ViewDataProcessor
}

// NewViewDataOptimizer 创建视图数据优化器
func NewViewDataOptimizer() *ViewDataOptimizer {
	return &ViewDataOptimizer{
		processor: NewViewDataProcessor(),
	}
}

// OptimizeViewQuery 优化视图查询
func (o *ViewDataOptimizer) OptimizeViewQuery(view *View, request ViewDataRequest) (ViewDataRequest, error) {
	// 根据视图类型和配置优化查询
	switch view.Type {
	case ViewTypeGrid:
		return o.optimizeGridQuery(view, request)
	case ViewTypeKanban:
		return o.optimizeKanbanQuery(view, request)
	case ViewTypeCalendar:
		return o.optimizeCalendarQuery(view, request)
	default:
		return request, nil
	}
}

// optimizeGridQuery 优化网格视图查询
func (o *ViewDataOptimizer) optimizeGridQuery(view *View, request ViewDataRequest) (ViewDataRequest, error) {
	gridRequest, ok := request.(*GridViewDataRequest)
	if !ok {
		return request, nil
	}

	// 优化分页大小
	if gridRequest.PageSize > 1000 {
		gridRequest.PageSize = 1000
	}
	if gridRequest.PageSize <= 0 {
		gridRequest.PageSize = 50
	}

	// 优化排序条件
	if len(gridRequest.Sorts) > 5 {
		gridRequest.Sorts = gridRequest.Sorts[:5] // 限制排序字段数量
	}

	return gridRequest, nil
}

// optimizeKanbanQuery 优化看板视图查询
func (o *ViewDataOptimizer) optimizeKanbanQuery(view *View, request ViewDataRequest) (ViewDataRequest, error) {
	// 看板视图优化逻辑
	return request, nil
}

// optimizeCalendarQuery 优化日历视图查询
func (o *ViewDataOptimizer) optimizeCalendarQuery(view *View, request ViewDataRequest) (ViewDataRequest, error) {
	// 日历视图优化逻辑
	return request, nil
}

// ViewDataValidator 视图数据验证器
type ViewDataValidator struct {
	processor *ViewDataProcessor
}

// NewViewDataValidator 创建视图数据验证器
func NewViewDataValidator() *ViewDataValidator {
	return &ViewDataValidator{
		processor: NewViewDataProcessor(),
	}
}

// ValidateViewData 验证视图数据
func (v *ViewDataValidator) ValidateViewData(view *View, data ViewDataResponse) error {
	// 检查数据类型是否匹配
	if data.GetType() != view.Type {
		return fmt.Errorf("数据类型不匹配: 期望 %s，实际 %s", view.Type, data.GetType())
	}

	// 根据视图类型进行具体验证
	switch view.Type {
	case ViewTypeGrid:
		return v.validateGridData(view, data)
	case ViewTypeKanban:
		return v.validateKanbanData(view, data)
	case ViewTypeCalendar:
		return v.validateCalendarData(view, data)
	default:
		return nil
	}
}

// validateGridData 验证网格视图数据
func (v *ViewDataValidator) validateGridData(view *View, data ViewDataResponse) error {
	gridData, ok := data.GetData().(*GridViewData)
	if !ok {
		return fmt.Errorf("网格视图数据类型错误")
	}

	// 验证分页信息
	if gridData.Page < 1 {
		return fmt.Errorf("页码必须大于0")
	}
	if gridData.PageSize < 1 || gridData.PageSize > 1000 {
		return fmt.Errorf("每页大小必须在1-1000之间")
	}

	return nil
}

// validateKanbanData 验证看板视图数据
func (v *ViewDataValidator) validateKanbanData(view *View, data ViewDataResponse) error {
	kanbanData, ok := data.GetData().(*KanbanViewData)
	if !ok {
		return fmt.Errorf("看板视图数据类型错误")
	}

	// 验证分组数据
	if len(kanbanData.Groups) == 0 {
		return fmt.Errorf("看板视图必须有至少一个分组")
	}

	return nil
}

// validateCalendarData 验证日历视图数据
func (v *ViewDataValidator) validateCalendarData(view *View, data ViewDataResponse) error {
	calendarData, ok := data.GetData().(*CalendarViewData)
	if !ok {
		return fmt.Errorf("日历视图数据类型错误")
	}

	// 验证事件数据
	for _, event := range calendarData.Events {
		if event.ID == "" {
			return fmt.Errorf("日历事件必须有ID")
		}
		if event.Title == "" {
			return fmt.Errorf("日历事件必须有标题")
		}
	}

	return nil
}
