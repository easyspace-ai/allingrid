package view

import (
	"context"
	"fmt"
	"sync"
)

// ViewTypeHandler 视图类型处理器接口
type ViewTypeHandler interface {
	// GetType 获取视图类型
	GetType() ViewType

	// GetInfo 获取视图类型信息
	GetInfo() ViewTypeInfo

	// CreateDefaultConfig 创建默认配置
	CreateDefaultConfig() ViewConfig

	// ValidateConfig 验证配置
	ValidateConfig(config ViewConfig) error

	// ProcessData 处理视图数据
	ProcessData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error)

	// SupportsFeature 检查是否支持特定功能
	SupportsFeature(feature string) bool

	// GetSupportedFeatures 获取支持的功能列表
	GetSupportedFeatures() []string

	// CanTransformTo 检查是否可以转换为其他视图类型
	CanTransformTo(targetType ViewType) bool

	// TransformConfig 转换配置到其他视图类型
	TransformConfig(config ViewConfig, targetType ViewType) (ViewConfig, error)
}

// ViewTypeInfo 视图类型信息
type ViewTypeInfo struct {
	Type        ViewType `json:"type"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Icon        string   `json:"icon"`
	Category    string   `json:"category"`
	Features    []string `json:"features"`
	IsDefault   bool     `json:"is_default"`
	IsEnabled   bool     `json:"is_enabled"`
}

// ViewDataRequest 视图数据请求接口
type ViewDataRequest interface {
	GetViewID() string
	GetType() ViewType
}

// ViewDataResponse 视图数据响应接口
type ViewDataResponse interface {
	GetType() ViewType
	GetData() interface{}
}

// BaseViewDataRequest 基础视图数据请求
type BaseViewDataRequest struct {
	ViewID string   `json:"view_id"`
	Type   ViewType `json:"type"`
}

// GetViewID 获取视图ID
func (r *BaseViewDataRequest) GetViewID() string {
	return r.ViewID
}

// GetType 获取视图类型
func (r *BaseViewDataRequest) GetType() ViewType {
	return r.Type
}

// BaseViewDataResponse 基础视图数据响应
type BaseViewDataResponse struct {
	Type ViewType    `json:"type"`
	Data interface{} `json:"data"`
}

// GetType 获取视图类型
func (r *BaseViewDataResponse) GetType() ViewType {
	return r.Type
}

// GetData 获取数据
func (r *BaseViewDataResponse) GetData() interface{} {
	return r.Data
}

// ViewTypeRegistry 视图类型注册表
type ViewTypeRegistry struct {
	handlers map[ViewType]ViewTypeHandler
	mutex    sync.RWMutex
}

// NewViewTypeRegistry 创建视图类型注册表
func NewViewTypeRegistry() *ViewTypeRegistry {
	registry := &ViewTypeRegistry{
		handlers: make(map[ViewType]ViewTypeHandler),
	}

	// 注册内置视图类型处理器
	registry.registerBuiltinHandlers()

	return registry
}

// RegisterHandler 注册视图类型处理器
func (r *ViewTypeRegistry) RegisterHandler(handler ViewTypeHandler) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	viewType := handler.GetType()
	if _, exists := r.handlers[viewType]; exists {
		return fmt.Errorf("视图类型 %s 已注册", viewType)
	}

	r.handlers[viewType] = handler
	return nil
}

// GetHandler 获取视图类型处理器
func (r *ViewTypeRegistry) GetHandler(viewType ViewType) (ViewTypeHandler, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	handler, exists := r.handlers[viewType]
	if !exists {
		return nil, fmt.Errorf("未找到视图类型 %s 的处理器", viewType)
	}

	return handler, nil
}

// GetAllHandlers 获取所有视图类型处理器
func (r *ViewTypeRegistry) GetAllHandlers() map[ViewType]ViewTypeHandler {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make(map[ViewType]ViewTypeHandler)
	for k, v := range r.handlers {
		result[k] = v
	}

	return result
}

// GetAllViewTypes 获取所有视图类型信息
func (r *ViewTypeRegistry) GetAllViewTypes() []ViewTypeInfo {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var infos []ViewTypeInfo
	for _, handler := range r.handlers {
		infos = append(infos, handler.GetInfo())
	}

	return infos
}

// IsSupported 检查视图类型是否支持
func (r *ViewTypeRegistry) IsSupported(viewType ViewType) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	_, exists := r.handlers[viewType]
	return exists
}

// ProcessViewData 处理视图数据
func (r *ViewTypeRegistry) ProcessViewData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	handler, err := r.GetHandler(view.Type)
	if err != nil {
		return nil, err
	}

	return handler.ProcessData(ctx, view, request)
}

// registerBuiltinHandlers 注册内置视图类型处理器
func (r *ViewTypeRegistry) registerBuiltinHandlers() {
	r.handlers[ViewTypeGrid] = NewGridViewHandler()
	r.handlers[ViewTypeKanban] = NewKanbanViewHandler()
	r.handlers[ViewTypeCalendar] = NewCalendarViewHandler()
	r.handlers[ViewTypeGallery] = NewGalleryViewHandler()
	r.handlers[ViewTypeForm] = NewFormViewHandler()
	r.handlers[ViewTypeChart] = NewChartViewHandler()
	r.handlers[ViewTypeTimeline] = NewTimelineViewHandler()
}

// BaseViewTypeHandler 基础视图类型处理器
type BaseViewTypeHandler struct {
	viewType ViewType
	info     ViewTypeInfo
}

// GetType 获取视图类型
func (h *BaseViewTypeHandler) GetType() ViewType {
	return h.viewType
}

// GetInfo 获取视图类型信息
func (h *BaseViewTypeHandler) GetInfo() ViewTypeInfo {
	return h.info
}

// SupportsFeature 检查是否支持特定功能
func (h *BaseViewTypeHandler) SupportsFeature(feature string) bool {
	for _, f := range h.info.Features {
		if f == feature {
			return true
		}
	}
	return false
}

// GetSupportedFeatures 获取支持的功能列表
func (h *BaseViewTypeHandler) GetSupportedFeatures() []string {
	return h.info.Features
}

// GridViewHandler 网格视图处理器
type GridViewHandler struct {
	BaseViewTypeHandler
}

// NewGridViewHandler 创建网格视图处理器
func NewGridViewHandler() ViewTypeHandler {
	return &GridViewHandler{
		BaseViewTypeHandler: BaseViewTypeHandler{
			viewType: ViewTypeGrid,
			info: ViewTypeInfo{
				Type:        ViewTypeGrid,
				Name:        "网格视图",
				Description: "以表格形式展示数据，支持排序、过滤、分组等功能",
				Icon:        "grid",
				Category:    "基础",
				Features:    []string{"filter", "sort", "group", "column_resize", "row_height", "pagination"},
				IsDefault:   true,
				IsEnabled:   true,
			},
		},
	}
}

// CreateDefaultConfig 创建默认配置
func (h *GridViewHandler) CreateDefaultConfig() ViewConfig {
	return &GridViewConfig{
		BaseViewConfig: BaseViewConfig{
			Type: ViewTypeGrid,
		},
		Columns:        []GridViewColumn{},
		RowHeight:      32,
		ShowRowNumbers: true,
		ShowCheckboxes: true,
		FrozenColumns:  0,
		PageSize:       50,
	}
}

// ValidateConfig 验证配置
func (h *GridViewHandler) ValidateConfig(config ViewConfig) error {
	gridConfig, ok := config.(*GridViewConfig)
	if !ok {
		return fmt.Errorf("配置类型不匹配")
	}
	return gridConfig.Validate()
}

// ProcessData 处理视图数据
func (h *GridViewHandler) ProcessData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	gridRequest, ok := request.(*GridViewDataRequest)
	if !ok {
		return nil, fmt.Errorf("请求类型不匹配")
	}

	// 这里应该调用数据层获取实际数据
	// 暂时返回模拟数据
	data := &GridViewData{
		Records:  []map[string]interface{}{},
		Total:    0,
		Page:     gridRequest.Page,
		PageSize: gridRequest.PageSize,
		Columns:  []GridViewColumn{},
		Config:   GridViewConfig{},
	}

	return &BaseViewDataResponse{
		Type: ViewTypeGrid,
		Data: data,
	}, nil
}

// CanTransformTo 检查是否可以转换为其他视图类型
func (h *GridViewHandler) CanTransformTo(targetType ViewType) bool {
	allowedTypes := []ViewType{ViewTypeKanban, ViewTypeCalendar, ViewTypeGallery}
	for _, t := range allowedTypes {
		if t == targetType {
			return true
		}
	}
	return false
}

// TransformConfig 转换配置到其他视图类型
func (h *GridViewHandler) TransformConfig(config ViewConfig, targetType ViewType) (ViewConfig, error) {
	gridConfig, ok := config.(*GridViewConfig)
	if !ok {
		return nil, fmt.Errorf("配置类型不匹配")
	}

	switch targetType {
	case ViewTypeKanban:
		return &KanbanViewConfig{
			BaseViewConfig: gridConfig.BaseViewConfig,
			GroupFieldID:   "", // 需要用户指定
			CardFields:     []string{},
			CardHeight:     120,
			ShowCount:      true,
			AllowDrag:      true,
		}, nil
	case ViewTypeCalendar:
		return &CalendarViewConfig{
			BaseViewConfig: gridConfig.BaseViewConfig,
			DateFieldID:    "", // 需要用户指定
			TitleFieldID:   "",
			ColorFieldID:   "",
			DefaultView:    "month",
		}, nil
	case ViewTypeGallery:
		return &GalleryViewConfig{
			BaseViewConfig: gridConfig.BaseViewConfig,
			ImageFieldID:   "", // 需要用户指定
			TitleFieldID:   "",
			CardFields:     []string{},
			CardSize:       "medium",
			Columns:        4,
			AspectRatio:    "4:3",
		}, nil
	default:
		return nil, fmt.Errorf("不支持转换为 %s 视图", targetType)
	}
}

// KanbanViewHandler 看板视图处理器
type KanbanViewHandler struct {
	BaseViewTypeHandler
}

// NewKanbanViewHandler 创建看板视图处理器
func NewKanbanViewHandler() ViewTypeHandler {
	return &KanbanViewHandler{
		BaseViewTypeHandler: BaseViewTypeHandler{
			viewType: ViewTypeKanban,
			info: ViewTypeInfo{
				Type:        ViewTypeKanban,
				Name:        "看板视图",
				Description: "以看板形式展示数据，支持拖拽操作",
				Icon:        "kanban",
				Category:    "项目管理",
				Features:    []string{"filter", "sort", "drag_drop", "group_by_field"},
				IsDefault:   false,
				IsEnabled:   true,
			},
		},
	}
}

// CreateDefaultConfig 创建默认配置
func (h *KanbanViewHandler) CreateDefaultConfig() ViewConfig {
	return &KanbanViewConfig{
		BaseViewConfig: BaseViewConfig{
			Type: ViewTypeKanban,
		},
		GroupFieldID: "",
		CardFields:   []string{},
		CardHeight:   120,
		ShowCount:    true,
		AllowDrag:    true,
	}
}

// ValidateConfig 验证配置
func (h *KanbanViewHandler) ValidateConfig(config ViewConfig) error {
	kanbanConfig, ok := config.(*KanbanViewConfig)
	if !ok {
		return fmt.Errorf("配置类型不匹配")
	}
	return kanbanConfig.Validate()
}

// ProcessData 处理视图数据
func (h *KanbanViewHandler) ProcessData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	kanbanRequest, ok := request.(*KanbanViewDataRequest)
	if !ok {
		return nil, fmt.Errorf("请求类型不匹配")
	}

	// 这里应该调用数据层获取实际数据
	// 暂时返回模拟数据
	data := &KanbanViewData{
		Groups: []KanbanGroup{},
		Config: KanbanViewConfig{},
	}

	_ = kanbanRequest // 避免未使用变量警告

	return &BaseViewDataResponse{
		Type: ViewTypeKanban,
		Data: data,
	}, nil
}

// CanTransformTo 检查是否可以转换为其他视图类型
func (h *KanbanViewHandler) CanTransformTo(targetType ViewType) bool {
	allowedTypes := []ViewType{ViewTypeGrid, ViewTypeCalendar}
	for _, t := range allowedTypes {
		if t == targetType {
			return true
		}
	}
	return false
}

// TransformConfig 转换配置到其他视图类型
func (h *KanbanViewHandler) TransformConfig(config ViewConfig, targetType ViewType) (ViewConfig, error) {
	kanbanConfig, ok := config.(*KanbanViewConfig)
	if !ok {
		return nil, fmt.Errorf("配置类型不匹配")
	}

	switch targetType {
	case ViewTypeGrid:
		return &GridViewConfig{
			BaseViewConfig: kanbanConfig.BaseViewConfig,
			Columns:        []GridViewColumn{},
			RowHeight:      32,
			ShowRowNumbers: true,
			ShowCheckboxes: true,
			PageSize:       50,
		}, nil
	default:
		return nil, fmt.Errorf("不支持转换为 %s 视图", targetType)
	}
}

// 其他视图处理器的简化实现...

// CalendarViewHandler 日历视图处理器
type CalendarViewHandler struct {
	BaseViewTypeHandler
}

// NewCalendarViewHandler 创建日历视图处理器
func NewCalendarViewHandler() ViewTypeHandler {
	return &CalendarViewHandler{
		BaseViewTypeHandler: BaseViewTypeHandler{
			viewType: ViewTypeCalendar,
			info: ViewTypeInfo{
				Type:        ViewTypeCalendar,
				Name:        "日历视图",
				Description: "以日历形式展示数据",
				Icon:        "calendar",
				Category:    "时间管理",
				Features:    []string{"filter", "date_field", "color_coding", "event_details"},
				IsDefault:   false,
				IsEnabled:   true,
			},
		},
	}
}

// CreateDefaultConfig 创建默认配置
func (h *CalendarViewHandler) CreateDefaultConfig() ViewConfig {
	return &CalendarViewConfig{
		BaseViewConfig: BaseViewConfig{Type: ViewTypeCalendar},
		DefaultView:    "month",
	}
}

// ValidateConfig 验证配置
func (h *CalendarViewHandler) ValidateConfig(config ViewConfig) error {
	calendarConfig, ok := config.(*CalendarViewConfig)
	if !ok {
		return fmt.Errorf("配置类型不匹配")
	}
	return calendarConfig.Validate()
}

// ProcessData 处理视图数据
func (h *CalendarViewHandler) ProcessData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	return &BaseViewDataResponse{Type: ViewTypeCalendar, Data: &CalendarViewData{}}, nil
}

// CanTransformTo 检查是否可以转换为其他视图类型
func (h *CalendarViewHandler) CanTransformTo(targetType ViewType) bool {
	return targetType == ViewTypeGrid || targetType == ViewTypeKanban
}

// TransformConfig 转换配置到其他视图类型
func (h *CalendarViewHandler) TransformConfig(config ViewConfig, targetType ViewType) (ViewConfig, error) {
	return nil, fmt.Errorf("暂不支持配置转换")
}

// 其他视图处理器的占位符实现
func NewGalleryViewHandler() ViewTypeHandler {
	return &BaseViewTypeHandler{
		viewType: ViewTypeGallery,
		info: ViewTypeInfo{
			Type: ViewTypeGallery, Name: "画廊视图", Description: "以画廊形式展示数据",
			Icon: "gallery", Category: "展示", Features: []string{"filter", "sort", "image_field"},
		},
	}
}

func NewFormViewHandler() ViewTypeHandler {
	return &BaseViewTypeHandler{
		viewType: ViewTypeForm,
		info: ViewTypeInfo{
			Type: ViewTypeForm, Name: "表单视图", Description: "以表单形式展示和编辑数据",
			Icon: "form", Category: "输入", Features: []string{"field_order", "field_visibility", "validation"},
		},
	}
}

func NewChartViewHandler() ViewTypeHandler {
	return &BaseViewTypeHandler{
		viewType: ViewTypeChart,
		info: ViewTypeInfo{
			Type: ViewTypeChart, Name: "图表视图", Description: "以图表形式展示数据统计",
			Icon: "chart", Category: "分析", Features: []string{"chart_type", "aggregation", "group_by"},
		},
	}
}

func NewTimelineViewHandler() ViewTypeHandler {
	return &BaseViewTypeHandler{
		viewType: ViewTypeTimeline,
		info: ViewTypeInfo{
			Type: ViewTypeTimeline, Name: "时间线视图", Description: "以时间线形式展示数据",
			Icon: "timeline", Category: "时间管理", Features: []string{"filter", "date_field", "milestone"},
		},
	}
}

// 为基础处理器添加默认实现
func (h *BaseViewTypeHandler) CreateDefaultConfig() ViewConfig {
	return &BaseViewConfig{Type: h.viewType}
}

func (h *BaseViewTypeHandler) ValidateConfig(config ViewConfig) error {
	return config.Validate()
}

func (h *BaseViewTypeHandler) ProcessData(ctx context.Context, view *View, request ViewDataRequest) (ViewDataResponse, error) {
	return &BaseViewDataResponse{Type: h.viewType, Data: nil}, nil
}

func (h *BaseViewTypeHandler) CanTransformTo(targetType ViewType) bool {
	return false
}

func (h *BaseViewTypeHandler) TransformConfig(config ViewConfig, targetType ViewType) (ViewConfig, error) {
	return nil, fmt.Errorf("不支持配置转换")
}

// 全局视图类型注册表实例
var globalViewTypeRegistry *ViewTypeRegistry
var registryOnce sync.Once

// GetGlobalViewTypeRegistry 获取全局视图类型注册表
func GetGlobalViewTypeRegistry() *ViewTypeRegistry {
	registryOnce.Do(func() {
		globalViewTypeRegistry = NewViewTypeRegistry()
	})
	return globalViewTypeRegistry
}

// RegisterViewTypeHandler 注册视图类型处理器到全局注册表
func RegisterViewTypeHandler(handler ViewTypeHandler) error {
	return GetGlobalViewTypeRegistry().RegisterHandler(handler)
}
