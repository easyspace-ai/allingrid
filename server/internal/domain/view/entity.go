package view

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// ViewType 视图类型枚举
type ViewType string

const (
	ViewTypeGrid     ViewType = "grid"     // 网格视图
	ViewTypeKanban   ViewType = "kanban"   // 看板视图
	ViewTypeCalendar ViewType = "calendar" // 日历视图
	ViewTypeGallery  ViewType = "gallery"  // 画廊视图
	ViewTypeForm     ViewType = "form"     // 表单视图
	ViewTypeChart    ViewType = "chart"    // 图表视图
	ViewTypeTimeline ViewType = "timeline" // 时间线视图
)

// View 数据视图实体 - 支持多种视图类型
type View struct {
	ID               string                 `json:"id"`
	TableID          string                 `json:"table_id"`
	Name             string                 `json:"name"`
	Description      *string                `json:"description"`
	Type             ViewType               `json:"type"`        // 视图类型
	Config           map[string]interface{} `json:"config"`      // 视图配置，存储为JSON
	IsDefault        bool                   `json:"is_default"`  // 是否为默认视图
	IsPublic         bool                   `json:"is_public"`   // 是否为公共视图
	ShareToken       *string                `json:"share_token"` // 分享令牌
	Version          int64                  `json:"version"`     // 视图版本号
	CreatedBy        string                 `json:"created_by"`
	CreatedTime      time.Time              `json:"created_at"`
	DeletedTime      *time.Time             `json:"deleted_time"`
	LastModifiedTime *time.Time             `json:"updated_at"`

	// 内部字段，不序列化
	parsedConfig ViewConfig `json:"-"` // 解析后的配置
}

// CreateViewRequest 创建视图请求
type CreateViewRequest struct {
	TableID     string
	Name        string
	Description *string
	Type        string
	Config      map[string]interface{}
	IsDefault   bool
	CreatedBy   string
}

// UpdateViewRequest 更新视图请求
type UpdateViewRequest struct {
	Name        *string
	Description *string
	Type        *string
	Config      map[string]interface{}
	IsDefault   *bool
}

// ListViewFilter 视图列表过滤条件
type ListViewFilter struct {
	TableID   *string
	Name      *string
	Type      *string
	CreatedBy *string
	Search    string
	OrderBy   string
	Order     string
	Limit     int
	Offset    int
}

// ViewConfig 视图配置接口
type ViewConfig interface {
	GetType() ViewType
	Validate() error
	ToMap() map[string]interface{}
	FromMap(data map[string]interface{}) error
}

// BaseViewConfig 基础视图配置
type BaseViewConfig struct {
	Type          ViewType               `json:"type"`
	Filters       []ViewFilter           `json:"filters,omitempty"`        // 过滤条件
	Sorts         []ViewSort             `json:"sorts,omitempty"`          // 排序条件
	Groups        []ViewGroup            `json:"groups,omitempty"`         // 分组条件
	VisibleFields []string               `json:"visible_fields,omitempty"` // 可见字段
	HiddenFields  []string               `json:"hidden_fields,omitempty"`  // 隐藏字段
	CustomFields  map[string]interface{} `json:"custom_fields,omitempty"`  // 自定义字段配置
}

// ViewFilter 视图过滤条件
type ViewFilter struct {
	FieldID  string      `json:"field_id"`
	Operator string      `json:"operator"` // eq, ne, gt, gte, lt, lte, like, in, not_in, between, is_empty, is_not_empty
	Value    interface{} `json:"value"`
	Logic    string      `json:"logic"`              // and, or
	GroupID  *string     `json:"group_id,omitempty"` // 过滤组ID
}

// ViewSort 视图排序条件
type ViewSort struct {
	FieldID   string `json:"field_id"`
	Direction string `json:"direction"` // asc, desc
	Priority  int    `json:"priority"`  // 排序优先级
}

// ViewGroup 视图分组条件
type ViewGroup struct {
	FieldID   string `json:"field_id"`
	Direction string `json:"direction"` // asc, desc
	Collapsed bool   `json:"collapsed"` // 是否折叠
}

// NewView 创建新的数据视图
func NewView(req CreateViewRequest) *View {
	now := time.Now()
	view := &View{
		ID:               utils.GenerateViewID(),
		TableID:          req.TableID,
		Name:             req.Name,
		Description:      req.Description,
		Type:             ViewType(req.Type),
		Config:           make(map[string]interface{}),
		IsDefault:        req.IsDefault,
		IsPublic:         false,
		Version:          1,
		CreatedBy:        req.CreatedBy,
		CreatedTime:      now,
		LastModifiedTime: &now,
	}

	// 设置配置
	if req.Config != nil {
		view.Config = req.Config
	}

	// 解析配置
	view.parseConfig()

	return view
}

// GetSupportedViewTypes 获取支持的视图类型
func GetSupportedViewTypes() []ViewType {
	return []ViewType{
		ViewTypeGrid,
		ViewTypeKanban,
		ViewTypeCalendar,
		ViewTypeGallery,
		ViewTypeForm,
		ViewTypeChart,
		ViewTypeTimeline,
	}
}

// IsValidViewType 检查视图类型是否有效
func IsValidViewType(viewType string) bool {
	supportedTypes := GetSupportedViewTypes()
	for _, t := range supportedTypes {
		if string(t) == viewType {
			return true
		}
	}
	return false
}

// GetViewTypeInfo 获取视图类型信息
func GetViewTypeInfo(viewType ViewType) map[string]interface{} {
	typeInfos := map[ViewType]map[string]interface{}{
		ViewTypeGrid: {
			"name":        "网格视图",
			"description": "以表格形式展示数据",
			"icon":        "grid",
			"features":    []string{"filter", "sort", "group", "column_resize", "row_height"},
		},
		ViewTypeKanban: {
			"name":        "看板视图",
			"description": "以看板形式展示数据",
			"icon":        "kanban",
			"features":    []string{"filter", "sort", "drag_drop", "group_by_field"},
		},
		ViewTypeCalendar: {
			"name":        "日历视图",
			"description": "以日历形式展示数据",
			"icon":        "calendar",
			"features":    []string{"filter", "date_field", "color_coding", "event_details"},
		},
		ViewTypeGallery: {
			"name":        "画廊视图",
			"description": "以卡片画廊形式展示数据",
			"icon":        "gallery",
			"features":    []string{"filter", "sort", "image_field", "card_size"},
		},
		ViewTypeForm: {
			"name":        "表单视图",
			"description": "以表单形式展示和编辑数据",
			"icon":        "form",
			"features":    []string{"field_order", "field_visibility", "validation", "layout"},
		},
		ViewTypeChart: {
			"name":        "图表视图",
			"description": "以图表形式展示数据统计",
			"icon":        "chart",
			"features":    []string{"chart_type", "aggregation", "group_by", "color_scheme"},
		},
		ViewTypeTimeline: {
			"name":        "时间线视图",
			"description": "以时间线形式展示数据",
			"icon":        "timeline",
			"features":    []string{"filter", "date_field", "milestone", "duration"},
		},
	}

	if info, exists := typeInfos[viewType]; exists {
		return info
	}

	return map[string]interface{}{
		"name":        "未知视图",
		"description": "未知的视图类型",
		"icon":        "unknown",
		"features":    []string{},
	}
}

// ValidateConfig 验证视图配置
func (v *View) ValidateConfig() error {
	if v.parsedConfig != nil {
		return v.parsedConfig.Validate()
	}

	// 基础验证
	if v.Type == "" {
		return fmt.Errorf("视图类型不能为空")
	}

	if !IsValidViewType(string(v.Type)) {
		return fmt.Errorf("不支持的视图类型: %s", v.Type)
	}

	return nil
}

// parseConfig 解析视图配置
func (v *View) parseConfig() error {
	if v.Config == nil {
		v.Config = make(map[string]interface{})
		// 获取默认配置
		configManager := GetGlobalConfigManager()
		defaultConfig, err := configManager.GetDefaultConfig(v.Type)
		if err != nil {
			return err
		}
		v.parsedConfig = defaultConfig
		v.Config = defaultConfig.ToMap()
		return nil
	}

	// 使用配置管理器解析配置
	configManager := GetGlobalConfigManager()
	parsedConfig, err := configManager.ParseConfig(v.Type, v.Config)
	if err != nil {
		return err
	}

	v.parsedConfig = parsedConfig
	return nil
}

// mapToStruct 将map转换为结构体
func (v *View) mapToStruct(data map[string]interface{}, target interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, target)
}

// GetParsedConfig 获取解析后的配置
func (v *View) GetParsedConfig() ViewConfig {
	return v.parsedConfig
}

// UpdateConfig 更新视图配置
func (v *View) UpdateConfig(config map[string]interface{}) error {
	// 使用配置管理器验证和解析配置
	configManager := GetGlobalConfigManager()

	// 解析新配置
	newParsedConfig, err := configManager.ParseConfig(v.Type, config)
	if err != nil {
		return err
	}

	// 验证配置
	if err := configManager.ValidateConfig(newParsedConfig); err != nil {
		return err
	}

	// 更新配置
	v.Config = config
	v.parsedConfig = newParsedConfig
	v.incrementVersion()
	return nil
}

// SetPublic 设置视图为公共视图
func (v *View) SetPublic(isPublic bool) {
	v.IsPublic = isPublic
	if isPublic && v.ShareToken == nil {
		token := utils.GenerateNanoID(32)
		v.ShareToken = &token
	} else if !isPublic {
		v.ShareToken = nil
	}
	v.incrementVersion()
}

// GenerateShareToken 生成分享令牌
func (v *View) GenerateShareToken() string {
	token := utils.GenerateNanoID(32)
	v.ShareToken = &token
	v.incrementVersion()
	return token
}

// RevokeShareToken 撤销分享令牌
func (v *View) RevokeShareToken() {
	v.ShareToken = nil
	v.IsPublic = false
	v.incrementVersion()
}

// CanBeShared 检查视图是否可以被分享
func (v *View) CanBeShared() bool {
	// 某些视图类型可能不支持分享
	switch v.Type {
	case ViewTypeForm:
		return true // 表单视图通常需要分享
	case ViewTypeGrid, ViewTypeKanban, ViewTypeCalendar, ViewTypeGallery:
		return true
	default:
		return false
	}
}

// Clone 克隆视图
func (v *View) Clone(newName string, createdBy string) *View {
	now := time.Now()

	// 深拷贝配置
	configCopy := make(map[string]interface{})
	for k, val := range v.Config {
		configCopy[k] = val
	}

	cloned := &View{
		ID:               utils.GenerateViewID(),
		TableID:          v.TableID,
		Name:             newName,
		Description:      v.Description,
		Type:             v.Type,
		Config:           configCopy,
		IsDefault:        false, // 克隆的视图不能是默认视图
		IsPublic:         false, // 克隆的视图不是公共视图
		ShareToken:       nil,
		Version:          1,
		CreatedBy:        createdBy,
		CreatedTime:      now,
		LastModifiedTime: &now,
	}

	// 解析配置
	cloned.parseConfig()

	return cloned
}

// Update 更新视图信息
func (v *View) Update(req UpdateViewRequest) error {
	if req.Name != nil {
		v.Name = *req.Name
	}
	if req.Description != nil {
		v.Description = req.Description
	}
	if req.Type != nil {
		newType := ViewType(*req.Type)
		if !IsValidViewType(string(newType)) {
			return fmt.Errorf("不支持的视图类型: %s", newType)
		}
		v.Type = newType
	}
	if req.Config != nil {
		if err := v.UpdateConfig(req.Config); err != nil {
			return err
		}
	}
	if req.IsDefault != nil {
		v.IsDefault = *req.IsDefault
	}

	v.incrementVersion()
	return nil
}

// SoftDelete 软删除视图
func (v *View) SoftDelete() {
	now := time.Now()
	v.DeletedTime = &now
	v.LastModifiedTime = &now
	v.incrementVersion()
}

// Restore 恢复已删除的视图
func (v *View) Restore() {
	v.DeletedTime = nil
	now := time.Now()
	v.LastModifiedTime = &now
	v.incrementVersion()
}

// IsDeleted 检查视图是否已删除
func (v *View) IsDeleted() bool {
	return v.DeletedTime != nil
}

// incrementVersion 递增版本号
func (v *View) incrementVersion() {
	v.Version++
	now := time.Now()
	v.LastModifiedTime = &now
}

// GridViewConfig 网格视图配置
type GridViewConfig struct {
	BaseViewConfig
	// 列配置
	Columns []GridViewColumn `json:"columns"`
	// 行配置
	RowHeight int `json:"row_height"` // 行高
	// 显示配置
	ShowRowNumbers bool `json:"show_row_numbers"` // 显示行号
	ShowCheckboxes bool `json:"show_checkboxes"`  // 显示复选框
	FrozenColumns  int  `json:"frozen_columns"`   // 冻结列数
	// 分页配置
	PageSize int `json:"page_size"` // 每页显示数量
}

// GetType 获取视图类型
func (c *GridViewConfig) GetType() ViewType {
	return ViewTypeGrid
}

// Validate 验证配置
func (c *GridViewConfig) Validate() error {
	if c.PageSize <= 0 {
		c.PageSize = 50 // 默认每页50条
	}
	if c.PageSize > 1000 {
		return fmt.Errorf("每页显示数量不能超过1000")
	}
	if c.RowHeight < 20 {
		c.RowHeight = 32 // 默认行高
	}
	return nil
}

// ToMap 转换为map
func (c *GridViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *GridViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// GridViewColumn 网格视图列配置
type GridViewColumn struct {
	FieldID    string `json:"field_id"`   // 字段ID
	FieldName  string `json:"field_name"` // 字段名称
	FieldType  string `json:"field_type"` // 字段类型
	Width      int    `json:"width"`      // 列宽
	Visible    bool   `json:"visible"`    // 是否可见
	Frozen     bool   `json:"frozen"`     // 是否冻结
	Sortable   bool   `json:"sortable"`   // 是否可排序
	Filterable bool   `json:"filterable"` // 是否可过滤
	Order      int    `json:"order"`      // 显示顺序
}

// GridViewSort 网格视图排序配置
type GridViewSort struct {
	FieldID string `json:"field_id"` // 字段ID
	Order   string `json:"order"`    // 排序方向：asc, desc
}

// GridViewFilter 网格视图过滤配置
type GridViewFilter struct {
	FieldID  string      `json:"field_id"` // 字段ID
	Operator string      `json:"operator"` // 操作符：eq, ne, gt, gte, lt, lte, like, in, not_in, between
	Value    interface{} `json:"value"`    // 过滤值
	Logic    string      `json:"logic"`    // 逻辑关系：and, or
}

// GridViewGroup 网格视图分组配置
type GridViewGroup struct {
	FieldID   string `json:"field_id"`  // 字段ID
	Order     string `json:"order"`     // 分组顺序：asc, desc
	Collapsed bool   `json:"collapsed"` // 是否折叠
}

// KanbanViewConfig 看板视图配置
type KanbanViewConfig struct {
	BaseViewConfig
	GroupFieldID string   `json:"group_field_id"` // 分组字段ID
	CardFields   []string `json:"card_fields"`    // 卡片显示字段
	CardHeight   int      `json:"card_height"`    // 卡片高度
	ShowCount    bool     `json:"show_count"`     // 显示数量
	AllowDrag    bool     `json:"allow_drag"`     // 允许拖拽
}

// GetType 获取视图类型
func (c *KanbanViewConfig) GetType() ViewType {
	return ViewTypeKanban
}

// Validate 验证配置
func (c *KanbanViewConfig) Validate() error {
	if c.GroupFieldID == "" {
		return fmt.Errorf("看板视图必须指定分组字段")
	}
	if c.CardHeight <= 0 {
		c.CardHeight = 120 // 默认卡片高度
	}
	return nil
}

// ToMap 转换为map
func (c *KanbanViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *KanbanViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// CalendarViewConfig 日历视图配置
type CalendarViewConfig struct {
	BaseViewConfig
	DateFieldID    string `json:"date_field_id"`    // 日期字段ID
	TitleFieldID   string `json:"title_field_id"`   // 标题字段ID
	ColorFieldID   string `json:"color_field_id"`   // 颜色字段ID
	StartTimeField string `json:"start_time_field"` // 开始时间字段
	EndTimeField   string `json:"end_time_field"`   // 结束时间字段
	AllDay         bool   `json:"all_day"`          // 全天事件
	DefaultView    string `json:"default_view"`     // 默认视图：month, week, day
}

// GetType 获取视图类型
func (c *CalendarViewConfig) GetType() ViewType {
	return ViewTypeCalendar
}

// Validate 验证配置
func (c *CalendarViewConfig) Validate() error {
	if c.DateFieldID == "" {
		return fmt.Errorf("日历视图必须指定日期字段")
	}
	if c.DefaultView == "" {
		c.DefaultView = "month" // 默认月视图
	}
	validViews := []string{"month", "week", "day"}
	isValid := false
	for _, view := range validViews {
		if c.DefaultView == view {
			isValid = true
			break
		}
	}
	if !isValid {
		return fmt.Errorf("无效的默认视图类型: %s", c.DefaultView)
	}
	return nil
}

// ToMap 转换为map
func (c *CalendarViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *CalendarViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// GalleryViewConfig 画廊视图配置
type GalleryViewConfig struct {
	BaseViewConfig
	ImageFieldID    string   `json:"image_field_id"`    // 图片字段ID
	TitleFieldID    string   `json:"title_field_id"`    // 标题字段ID
	SubtitleFieldID string   `json:"subtitle_field_id"` // 副标题字段ID
	CardFields      []string `json:"card_fields"`       // 卡片显示字段
	CardSize        string   `json:"card_size"`         // 卡片大小：small, medium, large
	Columns         int      `json:"columns"`           // 列数
	AspectRatio     string   `json:"aspect_ratio"`      // 宽高比：1:1, 4:3, 16:9
}

// GetType 获取视图类型
func (c *GalleryViewConfig) GetType() ViewType {
	return ViewTypeGallery
}

// Validate 验证配置
func (c *GalleryViewConfig) Validate() error {
	if c.CardSize == "" {
		c.CardSize = "medium" // 默认中等大小
	}
	validSizes := []string{"small", "medium", "large"}
	isValid := false
	for _, size := range validSizes {
		if c.CardSize == size {
			isValid = true
			break
		}
	}
	if !isValid {
		return fmt.Errorf("无效的卡片大小: %s", c.CardSize)
	}

	if c.Columns <= 0 {
		c.Columns = 4 // 默认4列
	}
	if c.Columns > 12 {
		return fmt.Errorf("列数不能超过12")
	}

	if c.AspectRatio == "" {
		c.AspectRatio = "4:3" // 默认宽高比
	}

	return nil
}

// ToMap 转换为map
func (c *GalleryViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *GalleryViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// FormViewConfig 表单视图配置
type FormViewConfig struct {
	BaseViewConfig
	Fields      []FormViewField `json:"fields"`       // 表单字段配置
	Layout      string          `json:"layout"`       // 布局：single, multi
	ColumnCount int             `json:"column_count"` // 列数
	ShowTitle   bool            `json:"show_title"`   // 显示标题
	ShowSubmit  bool            `json:"show_submit"`  // 显示提交按钮
}

// GetType 获取视图类型
func (c *FormViewConfig) GetType() ViewType {
	return ViewTypeForm
}

// Validate 验证配置
func (c *FormViewConfig) Validate() error {
	if c.Layout == "" {
		c.Layout = "single" // 默认单列布局
	}
	validLayouts := []string{"single", "multi"}
	isValid := false
	for _, layout := range validLayouts {
		if c.Layout == layout {
			isValid = true
			break
		}
	}
	if !isValid {
		return fmt.Errorf("无效的布局类型: %s", c.Layout)
	}

	if c.ColumnCount <= 0 {
		c.ColumnCount = 1 // 默认1列
	}
	if c.ColumnCount > 4 {
		return fmt.Errorf("列数不能超过4")
	}

	return nil
}

// ToMap 转换为map
func (c *FormViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *FormViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// GetType 获取基础视图类型
func (c *BaseViewConfig) GetType() ViewType {
	return c.Type
}

// Validate 验证基础配置
func (c *BaseViewConfig) Validate() error {
	// 基础验证逻辑
	return nil
}

// ToMap 转换为map
func (c *BaseViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *BaseViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// FormViewField 表单视图字段配置
type FormViewField struct {
	FieldID   string `json:"field_id"`   // 字段ID
	FieldName string `json:"field_name"` // 字段名称
	FieldType string `json:"field_type"` // 字段类型
	Required  bool   `json:"required"`   // 是否必填
	Visible   bool   `json:"visible"`    // 是否可见
	ReadOnly  bool   `json:"read_only"`  // 是否只读
	Width     int    `json:"width"`      // 字段宽度
	Order     int    `json:"order"`      // 显示顺序
}

// GridViewData 网格视图数据
type GridViewData struct {
	Records  []map[string]interface{} `json:"records"`   // 记录数据
	Total    int64                    `json:"total"`     // 总记录数
	Page     int                      `json:"page"`      // 当前页码
	PageSize int                      `json:"page_size"` // 每页大小
	Columns  []GridViewColumn         `json:"columns"`   // 列配置
	Config   GridViewConfig           `json:"config"`    // 视图配置
}

// GridViewDataRequest 网格视图数据请求
type GridViewDataRequest struct {
	ViewID   string           `json:"view_id"`
	Page     int              `json:"page"`
	PageSize int              `json:"page_size"`
	Sorts    []GridViewSort   `json:"sorts"`
	Filters  []GridViewFilter `json:"filters"`
	Groups   []GridViewGroup  `json:"groups"`
}

// FormViewData 表单视图数据
type FormViewData struct {
	Fields []FormViewField `json:"fields"` // 表单字段配置
	Config FormViewConfig  `json:"config"` // 表单配置
}

// FormViewDataRequest 表单视图数据请求
type FormViewDataRequest struct {
	ViewID string `json:"view_id"`
}

// KanbanViewData 看板视图数据
type KanbanViewData struct {
	Groups []KanbanGroup    `json:"groups"` // 看板分组
	Config KanbanViewConfig `json:"config"` // 看板配置
}

// KanbanGroup 看板分组
type KanbanGroup struct {
	ID        string                   `json:"id"`        // 分组ID
	Name      string                   `json:"name"`      // 分组名称
	Value     interface{}              `json:"value"`     // 分组值
	Cards     []map[string]interface{} `json:"cards"`     // 卡片数据
	Count     int                      `json:"count"`     // 卡片数量
	Collapsed bool                     `json:"collapsed"` // 是否折叠
}

// KanbanViewDataRequest 看板视图数据请求
type KanbanViewDataRequest struct {
	ViewID string `json:"view_id"`
}

// MoveKanbanCardRequest 移动看板卡片请求
type MoveKanbanCardRequest struct {
	ViewID    string `json:"view_id"`
	RecordID  string `json:"record_id"`
	FromGroup string `json:"from_group"`
	ToGroup   string `json:"to_group"`
}

// CalendarViewData 日历视图数据
type CalendarViewData struct {
	Events []CalendarEvent    `json:"events"` // 日历事件
	Config CalendarViewConfig `json:"config"` // 日历配置
}

// CalendarEvent 日历事件
type CalendarEvent struct {
	ID        string                 `json:"id"`         // 事件ID（记录ID）
	Title     string                 `json:"title"`      // 事件标题
	StartTime string                 `json:"start_time"` // 开始时间
	EndTime   string                 `json:"end_time"`   // 结束时间
	AllDay    bool                   `json:"all_day"`    // 是否全天事件
	Color     string                 `json:"color"`      // 事件颜色
	Data      map[string]interface{} `json:"data"`       // 其他数据
}

// CalendarViewDataRequest 日历视图数据请求
type CalendarViewDataRequest struct {
	ViewID    string `json:"view_id"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

// GalleryViewData 画廊视图数据
type GalleryViewData struct {
	Cards    []GalleryCard     `json:"cards"`     // 画廊卡片
	Total    int64             `json:"total"`     // 总卡片数
	Page     int               `json:"page"`      // 当前页码
	PageSize int               `json:"page_size"` // 每页大小
	Config   GalleryViewConfig `json:"config"`    // 画廊配置
}

// GalleryCard 画廊卡片
type GalleryCard struct {
	ID       string                 `json:"id"`       // 卡片ID（记录ID）
	Image    string                 `json:"image"`    // 图片URL
	Title    string                 `json:"title"`    // 标题
	Subtitle string                 `json:"subtitle"` // 副标题
	Data     map[string]interface{} `json:"data"`     // 其他数据
}

// GalleryViewDataRequest 画廊视图数据请求
type GalleryViewDataRequest struct {
	ViewID   string `json:"view_id"`
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
}

// GetViewID 实现ViewDataRequest接口
func (r *GridViewDataRequest) GetViewID() string {
	return r.ViewID
}

// GetType 实现ViewDataRequest接口
func (r *GridViewDataRequest) GetType() ViewType {
	return ViewTypeGrid
}

// GetViewID 实现ViewDataRequest接口
func (r *FormViewDataRequest) GetViewID() string {
	return r.ViewID
}

// GetType 实现ViewDataRequest接口
func (r *FormViewDataRequest) GetType() ViewType {
	return ViewTypeForm
}

// GetViewID 实现ViewDataRequest接口
func (r *KanbanViewDataRequest) GetViewID() string {
	return r.ViewID
}

// GetType 实现ViewDataRequest接口
func (r *KanbanViewDataRequest) GetType() ViewType {
	return ViewTypeKanban
}

// GetViewID 实现ViewDataRequest接口
func (r *CalendarViewDataRequest) GetViewID() string {
	return r.ViewID
}

// GetType 实现ViewDataRequest接口
func (r *CalendarViewDataRequest) GetType() ViewType {
	return ViewTypeCalendar
}

// GetViewID 实现ViewDataRequest接口
func (r *GalleryViewDataRequest) GetViewID() string {
	return r.ViewID
}

// GetType 实现ViewDataRequest接口
func (r *GalleryViewDataRequest) GetType() ViewType {
	return ViewTypeGallery
}
