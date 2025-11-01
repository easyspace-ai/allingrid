package view

import (
	"encoding/json"
	"fmt"
)

// ConfigManager 视图配置管理器
type ConfigManager struct{}

// NewConfigManager 创建配置管理器
func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

// ParseConfig 解析视图配置
func (cm *ConfigManager) ParseConfig(viewType ViewType, configData map[string]interface{}) (ViewConfig, error) {
	switch viewType {
	case ViewTypeGrid:
		var config GridViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析网格视图配置失败: %v", err)
		}
		config.Type = ViewTypeGrid
		return &config, nil

	case ViewTypeKanban:
		var config KanbanViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析看板视图配置失败: %v", err)
		}
		config.Type = ViewTypeKanban
		return &config, nil

	case ViewTypeCalendar:
		var config CalendarViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析日历视图配置失败: %v", err)
		}
		config.Type = ViewTypeCalendar
		return &config, nil

	case ViewTypeGallery:
		var config GalleryViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析画廊视图配置失败: %v", err)
		}
		config.Type = ViewTypeGallery
		return &config, nil

	case ViewTypeForm:
		var config FormViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析表单视图配置失败: %v", err)
		}
		config.Type = ViewTypeForm
		return &config, nil

	case ViewTypeChart:
		var config ChartViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析图表视图配置失败: %v", err)
		}
		config.Type = ViewTypeChart
		return &config, nil

	case ViewTypeTimeline:
		var config TimelineViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析时间线视图配置失败: %v", err)
		}
		config.Type = ViewTypeTimeline
		return &config, nil

	default:
		// 使用基础配置
		var config BaseViewConfig
		if err := cm.mapToStruct(configData, &config); err != nil {
			return nil, fmt.Errorf("解析基础视图配置失败: %v", err)
		}
		config.Type = viewType
		return &config, nil
	}
}

// SerializeConfig 序列化视图配置
func (cm *ConfigManager) SerializeConfig(config ViewConfig) (map[string]interface{}, error) {
	return config.ToMap(), nil
}

// ValidateConfig 验证视图配置
func (cm *ConfigManager) ValidateConfig(config ViewConfig) error {
	return config.Validate()
}

// GetDefaultConfig 获取默认配置
func (cm *ConfigManager) GetDefaultConfig(viewType ViewType) (ViewConfig, error) {
	switch viewType {
	case ViewTypeGrid:
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
		}, nil

	case ViewTypeKanban:
		return &KanbanViewConfig{
			BaseViewConfig: BaseViewConfig{
				Type: ViewTypeKanban,
			},
			GroupFieldID: "",
			CardFields:   []string{},
			CardHeight:   120,
			ShowCount:    true,
			AllowDrag:    true,
		}, nil

	case ViewTypeCalendar:
		return &CalendarViewConfig{
			BaseViewConfig: BaseViewConfig{
				Type: ViewTypeCalendar,
			},
			DateFieldID:    "",
			TitleFieldID:   "",
			ColorFieldID:   "",
			StartTimeField: "",
			EndTimeField:   "",
			AllDay:         false,
			DefaultView:    "month",
		}, nil

	case ViewTypeGallery:
		return &GalleryViewConfig{
			BaseViewConfig: BaseViewConfig{
				Type: ViewTypeGallery,
			},
			ImageFieldID:    "",
			TitleFieldID:    "",
			SubtitleFieldID: "",
			CardFields:      []string{},
			CardSize:        "medium",
			Columns:         4,
			AspectRatio:     "4:3",
		}, nil

	case ViewTypeForm:
		return &FormViewConfig{
			BaseViewConfig: BaseViewConfig{
				Type: ViewTypeForm,
			},
			Fields:      []FormViewField{},
			Layout:      "single",
			ColumnCount: 1,
			ShowTitle:   true,
			ShowSubmit:  true,
		}, nil

	case ViewTypeChart:
		return &ChartViewConfig{
			BaseViewConfig: BaseViewConfig{
				Type: ViewTypeChart,
			},
			ChartType:    "bar",
			XAxisField:   "",
			YAxisField:   "",
			GroupByField: "",
			Aggregation:  "count",
			ColorScheme:  "default",
		}, nil

	case ViewTypeTimeline:
		return &TimelineViewConfig{
			BaseViewConfig: BaseViewConfig{
				Type: ViewTypeTimeline,
			},
			StartDateField: "",
			EndDateField:   "",
			TitleField:     "",
			ColorField:     "",
			ShowMilestone:  true,
			ShowDuration:   true,
		}, nil

	default:
		return &BaseViewConfig{
			Type: viewType,
		}, nil
	}
}

// MergeConfig 合并配置
func (cm *ConfigManager) MergeConfig(baseConfig, updateConfig ViewConfig) (ViewConfig, error) {
	if baseConfig.GetType() != updateConfig.GetType() {
		return nil, fmt.Errorf("配置类型不匹配")
	}

	// 将更新配置转换为map
	updateMap := updateConfig.ToMap()
	baseMap := baseConfig.ToMap()

	// 合并配置
	for key, value := range updateMap {
		if value != nil {
			baseMap[key] = value
		}
	}

	// 重新解析配置
	return cm.ParseConfig(baseConfig.GetType(), baseMap)
}

// mapToStruct 将map转换为结构体
func (cm *ConfigManager) mapToStruct(data map[string]interface{}, target interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, target)
}

// structToMap 将结构体转换为map
func (cm *ConfigManager) structToMap(source interface{}) (map[string]interface{}, error) {
	jsonData, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(jsonData, &result)
	return result, err
}

// ChartViewConfig 图表视图配置
type ChartViewConfig struct {
	BaseViewConfig
	ChartType    string `json:"chart_type"`     // 图表类型：bar, line, pie, area, scatter
	XAxisField   string `json:"x_axis_field"`   // X轴字段
	YAxisField   string `json:"y_axis_field"`   // Y轴字段
	GroupByField string `json:"group_by_field"` // 分组字段
	Aggregation  string `json:"aggregation"`    // 聚合方式：count, sum, avg, min, max
	ColorScheme  string `json:"color_scheme"`   // 颜色方案
}

// GetType 获取视图类型
func (c *ChartViewConfig) GetType() ViewType {
	return ViewTypeChart
}

// Validate 验证配置
func (c *ChartViewConfig) Validate() error {
	validChartTypes := []string{"bar", "line", "pie", "area", "scatter"}
	isValid := false
	for _, chartType := range validChartTypes {
		if c.ChartType == chartType {
			isValid = true
			break
		}
	}
	if !isValid {
		return fmt.Errorf("无效的图表类型: %s", c.ChartType)
	}

	validAggregations := []string{"count", "sum", "avg", "min", "max"}
	isValid = false
	for _, agg := range validAggregations {
		if c.Aggregation == agg {
			isValid = true
			break
		}
	}
	if !isValid {
		return fmt.Errorf("无效的聚合方式: %s", c.Aggregation)
	}

	return nil
}

// ToMap 转换为map
func (c *ChartViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *ChartViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// TimelineViewConfig 时间线视图配置
type TimelineViewConfig struct {
	BaseViewConfig
	StartDateField string `json:"start_date_field"` // 开始日期字段
	EndDateField   string `json:"end_date_field"`   // 结束日期字段
	TitleField     string `json:"title_field"`      // 标题字段
	ColorField     string `json:"color_field"`      // 颜色字段
	ShowMilestone  bool   `json:"show_milestone"`   // 显示里程碑
	ShowDuration   bool   `json:"show_duration"`    // 显示持续时间
}

// GetType 获取视图类型
func (c *TimelineViewConfig) GetType() ViewType {
	return ViewTypeTimeline
}

// Validate 验证配置
func (c *TimelineViewConfig) Validate() error {
	if c.StartDateField == "" {
		return fmt.Errorf("时间线视图必须指定开始日期字段")
	}
	return nil
}

// ToMap 转换为map
func (c *TimelineViewConfig) ToMap() map[string]interface{} {
	data, _ := json.Marshal(c)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// FromMap 从map构建配置
func (c *TimelineViewConfig) FromMap(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, c)
}

// ViewConfigValidator 视图配置验证器
type ViewConfigValidator struct {
	configManager *ConfigManager
}

// NewViewConfigValidator 创建配置验证器
func NewViewConfigValidator() *ViewConfigValidator {
	return &ViewConfigValidator{
		configManager: NewConfigManager(),
	}
}

// ValidateViewConfig 验证视图配置
func (v *ViewConfigValidator) ValidateViewConfig(viewType ViewType, configData map[string]interface{}) error {
	config, err := v.configManager.ParseConfig(viewType, configData)
	if err != nil {
		return err
	}

	return config.Validate()
}

// ValidateViewTypeTransition 验证视图类型转换
func (v *ViewConfigValidator) ValidateViewTypeTransition(fromType, toType ViewType) error {
	// 定义允许的视图类型转换
	allowedTransitions := map[ViewType][]ViewType{
		ViewTypeGrid:     {ViewTypeKanban, ViewTypeCalendar, ViewTypeGallery},
		ViewTypeKanban:   {ViewTypeGrid, ViewTypeCalendar},
		ViewTypeCalendar: {ViewTypeGrid, ViewTypeKanban},
		ViewTypeGallery:  {ViewTypeGrid},
		ViewTypeForm:     {},                 // 表单视图不允许转换为其他类型
		ViewTypeChart:    {},                 // 图表视图不允许转换为其他类型
		ViewTypeTimeline: {ViewTypeCalendar}, // 时间线视图可以转换为日历视图
	}

	if allowed, exists := allowedTransitions[fromType]; exists {
		for _, allowedType := range allowed {
			if allowedType == toType {
				return nil
			}
		}
	}

	return fmt.Errorf("不支持从 %s 视图转换为 %s 视图", fromType, toType)
}

// GetCompatibleFieldTypes 获取视图类型兼容的字段类型
func (v *ViewConfigValidator) GetCompatibleFieldTypes(viewType ViewType) []string {
	compatibleTypes := map[ViewType][]string{
		ViewTypeGrid: {
			"text", "number", "boolean", "date", "datetime", "time",
			"select", "multi_select", "email", "url", "phone",
			"currency", "percent", "rating", "progress",
			"image", "file", "link", "lookup", "rollup", "formula",
		},
		ViewTypeKanban: {
			"text", "select", "multi_select", "boolean", "date",
			"number", "currency", "percent", "rating",
		},
		ViewTypeCalendar: {
			"date", "datetime", "text", "select", "multi_select",
		},
		ViewTypeGallery: {
			"image", "file", "text", "select", "multi_select",
			"date", "number", "currency",
		},
		ViewTypeForm: {
			"text", "number", "boolean", "date", "datetime", "time",
			"select", "multi_select", "radio", "checkbox",
			"email", "url", "phone", "currency", "percent",
			"rating", "progress", "image", "file",
		},
		ViewTypeChart: {
			"number", "currency", "percent", "date", "datetime",
			"select", "multi_select", "boolean",
		},
		ViewTypeTimeline: {
			"date", "datetime", "text", "select", "multi_select",
		},
	}

	if types, exists := compatibleTypes[viewType]; exists {
		return types
	}

	return []string{}
}

// 全局配置管理器实例
var globalConfigManager *ConfigManager

// GetGlobalConfigManager 获取全局配置管理器
func GetGlobalConfigManager() *ConfigManager {
	if globalConfigManager == nil {
		globalConfigManager = NewConfigManager()
	}
	return globalConfigManager
}
