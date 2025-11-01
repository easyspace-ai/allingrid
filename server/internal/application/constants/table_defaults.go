package constants

// Table 默认值常量
// 参考 teable-develop/apps/nestjs-backend/src/features/table/constant.ts

// DefaultViewName 默认视图名称
const DefaultViewName = "Grid view"

// DefaultViewType 默认视图类型
const DefaultViewType = "grid"

// DefaultFieldName 默认字段名称
const DefaultFieldName = "name"

// DefaultFieldType 默认字段类型
const DefaultFieldType = "text"

// ViewConfig 视图配置
type ViewConfig struct {
	Name        string                   `json:"name"`
	Type        string                   `json:"type"`
	Description string                   `json:"description,omitempty"`
	ColumnMeta  []map[string]interface{} `json:"columnMeta,omitempty"`
}

// FieldConfig 字段配置
type FieldConfig struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Description string                 `json:"description,omitempty"`
	Required    bool                   `json:"required"`
	Unique      bool                   `json:"unique"`
	IsPrimary   bool                   `json:"isPrimary,omitempty"`
	Options     map[string]interface{} `json:"options,omitempty"`
}

// GetDefaultViews 获取默认视图配置
// 对应 Teable 的 DEFAULT_VIEWS
func GetDefaultViews() []ViewConfig {
	return []ViewConfig{
		{
			Name:       DefaultViewName,
			Type:       DefaultViewType,
			ColumnMeta: nil, // 使用nil表示空的ColumnMeta，数据库会存储为NULL
		},
	}
}

// GetDefaultFields 获取默认字段配置
// 对应 Teable 的 DEFAULT_FIELDS
// 根据 UI 需求，新建表时默认创建三个字段：文本字段、单选字段、日期字段
func GetDefaultFields() []FieldConfig {
	return []FieldConfig{
		{
			Name:      "文本",   // 文本字段，对应图片中的 "A≡ 文本"
			Type:      "text", // 文本字段类型
			Required:  false,
			Unique:    false,
			IsPrimary: true, // 第一个字段默认为主字段
			Options:   map[string]interface{}{
				// 可以根据需要添加文本字段的选项，比如是否支持选择等
			},
		},
		{
			Name:     "单选", // 单选字段，对应图片中的 "单选"
			Type:     "singleSelect",
			Required: false,
			Unique:   false,
			Options: map[string]interface{}{
				"choices": []map[string]interface{}{
					{
						"id":    "opt1",
						"name":  "选项1",
						"color": "blue",
					},
					{
						"id":    "opt2",
						"name":  "选项2",
						"color": "green",
					},
				},
				"preventAutoNewOptions": true, // 防止自动添加新选项
			},
		},
		{
			Name:     "日期", // 日期字段，对应图片中的 "日期"
			Type:     "date",
			Required: false,
			Unique:   false,
			Options: map[string]interface{}{
				"format":       "YYYY-MM-DD",
				"include_time": false, // 默认只包含日期，不包含时间
			},
		},
	}
}
