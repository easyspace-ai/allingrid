package valueobject

// FieldOptions 字段选项值对象
// 不同字段类型有不同的选项配置
type FieldOptions struct {
	// Formula 选项
	Formula *FormulaOptions

	// Rollup 选项
	Rollup *RollupOptions

	// Lookup 选项
	Lookup *LookupOptions

	// Link 选项
	Link *LinkOptions

	// Select 选项
	Select *SelectOptions

	// Number 选项
	Number *NumberOptions

	// Date 选项
	Date *DateOptions

	// AI 选项
	AI *AIOptions

	// Count 选项
	Count *CountOptions

	// Duration 选项
	Duration *DurationOptions

	// Button 选项
	Button *ButtonOptions

	// User 选项
	User *UserOptions

	// Rating 选项
	Rating *RatingOptions

	// 通用配置（可选，某些字段类型会使用）
	ShowAs     *ShowAsOptions     `json:"showAs,omitempty"`
	Formatting *FormattingOptions `json:"formatting,omitempty"`
}

// ShowAsOptions 显示方式配置（参考 Teable）
type ShowAsOptions struct {
	Type   string                 `json:"type,omitempty"`   // bar, line, ring, etc.
	Color  string                 `json:"color,omitempty"`  // 颜色配置
	Config map[string]interface{} `json:"config,omitempty"` // 其他显示配置
}

// FormattingOptions 格式化选项（通用）
type FormattingOptions struct {
	Type       string `json:"type,omitempty"`       // number, date, text
	Precision  *int   `json:"precision,omitempty"`  // 数字精度
	DateFormat string `json:"dateFormat,omitempty"` // 日期格式
	TimeFormat string `json:"timeFormat,omitempty"` // 时间格式
	TimeZone   string `json:"timeZone,omitempty"`   // 时区
	ShowCommas bool   `json:"showCommas,omitempty"` // 显示千分位
	Currency   string `json:"currency,omitempty"`   // 货币类型
}

// FilterOptions 过滤选项（用于 Link 字段等）
type FilterOptions struct {
	Conjunction string            `json:"conjunction,omitempty"` // and, or
	Conditions  []FilterCondition `json:"conditions,omitempty"`
}

// FilterCondition 过滤条件
type FilterCondition struct {
	FieldID  string      `json:"fieldId"`
	Operator string      `json:"operator"` // is, isNot, contains, etc.
	Value    interface{} `json:"value"`
}

// FormulaOptions 公式字段选项
type FormulaOptions struct {
	Expression string             `json:"expression"`
	TimeZone   string             `json:"timeZone,omitempty"` // 时区配置（参考 Teable）
	Formatting *FormattingOptions `json:"formatting,omitempty"`
	ShowAs     *ShowAsOptions     `json:"showAs,omitempty"` // 显示配置（参考 Teable）
}

// RollupOptions Rollup字段选项
type RollupOptions struct {
	LinkFieldID         string             `json:"link_field_id"`
	RollupFieldID       string             `json:"rollup_field_id"`
	AggregationFunction string             `json:"aggregation_function"` // sum, count, avg, min, max, etc.
	Expression          string             `json:"expression,omitempty"`
	TimeZone            string             `json:"timeZone,omitempty"`   // 时区配置（参考 Teable）
	Formatting          *FormattingOptions `json:"formatting,omitempty"` // 格式化配置（参考 Teable）
	ShowAs              *ShowAsOptions     `json:"showAs,omitempty"`     // 显示配置（参考 Teable）
}

// LookupOptions Lookup字段选项
type LookupOptions struct {
	LinkFieldID   string             `json:"link_field_id"`
	LookupFieldID string             `json:"lookup_field_id"`
	Formatting    *FormattingOptions `json:"formatting,omitempty"` // 格式化配置（参考 Teable）
	ShowAs        *ShowAsOptions     `json:"showAs,omitempty"`     // 显示配置（参考 Teable）
}

// LinkOptions 链接字段选项
type LinkOptions struct {
	LinkedTableID     string `json:"linked_table_id"`
	ForeignKeyFieldID string `json:"foreign_key_field_id,omitempty"`
	SymmetricFieldID  string `json:"symmetric_field_id,omitempty"`
	Relationship      string `json:"relationship"` // one_to_one, one_to_many, many_to_one, many_to_many
	IsSymmetric       bool   `json:"is_symmetric"`
	AllowMultiple     bool   `json:"allow_multiple"`
	// 高级功能（参考 Teable）
	BaseID          string         `json:"baseId,omitempty"`          // 跨 Base 链接支持
	LookupFieldID   string         `json:"lookupFieldId,omitempty"`   // 显示字段
	FilterByViewID  *string        `json:"filterByViewId,omitempty"`  // 视图过滤
	VisibleFieldIDs []string       `json:"visibleFieldIds,omitempty"` // 可见字段列表
	Filter          *FilterOptions `json:"filter,omitempty"`          // 复杂过滤条件
	// 数据库实现细节（可选，用于兼容旧版）
	FkHostTableName string `json:"fkHostTableName,omitempty"` // 存储外键的表名
	SelfKeyName     string `json:"selfKeyName,omitempty"`     // 自身主键字段名
	ForeignKeyName  string `json:"foreignKeyName,omitempty"`  // 外键字段名
}

// SelectOptions 选择字段选项
type SelectOptions struct {
	Choices               []SelectChoice `json:"choices"`
	DefaultValue          interface{}    `json:"defaultValue,omitempty"`          // 默认值（单选为 string，多选为 []string）
	PreventAutoNewOptions bool           `json:"preventAutoNewOptions,omitempty"` // 防止自动添加新选项（参考 Teable）
}

// SelectChoice 选择项
type SelectChoice struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color,omitempty"`
}

// NumberOptions 数字字段选项
type NumberOptions struct {
	Precision    *int           `json:"precision,omitempty"`    // 小数位数
	Format       string         `json:"format,omitempty"`       // decimal, percent, currency
	Currency     string         `json:"currency,omitempty"`     // USD, CNY, EUR, etc.
	ShowCommas   bool           `json:"show_commas,omitempty"`  // 是否显示千分位
	Min          *int           `json:"min,omitempty"`          // 最小值（仍保留，用于兼容）
	Max          *int           `json:"max,omitempty"`          // 最大值（仍保留，用于兼容）
	MinValue     *int           `json:"minValue,omitempty"`     // 最小值（API使用camelCase）
	MaxValue     *int           `json:"maxValue,omitempty"`     // 最大值（API使用camelCase）
	DefaultValue *float64       `json:"defaultValue,omitempty"` // 默认值（参考 Teable）
	ShowAs       *ShowAsOptions `json:"showAs,omitempty"`       // 显示配置（参考 Teable）
}

// DateOptions 日期字段选项
type DateOptions struct {
	Format       string  `json:"format,omitempty"`       // YYYY-MM-DD, MM/DD/YYYY, etc.
	IncludeTime  bool    `json:"include_time,omitempty"` // 是否包含时间
	TimeFormat   string  `json:"time_format,omitempty"`  // 12h, 24h
	TimeZone     string  `json:"timezone,omitempty"`     // UTC, Asia/Shanghai, etc.
	DefaultValue *string `json:"defaultValue,omitempty"` // 默认值，如 "now" 或具体日期字符串（参考 Teable）
}

// AIOptions AI字段选项
type AIOptions struct {
	Provider string                 `json:"provider"`         // openai, anthropic, etc.
	Model    string                 `json:"model"`            // gpt-4, claude-3, etc.
	Prompt   string                 `json:"prompt"`           // AI提示词
	Config   map[string]interface{} `json:"config,omitempty"` // 其他配置
}

// CountOptions Count字段选项
type CountOptions struct {
	LinkFieldID string `json:"link_field_id"`    // 被计数的Link字段ID
	Filter      string `json:"filter,omitempty"` // 可选的过滤条件
}

// DurationOptions Duration字段选项
type DurationOptions struct {
	Format string `json:"format"` // h:mm, h:mm:ss, d:h:mm, d:h:mm:ss
}

// ButtonOptions Button字段选项
type ButtonOptions struct {
	Label  string                 `json:"label"`            // 按钮文本
	Action string                 `json:"action"`           // 动作类型：open_url, run_script, trigger_automation
	Config map[string]interface{} `json:"config,omitempty"` // 动作配置
}

// UserOptions User字段选项
type UserOptions struct {
	IsMultiple bool `json:"is_multiple"` // 是否允许多用户
}

// RatingOptions Rating字段选项
type RatingOptions struct {
	Max  int    `json:"max"`            // 最大星数（1-10）
	Icon string `json:"icon,omitempty"` // 图标类型：star, heart, thumb, etc.
}

// NewFieldOptions 创建空字段选项
func NewFieldOptions() *FieldOptions {
	return &FieldOptions{}
}

// WithFormula 设置公式选项
func (fo *FieldOptions) WithFormula(expression string) *FieldOptions {
	fo.Formula = &FormulaOptions{
		Expression: expression,
	}
	return fo
}

// WithRollup 设置Rollup选项
func (fo *FieldOptions) WithRollup(linkFieldID, rollupFieldID, aggregationFunc string) *FieldOptions {
	fo.Rollup = &RollupOptions{
		LinkFieldID:         linkFieldID,
		RollupFieldID:       rollupFieldID,
		AggregationFunction: aggregationFunc,
	}
	return fo
}

// WithLookup 设置Lookup选项
func (fo *FieldOptions) WithLookup(linkFieldID, lookupFieldID string) *FieldOptions {
	fo.Lookup = &LookupOptions{
		LinkFieldID:   linkFieldID,
		LookupFieldID: lookupFieldID,
	}
	return fo
}

// WithLink 设置Link选项
func (fo *FieldOptions) WithLink(linkedTableID, relationship string, isSymmetric bool) *FieldOptions {
	fo.Link = &LinkOptions{
		LinkedTableID: linkedTableID,
		Relationship:  relationship,
		IsSymmetric:   isSymmetric,
	}
	return fo
}

// WithSelect 设置Select选项
func (fo *FieldOptions) WithSelect(choices []SelectChoice) *FieldOptions {
	fo.Select = &SelectOptions{
		Choices: choices,
	}
	return fo
}

// HasFormula 是否有公式选项
func (fo *FieldOptions) HasFormula() bool {
	return fo.Formula != nil
}

// HasRollup 是否有Rollup选项
func (fo *FieldOptions) HasRollup() bool {
	return fo.Rollup != nil
}

// HasLookup 是否有Lookup选项
func (fo *FieldOptions) HasLookup() bool {
	return fo.Lookup != nil
}

// HasLink 是否有Link选项
func (fo *FieldOptions) HasLink() bool {
	return fo.Link != nil
}

// HasSelect 是否有Select选项
func (fo *FieldOptions) HasSelect() bool {
	return fo.Select != nil
}

// HasCount 是否有Count选项
func (fo *FieldOptions) HasCount() bool {
	return fo.Count != nil
}

// HasDuration 是否有Duration选项
func (fo *FieldOptions) HasDuration() bool {
	return fo.Duration != nil
}

// HasButton 是否有Button选项
func (fo *FieldOptions) HasButton() bool {
	return fo.Button != nil
}

// HasUser 是否有User选项
func (fo *FieldOptions) HasUser() bool {
	return fo.User != nil
}

// HasRating 是否有Rating选项
func (fo *FieldOptions) HasRating() bool {
	return fo.Rating != nil
}

// WithCount 设置Count选项
func (fo *FieldOptions) WithCount(linkFieldID string) *FieldOptions {
	fo.Count = &CountOptions{
		LinkFieldID: linkFieldID,
	}
	return fo
}

// WithDuration 设置Duration选项
func (fo *FieldOptions) WithDuration(format string) *FieldOptions {
	fo.Duration = &DurationOptions{
		Format: format,
	}
	return fo
}

// WithButton 设置Button选项
func (fo *FieldOptions) WithButton(label, action string) *FieldOptions {
	fo.Button = &ButtonOptions{
		Label:  label,
		Action: action,
	}
	return fo
}

// WithUser 设置User选项
func (fo *FieldOptions) WithUser(isMultiple bool) *FieldOptions {
	fo.User = &UserOptions{
		IsMultiple: isMultiple,
	}
	return fo
}

// WithRating 设置Rating选项
func (fo *FieldOptions) WithRating(max int, icon string) *FieldOptions {
	fo.Rating = &RatingOptions{
		Max:  max,
		Icon: icon,
	}
	return fo
}
