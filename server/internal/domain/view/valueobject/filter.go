package valueobject

import (
	"encoding/json"
	"fmt"
)

// FilterOperator 过滤器操作符
type FilterOperator string

const (
	FilterOperatorAnd FilterOperator = "and" // 与
	FilterOperatorOr  FilterOperator = "or"  // 或
)

// FilterItemOperator 过滤项操作符
type FilterItemOperator string

const (
	// 通用操作符
	FilterItemOpIs          FilterItemOperator = "is"          // 等于
	FilterItemOpIsNot       FilterItemOperator = "isNot"       // 不等于
	FilterItemOpContains    FilterItemOperator = "contains"    // 包含
	FilterItemOpNotContains FilterItemOperator = "notContains" // 不包含
	FilterItemOpIsEmpty     FilterItemOperator = "isEmpty"     // 为空
	FilterItemOpIsNotEmpty  FilterItemOperator = "isNotEmpty"  // 不为空

	// 数值比较
	FilterItemOpGreater      FilterItemOperator = "isGreater"      // 大于
	FilterItemOpGreaterEqual FilterItemOperator = "isGreaterEqual" // 大于等于
	FilterItemOpLess         FilterItemOperator = "isLess"         // 小于
	FilterItemOpLessEqual    FilterItemOperator = "isLessEqual"    // 小于等于

	// 日期操作
	FilterItemOpIsBefore FilterItemOperator = "isBefore" // 早于
	FilterItemOpIsAfter  FilterItemOperator = "isAfter"  // 晚于
	FilterItemOpIsWithin FilterItemOperator = "isWithin" // 在范围内

	// 数组操作
	FilterItemOpHasAnyOf     FilterItemOperator = "hasAnyOf"     // 包含任意一个
	FilterItemOpHasAllOf     FilterItemOperator = "hasAllOf"     // 包含全部
	FilterItemOpHasNoneOf    FilterItemOperator = "hasNoneOf"    // 不包含任何
	FilterItemOpIsExactly    FilterItemOperator = "isExactly"    // 完全匹配
	FilterItemOpIsNotExactly FilterItemOperator = "isNotExactly" // 不完全匹配
)

// Filter 过滤器值对象
type Filter struct {
	Operator FilterOperator `json:"operator"` // and 或 or
	Filters  []FilterItem   `json:"filters"`  // 过滤项列表
}

// FilterItem 过滤项
type FilterItem struct {
	FieldID  string             `json:"fieldId"`  // 字段ID
	Operator FilterItemOperator `json:"operator"` // 操作符
	Value    interface{}        `json:"value"`    // 值
}

// NewFilter 创建过滤器值对象
func NewFilter(data map[string]interface{}) (*Filter, error) {
	if data == nil {
		return nil, nil
	}

	// 序列化再反序列化，确保数据格式正确
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("invalid filter data: %w", err)
	}

	var filter Filter
	if err := json.Unmarshal(jsonData, &filter); err != nil {
		return nil, fmt.Errorf("failed to parse filter: %w", err)
	}

	// 验证
	if err := filter.Validate(); err != nil {
		return nil, err
	}

	return &filter, nil
}

// Validate 验证过滤器
func (f *Filter) Validate() error {
	if f == nil {
		return nil
	}

	// 验证操作符
	if f.Operator != FilterOperatorAnd && f.Operator != FilterOperatorOr {
		return fmt.Errorf("invalid filter operator: %s", f.Operator)
	}

	// 验证过滤项
	if len(f.Filters) == 0 {
		return fmt.Errorf("filter must have at least one filter item")
	}

	for i, item := range f.Filters {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("invalid filter item at index %d: %w", i, err)
		}
	}

	return nil
}

// Validate 验证过滤项
func (fi *FilterItem) Validate() error {
	// 验证字段ID
	if fi.FieldID == "" {
		return fmt.Errorf("field ID is required")
	}

	// 验证操作符
	if !fi.isValidOperator() {
		return fmt.Errorf("invalid operator: %s", fi.Operator)
	}

	// 某些操作符不需要值
	if fi.requiresValue() && fi.Value == nil {
		return fmt.Errorf("operator %s requires a value", fi.Operator)
	}

	return nil
}

// isValidOperator 检查操作符是否有效
func (fi *FilterItem) isValidOperator() bool {
	validOperators := map[FilterItemOperator]bool{
		FilterItemOpIs:           true,
		FilterItemOpIsNot:        true,
		FilterItemOpContains:     true,
		FilterItemOpNotContains:  true,
		FilterItemOpIsEmpty:      true,
		FilterItemOpIsNotEmpty:   true,
		FilterItemOpGreater:      true,
		FilterItemOpGreaterEqual: true,
		FilterItemOpLess:         true,
		FilterItemOpLessEqual:    true,
		FilterItemOpIsBefore:     true,
		FilterItemOpIsAfter:      true,
		FilterItemOpIsWithin:     true,
		FilterItemOpHasAnyOf:     true,
		FilterItemOpHasAllOf:     true,
		FilterItemOpHasNoneOf:    true,
		FilterItemOpIsExactly:    true,
		FilterItemOpIsNotExactly: true,
	}
	return validOperators[fi.Operator]
}

// requiresValue 检查操作符是否需要值
func (fi *FilterItem) requiresValue() bool {
	noValueOperators := map[FilterItemOperator]bool{
		FilterItemOpIsEmpty:    true,
		FilterItemOpIsNotEmpty: true,
	}
	return !noValueOperators[fi.Operator]
}

// ToMap 转换为Map
func (f *Filter) ToMap() map[string]interface{} {
	if f == nil {
		return nil
	}

	filters := make([]map[string]interface{}, len(f.Filters))
	for i, item := range f.Filters {
		filters[i] = map[string]interface{}{
			"fieldId":  item.FieldID,
			"operator": item.Operator,
			"value":    item.Value,
		}
	}

	return map[string]interface{}{
		"operator": f.Operator,
		"filters":  filters,
	}
}

// IsEmpty 检查过滤器是否为空
func (f *Filter) IsEmpty() bool {
	return f == nil || len(f.Filters) == 0
}

// GetFieldIDs 获取所有涉及的字段ID
func (f *Filter) GetFieldIDs() []string {
	if f == nil {
		return []string{}
	}

	fieldIDs := make([]string, 0, len(f.Filters))
	seen := make(map[string]bool)

	for _, item := range f.Filters {
		if !seen[item.FieldID] {
			fieldIDs = append(fieldIDs, item.FieldID)
			seen[item.FieldID] = true
		}
	}

	return fieldIDs
}
