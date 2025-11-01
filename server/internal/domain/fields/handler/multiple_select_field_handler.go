package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// MultipleSelectFieldHandler MultipleSelect字段处理器（对齐原版MultipleSelectFieldCore）
//
// 设计哲学：
//   - 单一职责：专注于多选字段的业务逻辑
//   - 优雅验证：数组值的完整性和一致性检查
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - 字段值是字符串数组，每个元素是choices中某个选项的name
//   - nil值或空数组表示未选择
//   - 每个值都会被清理（trim、移除换行符）
//   - 不存在的选项名称会被拒绝
//   - 数组中不能有重复值
//
// 与SingleSelect的区别：
//   - 值类型：[]string vs string
//   - 允许多选
//   - 需要额外的去重逻辑
type MultipleSelectFieldHandler struct {
	*BaseFieldHandler
}

// NewMultipleSelectFieldHandler 创建MultipleSelect字段处理器（完美架构）
func NewMultipleSelectFieldHandler() *MultipleSelectFieldHandler {
	return &MultipleSelectFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeMultipleSelect),
	}
}

// ValidateValue 验证字段值的合法性（对齐原版validateCellValue）
// 参数:
//   - ctx: 上下文
//   - field: 字段实体
//   - value: 待验证的值（应为[]string或[]interface{}）
//
// 验证规则：
//  1. nil值合法（表示未选择）
//  2. 空数组合法
//  3. 必须是数组类型
//  4. 数组中每个元素必须是string
//  5. 每个值必须是choices中的name
//  6. 数组中不能有重复值
//
// 设计考量：
//   - 多层验证：类型→结构→业务规则
//   - 友好的错误信息，指出具体哪个元素有问题
//   - 性能优化：使用map去重，O(n)复杂度
func (h *MultipleSelectFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// nil值合法
	if value == nil {
		return nil
	}

	// 类型检查：必须是数组
	var strValues []string

	switch v := value.(type) {
	case []string:
		strValues = v
	case []interface{}:
		// 转换为字符串数组
		strValues = make([]string, len(v))
		for i, item := range v {
			str, ok := item.(string)
			if !ok {
				return fields.NewDomainError(
					"INVALID_ARRAY_ELEMENT_TYPE",
					fmt.Sprintf("array element at index %d must be string, got %T", i, item),
					nil,
				)
			}
			strValues[i] = str
		}
	default:
		return fields.NewDomainError(
			"INVALID_VALUE_TYPE",
			fmt.Sprintf("multipleSelect value must be an array, got %T", value),
			nil,
		)
	}

	// 空数组合法
	if len(strValues) == 0 {
		return nil
	}

	// 获取字段配置
	options := field.Options()
	if options == nil || options.Select == nil {
		return fields.NewDomainError(
			"SELECT_OPTIONS_NOT_CONFIGURED",
			"select field options not configured",
			nil,
		)
	}

	// 构建choices name集合（性能优化）
	choicesSet := make(map[string]bool)
	for _, choice := range options.Select.Choices {
		choicesSet[choice.Name] = true
	}

	// 检查重复值和有效性
	seenValues := make(map[string]bool)
	for i, val := range strValues {
		trimmedVal := strings.TrimSpace(val)

		// 检查重复
		if seenValues[trimmedVal] {
			return fields.NewDomainError(
				"DUPLICATE_VALUE",
				fmt.Sprintf("duplicate value '%s' at index %d", trimmedVal, i),
				nil,
			)
		}
		seenValues[trimmedVal] = true

		// 检查是否为有效choice
		if !choicesSet[trimmedVal] {
			availableChoices := make([]string, 0, len(options.Select.Choices))
			for _, choice := range options.Select.Choices {
				availableChoices = append(availableChoices, choice.Name)
			}
			return fields.NewDomainError(
				"INVALID_CHOICE_VALUE",
				fmt.Sprintf("value '%s' at index %d is not a valid choice. Available: [%s]",
					trimmedVal, i, strings.Join(availableChoices, ", ")),
				nil,
			)
		}
	}

	return nil
}

// TransformValue 转换字段值（实现FieldHandler接口）
// 转换逻辑：
//  1. nil值或空数组返回nil
//  2. 每个字符串值进行清理（trim、移除换行符）
//  3. 自动去重
//  4. 移除空字符串元素
//
// 设计考量：
//   - 数据清洗：确保数组中每个元素都是干净的
//   - 自动去重：避免重复数据
//   - 保持顺序：去重时保留首次出现的顺序
func (h *MultipleSelectFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// 转换为字符串数组
	var strValues []string

	switch v := value.(type) {
	case []string:
		strValues = v
	case []interface{}:
		for _, item := range v {
			if str, ok := item.(string); ok {
				strValues = append(strValues, str)
			}
		}
	default:
		// 非数组类型，无法转换
		return nil, nil
	}

	// 空数组返回nil
	if len(strValues) == 0 {
		return nil, nil
	}

	// 清理每个值并去重
	seen := make(map[string]bool)
	cleaned := make([]string, 0, len(strValues))

	for _, val := range strValues {
		// 清理值
		cleanedVal := strings.ReplaceAll(val, "\n", " ")
		cleanedVal = strings.ReplaceAll(cleanedVal, "\r", " ")
		cleanedVal = strings.TrimSpace(cleanedVal)

		// 跳过空值和重复值
		if cleanedVal != "" && !seen[cleanedVal] {
			cleaned = append(cleaned, cleanedVal)
			seen[cleanedVal] = true
		}
	}

	// 如果清理后为空，返回nil
	if len(cleaned) == 0 {
		return nil, nil
	}

	return cleaned, nil
}

// FormatValue 格式化字段值（用于显示）
// 将数组格式化为逗号分隔的字符串
//
// 设计考量：
//   - 与原版cellValue2String对齐
//   - 数组元素用逗号空格分隔，提升可读性
func (h *MultipleSelectFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return "", nil
	}

	if strArray, ok := value.([]string); ok {
		return strings.Join(strArray, ", "), nil
	}

	return "", nil
}

// SupportsOptions 是否支持选项配置
func (h *MultipleSelectFieldHandler) SupportsOptions() bool {
	return true
}

// ValidateOptions 验证字段选项配置（复用SingleSelect的验证逻辑）
// MultipleSelect和SingleSelect的配置结构完全相同
func (h *MultipleSelectFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	options := field.Options()
	if options == nil || options.Select == nil {
		return fields.NewDomainError(
			"SELECT_OPTIONS_NOT_CONFIGURED",
			"select field options not configured",
			nil,
		)
	}

	selectOpts := options.Select

	// 必须至少有一个选项
	if len(selectOpts.Choices) == 0 {
		return fields.NewDomainError(
			"EMPTY_CHOICES",
			"choices cannot be empty",
			nil,
		)
	}

	// 检查选项名称和ID的唯一性
	nameSet := make(map[string]bool)
	idSet := make(map[string]bool)

	for i, choice := range selectOpts.Choices {
		// 验证名称
		if strings.TrimSpace(choice.Name) == "" {
			return fields.NewDomainError(
				"EMPTY_CHOICE_NAME",
				fmt.Sprintf("choice name cannot be empty at index %d", i),
				nil,
			)
		}

		// 检查名称重复
		if nameSet[choice.Name] {
			return fields.NewDomainError(
				"DUPLICATE_CHOICE_NAME",
				fmt.Sprintf("duplicate choice name: '%s' at index %d", choice.Name, i),
				nil,
			)
		}
		nameSet[choice.Name] = true

		// 检查ID重复
		if choice.ID != "" {
			if idSet[choice.ID] {
				return fields.NewDomainError(
					"DUPLICATE_CHOICE_ID",
					fmt.Sprintf("duplicate choice ID: '%s' at index %d", choice.ID, i),
					nil,
				)
			}
			idSet[choice.ID] = true
		}
	}

	return nil
}
