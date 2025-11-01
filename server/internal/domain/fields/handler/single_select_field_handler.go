package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// SingleSelectFieldHandler SingleSelect字段处理器（对齐原版SingleSelectFieldCore）
//
// 设计哲学：
//   - 单一职责：只处理SingleSelect字段的业务逻辑
//   - 优雅验证：提供清晰的验证规则和错误信息
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - 字段值必须是choices中某个选项的name（字符串）
//   - nil值表示未选择状态
//   - 值会被自动trim和清理（移除换行符）
//   - 不存在的选项名称会被拒绝
//
// 对齐原版：
//   - convertStringToCellValue - 字符串转换为单元格值
//   - validateCellValue - 验证单元格值的合法性
//   - repair - 修复无效值
//   - cellValue2String - 单元格值转字符串
type SingleSelectFieldHandler struct {
	*BaseFieldHandler
}

// NewSingleSelectFieldHandler 创建SingleSelect字段处理器（完美架构）
// 返回:
//   - *SingleSelectFieldHandler: 处理器实例
//
// 设计考量：
//   - 继承BaseFieldHandler，复用通用逻辑
//   - 遵循依赖注入原则
//   - 保持构造函数的简洁性
func NewSingleSelectFieldHandler() *SingleSelectFieldHandler {
	return &SingleSelectFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeSingleSelect),
	}
}

// ValidateValue 验证字段值的合法性（对齐原版validateCellValue）
// 参数:
//   - ctx: 上下文
//   - field: 字段实体
//   - value: 待验证的值
//
// 返回:
//   - error: 验证失败时的错误信息
//
// 验证规则：
//  1. nil值合法（表示未选择）
//  2. 空字符串视为nil
//  3. 必须是string类型
//  4. 值必须是choices中某个选项的name
//
// 设计考量：
//   - 遵循"宽进严出"原则，提供友好的错误信息
//   - 类型检查和业务规则检查清晰分离
//   - 错误信息包含具体的失败原因和可选值列表
func (h *SingleSelectFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// nil值合法（未选择状态）
	if value == nil {
		return nil
	}

	// 类型检查：必须是字符串
	strValue, ok := value.(string)
	if !ok {
		return fields.NewDomainError(
			"INVALID_VALUE_TYPE",
			fmt.Sprintf("singleSelect value must be a string, got %T", value),
			nil,
		)
	}

	// 空字符串视为nil，合法
	trimmedValue := strings.TrimSpace(strValue)
	if trimmedValue == "" {
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

	// 业务规则检查：值必须在choices中
	found := false
	for _, choice := range options.Select.Choices {
		if choice.Name == trimmedValue {
			found = true
			break
		}
	}

	if !found {
		// 提供友好的错误信息，包含可选值列表
		availableChoices := make([]string, len(options.Select.Choices))
		for i, choice := range options.Select.Choices {
			availableChoices[i] = choice.Name
		}
		return fields.NewDomainError(
			"INVALID_CHOICE_VALUE",
			fmt.Sprintf("value '%s' is not a valid choice. Available: [%s]",
				trimmedValue, strings.Join(availableChoices, ", ")),
			nil,
		)
	}

	return nil
}

// TransformValue 转换字段值（对齐原版，实现FieldHandler接口）
// 参数:
//   - ctx: 上下文
//   - field: 字段实体
//   - value: 原始值
//
// 返回:
//   - interface{}: 转换后的值
//   - error: 转换失败时的错误
//
// 转换逻辑：
//  1. nil值直接返回
//  2. 字符串值进行清理（trim、移除换行符）
//  3. 其他类型尝试转换为字符串
//
// 设计考量：
//   - 提供数据清洗能力
//   - 保证存储前数据的一致性
//   - 对齐原版的清理逻辑
func (h *SingleSelectFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// 字符串值清理
	if strValue, ok := value.(string); ok {
		// 移除换行符和首尾空格（对齐原版）
		cleanedValue := strings.ReplaceAll(strValue, "\n", " ")
		cleanedValue = strings.ReplaceAll(cleanedValue, "\r", " ")
		cleanedValue = strings.TrimSpace(cleanedValue)

		if cleanedValue == "" {
			return nil, nil
		}

		return cleanedValue, nil
	}

	// 其他类型尝试转换为字符串
	return fmt.Sprintf("%v", value), nil
}

// FormatValue 格式化字段值（用于显示）
// 设计考量：
//   - SingleSelect值本身就是可读的字符串
//   - 直接返回即可，无需特殊格式化
func (h *SingleSelectFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return value, nil
}

// SupportsOptions 是否支持选项配置
// SingleSelect字段必须配置choices选项
func (h *SingleSelectFieldHandler) SupportsOptions() bool {
	return true
}

// ValidateOptions 验证字段选项配置的合法性
// 参数:
//   - ctx: 上下文
//   - field: 字段实体
//
// 返回:
//   - error: 配置无效时的错误信息
//
// 验证规则：
//  1. choices不能为空
//  2. 每个choice的name不能为空
//  3. choice的name不能重复
//  4. choice的ID不能重复
//
// 设计考量：
//   - 配置验证前置，避免运行时错误
//   - 提供详细的错误信息，便于定位问题
//   - 检查业务规则的完整性
func (h *SingleSelectFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
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
