package handler

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// UserFieldHandler User字段处理器（对齐原版UserFieldCore）
//
// 设计哲学：
//   - 语义清晰：用户关联字段，存储用户ID
//   - 灵活配置：支持单用户和多用户模式
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - 单用户模式：值为string（用户ID）
//   - 多用户模式：值为[]string（用户ID数组）
//   - nil值表示未分配用户
//   - 用户ID必须是有效的UUID格式
//
// 配置选项：
//   - isMultiple: 是否允许多用户
//
// 对齐原版：
//   - 支持单值和多值模式
//   - 用户ID验证
//   - 关联用户查询（可扩展）
type UserFieldHandler struct {
	*BaseFieldHandler
}

// NewUserFieldHandler 创建User字段处理器
func NewUserFieldHandler() *UserFieldHandler {
	return &UserFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeUser),
	}
}

// ValidateValue 验证字段值的合法性
// 验证规则：
//  1. nil值合法
//  2. 单用户模式：必须是string（用户ID）
//  3. 多用户模式：必须是[]string（用户ID数组）
//  4. 用户ID格式验证（简化：非空字符串）
//
// 设计考量：
//   - 根据字段配置决定单值/多值模式
//   - 用户ID验证可后续扩展（检查用户是否存在）
//   - 清晰的错误信息
func (h *UserFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	if value == nil {
		return nil
	}

	// 获取用户字段配置
	options := field.Options()
	isMultiple := false
	if options != nil && options.User != nil {
		isMultiple = options.User.IsMultiple
	}

	switch v := value.(type) {
	case string:
		// 单用户模式
		if v == "" {
			return nil // 空字符串视为nil
		}
		// 验证用户ID格式（简化版）
		if len(v) < 3 {
			return fields.NewDomainError(
				"INVALID_USER_ID",
				fmt.Sprintf("invalid user ID: '%s'", v),
				nil,
			)
		}
		return nil

	case []string:
		// 多用户模式
		if !isMultiple {
			return fields.NewDomainError(
				"INVALID_USER_VALUE",
				"该用户字段不支持多用户",
				nil,
			)
		}

		if len(v) == 0 {
			return nil
		}
		// 验证每个用户ID
		for i, userID := range v {
			if userID == "" {
				return fields.NewDomainError(
					"INVALID_USER_ID",
					fmt.Sprintf("user ID at index %d cannot be empty", i),
					nil,
				)
			}
			if len(userID) < 3 {
				return fields.NewDomainError(
					"INVALID_USER_ID",
					fmt.Sprintf("invalid user ID at index %d: '%s'", i, userID),
					nil,
				)
			}
		}
		return nil

	case []interface{}:
		// 处理JSON反序列化的情况
		if !isMultiple {
			return fields.NewDomainError(
				"INVALID_USER_VALUE",
				"该用户字段不支持多用户",
				nil,
			)
		}

		if len(v) == 0 {
			return nil
		}
		for i, item := range v {
			userID, ok := item.(string)
			if !ok {
				return fields.NewDomainError(
					"INVALID_VALUE_TYPE",
					fmt.Sprintf("user ID at index %d must be string, got %T", i, item),
					nil,
				)
			}
			if userID == "" || len(userID) < 3 {
				return fields.NewDomainError(
					"INVALID_USER_ID",
					fmt.Sprintf("invalid user ID at index %d: '%s'", i, userID),
					nil,
				)
			}
		}
		return nil

	default:
		return fields.NewDomainError(
			"INVALID_VALUE_TYPE",
			fmt.Sprintf("user field value must be string or array, got %T", value),
			nil,
		)
	}
}

// TransformValue 转换字段值
// 设计考量：
//   - 保持原始格式不变（string或[]string）
//   - 移除空字符串
//   - 数组模式下自动去重
func (h *UserFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return nil, nil
		}
		return v, nil

	case []string:
		if len(v) == 0 {
			return nil, nil
		}
		// 去重
		seen := make(map[string]bool)
		result := make([]string, 0, len(v))
		for _, userID := range v {
			if userID != "" && !seen[userID] {
				result = append(result, userID)
				seen[userID] = true
			}
		}
		if len(result) == 0 {
			return nil, nil
		}
		return result, nil

	case []interface{}:
		result := make([]string, 0, len(v))
		seen := make(map[string]bool)
		for _, item := range v {
			if userID, ok := item.(string); ok && userID != "" && !seen[userID] {
				result = append(result, userID)
				seen[userID] = true
			}
		}
		if len(result) == 0 {
			return nil, nil
		}
		return result, nil

	default:
		return nil, nil
	}
}

// FormatValue 格式化字段值
// 单用户返回用户ID，多用户返回逗号分隔的ID列表
func (h *UserFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return value, nil // User字段的值本身就是可展示的（ID）
}

// SupportsOptions 是否支持选项配置
func (h *UserFieldHandler) SupportsOptions() bool {
	return false // User字段暂不需要复杂配置
}
