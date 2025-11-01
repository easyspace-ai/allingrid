package handler

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// DurationFieldHandler Duration字段处理器（对齐原版DurationFieldCore）
//
// 设计哲学：
//   - 语义明确：时长字段，存储秒数
//   - 格式灵活：支持多种展示格式
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - 字段值必须是整数（秒数）
//   - 值范围：>= 0（时长不能为负）
//   - nil值表示未设置时长
//   - 支持自定义格式（h:mm, h:mm:ss等）
//
// 配置选项：
//   - format: 显示格式（如"h:mm:ss"）
//
// 存储设计：
//   - 统一存储为秒数（int）
//   - 展示时根据format格式化
//
// 对齐原版：
//   - 秒数存储
//   - 格式化展示
type DurationFieldHandler struct {
	*BaseFieldHandler
}

// NewDurationFieldHandler 创建Duration字段处理器
func NewDurationFieldHandler() *DurationFieldHandler {
	return &DurationFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeDuration),
	}
}

// ValidateValue 验证字段值的合法性
// 验证规则：
//  1. nil值合法
//  2. 必须是数字类型
//  3. 值必须>=0（时长不能为负）
//
// 设计考量：
//   - 支持多种数字类型输入
//   - 统一转换为int进行验证
//   - 负数检查确保语义正确
func (h *DurationFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	if value == nil {
		return nil
	}

	// 转换为int进行验证
	var intValue int

	switch v := value.(type) {
	case int:
		intValue = v
	case int32:
		intValue = int(v)
	case int64:
		intValue = int(v)
	case float32:
		intValue = int(v)
	case float64:
		intValue = int(v)
	default:
		return fields.NewDomainError(
			"INVALID_VALUE_TYPE",
			fmt.Sprintf("duration value must be a number, got %T", value),
			nil,
		)
	}

	// 时长不能为负
	if intValue < 0 {
		return fields.NewDomainError(
			"INVALID_DURATION_VALUE",
			fmt.Sprintf("duration cannot be negative, got %d", intValue),
			nil,
		)
	}

	return nil
}

// TransformValue 转换字段值
// 将各种数字类型统一转换为int（秒数）
func (h *DurationFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return nil, nil
	}
}

// FormatValue 格式化字段值
// 根据format配置格式化时长
//
// 格式示例：
//   - h:mm -> "2:30"
//   - h:mm:ss -> "2:30:45"
//
// 设计考量：
//   - 前端负责格式化展示
//   - 后端返回原始秒数
//   - 可扩展：后续可支持服务端格式化
func (h *DurationFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return value, nil // 返回秒数，前端根据format格式化
}

// SupportsOptions 是否支持选项配置
func (h *DurationFieldHandler) SupportsOptions() bool {
	return true // Duration字段支持format配置
}

// ValidateOptions 验证字段选项配置
func (h *DurationFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	validFormats := []string{"h:mm", "h:mm:ss", "d:h:mm", "d:h:mm:ss"}

	options := field.Options()
	if options != nil && options.Duration != nil {
		format := options.Duration.Format
		if format == "" {
			return nil // format为空时使用默认值
		}

		// 验证格式是否有效
		isValid := false
		for _, vf := range validFormats {
			if format == vf {
				isValid = true
				break
			}
		}

		if !isValid {
			return fields.NewDomainError(
				"INVALID_DURATION_FORMAT",
				"Duration格式无效，支持的格式: h:mm, h:mm:ss, d:h:mm, d:h:mm:ss",
				nil,
			)
		}
	}

	return nil
}
