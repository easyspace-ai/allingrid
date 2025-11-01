package handler

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// RatingFieldHandler Rating字段处理器（对齐原版RatingFieldCore）
//
// 设计哲学：
//   - 语义明确：评分字段，存储0-max的整数值
//   - 配置驱动：max和icon可自定义
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - 字段值必须是整数
//   - 值范围：0 <= value <= max
//   - nil值表示未评分
//   - 默认max为5（五星评分）
//
// 配置选项：
//   - max: 最大评分值（如5表示五星）
//   - icon: 评分图标（如⭐）
//
// 对齐原版：
//   - 评分范围验证
//   - 可配置的max值
//   - 图标自定义
type RatingFieldHandler struct {
	*BaseFieldHandler
}

// NewRatingFieldHandler 创建Rating字段处理器
func NewRatingFieldHandler() *RatingFieldHandler {
	return &RatingFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeRating),
	}
}

// ValidateValue 验证字段值的合法性
// 验证规则：
//  1. nil值合法
//  2. 必须是数字类型（int, int64, float64等）
//  3. 值范围：0 <= value <= max
//  4. 负数不允许
//
// 设计考量：
//   - 支持多种数字类型（int, int64, float64等）
//   - 范围检查确保评分有效性
//   - 清晰的错误信息
func (h *RatingFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
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
			fmt.Sprintf("rating value must be a number, got %T", value),
			nil,
		)
	}

	// 验证范围
	if intValue < 0 {
		return fields.NewDomainError(
			"INVALID_RATING_VALUE",
			fmt.Sprintf("rating value cannot be negative, got %d", intValue),
			nil,
		)
	}

	// 获取max配置（默认5）
	max := 5
	options := field.Options()
	if options != nil && options.Rating != nil {
		if options.Rating.Max > 0 {
			max = options.Rating.Max
		}
	}

	if intValue > max {
		return fields.NewDomainError(
			"RATING_OUT_OF_RANGE",
			fmt.Sprintf("rating value %d exceeds maximum %d", intValue, max),
			nil,
		)
	}

	return nil
}

// TransformValue 转换字段值
// 将各种数字类型统一转换为int
//
// 设计考量：
//   - 统一数据类型，简化存储
//   - float转int时直接截断（不四舍五入）
//   - 保持与原版一致的转换逻辑
func (h *RatingFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
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
		// 无法转换的类型
		return nil, nil
	}
}

// FormatValue 格式化字段值
// 返回数字本身，前端根据icon配置渲染
func (h *RatingFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return value, nil
}

// SupportsOptions 是否支持选项配置
func (h *RatingFieldHandler) SupportsOptions() bool {
	return true // Rating字段支持max和icon配置
}

// ValidateOptions 验证字段选项配置
// 验证规则：
//   - max必须>=1且<=10（合理的评分范围）
//
// 设计考量：
//   - max范围限制，避免不合理配置
//   - icon为可选项，可以为空
func (h *RatingFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	options := field.Options()
	if options != nil && options.Rating != nil {
		max := options.Rating.Max
		if max < 1 || max > 10 {
			return fields.NewDomainError(
				"INVALID_RATING_OPTIONS",
				"Rating max必须在1-10之间",
				nil,
			)
		}
		// Icon可选，不验证
	}

	return nil
}
