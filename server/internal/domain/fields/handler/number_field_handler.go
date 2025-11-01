package handler

import (
	"context"
	"fmt"
	"math"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// NumberFieldHandler 数字字段处理器
type NumberFieldHandler struct {
	*BaseFieldHandler
}

// NewNumberFieldHandler 创建数字字段处理器
func NewNumberFieldHandler() *NumberFieldHandler {
	return &NumberFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeNumber),
	}
}

// ValidateValue 验证数字值
func (h *NumberFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// nil值检查
	if value == nil {
		if field.IsRequired() {
			return fields.ErrRequiredFieldEmpty
		}
		return nil
	}

	// 类型检查和转换
	var num float64
	switch v := value.(type) {
	case float64:
		num = v
	case float32:
		num = float64(v)
	case int:
		num = float64(v)
	case int64:
		num = float64(v)
	case int32:
		num = float64(v)
	case string:
		// 尝试从字符串解析
		_, err := fmt.Sscanf(v, "%f", &num)
		if err != nil {
			return fields.NewDomainError(
				"INVALID_NUMBER_VALUE",
				"cannot parse string as number",
				err,
			)
		}
	default:
		return fields.NewDomainError(
			"INVALID_NUMBER_VALUE",
			fmt.Sprintf("number field expects numeric value, got %T", value),
			nil,
		)
	}

	// 检查是否为有效数字
	if math.IsNaN(num) || math.IsInf(num, 0) {
		return fields.NewDomainError(
			"INVALID_NUMBER_VALUE",
			"number value is NaN or Infinity",
			nil,
		)
	}

	return nil
}

// TransformValue 转换数字值
func (h *NumberFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// 转换为 float64
	var num float64
	switch v := value.(type) {
	case float64:
		num = v
	case float32:
		num = float64(v)
	case int:
		num = float64(v)
	case int64:
		num = float64(v)
	case int32:
		num = float64(v)
	case string:
		_, err := fmt.Sscanf(v, "%f", &num)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fields.ErrValueTypeMismatch
	}

	// 应用精度
	if field.Options() != nil && field.Options().Number != nil && field.Options().Number.Precision != nil {
		precision := *field.Options().Number.Precision
		multiplier := math.Pow(10, float64(precision))
		num = math.Round(num*multiplier) / multiplier
	}

	return num, nil
}

// FormatValue 格式化数字值
func (h *NumberFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	num, ok := value.(float64)
	if !ok {
		return value, nil
	}

	// 根据选项格式化
	if field.Options() != nil && field.Options().Number != nil {
		options := field.Options().Number

		// 应用精度
		if options.Precision != nil {
			precision := *options.Precision
			format := fmt.Sprintf("%%.%df", precision)
			return fmt.Sprintf(format, num), nil
		}

		// 应用格式
		switch options.Format {
		case "percent":
			return fmt.Sprintf("%.2f%%", num*100), nil
		case "currency":
			currency := "USD"
			if options.Currency != "" {
				currency = options.Currency
			}
			return fmt.Sprintf("%s %.2f", currency, num), nil
		}
	}

	return num, nil
}

// SupportsOptions 支持选项配置
func (h *NumberFieldHandler) SupportsOptions() bool {
	return true
}

// ValidateOptions 验证数字字段选项
func (h *NumberFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	if field.Options() == nil || field.Options().Number == nil {
		return nil
	}

	options := field.Options().Number

	// 验证精度
	if options.Precision != nil {
		if *options.Precision < 0 || *options.Precision > 10 {
			return fields.NewDomainError(
				"INVALID_PRECISION",
				"precision must be between 0 and 10",
				nil,
			)
		}
	}

	// 验证格式
	if options.Format != "" {
		validFormats := map[string]bool{
			"decimal":  true,
			"percent":  true,
			"currency": true,
		}
		if !validFormats[options.Format] {
			return fields.NewDomainError(
				"INVALID_NUMBER_FORMAT",
				fmt.Sprintf("invalid number format: %s", options.Format),
				nil,
			)
		}
	}

	return nil
}
