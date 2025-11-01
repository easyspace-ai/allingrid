package handler

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// RollupFieldHandler Rollup字段处理器
type RollupFieldHandler struct {
	*BaseFieldHandler
	aggregator AggregationService
}

// AggregationService 聚合服务接口
type AggregationService interface {
	// Aggregate 执行聚合计算
	Aggregate(ctx context.Context, function string, values []interface{}) (interface{}, error)

	// SupportedFunctions 获取支持的聚合函数列表
	SupportedFunctions() []string
}

// NewRollupFieldHandler 创建Rollup字段处理器
func NewRollupFieldHandler(aggregator AggregationService) *RollupFieldHandler {
	return &RollupFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeRollup),
		aggregator:       aggregator,
	}
}

// ValidateValue 验证Rollup字段值（Rollup字段值由系统计算）
func (h *RollupFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// Rollup字段值由系统计算，不允许手动设置
	return fields.NewDomainError(
		"CANNOT_SET_ROLLUP_VALUE",
		"rollup field value is computed automatically",
		nil,
	)
}

// TransformValue 转换Rollup字段值
func (h *RollupFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	// Rollup字段不存储值
	return nil, nil
}

// ComputeValue 计算Rollup字段值
func (h *RollupFieldHandler) ComputeValue(
	ctx context.Context,
	field *entity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	// 获取Rollup配置
	if field.Options() == nil || field.Options().Rollup == nil {
		return nil, fields.ErrInvalidRollupConfig
	}

	rollupOpts := field.Options().Rollup

	// 获取关联记录
	linkedRecords, exists := relatedRecords[rollupOpts.LinkFieldID]
	if !exists || len(linkedRecords) == 0 {
		// 没有关联记录，返回空值或默认值
		return h.getEmptyAggregationValue(rollupOpts.AggregationFunction), nil
	}

	// 提取要聚合的字段值
	values := make([]interface{}, 0, len(linkedRecords))
	for _, linkedRecord := range linkedRecords {
		if val, exists := linkedRecord[rollupOpts.RollupFieldID]; exists {
			values = append(values, val)
		}
	}

	// 执行聚合计算
	result, err := h.aggregator.Aggregate(ctx, rollupOpts.AggregationFunction, values)
	if err != nil {
		return nil, fmt.Errorf("failed to compute rollup: %w", err)
	}

	return result, nil
}

// GetDependencies 获取Rollup依赖的字段
func (h *RollupFieldHandler) GetDependencies(ctx context.Context, field *entity.Field) ([]string, error) {
	if field.Options() == nil || field.Options().Rollup == nil {
		return []string{}, nil
	}

	rollupOpts := field.Options().Rollup

	// Rollup依赖link字段和被rollup的字段
	dependencies := []string{
		rollupOpts.LinkFieldID,
		rollupOpts.RollupFieldID,
	}

	return dependencies, nil
}

// IsAsync Rollup计算可能比较耗时，使用异步
func (h *RollupFieldHandler) IsAsync() bool {
	return true
}

// SupportsOptions Rollup字段支持选项
func (h *RollupFieldHandler) SupportsOptions() bool {
	return true
}

// ValidateOptions 验证Rollup选项
func (h *RollupFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	if field.Options() == nil || field.Options().Rollup == nil {
		return fields.ErrMissingRequiredOption
	}

	rollupOpts := field.Options().Rollup

	// 验证必填字段
	if rollupOpts.LinkFieldID == "" {
		return fields.NewDomainError(
			"MISSING_LINK_FIELD_ID",
			"rollup must specify link field",
			nil,
		)
	}

	if rollupOpts.RollupFieldID == "" {
		return fields.NewDomainError(
			"MISSING_ROLLUP_FIELD_ID",
			"rollup must specify target field",
			nil,
		)
	}

	if rollupOpts.AggregationFunction == "" {
		return fields.NewDomainError(
			"MISSING_AGGREGATION_FUNCTION",
			"rollup must specify aggregation function",
			nil,
		)
	}

	// 验证聚合函数
	if !h.isValidAggregationFunction(rollupOpts.AggregationFunction) {
		return fields.NewDomainError(
			"INVALID_AGGREGATION_FUNCTION",
			fmt.Sprintf("unsupported aggregation function: %s", rollupOpts.AggregationFunction),
			nil,
		)
	}

	return nil
}

// getEmptyAggregationValue 获取空聚合值
func (h *RollupFieldHandler) getEmptyAggregationValue(function string) interface{} {
	switch function {
	case "count", "countall", "counta":
		return 0
	case "sum", "average", "avg":
		return 0.0
	case "min", "max":
		return nil
	case "and":
		return true
	case "or", "xor":
		return false
	case "array_join", "concatenate":
		return ""
	case "array_unique", "array_compact":
		return []interface{}{}
	default:
		return nil
	}
}

// isValidAggregationFunction 检查是否为有效的聚合函数
func (h *RollupFieldHandler) isValidAggregationFunction(function string) bool {
	validFunctions := map[string]bool{
		"sum":           true,
		"count":         true,
		"countall":      true,
		"counta":        true,
		"average":       true,
		"avg":           true,
		"min":           true,
		"max":           true,
		"and":           true,
		"or":            true,
		"xor":           true,
		"array_join":    true,
		"array_unique":  true,
		"array_compact": true,
		"concatenate":   true,
	}

	return validFunctions[function]
}
