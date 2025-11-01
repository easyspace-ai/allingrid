package handler

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// FormulaFieldHandler 公式字段处理器
type FormulaFieldHandler struct {
	*BaseFieldHandler
	evaluator FormulaEvaluator
}

// FormulaEvaluator 公式评估器接口
type FormulaEvaluator interface {
	// Evaluate 评估公式
	Evaluate(ctx context.Context, expression string, variables map[string]interface{}) (interface{}, error)

	// Parse 解析公式，提取依赖的字段
	Parse(expression string) ([]string, error)

	// Validate 验证公式语法
	Validate(expression string) error
}

// NewFormulaFieldHandler 创建公式字段处理器
func NewFormulaFieldHandler(evaluator FormulaEvaluator) *FormulaFieldHandler {
	return &FormulaFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeFormula),
		evaluator:        evaluator,
	}
}

// ValidateValue 验证公式字段值（公式字段值由系统计算，不接受外部输入）
func (h *FormulaFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// 公式字段值由系统计算，不允许手动设置
	return fields.NewDomainError(
		"CANNOT_SET_FORMULA_VALUE",
		"formula field value is computed automatically",
		nil,
	)
}

// TransformValue 转换公式字段值
func (h *FormulaFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	// 公式字段不存储值
	return nil, nil
}

// ComputeValue 计算公式字段值
func (h *FormulaFieldHandler) ComputeValue(
	ctx context.Context,
	field *entity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	// 获取公式表达式
	if field.Options() == nil || field.Options().Formula == nil {
		return nil, fields.NewDomainError(
			"MISSING_FORMULA_EXPRESSION",
			"formula field must have expression",
			nil,
		)
	}

	expression := field.Options().Formula.Expression

	// 评估公式
	result, err := h.evaluator.Evaluate(ctx, expression, record)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate formula: %w", err)
	}

	return result, nil
}

// GetDependencies 获取公式依赖的字段
func (h *FormulaFieldHandler) GetDependencies(ctx context.Context, field *entity.Field) ([]string, error) {
	if field.Options() == nil || field.Options().Formula == nil {
		return []string{}, nil
	}

	expression := field.Options().Formula.Expression

	// 解析公式，提取字段引用
	dependencies, err := h.evaluator.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("failed to parse formula: %w", err)
	}

	return dependencies, nil
}

// IsAsync 公式计算是同步的
func (h *FormulaFieldHandler) IsAsync() bool {
	return false
}

// SupportsOptions 公式字段支持选项
func (h *FormulaFieldHandler) SupportsOptions() bool {
	return true
}

// ValidateOptions 验证公式选项
func (h *FormulaFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	if field.Options() == nil || field.Options().Formula == nil {
		return fields.ErrMissingRequiredOption
	}

	expression := field.Options().Formula.Expression
	if expression == "" {
		return fields.NewDomainError(
			"EMPTY_FORMULA_EXPRESSION",
			"formula expression cannot be empty",
			nil,
		)
	}

	// 验证公式语法
	if err := h.evaluator.Validate(expression); err != nil {
		return fields.NewDomainError(
			"INVALID_FORMULA_SYNTAX",
			"invalid formula syntax",
			err,
		)
	}

	return nil
}
