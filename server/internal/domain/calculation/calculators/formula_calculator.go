package calculators

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/service"
)

// FormulaCalculator 公式字段计算器
type FormulaCalculator struct {
	BaseCalculator
	evaluator service.FormulaEvaluator
}

// NewFormulaCalculator 创建公式计算器
func NewFormulaCalculator(evaluator service.FormulaEvaluator) *FormulaCalculator {
	return &FormulaCalculator{
		evaluator: evaluator,
	}
}

// CanCalculate 判断是否可以计算该字段
func (c *FormulaCalculator) CanCalculate(field *entity.Field) bool {
	return field.Type().String() == valueobject.TypeFormula
}

// Calculate 计算公式字段值
func (c *FormulaCalculator) Calculate(
	ctx context.Context,
	field *entity.Field,
	record map[string]interface{},
	fieldMap map[string]*entity.Field,
) (interface{}, error) {
	options := field.Options()
	if options == nil || options.Formula == nil {
		return nil, fmt.Errorf("formula options not found")
	}

	expression := options.Formula.Expression
	if expression == "" {
		return nil, nil
	}

	// 构建字段实例映射（FormulaEvaluator需要）
	fieldInstanceMap := make(service.FieldInstanceMap)
	for id, f := range fieldMap {
		fieldInstanceMap[id] = f
	}

	// 调用公式评估器
	result, err := c.evaluator.Evaluate(expression, fieldInstanceMap, record)
	if err != nil {
		return nil, fmt.Errorf("formula evaluation failed: %w", err)
	}

	return result, nil
}
