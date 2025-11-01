package calculators

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/service"
)

// CalculatorFactory 计算器工厂
type CalculatorFactory struct {
	formulaCalc *FormulaCalculator
	lookupCalc  *LookupCalculator
	rollupCalc  *RollupCalculator
}

// NewCalculatorFactory 创建计算器工厂
func NewCalculatorFactory(
	evaluator service.FormulaEvaluator,
	recordRepo RecordRepository,
) *CalculatorFactory {
	return &CalculatorFactory{
		formulaCalc: NewFormulaCalculator(evaluator),
		lookupCalc:  NewLookupCalculator(recordRepo),
		rollupCalc:  NewRollupCalculator(recordRepo),
	}
}

// GetCalculator 根据字段类型获取计算器
func (f *CalculatorFactory) GetCalculator(field *entity.Field) Calculator {
	fieldType := field.Type().String()

	switch fieldType {
	case valueobject.TypeFormula:
		return f.formulaCalc
	case valueobject.TypeLookup:
		return f.lookupCalc
	case valueobject.TypeRollup:
		return f.rollupCalc
	default:
		return nil
	}
}

// CanCalculate 判断字段是否可以计算
func (f *CalculatorFactory) CanCalculate(field *entity.Field) bool {
	return f.GetCalculator(field) != nil
}
