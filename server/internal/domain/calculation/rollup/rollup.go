package rollup

import (
	"fmt"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula/functions"
)

// RollupFunction Rollup支持的汇总函数（对齐原版 ROLLUP_FUNCTIONS）
var RollupFunctions = []string{
	"countall({values})",
	"counta({values})",
	"count({values})",
	"sum({values})",
	"max({values})",
	"min({values})",
	"and({values})",
	"or({values})",
	"xor({values})",
	"array_join({values})",
	"array_unique({values})",
	"array_compact({values})",
	"concatenate({values})",
}

// RollupOptions Rollup字段配置（对齐原版 IRollupFieldOptions）
type RollupOptions struct {
	Expression string `json:"expression"` // 汇总表达式
	TimeZone   string `json:"timeZone"`   // 时区
}

// RollupCalculator Rollup汇总计算器
type RollupCalculator struct {
	timeZone string
}

// NewRollupCalculator 创建Rollup计算器
func NewRollupCalculator(timeZone string) *RollupCalculator {
	return &RollupCalculator{
		timeZone: timeZone,
	}
}

// Calculate 计算Rollup字段值（对齐原版）
// expression: 汇总表达式，如 "sum({values})"
// lookupValues: 从关联表查找到的值数组
// returns: 汇总后的结果
func (c *RollupCalculator) Calculate(expression string, lookupValues []interface{}) (interface{}, error) {
	// 创建虚拟字段用于求值（对齐原版）
	virtualField := &functions.TypedValue{
		Value:      lookupValues,
		Type:       functions.CellValueTypeString, // 根据实际类型可调整
		IsMultiple: true,
	}

	// 构建依赖上下文
	dependencies := map[string]interface{}{
		"values": virtualField,
	}

	// 构建记录上下文
	record := map[string]interface{}{
		"fields": map[string]interface{}{
			"values": lookupValues,
		},
	}

	// 使用公式引擎求值（对齐原版）
	result, err := formula.Evaluate(expression, dependencies, record, c.timeZone)
	if err != nil {
		return nil, fmt.Errorf("rollup calculation failed: %w", err)
	}

	return result.Value, nil
}

// ValidateExpression 验证Rollup表达式是否合法
func ValidateExpression(expression string) bool {
	for _, validExpr := range RollupFunctions {
		if expression == validExpr {
			return true
		}
	}
	return false
}

// GetDefaultExpression 获取默认汇总表达式
func GetDefaultExpression() string {
	return RollupFunctions[0] // "countall({values})"
}
