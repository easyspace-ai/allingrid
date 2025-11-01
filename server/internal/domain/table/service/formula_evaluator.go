package service

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"

	"github.com/Knetic/govaluate"
)

// FieldInstanceMap 字段实例映射
type FieldInstanceMap map[string]*fieldEntity.Field

// FormulaEvaluator 公式评估器接口
type FormulaEvaluator interface {
	Evaluate(expression string, fieldMap FieldInstanceMap, recordData map[string]interface{}) (interface{}, error)
}

// DefaultFormulaEvaluator 默认公式计算器实现
// 支持虚拟字段引用（如 {values}）
type DefaultFormulaEvaluator struct{}

// NewDefaultFormulaEvaluator 创建默认公式计算器
func NewDefaultFormulaEvaluator() FormulaEvaluator {
	return &DefaultFormulaEvaluator{}
}

// Evaluate 评估公式表达式
// 参考 teable-develop 的 evaluate 实现
func (e *DefaultFormulaEvaluator) Evaluate(
	expression string,
	fieldMap FieldInstanceMap,
	recordData map[string]interface{},
) (interface{}, error) {
	// 检查是否是聚合函数表达式
	if e.isAggregateFunction(expression) {
		return e.evaluateAggregateFunction(expression, fieldMap, recordData)
	}

	// 准备表达式（替换字段引用为实际值）
	preparedExpr, err := e.prepareExpression(expression, fieldMap, recordData)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare expression: %w", err)
	}

	// 使用 govaluate 评估表达式
	result, err := e.evaluateExpression(preparedExpr)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression: %w", err)
	}

	return result, nil
}

// isAggregateFunction 检查是否是聚合函数
func (e *DefaultFormulaEvaluator) isAggregateFunction(expression string) bool {
	aggregateFunctions := []string{
		"sum", "count", "countall", "counta", "average", "avg",
		"min", "max", "and", "or", "xor",
		"array_join", "array_unique", "array_compact", "concatenate",
	}

	lowerExpr := strings.ToLower(strings.TrimSpace(expression))
	for _, fn := range aggregateFunctions {
		if strings.HasPrefix(lowerExpr, fn+"(") {
			return true
		}
	}

	return false
}

// evaluateAggregateFunction 评估聚合函数
func (e *DefaultFormulaEvaluator) evaluateAggregateFunction(
	expression string,
	fieldMap FieldInstanceMap,
	recordData map[string]interface{},
) (interface{}, error) {
	// 解析函数名和参数
	re := regexp.MustCompile(`^(\w+)\((.*)\)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(expression))
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid aggregate function expression: %s", expression)
	}

	functionName := strings.ToLower(matches[1])
	argument := strings.TrimSpace(matches[2])

	// 提取参数中的字段引用
	fieldRef := strings.Trim(argument, "{}")

	// 获取字段值
	value, ok := recordData[fieldRef]
	if !ok {
		return nil, nil
	}

	// 转换为数组（如果不是数组）
	var values []interface{}
	switch v := value.(type) {
	case []interface{}:
		values = v
	case nil:
		return nil, nil
	default:
		values = []interface{}{v}
	}

	// 执行聚合函数
	return e.executeAggregateFunction(functionName, values)
}

// executeAggregateFunction 执行聚合函数
func (e *DefaultFormulaEvaluator) executeAggregateFunction(
	functionName string,
	values []interface{},
) (interface{}, error) {
	switch functionName {
	case "sum":
		return e.sum(values), nil

	case "count":
		return e.count(values), nil

	case "countall":
		return len(values), nil

	case "counta":
		return e.counta(values), nil

	case "average", "avg":
		return e.average(values), nil

	case "min":
		return e.min(values), nil

	case "max":
		return e.max(values), nil

	case "and":
		return e.and(values), nil

	case "or":
		return e.or(values), nil

	case "xor":
		return e.xor(values), nil

	case "array_join":
		return e.arrayJoin(values), nil

	case "array_unique":
		return e.arrayUnique(values), nil

	case "array_compact":
		return e.arrayCompact(values), nil

	case "concatenate":
		return e.concatenate(values), nil

	default:
		return nil, fmt.Errorf("unsupported aggregate function: %s", functionName)
	}
}

// 聚合函数实现

func (e *DefaultFormulaEvaluator) sum(values []interface{}) interface{} {
	sum := 0.0
	for _, v := range values {
		if num, ok := e.toFloat(v); ok {
			sum += num
		}
	}
	return sum
}

func (e *DefaultFormulaEvaluator) count(values []interface{}) interface{} {
	count := 0
	for _, v := range values {
		if _, ok := e.toFloat(v); ok {
			count++
		}
	}
	return count
}

func (e *DefaultFormulaEvaluator) counta(values []interface{}) interface{} {
	count := 0
	for _, v := range values {
		if v != nil && v != "" {
			count++
		}
	}
	return count
}

func (e *DefaultFormulaEvaluator) average(values []interface{}) interface{} {
	sum := 0.0
	count := 0
	for _, v := range values {
		if num, ok := e.toFloat(v); ok {
			sum += num
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

func (e *DefaultFormulaEvaluator) min(values []interface{}) interface{} {
	var min *float64
	for _, v := range values {
		if num, ok := e.toFloat(v); ok {
			if min == nil || num < *min {
				min = &num
			}
		}
	}
	if min == nil {
		return nil
	}
	return *min
}

func (e *DefaultFormulaEvaluator) max(values []interface{}) interface{} {
	var max *float64
	for _, v := range values {
		if num, ok := e.toFloat(v); ok {
			if max == nil || num > *max {
				max = &num
			}
		}
	}
	if max == nil {
		return nil
	}
	return *max
}

func (e *DefaultFormulaEvaluator) and(values []interface{}) interface{} {
	for _, v := range values {
		if b, ok := e.toBool(v); ok {
			if !b {
				return false
			}
		}
	}
	return true
}

func (e *DefaultFormulaEvaluator) or(values []interface{}) interface{} {
	for _, v := range values {
		if b, ok := e.toBool(v); ok {
			if b {
				return true
			}
		}
	}
	return false
}

func (e *DefaultFormulaEvaluator) xor(values []interface{}) interface{} {
	trueCount := 0
	for _, v := range values {
		if b, ok := e.toBool(v); ok && b {
			trueCount++
		}
	}
	return trueCount%2 == 1
}

func (e *DefaultFormulaEvaluator) arrayJoin(values []interface{}) interface{} {
	var result []string
	for _, v := range values {
		if v != nil {
			result = append(result, fmt.Sprintf("%v", v))
		}
	}
	return strings.Join(result, ", ")
}

func (e *DefaultFormulaEvaluator) arrayUnique(values []interface{}) interface{} {
	seen := make(map[string]bool)
	var result []interface{}
	for _, v := range values {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	return result
}

func (e *DefaultFormulaEvaluator) arrayCompact(values []interface{}) interface{} {
	var result []interface{}
	for _, v := range values {
		if v != nil && v != "" {
			result = append(result, v)
		}
	}
	return result
}

func (e *DefaultFormulaEvaluator) concatenate(values []interface{}) interface{} {
	var result strings.Builder
	for _, v := range values {
		if v != nil {
			result.WriteString(fmt.Sprintf("%v", v))
		}
	}
	return result.String()
}

// 工具函数

func (e *DefaultFormulaEvaluator) toFloat(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func (e *DefaultFormulaEvaluator) toBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b, true
		}
		return false, false
	case float64:
		return v != 0, true
	case int:
		return v != 0, true
	default:
		return false, false
	}
}

// prepareExpression 准备表达式（替换字段引用）
func (e *DefaultFormulaEvaluator) prepareExpression(
	expression string,
	fieldMap FieldInstanceMap,
	recordData map[string]interface{},
) (string, error) {
	// 查找所有字段引用 {field_name}
	re := regexp.MustCompile(`\{([^}]+)\}`)

	result := re.ReplaceAllStringFunc(expression, func(match string) string {
		fieldName := strings.Trim(match, "{}")

		// 获取字段值
		if value, exists := recordData[fieldName]; exists {
			return e.valueToExpressionString(value)
		}

		// 字段不存在，使用 null
		return "null"
	})

	return result, nil
}

// valueToExpressionString 将值转换为表达式字符串
func (e *DefaultFormulaEvaluator) valueToExpressionString(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case string:
		// 转义字符串
		escaped := strings.ReplaceAll(v, `"`, `\"`)
		return fmt.Sprintf(`"%s"`, escaped)
	case bool:
		return fmt.Sprintf("%t", v)
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf(`"%v"`, v)
	}
}

// evaluateExpression 评估表达式
func (e *DefaultFormulaEvaluator) evaluateExpression(expression string) (interface{}, error) {
	// 处理 null 值
	if expression == "null" {
		return nil, nil
	}

	// 创建可评估表达式
	expr, err := govaluate.NewEvaluableExpression(expression)
	if err != nil {
		return nil, err
	}

	// 添加自定义函数
	functions := e.getCustomFunctions()

	// 评估表达式
	result, err := expr.Evaluate(functions)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// getCustomFunctions 获取自定义函数
func (e *DefaultFormulaEvaluator) getCustomFunctions() map[string]interface{} {
	return map[string]interface{}{
		"ABS": func(args ...interface{}) (interface{}, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("ABS requires 1 argument")
			}
			if val, ok := e.toFloat(args[0]); ok {
				return math.Abs(val), nil
			}
			return nil, fmt.Errorf("ABS argument must be a number")
		},
		"ROUND": func(args ...interface{}) (interface{}, error) {
			if len(args) < 1 || len(args) > 2 {
				return nil, fmt.Errorf("ROUND requires 1 or 2 arguments")
			}
			val, ok := e.toFloat(args[0])
			if !ok {
				return nil, fmt.Errorf("ROUND first argument must be a number")
			}
			precision := 0
			if len(args) == 2 {
				if p, ok := e.toFloat(args[1]); ok {
					precision = int(p)
				}
			}
			multiplier := math.Pow(10, float64(precision))
			return math.Round(val*multiplier) / multiplier, nil
		},
		"CONCAT": func(args ...interface{}) (interface{}, error) {
			var result strings.Builder
			for _, arg := range args {
				result.WriteString(fmt.Sprintf("%v", arg))
			}
			return result.String(), nil
		},
		"IF": func(args ...interface{}) (interface{}, error) {
			if len(args) != 3 {
				return nil, fmt.Errorf("IF requires 3 arguments")
			}
			condition, ok := e.toBool(args[0])
			if !ok {
				return nil, fmt.Errorf("IF condition must be a boolean")
			}
			if condition {
				return args[1], nil
			}
			return args[2], nil
		},
	}
}
