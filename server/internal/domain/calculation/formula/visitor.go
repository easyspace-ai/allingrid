package formula

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula/functions"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula/parser"

	"github.com/antlr4-go/antlr/v4"
)

// 类型别名，使用functions包的定义
type TypedValue = functions.TypedValue
type CellValueType = functions.CellValueType

const (
	CellValueTypeString   = functions.CellValueTypeString
	CellValueTypeNumber   = functions.CellValueTypeNumber
	CellValueTypeBoolean  = functions.CellValueTypeBoolean
	CellValueTypeDateTime = functions.CellValueTypeDateTime
	CellValueTypeNull     = functions.CellValueTypeNull
)

var NewTypedValue = functions.NewTypedValue

// EvalVisitor 公式求值访问者（完全对齐原版 EvalVisitor）
type EvalVisitor struct {
	*parser.BaseFormulaVisitor
	dependencies map[string]interface{}      // 依赖的字段映射
	record       interface{}                 // 当前记录
	timeZone     string                      // 时区
	funcRegistry *functions.FunctionRegistry // 函数注册表
}

// NewEvalVisitor 创建求值访问者
func NewEvalVisitor(
	dependencies map[string]interface{},
	record interface{},
	timeZone string,
) *EvalVisitor {
	if timeZone == "" {
		timeZone = "UTC"
	}

	return &EvalVisitor{
		BaseFormulaVisitor: &parser.BaseFormulaVisitor{},
		dependencies:       dependencies,
		record:             record,
		timeZone:           timeZone,
		funcRegistry:       functions.NewFunctionRegistry(),
	}
}

// Visit 访问节点
func (v *EvalVisitor) Visit(tree antlr.ParseTree) interface{} {
	return tree.Accept(v)
}

// VisitRoot 访问根节点（对齐原版）
func (v *EvalVisitor) VisitRoot(ctx *parser.RootContext) interface{} {
	return v.Visit(ctx.Expr())
}

// VisitStringLiteral 访问字符串字面量（对齐原版）
func (v *EvalVisitor) VisitStringLiteral(ctx *parser.StringLiteralContext) interface{} {
	// 提取字符串值（去掉引号）
	quotedString := ctx.GetText()
	rawString := quotedString[1 : len(quotedString)-1]

	// 处理转义字符（对齐原版）
	unescapedString := v.unescapeString(rawString)

	return NewTypedValue(unescapedString, CellValueTypeString)
}

// unescapeString 反转义字符串（对齐原版）
func (v *EvalVisitor) unescapeString(str string) string {
	replacer := strings.NewReplacer(
		"\\n", "\n",
		"\\r", "\r",
		"\\t", "\t",
		"\\b", "\b",
		"\\f", "\f",
		"\\v", "\v",
		"\\\\", "\\",
		"\\\"", "\"",
		"\\'", "'",
	)
	return replacer.Replace(str)
}

// VisitIntegerLiteral 访问整数字面量（对齐原版）
func (v *EvalVisitor) VisitIntegerLiteral(ctx *parser.IntegerLiteralContext) interface{} {
	text := ctx.GetText()
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return NewTypedValue(0, CellValueTypeNumber)
	}

	return NewTypedValue(float64(value), CellValueTypeNumber)
}

// VisitDecimalLiteral 访问小数字面量（对齐原版）
func (v *EvalVisitor) VisitDecimalLiteral(ctx *parser.DecimalLiteralContext) interface{} {
	text := ctx.GetText()
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return NewTypedValue(0.0, CellValueTypeNumber)
	}

	return NewTypedValue(value, CellValueTypeNumber)
}

// VisitBooleanLiteral 访问布尔字面量（对齐原版）
func (v *EvalVisitor) VisitBooleanLiteral(ctx *parser.BooleanLiteralContext) interface{} {
	text := strings.ToUpper(ctx.GetText())
	value := text == "TRUE"

	return NewTypedValue(value, CellValueTypeBoolean)
}

// VisitLeftWhitespaceOrComments 访问左空白或注释（对齐原版）
func (v *EvalVisitor) VisitLeftWhitespaceOrComments(ctx *parser.LeftWhitespaceOrCommentsContext) interface{} {
	return v.Visit(ctx.Expr())
}

// VisitRightWhitespaceOrComments 访问右空白或注释（对齐原版）
func (v *EvalVisitor) VisitRightWhitespaceOrComments(ctx *parser.RightWhitespaceOrCommentsContext) interface{} {
	return v.Visit(ctx.Expr())
}

// VisitBrackets 访问括号表达式（对齐原版）
func (v *EvalVisitor) VisitBrackets(ctx *parser.BracketsContext) interface{} {
	return v.Visit(ctx.Expr())
}

// VisitUnaryOp 访问一元运算符（对齐原版）
func (v *EvalVisitor) VisitUnaryOp(ctx *parser.UnaryOpContext) interface{} {
	operand := v.Visit(ctx.Expr()).(*TypedValue)

	// 负号运算
	if operand.Type == CellValueTypeNumber {
		return NewTypedValue(-operand.AsNumber(), CellValueTypeNumber)
	}

	// 其他类型返回0
	return NewTypedValue(0, CellValueTypeNumber)
}

// VisitBinaryOp 访问二元运算符（对齐原版）
func (v *EvalVisitor) VisitBinaryOp(ctx *parser.BinaryOpContext) interface{} {
	left := v.Visit(ctx.Expr(0)).(*TypedValue)
	right := v.Visit(ctx.Expr(1)).(*TypedValue)

	// 根据运算符类型执行运算（对齐原版）
	switch {
	case ctx.PLUS() != nil:
		return v.evalPlus(left, right)
	case ctx.MINUS() != nil:
		return v.evalMinus(left, right)
	case ctx.STAR() != nil:
		return v.evalMultiply(left, right)
	case ctx.SLASH() != nil:
		return v.evalDivide(left, right)
	case ctx.PERCENT() != nil:
		return v.evalModulo(left, right)
	case ctx.AMP() != nil:
		return v.evalConcat(left, right)
	case ctx.AMP_AMP() != nil:
		return v.evalAnd(left, right)
	case ctx.PIPE_PIPE() != nil:
		return v.evalOr(left, right)
	case ctx.EQUAL() != nil:
		return v.evalEqual(left, right)
	case ctx.BANG_EQUAL() != nil:
		return v.evalNotEqual(left, right)
	case ctx.GT() != nil:
		return v.evalGreater(left, right)
	case ctx.GTE() != nil:
		return v.evalGreaterEqual(left, right)
	case ctx.LT() != nil:
		return v.evalLess(left, right)
	case ctx.LTE() != nil:
		return v.evalLessEqual(left, right)
	default:
		return NewTypedValue(nil, CellValueTypeNull)
	}
}

// 二元运算实现（对齐原版）

func (v *EvalVisitor) evalPlus(left, right *TypedValue) *TypedValue {
	// 数字 + 数字 = 数字（对齐原版）
	if left.Type == CellValueTypeNumber && right.Type == CellValueTypeNumber {
		return NewTypedValue(left.AsNumber()+right.AsNumber(), CellValueTypeNumber)
	}

	// 其他情况：字符串连接（对齐原版）
	return NewTypedValue(fmt.Sprintf("%v%v", left.Value, right.Value), CellValueTypeString)
}

func (v *EvalVisitor) evalMinus(left, right *TypedValue) *TypedValue {
	return NewTypedValue(left.AsNumber()-right.AsNumber(), CellValueTypeNumber)
}

func (v *EvalVisitor) evalMultiply(left, right *TypedValue) *TypedValue {
	return NewTypedValue(left.AsNumber()*right.AsNumber(), CellValueTypeNumber)
}

func (v *EvalVisitor) evalDivide(left, right *TypedValue) *TypedValue {
	rightNum := right.AsNumber()
	if rightNum == 0 {
		// 除以0返回错误（对齐原版错误处理）
		return NewTypedValue("#ERROR: division by zero", CellValueTypeString)
	}
	return NewTypedValue(left.AsNumber()/rightNum, CellValueTypeNumber)
}

func (v *EvalVisitor) evalModulo(left, right *TypedValue) *TypedValue {
	leftInt := int(left.AsNumber())
	rightInt := int(right.AsNumber())
	if rightInt == 0 {
		return NewTypedValue(nil, CellValueTypeNull)
	}
	return NewTypedValue(float64(leftInt%rightInt), CellValueTypeNumber)
}

func (v *EvalVisitor) evalConcat(left, right *TypedValue) *TypedValue {
	// & 运算符：字符串连接（对齐原版）
	return NewTypedValue(fmt.Sprintf("%v%v", left.Value, right.Value), CellValueTypeString)
}

func (v *EvalVisitor) evalAnd(left, right *TypedValue) *TypedValue {
	return NewTypedValue(left.AsBoolean() && right.AsBoolean(), CellValueTypeBoolean)
}

func (v *EvalVisitor) evalOr(left, right *TypedValue) *TypedValue {
	return NewTypedValue(left.AsBoolean() || right.AsBoolean(), CellValueTypeBoolean)
}

func (v *EvalVisitor) evalEqual(left, right *TypedValue) *TypedValue {
	return NewTypedValue(v.compareValues(left, right) == 0, CellValueTypeBoolean)
}

func (v *EvalVisitor) evalNotEqual(left, right *TypedValue) *TypedValue {
	return NewTypedValue(v.compareValues(left, right) != 0, CellValueTypeBoolean)
}

func (v *EvalVisitor) evalGreater(left, right *TypedValue) *TypedValue {
	return NewTypedValue(v.compareValues(left, right) > 0, CellValueTypeBoolean)
}

func (v *EvalVisitor) evalGreaterEqual(left, right *TypedValue) *TypedValue {
	return NewTypedValue(v.compareValues(left, right) >= 0, CellValueTypeBoolean)
}

func (v *EvalVisitor) evalLess(left, right *TypedValue) *TypedValue {
	return NewTypedValue(v.compareValues(left, right) < 0, CellValueTypeBoolean)
}

func (v *EvalVisitor) evalLessEqual(left, right *TypedValue) *TypedValue {
	return NewTypedValue(v.compareValues(left, right) <= 0, CellValueTypeBoolean)
}

// compareValues 比较两个值（对齐原版）
func (v *EvalVisitor) compareValues(left, right *TypedValue) int {
	// 都是数字：数值比较
	if left.Type == CellValueTypeNumber && right.Type == CellValueTypeNumber {
		leftNum := left.AsNumber()
		rightNum := right.AsNumber()
		if leftNum < rightNum {
			return -1
		} else if leftNum > rightNum {
			return 1
		}
		return 0
	}

	// 字符串比较
	leftStr := fmt.Sprintf("%v", left.Value)
	rightStr := fmt.Sprintf("%v", right.Value)
	return strings.Compare(leftStr, rightStr)
}

// VisitFieldReferenceCurly 访问字段引用（对齐原版）
// 支持字段名称和字段ID两种引用方式：{字段名} 或 {fieldId}
func (v *EvalVisitor) VisitFieldReferenceCurly(ctx *parser.FieldReferenceCurlyContext) interface{} {
	// 提取字段ID/名称（去掉花括号）
	fieldRef := ctx.GetText()
	fieldKey := fieldRef[1 : len(fieldRef)-1]

	// 从依赖（recordData）中获取字段值
	if fieldValue, ok := v.dependencies[fieldKey]; ok {
		return v.convertToTypedValue(fieldValue)
	}

	// 字段不存在，返回null（对齐原版）
	return NewTypedValue(nil, CellValueTypeNull)
}

// convertToTypedValue 将Go值转换为TypedValue（对齐原版类型推断）
func (v *EvalVisitor) convertToTypedValue(value interface{}) *TypedValue {
	if value == nil {
		return NewTypedValue(nil, CellValueTypeNull)
	}

	// 根据值的类型推断CellValueType
	switch val := value.(type) {
	case string:
		return NewTypedValue(val, CellValueTypeString)
	case float64:
		return NewTypedValue(val, CellValueTypeNumber)
	case int:
		return NewTypedValue(float64(val), CellValueTypeNumber)
	case int64:
		return NewTypedValue(float64(val), CellValueTypeNumber)
	case bool:
		return NewTypedValue(val, CellValueTypeBoolean)
	case []interface{}:
		// 多值字段（对齐原版）
		return &TypedValue{
			Value:      val,
			Type:       CellValueTypeString, // 默认字符串类型
			IsMultiple: true,
			Field:      value,
		}
	case []string:
		// 字符串数组
		interfaceSlice := make([]interface{}, len(val))
		for i, v := range val {
			interfaceSlice[i] = v
		}
		return &TypedValue{
			Value:      interfaceSlice,
			Type:       CellValueTypeString,
			IsMultiple: true,
			Field:      value,
		}
	default:
		// 其他类型转为字符串（对齐原版）
		return NewTypedValue(fmt.Sprintf("%v", value), CellValueTypeString)
	}
}

// VisitFunctionCall 访问函数调用（对齐原版）
func (v *EvalVisitor) VisitFunctionCall(ctx *parser.FunctionCallContext) interface{} {
	funcName := strings.ToUpper(ctx.Func_name().GetText())

	// 获取函数实现
	fn := v.funcRegistry.GetFunction(funcName)
	if fn == nil {
		// 未知函数，返回错误
		return NewTypedValue(
			fmt.Sprintf("#ERROR: Unknown function: %s", funcName),
			CellValueTypeString,
		)
	}

	// 求值所有参数
	params := []*TypedValue{}
	if allExpr := ctx.AllExpr(); allExpr != nil {
		for _, exprCtx := range allExpr {
			param := v.Visit(exprCtx).(*TypedValue)
			params = append(params, param)
		}
	}

	// 验证参数
	if err := fn.ValidateParams(params); err != nil {
		return NewTypedValue(
			fmt.Sprintf("#ERROR: %s", err.Error()),
			CellValueTypeString,
		)
	}

	// 创建函数上下文
	context := functions.NewFormulaContext(v.record, v.timeZone, v.dependencies)

	// 执行函数
	result, err := fn.Eval(params, context)
	if err != nil {
		return NewTypedValue(
			fmt.Sprintf("#ERROR: %s", err.Error()),
			CellValueTypeString,
		)
	}

	return result
}
