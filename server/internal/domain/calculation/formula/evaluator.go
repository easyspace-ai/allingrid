package formula

import (
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula/parser"

	"github.com/antlr4-go/antlr/v4"
)

// Evaluate 求值公式表达式（完全对齐原版 evaluate函数）
// 这是计算引擎的核心入口函数
func Evaluate(
	expression string,
	dependencies map[string]interface{},
	record interface{},
	timeZone string,
) (*TypedValue, error) {
	// 1. 创建输入流（对齐原版）
	input := antlr.NewInputStream(expression)

	// 2. 词法分析（对齐原版）
	lexer := parser.NewFormulaLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)

	// 3. 语法分析（对齐原版）
	p := parser.NewFormula(stream)

	// 4. 移除默认错误监听器，添加自定义监听器（对齐原版）
	p.RemoveErrorListeners()
	errorListener := NewErrorListener()
	p.AddErrorListener(errorListener)

	// 5. 生成AST（对齐原版）
	tree := p.Root()

	// 6. 检查语法错误
	if errorListener.HasErrors() {
		return nil, fmt.Errorf("syntax error: %s", errorListener.GetFirstError())
	}

	// 7. 使用访问者模式求值（对齐原版）
	visitor := NewEvalVisitor(dependencies, record, timeZone)
	result := visitor.Visit(tree)

	// 8. 类型断言
	if typedValue, ok := result.(*TypedValue); ok {
		// 检查是否包含错误（对齐原版错误处理）
		if typedValue.Type == CellValueTypeString {
			if str, ok := typedValue.Value.(string); ok {
				// 检测所有#ERROR开头的错误字符串
				if len(str) >= 7 && str[:7] == "#ERROR" {
					// #ERROR: message 格式
					if len(str) > 8 && str[:8] == "#ERROR: " {
						return nil, fmt.Errorf("%s", str[8:])
					}
					// #ERROR! 格式
					if str == "#ERROR!" {
						return nil, fmt.Errorf("ERROR!")
					}
					// 其他#ERROR开头的
					return nil, fmt.Errorf("%s", str)
				}
			}
		}
		return typedValue, nil
	}

	return nil, fmt.Errorf("unexpected result type")
}

// ErrorListener 错误监听器（对齐原版 FormulaErrorListener）
type ErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

// NewErrorListener 创建错误监听器
func NewErrorListener() *ErrorListener {
	return &ErrorListener{
		DefaultErrorListener: antlr.NewDefaultErrorListener(),
		errors:               []string{},
	}
}

// SyntaxError 语法错误回调（对齐原版）
func (l *ErrorListener) SyntaxError(
	recognizer antlr.Recognizer,
	offendingSymbol interface{},
	line, column int,
	msg string,
	e antlr.RecognitionException,
) {
	errorMsg := fmt.Sprintf("line %d:%d %s", line, column, msg)
	l.errors = append(l.errors, errorMsg)
}

// HasErrors 是否有错误
func (l *ErrorListener) HasErrors() bool {
	return len(l.errors) > 0
}

// GetErrors 获取所有错误
func (l *ErrorListener) GetErrors() []string {
	return l.errors
}

// GetFirstError 获取第一个错误
func (l *ErrorListener) GetFirstError() string {
	if len(l.errors) > 0 {
		return l.errors[0]
	}
	return ""
}
