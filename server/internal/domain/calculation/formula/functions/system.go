package functions

import "fmt"

// BaseSystemFunc 系统函数基类
type BaseSystemFunc struct {
	name                string
	acceptValueType     map[CellValueType]bool
	acceptMultipleValue bool
}

func (f *BaseSystemFunc) Name() string                            { return f.name }
func (f *BaseSystemFunc) Type() FormulaFuncType                   { return FuncTypeSystem }
func (f *BaseSystemFunc) AcceptValueType() map[CellValueType]bool { return f.acceptValueType }
func (f *BaseSystemFunc) AcceptMultipleValue() bool               { return f.acceptMultipleValue }

// =========== RECORD_ID 函数 ===========
// 对齐原版 RecordId

type RecordIdFunc struct {
	BaseSystemFunc
}

func NewRecordIdFunc() *RecordIdFunc {
	return &RecordIdFunc{
		BaseSystemFunc: BaseSystemFunc{
			name: "RECORD_ID",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *RecordIdFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *RecordIdFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeString, false, nil
}

func (f *RecordIdFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 从上下文中获取当前记录的ID（对齐原版）
	if context.Record != nil {
		if record, ok := context.Record.(map[string]interface{}); ok {
			if id, ok := record["id"].(string); ok {
				return NewTypedValue(id, CellValueTypeString), nil
			}
		}
	}

	return NewTypedValue(nil, CellValueTypeNull), nil
}

// =========== AUTO_NUMBER 函数 ===========
// 对齐原版 AutoNumber

type AutoNumberFunc struct {
	BaseSystemFunc
}

func NewAutoNumberFunc() *AutoNumberFunc {
	return &AutoNumberFunc{
		BaseSystemFunc: BaseSystemFunc{
			name: "AUTO_NUMBER",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *AutoNumberFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *AutoNumberFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeNumber, false, nil
}

func (f *AutoNumberFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 从上下文中获取自动编号（对齐原版）
	if context.Record != nil {
		if record, ok := context.Record.(map[string]interface{}); ok {
			if autoNum, ok := record["auto_number"].(float64); ok {
				return NewTypedValue(autoNum, CellValueTypeNumber), nil
			}
			if autoNum, ok := record["auto_number"].(int); ok {
				return NewTypedValue(float64(autoNum), CellValueTypeNumber), nil
			}
		}
	}

	return NewTypedValue(nil, CellValueTypeNull), nil
}

// =========== TEXT_ALL 函数 ===========
// 对齐原版 TextAll（连接所有文本字段）

type TextAllFunc struct {
	BaseSystemFunc
}

func NewTextAllFunc() *TextAllFunc {
	return &TextAllFunc{
		BaseSystemFunc: BaseSystemFunc{
			name: "TEXT_ALL",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *TextAllFunc) ValidateParams(params []*TypedValue) error {
	// TEXT_ALL接受0或1个参数（分隔符）
	if len(params) > 1 {
		return fmt.Errorf("%s needs 0 or 1 param", f.Name())
	}
	return nil
}

func (f *TextAllFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *TextAllFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	separator := " " // 默认分隔符
	if len(params) > 0 {
		separator = params[0].AsString()
	}

	// 从dependencies中获取所有文本字段并连接（对齐原版）
	var textParts []string

	if context.Dependencies != nil {
		for _, field := range context.Dependencies {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				if fieldType, ok := fieldMap["type"].(string); ok {
					// 只处理文本类型字段
					if fieldType == "singleLineText" || fieldType == "longText" {
						if value, ok := fieldMap["value"]; ok && value != nil {
							textParts = append(textParts, fmt.Sprintf("%v", value))
						}
					}
				}
			}
		}
	}

	result := ""
	if len(textParts) > 0 {
		result = textParts[0]
		for i := 1; i < len(textParts); i++ {
			result += separator + textParts[i]
		}
	}

	return NewTypedValue(result, CellValueTypeString), nil
}
