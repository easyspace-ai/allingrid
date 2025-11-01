package functions

import "fmt"

// =========== SWITCH 函数 ===========
// 对齐原版 Switch

type SwitchFunc struct {
	BaseLogicalFunc
}

func NewSwitchFunc() *SwitchFunc {
	return &SwitchFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: "SWITCH",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString:   true,
				CellValueTypeNumber:   true,
				CellValueTypeBoolean:  true,
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *SwitchFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *SwitchFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}

	paramsLength := len(params)

	// 如果只有2个参数，返回第二个参数的类型
	if paramsLength <= 2 {
		return params[1].Type, params[1].IsMultiple, nil
	}

	// 检查所有结果值的类型，如果类型不一致，返回string
	expectedType := params[2].Type
	expectedIsMultiple := params[2].IsMultiple

	for i := 2; i < paramsLength; i += 2 {
		if !params[i].IsNull() {
			if expectedType != params[i].Type {
				expectedType = CellValueTypeString
			}
			if expectedIsMultiple != params[i].IsMultiple {
				expectedIsMultiple = false
			}
		}
	}

	// 如果有默认值（偶数个参数），也检查默认值的类型
	if paramsLength%2 == 0 {
		defaultParam := params[paramsLength-1]
		if !defaultParam.IsNull() {
			if expectedType != defaultParam.Type {
				expectedType = CellValueTypeString
			}
			if expectedIsMultiple != defaultParam.IsMultiple {
				expectedIsMultiple = false
			}
		}
	}

	return expectedType, expectedIsMultiple, nil
}

func (f *SwitchFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	paramsLength := len(params)
	expression := params[0]

	// 如果参数个数是偶数，最后一个是默认值
	if paramsLength%2 == 0 {
		defaultValue := params[paramsLength-1]

		// 检查所有case
		for i := 1; i < paramsLength-1; i += 2 {
			currentCase := params[i]
			currentValue := params[i+1]

			// 比较值（对齐原版：需要类型和值都相等）
			if compareValues(expression, currentCase) {
				return currentValue, nil
			}
		}
		return defaultValue, nil
	}

	// 没有默认值的情况
	for i := 1; i < paramsLength; i += 2 {
		currentCase := params[i]
		currentValue := params[i+1]

		if compareValues(expression, currentCase) {
			return currentValue, nil
		}
	}

	// 没有匹配且没有默认值，返回null
	return NewTypedValue(nil, CellValueTypeNull), nil
}

// compareValues 比较两个TypedValue是否相等
func compareValues(a, b *TypedValue) bool {
	if a.Type != b.Type {
		return false
	}

	// 对于简单类型，直接比较值
	if a.Value == nil && b.Value == nil {
		return true
	}
	if a.Value == nil || b.Value == nil {
		return false
	}

	return fmt.Sprintf("%v", a.Value) == fmt.Sprintf("%v", b.Value)
}

// =========== XOR 函数 ===========
// 对齐原版 Xor（异或：true的个数为奇数时返回true）

type XorFunc struct {
	BaseLogicalFunc
}

func NewXorFunc() *XorFunc {
	return &XorFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: "XOR",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeBoolean: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *XorFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *XorFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *XorFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	count := 0

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况：计算所有true的个数
			if arr, ok := param.Value.([]interface{}); ok {
				for _, v := range arr {
					if v != nil && toBool(v) {
						count++
					}
				}
			}
		} else {
			// 单值情况
			if param.AsBoolean() {
				count++
			}
		}
	}

	// XOR: true的个数是奇数时返回true（对齐原版：count & 1）
	return NewTypedValue(count%2 == 1, CellValueTypeBoolean), nil
}

// =========== ERROR 函数 ===========
// 对齐原版 FormulaError

type ErrorFunc struct {
	BaseLogicalFunc
}

func NewErrorFunc() *ErrorFunc {
	return &ErrorFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: "ERROR",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *ErrorFunc) ValidateParams(params []*TypedValue) error {
	// ERROR函数接受任意参数
	return nil
}

func (f *ErrorFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeString, false, nil
}

func (f *ErrorFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	errText := ""
	if len(params) > 0 {
		errText = params[0].AsString()
	}

	// 返回错误格式的字符串（对齐原版）
	errorMsg := "#ERROR!"
	if errText != "" {
		errorMsg = "#ERROR: " + errText
	}

	return NewTypedValue(errorMsg, CellValueTypeString), nil
}

// =========== IS_ERROR 函数 ===========
// 对齐原版 IsError

type IsErrorFunc struct {
	BaseLogicalFunc
}

func NewIsErrorFunc() *IsErrorFunc {
	return &IsErrorFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: "IS_ERROR",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString:   true,
				CellValueTypeNumber:   true,
				CellValueTypeBoolean:  true,
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *IsErrorFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s only allow 1 param", f.Name())
	}
	return nil
}

func (f *IsErrorFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *IsErrorFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0]

	// 检查值是否是错误格式（对齐原版）
	if value.Type == CellValueTypeString {
		if str, ok := value.Value.(string); ok {
			isError := len(str) >= 7 && str[:7] == "#ERROR"
			return NewTypedValue(isError, CellValueTypeBoolean), nil
		}
	}

	return NewTypedValue(false, CellValueTypeBoolean), nil
}

// toBool 将值转换为布尔值
func toBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case int:
		return val != 0
	case string:
		return val != ""
	default:
		return false
	}
}
