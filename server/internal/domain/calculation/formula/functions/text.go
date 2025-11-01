package functions

import (
	"fmt"
	"strings"
)

// BaseTextFunc 文本函数基类
type BaseTextFunc struct {
	name                string
	acceptValueType     map[CellValueType]bool
	acceptMultipleValue bool
}

func (f *BaseTextFunc) Name() string {
	return f.name
}

func (f *BaseTextFunc) Type() FormulaFuncType {
	return FuncTypeText
}

func (f *BaseTextFunc) AcceptValueType() map[CellValueType]bool {
	return f.acceptValueType
}

func (f *BaseTextFunc) AcceptMultipleValue() bool {
	return f.acceptMultipleValue
}

// =========== CONCATENATE 函数 ===========
// 对齐原版 Concatenate

type ConcatenateFunc struct {
	BaseTextFunc
}

func NewConcatenateFunc() *ConcatenateFunc {
	return &ConcatenateFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncConcatenate,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *ConcatenateFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *ConcatenateFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *ConcatenateFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	var result strings.Builder

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况：用逗号连接
			if arr, ok := param.Value.([]interface{}); ok {
				for i, v := range arr {
					if i > 0 {
						result.WriteString(", ")
					}
					result.WriteString(fmt.Sprintf("%v", v))
				}
			}
		} else {
			// 单值情况
			result.WriteString(fmt.Sprintf("%v", param.Value))
		}
	}

	return NewTypedValue(result.String(), CellValueTypeString), nil
}

// =========== LEFT 函数 ===========
// 对齐原版 Left

type LeftFunc struct {
	BaseTextFunc
}

func NewLeftFunc() *LeftFunc {
	return &LeftFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncLeft,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *LeftFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *LeftFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *LeftFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 参数1: 字符串
	str := params[0].AsString()

	// 参数2: 长度
	length := int(params[1].AsNumber())

	if length < 0 {
		length = 0
	}

	if length > len(str) {
		length = len(str)
	}

	result := str[:length]
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== RIGHT 函数 ===========
// 对齐原版 Right

type RightFunc struct {
	BaseTextFunc
}

func NewRightFunc() *RightFunc {
	return &RightFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncRight,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *RightFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *RightFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *RightFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	str := params[0].AsString()
	length := int(params[1].AsNumber())

	if length < 0 {
		length = 0
	}

	if length > len(str) {
		length = len(str)
	}

	start := len(str) - length
	result := str[start:]

	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== UPPER 函数 ===========
// 对齐原版 Upper

type UpperFunc struct {
	BaseTextFunc
}

func NewUpperFunc() *UpperFunc {
	return &UpperFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncUpper,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *UpperFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *UpperFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *UpperFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	str := params[0].AsString()
	result := strings.ToUpper(str)
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== LOWER 函数 ===========
// 对齐原版 Lower

type LowerFunc struct {
	BaseTextFunc
}

func NewLowerFunc() *LowerFunc {
	return &LowerFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncLower,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *LowerFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *LowerFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *LowerFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	str := params[0].AsString()
	result := strings.ToLower(str)
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== TRIM 函数 ===========
// 对齐原版 Trim

type TrimFunc struct {
	BaseTextFunc
}

func NewTrimFunc() *TrimFunc {
	return &TrimFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncTrim,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *TrimFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *TrimFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *TrimFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	str := params[0].AsString()
	result := strings.TrimSpace(str)
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== LEN 函数 ===========
// 对齐原版 Len

type LenFunc struct {
	BaseTextFunc
}

func NewLenFunc() *LenFunc {
	return &LenFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncLen,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *LenFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *LenFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *LenFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	str := params[0].AsString()
	length := len(str)
	return NewTypedValue(float64(length), CellValueTypeNumber), nil
}

// =========== FIND 函数 ===========
// 对齐原版 Find

type FindFunc struct {
	BaseTextFunc
}

func NewFindFunc() *FindFunc {
	return &FindFunc{
		BaseTextFunc: BaseTextFunc{
			name: FuncFind,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *FindFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *FindFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *FindFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	findString := params[0].AsString()
	targetString := params[1].AsString()

	if findString == "" || targetString == "" {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 起始位置（可选参数3）
	startPos := 0
	if len(params) >= 3 {
		startPos = int(params[2].AsNumber())
		if startPos > 0 {
			startPos-- // 转为0基索引
		}
		if startPos < 0 {
			startPos = 0
		}
	}

	// 查找位置（1基索引，对齐原版）
	index := strings.Index(targetString[startPos:], findString)
	if index == -1 {
		return NewTypedValue(float64(0), CellValueTypeNumber), nil
	}

	// 返回1基索引位置（对齐原版）
	position := float64(startPos + index + 1)
	return NewTypedValue(position, CellValueTypeNumber), nil
}

// ConvertValueToString 转换值为字符串（对齐原版 convertValueToString）
func ConvertValueToString(param *TypedValue, separator string) string {
	if param == nil || param.IsNull() {
		return ""
	}

	if param.IsMultiple {
		if arr, ok := param.Value.([]interface{}); ok {
			parts := make([]string, len(arr))
			for i, v := range arr {
				parts[i] = fmt.Sprintf("%v", v)
			}
			return strings.Join(parts, separator)
		}
	}

	return fmt.Sprintf("%v", param.Value)
}
