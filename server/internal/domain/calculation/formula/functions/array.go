package functions

import (
	"fmt"
	"strings"
)

// BaseArrayFunc 数组函数基类
type BaseArrayFunc struct {
	name                string
	acceptValueType     map[CellValueType]bool
	acceptMultipleValue bool
}

func (f *BaseArrayFunc) Name() string                            { return f.name }
func (f *BaseArrayFunc) Type() FormulaFuncType                   { return FuncTypeArray }
func (f *BaseArrayFunc) AcceptValueType() map[CellValueType]bool { return f.acceptValueType }
func (f *BaseArrayFunc) AcceptMultipleValue() bool               { return f.acceptMultipleValue }

// =========== COUNT 函数 ===========
// 对齐原版 Count（仅计数数字）

type CountFunc struct {
	BaseArrayFunc
}

func NewCountFunc() *CountFunc {
	return &CountFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "COUNT",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *CountFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *CountFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *CountFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	count := 0

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况：计数所有数字
			if arr, ok := param.Value.([]interface{}); ok {
				for _, v := range arr {
					if isNumeric(v) {
						count++
					}
				}
			}
		} else {
			// 单值情况：只有数字类型才计数
			if param.Type == CellValueTypeNumber && !param.IsNull() {
				count++
			}
		}
	}

	return NewTypedValue(float64(count), CellValueTypeNumber), nil
}

// =========== COUNTA 函数 ===========
// 对齐原版 CountA（计数非空值）

type CountAFunc struct {
	BaseArrayFunc
}

func NewCountAFunc() *CountAFunc {
	return &CountAFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "COUNTA",
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

func (f *CountAFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *CountAFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *CountAFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	count := 0

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况：计数所有非空值
			if arr, ok := param.Value.([]interface{}); ok {
				for _, v := range arr {
					if v != nil {
						count++
					}
				}
			}
		} else {
			// 单值情况：非空就计数
			if !param.IsNull() {
				count++
			}
		}
	}

	return NewTypedValue(float64(count), CellValueTypeNumber), nil
}

// =========== COUNTALL 函数 ===========
// 对齐原版 CountAll（计数所有值，包括空值）

type CountAllFunc struct {
	BaseArrayFunc
}

func NewCountAllFunc() *CountAllFunc {
	return &CountAllFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "COUNTALL",
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

func (f *CountAllFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *CountAllFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *CountAllFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	count := 0

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况：计数所有元素（包括null）
			if arr, ok := param.Value.([]interface{}); ok {
				count += len(arr)
			}
		} else {
			// 单值情况：总是计数
			count++
		}
	}

	return NewTypedValue(float64(count), CellValueTypeNumber), nil
}

// =========== ARRAY_JOIN 函数 ===========
// 对齐原版 ArrayJoin

type ArrayJoinFunc struct {
	BaseArrayFunc
}

func NewArrayJoinFunc() *ArrayJoinFunc {
	return &ArrayJoinFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "ARRAY_JOIN",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *ArrayJoinFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *ArrayJoinFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *ArrayJoinFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	if len(params) == 0 {
		return NewTypedValue("", CellValueTypeString), nil
	}

	// 第一个参数是数组
	separator := ", " // 默认分隔符
	if len(params) > 1 {
		separator = params[1].AsString()
	}

	var parts []string

	if params[0].IsMultiple {
		if arr, ok := params[0].Value.([]interface{}); ok {
			for _, v := range arr {
				if v != nil {
					parts = append(parts, fmt.Sprintf("%v", v))
				}
			}
		}
	} else {
		if !params[0].IsNull() {
			parts = append(parts, params[0].AsString())
		}
	}

	return NewTypedValue(strings.Join(parts, separator), CellValueTypeString), nil
}

// =========== ARRAY_UNIQUE 函数 ===========
// 对齐原版 ArrayUnique

type ArrayUniqueFunc struct {
	BaseArrayFunc
}

func NewArrayUniqueFunc() *ArrayUniqueFunc {
	return &ArrayUniqueFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "ARRAY_UNIQUE",
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

func (f *ArrayUniqueFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *ArrayUniqueFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	// 返回数组类型
	return params[0].Type, true, nil
}

func (f *ArrayUniqueFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	if !params[0].IsMultiple {
		// 如果不是数组，直接返回
		return params[0], nil
	}

	arr, ok := params[0].Value.([]interface{})
	if !ok {
		return params[0], nil
	}

	// 去重
	seen := make(map[string]bool)
	var unique []interface{}

	for _, v := range arr {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, v)
		}
	}

	result := &TypedValue{
		Value:      unique,
		Type:       params[0].Type,
		IsMultiple: true,
	}
	return result, nil
}

// =========== ARRAY_FLATTEN 函数 ===========
// 对齐原版 ArrayFlatten

type ArrayFlattenFunc struct {
	BaseArrayFunc
}

func NewArrayFlattenFunc() *ArrayFlattenFunc {
	return &ArrayFlattenFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "ARRAY_FLATTEN",
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

func (f *ArrayFlattenFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *ArrayFlattenFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	// 返回数组类型
	if len(params) > 0 {
		return params[0].Type, true, nil
	}
	return CellValueTypeString, true, nil
}

func (f *ArrayFlattenFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	var flattened []interface{}

	for _, param := range params {
		if param.IsMultiple {
			if arr, ok := param.Value.([]interface{}); ok {
				flattened = append(flattened, arr...)
			}
		} else {
			if !param.IsNull() {
				flattened = append(flattened, param.Value)
			}
		}
	}

	resultType := CellValueTypeString
	if len(params) > 0 {
		resultType = params[0].Type
	}

	result := &TypedValue{
		Value:      flattened,
		Type:       resultType,
		IsMultiple: true,
	}
	return result, nil
}

// =========== ARRAY_COMPACT 函数 ===========
// 对齐原版 ArrayCompact（去除空值）

type ArrayCompactFunc struct {
	BaseArrayFunc
}

func NewArrayCompactFunc() *ArrayCompactFunc {
	return &ArrayCompactFunc{
		BaseArrayFunc: BaseArrayFunc{
			name: "ARRAY_COMPACT",
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

func (f *ArrayCompactFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *ArrayCompactFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return params[0].Type, true, nil
}

func (f *ArrayCompactFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	if !params[0].IsMultiple {
		// 如果不是数组，直接返回
		return params[0], nil
	}

	arr, ok := params[0].Value.([]interface{})
	if !ok {
		return params[0], nil
	}

	// 去除空值
	var compact []interface{}
	for _, v := range arr {
		if v != nil {
			// 也去除空字符串
			if str, ok := v.(string); ok && str == "" {
				continue
			}
			compact = append(compact, v)
		}
	}

	result := &TypedValue{
		Value:      compact,
		Type:       params[0].Type,
		IsMultiple: true,
	}
	return result, nil
}

// isNumeric 检查值是否为数字类型
func isNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}
