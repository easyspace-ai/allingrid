package functions

import (
	"fmt"
	"math"
)

// BaseNumericFunc 数值函数基类
type BaseNumericFunc struct {
	name                string
	acceptValueType     map[CellValueType]bool
	acceptMultipleValue bool
}

func (f *BaseNumericFunc) Name() string {
	return f.name
}

func (f *BaseNumericFunc) Type() FormulaFuncType {
	return FuncTypeNumeric
}

func (f *BaseNumericFunc) AcceptValueType() map[CellValueType]bool {
	return f.acceptValueType
}

func (f *BaseNumericFunc) AcceptMultipleValue() bool {
	return f.acceptMultipleValue
}

// =========== SUM 函数 ===========
// 对齐原版 Sum

type SumFunc struct {
	BaseNumericFunc
}

func NewSumFunc() *SumFunc {
	return &SumFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: FuncSum,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *SumFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}

	// 检查参数类型（对齐原版）
	for i, param := range params {
		if param != nil && param.Type == CellValueTypeString {
			return fmt.Errorf("%s can't process string type param at %d", f.Name(), i+1)
		}
	}

	return nil
}

func (f *SumFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *SumFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	sum := 0.0

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况：求和所有值（对齐原版）
			if arr, ok := param.Value.([]interface{}); ok {
				for _, v := range arr {
					if v != nil {
						if num, ok := v.(float64); ok {
							sum += num
						} else if num, ok := v.(int); ok {
							sum += float64(num)
						}
					}
				}
			}
		} else {
			// 单值情况
			sum += param.AsNumber()
		}
	}

	return NewTypedValue(sum, CellValueTypeNumber), nil
}

// =========== AVERAGE 函数 ===========
// 对齐原版 Average

type AverageFunc struct {
	BaseNumericFunc
}

func NewAverageFunc() *AverageFunc {
	return &AverageFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: FuncAverage,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *AverageFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}

	for i, param := range params {
		if param != nil && param.Type == CellValueTypeString {
			return fmt.Errorf("%s can't process string type param at %d", f.Name(), i+1)
		}
	}

	return nil
}

func (f *AverageFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *AverageFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	totalValue := 0.0
	totalCount := 0

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况（对齐原版）
			if arr, ok := param.Value.([]interface{}); ok {
				totalCount += len(arr)
				for _, v := range arr {
					if v != nil {
						if num, ok := v.(float64); ok {
							totalValue += num
						} else if num, ok := v.(int); ok {
							totalValue += float64(num)
						}
					}
				}
			}
		} else {
			// 单值情况
			totalCount++
			totalValue += param.AsNumber()
		}
	}

	if totalCount == 0 {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	avg := totalValue / float64(totalCount)
	return NewTypedValue(avg, CellValueTypeNumber), nil
}

// =========== MAX 函数 ===========
// 对齐原版 Max

type MaxFunc struct {
	BaseNumericFunc
}

func NewMaxFunc() *MaxFunc {
	return &MaxFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: FuncMax,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber:   true,
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *MaxFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}

	for i, param := range params {
		if param != nil && param.Type != CellValueTypeNumber && param.Type != CellValueTypeDateTime {
			return fmt.Errorf("%s can only process number or datetime type param at %d", f.Name(), i+1)
		}
	}

	return nil
}

func (f *MaxFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}

	// 返回第一个参数的类型（对齐原版）
	returnType := CellValueTypeNumber
	if len(params) > 0 && params[0] != nil {
		returnType = params[0].Type
	}

	return returnType, false, nil
}

func (f *MaxFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	var max *float64

	for _, param := range params {
		if param.IsMultiple {
			// 多值情况
			if arr, ok := param.Value.([]interface{}); ok {
				for _, v := range arr {
					if v != nil {
						value := toNumber(v)
						if value != nil {
							if max == nil || *value > *max {
								max = value
							}
						}
					}
				}
			}
		} else {
			// 单值情况
			if !param.IsNull() {
				value := param.AsNumber()
				if max == nil || value > *max {
					max = &value
				}
			}
		}
	}

	if max == nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(*max, CellValueTypeNumber), nil
}

// =========== MIN 函数 ===========
// 对齐原版 Min

type MinFunc struct {
	BaseNumericFunc
}

func NewMinFunc() *MinFunc {
	return &MinFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: FuncMin,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber:   true,
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *MinFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}

	for i, param := range params {
		if param != nil && param.Type != CellValueTypeNumber && param.Type != CellValueTypeDateTime {
			return fmt.Errorf("%s can only process number or datetime type param at %d", f.Name(), i+1)
		}
	}

	return nil
}

func (f *MinFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}

	returnType := CellValueTypeNumber
	if len(params) > 0 && params[0] != nil {
		returnType = params[0].Type
	}

	return returnType, false, nil
}

func (f *MinFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	var min *float64

	for _, param := range params {
		if param.IsMultiple {
			if arr, ok := param.Value.([]interface{}); ok {
				for _, v := range arr {
					if v != nil {
						value := toNumber(v)
						if value != nil {
							if min == nil || *value < *min {
								min = value
							}
						}
					}
				}
			}
		} else {
			if !param.IsNull() {
				value := param.AsNumber()
				if min == nil || value < *min {
					min = &value
				}
			}
		}
	}

	if min == nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(*min, CellValueTypeNumber), nil
}

// =========== ROUND 函数 ===========
// 对齐原版 Round

type RoundFunc struct {
	BaseNumericFunc
}

func NewRoundFunc() *RoundFunc {
	return &RoundFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: FuncRound,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *RoundFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *RoundFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *RoundFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0].AsNumber()

	// 精度（可选参数2）
	precision := 0
	if len(params) >= 2 {
		precision = int(params[1].AsNumber())
	}

	multiplier := math.Pow(10, float64(precision))
	result := math.Round(value*multiplier) / multiplier

	return NewTypedValue(result, CellValueTypeNumber), nil
}

// =========== ABS 函数 ===========
// 对齐原版 Abs

type AbsFunc struct {
	BaseNumericFunc
}

func NewAbsFunc() *AbsFunc {
	return &AbsFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: FuncAbs,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *AbsFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *AbsFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *AbsFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0].AsNumber()
	result := math.Abs(value)
	return NewTypedValue(result, CellValueTypeNumber), nil
}

// 辅助函数

func toNumber(v interface{}) *float64 {
	switch val := v.(type) {
	case float64:
		return &val
	case float32:
		f := float64(val)
		return &f
	case int:
		f := float64(val)
		return &f
	case int64:
		f := float64(val)
		return &f
	case int32:
		f := float64(val)
		return &f
	default:
		return nil
	}
}

// =========== CEILING 函数 ===========

type CeilingFunc struct {
	BaseNumericFunc
}

func NewCeilingFunc() *CeilingFunc {
	return &CeilingFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "CEILING",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *CeilingFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *CeilingFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *CeilingFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	num := params[0].AsNumber()
	return NewTypedValue(math.Ceil(num), CellValueTypeNumber), nil
}

// =========== FLOOR 函数 ===========

type FloorFunc struct {
	BaseNumericFunc
}

func NewFloorFunc() *FloorFunc {
	return &FloorFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "FLOOR",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *FloorFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *FloorFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *FloorFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	num := params[0].AsNumber()
	return NewTypedValue(math.Floor(num), CellValueTypeNumber), nil
}

// =========== SQRT 函数 ===========

type SqrtFunc struct {
	BaseNumericFunc
}

func NewSqrtFunc() *SqrtFunc {
	return &SqrtFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "SQRT",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *SqrtFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *SqrtFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *SqrtFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	num := params[0].AsNumber()
	return NewTypedValue(math.Sqrt(num), CellValueTypeNumber), nil
}

// =========== POWER 函数 ===========

type PowerFunc struct {
	BaseNumericFunc
}

func NewPowerFunc() *PowerFunc {
	return &PowerFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "POWER",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *PowerFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 2 {
		return fmt.Errorf("%s needs exactly 2 params", f.Name())
	}
	return nil
}

func (f *PowerFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *PowerFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	base := params[0].AsNumber()
	exponent := params[1].AsNumber()
	return NewTypedValue(math.Pow(base, exponent), CellValueTypeNumber), nil
}

// =========== MOD 函数 ===========

type ModFunc struct {
	BaseNumericFunc
}

func NewModFunc() *ModFunc {
	return &ModFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "MOD",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *ModFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 2 {
		return fmt.Errorf("%s needs exactly 2 params", f.Name())
	}
	return nil
}

func (f *ModFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *ModFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dividend := params[0].AsNumber()
	divisor := params[1].AsNumber()

	if divisor == 0 {
		return NewTypedValue(nil, CellValueTypeNull), fmt.Errorf("division by zero")
	}

	return NewTypedValue(math.Mod(dividend, divisor), CellValueTypeNumber), nil
}
