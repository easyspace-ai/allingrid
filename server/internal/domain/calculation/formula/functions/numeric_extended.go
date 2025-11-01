package functions

import (
	"fmt"
	"math"
	"strconv"
)

// =========== INT 函数 ===========
// 对齐原版 Int

type IntFunc struct {
	BaseNumericFunc
}

func NewIntFunc() *IntFunc {
	return &IntFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "INT",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *IntFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *IntFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *IntFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	num := params[0].AsNumber()
	return NewTypedValue(math.Floor(num), CellValueTypeNumber), nil
}

// =========== EVEN 函数 ===========
// 对齐原版 Even

type EvenFunc struct {
	BaseNumericFunc
}

func NewEvenFunc() *EvenFunc {
	return &EvenFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "EVEN",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *EvenFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *EvenFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *EvenFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0].AsNumber()

	var roundedValue float64
	if value > 0 {
		roundedValue = math.Ceil(value)
	} else {
		roundedValue = math.Floor(value)
	}

	// 如果已经是偶数，直接返回
	if int(roundedValue)%2 == 0 {
		return NewTypedValue(roundedValue, CellValueTypeNumber), nil
	}

	// 否则向远离0的方向找下一个偶数
	if roundedValue > 0 {
		return NewTypedValue(roundedValue+1, CellValueTypeNumber), nil
	}
	return NewTypedValue(roundedValue-1, CellValueTypeNumber), nil
}

// =========== ODD 函数 ===========
// 对齐原版 Odd

type OddFunc struct {
	BaseNumericFunc
}

func NewOddFunc() *OddFunc {
	return &OddFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "ODD",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *OddFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *OddFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *OddFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0].AsNumber()

	var roundedValue float64
	if value > 0 {
		roundedValue = math.Ceil(value)
	} else {
		roundedValue = math.Floor(value)
	}

	// 如果已经是奇数，直接返回
	if int(roundedValue)%2 != 0 {
		return NewTypedValue(roundedValue, CellValueTypeNumber), nil
	}

	// 否则向远离0的方向找下一个奇数
	if roundedValue > 0 {
		return NewTypedValue(roundedValue+1, CellValueTypeNumber), nil
	}
	return NewTypedValue(roundedValue-1, CellValueTypeNumber), nil
}

// =========== ROUNDUP 函数 ===========
// 对齐原版 RoundUp

type RoundUpFunc struct {
	BaseNumericFunc
}

func NewRoundUpFunc() *RoundUpFunc {
	return &RoundUpFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "ROUNDUP",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *RoundUpFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *RoundUpFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *RoundUpFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0].AsNumber()
	precision := 0
	if len(params) >= 2 {
		precision = int(math.Floor(params[1].AsNumber()))
	}

	offset := math.Pow(10, float64(precision))
	var roundFn func(float64) float64
	if value > 0 {
		roundFn = math.Ceil
	} else {
		roundFn = math.Floor
	}

	return NewTypedValue(roundFn(value*offset)/offset, CellValueTypeNumber), nil
}

// =========== ROUNDDOWN 函数 ===========
// 对齐原版 RoundDown

type RoundDownFunc struct {
	BaseNumericFunc
}

func NewRoundDownFunc() *RoundDownFunc {
	return &RoundDownFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "ROUNDDOWN",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *RoundDownFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *RoundDownFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *RoundDownFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	value := params[0].AsNumber()
	precision := 0
	if len(params) >= 2 {
		precision = int(math.Floor(params[1].AsNumber()))
	}

	offset := math.Pow(10, float64(precision))
	var roundFn func(float64) float64
	if value > 0 {
		roundFn = math.Floor
	} else {
		roundFn = math.Ceil
	}

	return NewTypedValue(roundFn(value*offset)/offset, CellValueTypeNumber), nil
}

// =========== VALUE 函数 ===========
// 对齐原版 Value

type ValueFunc struct {
	BaseNumericFunc
}

func NewValueFunc() *ValueFunc {
	return &ValueFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "VALUE",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *ValueFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *ValueFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *ValueFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	str := params[0].AsString()

	// 尝试解析为数字
	num, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(num, CellValueTypeNumber), nil
}

// =========== EXP 函数 ===========
// 对齐原版 Exp

type ExpFunc struct {
	BaseNumericFunc
}

func NewExpFunc() *ExpFunc {
	return &ExpFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "EXP",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *ExpFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *ExpFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *ExpFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	num := params[0].AsNumber()
	return NewTypedValue(math.Exp(num), CellValueTypeNumber), nil
}

// =========== LOG 函数 ===========
// 对齐原版 Log

type LogFunc struct {
	BaseNumericFunc
}

func NewLogFunc() *LogFunc {
	return &LogFunc{
		BaseNumericFunc: BaseNumericFunc{
			name: "LOG",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *LogFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *LogFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *LogFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	num := params[0].AsNumber()

	if num <= 0 {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	base := 10.0 // 默认底数为10
	if len(params) >= 2 {
		base = params[1].AsNumber()
		if base <= 0 || base == 1 {
			return NewTypedValue(nil, CellValueTypeNull), nil
		}
	}

	// log_base(x) = ln(x) / ln(base)
	result := math.Log(num) / math.Log(base)
	return NewTypedValue(result, CellValueTypeNumber), nil
}
