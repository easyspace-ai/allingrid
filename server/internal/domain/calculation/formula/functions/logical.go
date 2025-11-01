package functions

import "fmt"

// BaseLogicalFunc 逻辑函数基类
type BaseLogicalFunc struct {
	name                string
	acceptValueType     map[CellValueType]bool
	acceptMultipleValue bool
}

func (f *BaseLogicalFunc) Name() string                            { return f.name }
func (f *BaseLogicalFunc) Type() FormulaFuncType                   { return FuncTypeLogical }
func (f *BaseLogicalFunc) AcceptValueType() map[CellValueType]bool { return f.acceptValueType }
func (f *BaseLogicalFunc) AcceptMultipleValue() bool               { return f.acceptMultipleValue }

// =========== IF 函数 ===========
// 对齐原版 If

type IfFunc struct {
	BaseLogicalFunc
}

func NewIfFunc() *IfFunc {
	return &IfFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: FuncIf,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeBoolean: true,
				CellValueTypeString:  true,
				CellValueTypeNumber:  true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *IfFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 || len(params) > 3 {
		return fmt.Errorf("%s needs 2 or 3 params", f.Name())
	}
	return nil
}

func (f *IfFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	// 返回值类型取决于then分支的类型（对齐原版）
	if len(params) >= 2 {
		return params[1].Type, params[1].IsMultiple, nil
	}
	return CellValueTypeString, false, nil
}

func (f *IfFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 参数1: 条件
	condition := params[0].AsBoolean()

	if condition {
		// 返回then分支（参数2）
		return params[1], nil
	}

	// 返回else分支（参数3，如果有）
	if len(params) >= 3 {
		return params[2], nil
	}

	// 没有else分支，返回空字符串（对齐原版）
	return NewTypedValue("", CellValueTypeString), nil
}

// =========== AND 函数 ===========
// 对齐原版 And

type AndFunc struct {
	BaseLogicalFunc
}

func NewAndFunc() *AndFunc {
	return &AndFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: FuncAnd,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeBoolean: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *AndFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *AndFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *AndFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	for _, param := range params {
		if !param.AsBoolean() {
			return NewTypedValue(false, CellValueTypeBoolean), nil
		}
	}
	return NewTypedValue(true, CellValueTypeBoolean), nil
}

// =========== OR 函数 ===========
// 对齐原版 Or

type OrFunc struct {
	BaseLogicalFunc
}

func NewOrFunc() *OrFunc {
	return &OrFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: FuncOr,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeBoolean: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *OrFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *OrFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *OrFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	for _, param := range params {
		if param.AsBoolean() {
			return NewTypedValue(true, CellValueTypeBoolean), nil
		}
	}
	return NewTypedValue(false, CellValueTypeBoolean), nil
}

// =========== NOT 函数 ===========
// 对齐原版 Not

type NotFunc struct {
	BaseLogicalFunc
}

func NewNotFunc() *NotFunc {
	return &NotFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: FuncNot,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeBoolean: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *NotFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *NotFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *NotFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	return NewTypedValue(!params[0].AsBoolean(), CellValueTypeBoolean), nil
}

// =========== BLANK 函数 ===========
// 对齐原版 Blank

type BlankFunc struct {
	BaseLogicalFunc
}

func NewBlankFunc() *BlankFunc {
	return &BlankFunc{
		BaseLogicalFunc: BaseLogicalFunc{
			name: FuncBlank,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *BlankFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *BlankFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeString, false, nil
}

func (f *BlankFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	return NewTypedValue("", CellValueTypeString), nil
}
