package functions

import (
	"fmt"
	"time"
)

// BaseDateTimeFunc 日期时间函数基类
type BaseDateTimeFunc struct {
	name                string
	acceptValueType     map[CellValueType]bool
	acceptMultipleValue bool
}

func (f *BaseDateTimeFunc) Name() string                            { return f.name }
func (f *BaseDateTimeFunc) Type() FormulaFuncType                   { return FuncTypeDateTime }
func (f *BaseDateTimeFunc) AcceptValueType() map[CellValueType]bool { return f.acceptValueType }
func (f *BaseDateTimeFunc) AcceptMultipleValue() bool               { return f.acceptMultipleValue }

// =========== TODAY 函数 ===========
// 对齐原版 Today

type TodayFunc struct {
	BaseDateTimeFunc
}

func NewTodayFunc() *TodayFunc {
	return &TodayFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncToday,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *TodayFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *TodayFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeDateTime, false, nil
}

func (f *TodayFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 获取时区
	loc, err := time.LoadLocation(context.TimeZone)
	if err != nil {
		loc = time.UTC
	}

	// 获取今天的日期（零点）
	now := time.Now().In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)

	return NewTypedValue(today.Format(time.RFC3339), CellValueTypeDateTime), nil
}

// =========== NOW 函数 ===========
// 对齐原版 Now

type NowFunc struct {
	BaseDateTimeFunc
}

func NewNowFunc() *NowFunc {
	return &NowFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncNow,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *NowFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *NowFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeDateTime, false, nil
}

func (f *NowFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 获取时区
	loc, err := time.LoadLocation(context.TimeZone)
	if err != nil {
		loc = time.UTC
	}

	// 当前时间
	now := time.Now().In(loc)

	return NewTypedValue(now.Format(time.RFC3339), CellValueTypeDateTime), nil
}

// =========== YEAR 函数 ===========
// 对齐原版 Year

type YearFunc struct {
	BaseDateTimeFunc
}

func NewYearFunc() *YearFunc {
	return &YearFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncYear,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *YearFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *YearFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *YearFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr, ok := params[0].Value.(string)
	if !ok {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(float64(t.Year()), CellValueTypeNumber), nil
}

// =========== MONTH 函数 ===========
// 对齐原版 Month

type MonthFunc struct {
	BaseDateTimeFunc
}

func NewMonthFunc() *MonthFunc {
	return &MonthFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncMonth,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *MonthFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *MonthFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *MonthFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr, ok := params[0].Value.(string)
	if !ok {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(float64(t.Month()), CellValueTypeNumber), nil
}

// =========== DAY 函数 ===========
// 对齐原版 Day

type DayFunc struct {
	BaseDateTimeFunc
}

func NewDayFunc() *DayFunc {
	return &DayFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncDay,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *DayFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *DayFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *DayFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr, ok := params[0].Value.(string)
	if !ok {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(float64(t.Day()), CellValueTypeNumber), nil
}

// =========== HOUR 函数 ===========
// 对齐原版 Hour

type HourFunc struct {
	BaseDateTimeFunc
}

func NewHourFunc() *HourFunc {
	return &HourFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncHour,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *HourFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *HourFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *HourFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr, ok := params[0].Value.(string)
	if !ok {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(float64(t.Hour()), CellValueTypeNumber), nil
}

// =========== MINUTE 函数 ===========

type MinuteFunc struct {
	BaseDateTimeFunc
}

func NewMinuteFunc() *MinuteFunc {
	return &MinuteFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncMinute,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *MinuteFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *MinuteFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *MinuteFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr, ok := params[0].Value.(string)
	if !ok {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(float64(t.Minute()), CellValueTypeNumber), nil
}

// =========== SECOND 函数 ===========

type SecondFunc struct {
	BaseDateTimeFunc
}

func NewSecondFunc() *SecondFunc {
	return &SecondFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: FuncSecond,
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *SecondFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *SecondFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *SecondFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr, ok := params[0].Value.(string)
	if !ok {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(float64(t.Second()), CellValueTypeNumber), nil
}
