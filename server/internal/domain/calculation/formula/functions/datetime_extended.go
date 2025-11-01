package functions

import (
	"fmt"
	"strings"
	"time"
)

// =========== DATETIME_DIFF 函数 ===========
// 对齐原版 DatetimeDiff

type DatetimeDiffFunc struct {
	BaseDateTimeFunc
}

func NewDatetimeDiffFunc() *DatetimeDiffFunc {
	return &DatetimeDiffFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "DATETIME_DIFF",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
				CellValueTypeBoolean:  true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *DatetimeDiffFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *DatetimeDiffFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *DatetimeDiffFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 参数1: 开始日期
	startDateStr := params[0].AsString()
	startDate, err := time.Parse(time.RFC3339, startDateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 参数2: 结束日期
	endDateStr := params[1].AsString()
	endDate, err := time.Parse(time.RFC3339, endDateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 参数3: 单位（可选，默认为"day"）
	unit := "day"
	if len(params) >= 3 {
		unit = params[2].AsString()
	}

	// 参数4: 是否返回小数（可选，默认false）
	isFloat := false
	if len(params) >= 4 {
		isFloat = params[3].AsBoolean()
	}

	// 计算差值
	diff := calculateDateDiff(startDate, endDate, unit, isFloat)
	return NewTypedValue(diff, CellValueTypeNumber), nil
}

// calculateDateDiff 计算两个日期的差值
func calculateDateDiff(start, end time.Time, unit string, isFloat bool) float64 {
	duration := start.Sub(end)

	switch unit {
	case "second", "seconds":
		if isFloat {
			return duration.Seconds()
		}
		return float64(int(duration.Seconds()))

	case "minute", "minutes":
		if isFloat {
			return duration.Minutes()
		}
		return float64(int(duration.Minutes()))

	case "hour", "hours":
		if isFloat {
			return duration.Hours()
		}
		return float64(int(duration.Hours()))

	case "day", "days":
		if isFloat {
			return duration.Hours() / 24
		}
		return float64(int(duration.Hours() / 24))

	case "week", "weeks":
		if isFloat {
			return duration.Hours() / 24 / 7
		}
		return float64(int(duration.Hours() / 24 / 7))

	case "month", "months":
		// 月份差值：年差*12 + 月差
		months := (start.Year()-end.Year())*12 + int(start.Month()) - int(end.Month())
		if isFloat {
			// 考虑天数的小数部分
			dayDiff := float64(start.Day()-end.Day()) / 30.0
			return float64(months) + dayDiff
		}
		return float64(months)

	case "year", "years":
		years := start.Year() - end.Year()
		if isFloat {
			// 考虑月份和天数的小数部分
			monthDiff := float64(int(start.Month())-int(end.Month())) / 12.0
			dayDiff := float64(start.Day()-end.Day()) / 365.0
			return float64(years) + monthDiff + dayDiff
		}
		return float64(years)

	default:
		// 默认返回天数
		return float64(int(duration.Hours() / 24))
	}
}

// =========== DATE_ADD 函数 ===========
// 对齐原版 DateAdd

type DateAddFunc struct {
	BaseDateTimeFunc
}

func NewDateAddFunc() *DateAddFunc {
	return &DateAddFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "DATE_ADD",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
				CellValueTypeNumber:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *DateAddFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 3 {
		return fmt.Errorf("%s needs at least 3 params", f.Name())
	}
	return nil
}

func (f *DateAddFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeDateTime, false, nil
}

func (f *DateAddFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 参数1: 日期
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 参数2: 数量
	count := int(params[1].AsNumber())

	// 参数3: 单位
	unit := params[2].AsString()

	// 执行日期加法
	newDate := addToDate(date, count, unit)

	return NewTypedValue(newDate.Format(time.RFC3339), CellValueTypeDateTime), nil
}

// addToDate 给日期加上指定数量的单位
func addToDate(date time.Time, count int, unit string) time.Time {
	switch unit {
	case "second", "seconds":
		return date.Add(time.Duration(count) * time.Second)

	case "minute", "minutes":
		return date.Add(time.Duration(count) * time.Minute)

	case "hour", "hours":
		return date.Add(time.Duration(count) * time.Hour)

	case "day", "days":
		return date.AddDate(0, 0, count)

	case "week", "weeks":
		return date.AddDate(0, 0, count*7)

	case "month", "months":
		return date.AddDate(0, count, 0)

	case "year", "years":
		return date.AddDate(count, 0, 0)

	default:
		// 默认按天处理
		return date.AddDate(0, 0, count)
	}
}

// =========== WEEKNUM 函数 ===========
// 对齐原版 WeekNum

type WeekNumFunc struct {
	BaseDateTimeFunc
}

func NewWeekNumFunc() *WeekNumFunc {
	return &WeekNumFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "WEEKNUM",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *WeekNumFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *WeekNumFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *WeekNumFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 获取年份第一天
	yearStart := time.Date(date.Year(), 1, 1, 0, 0, 0, 0, date.Location())

	// 计算是第几周
	days := int(date.Sub(yearStart).Hours() / 24)
	weekNum := (days / 7) + 1

	return NewTypedValue(float64(weekNum), CellValueTypeNumber), nil
}

// =========== WEEKDAY 函数 ===========
// 对齐原版 Weekday

type WeekdayFunc struct {
	BaseDateTimeFunc
}

func NewWeekdayFunc() *WeekdayFunc {
	return &WeekdayFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "WEEKDAY",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *WeekdayFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *WeekdayFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *WeekdayFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 返回星期几的名称
	weekdays := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	weekday := weekdays[date.Weekday()]

	return NewTypedValue(weekday, CellValueTypeString), nil
}

// =========== IS_SAME 函数 ===========

type IsSameFunc struct {
	BaseDateTimeFunc
}

func NewIsSameFunc() *IsSameFunc {
	return &IsSameFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "IS_SAME",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *IsSameFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *IsSameFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *IsSameFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	date1Str := params[0].AsString()
	date1, err := time.Parse(time.RFC3339, date1Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	date2Str := params[1].AsString()
	date2, err := time.Parse(time.RFC3339, date2Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 单位（可选，默认为day）
	unit := "day"
	if len(params) >= 3 {
		unit = params[2].AsString()
	}

	isSame := areDatesSame(date1, date2, unit)
	return NewTypedValue(isSame, CellValueTypeBoolean), nil
}

// =========== IS_AFTER 函数 ===========

type IsAfterFunc struct {
	BaseDateTimeFunc
}

func NewIsAfterFunc() *IsAfterFunc {
	return &IsAfterFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "IS_AFTER",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *IsAfterFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *IsAfterFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *IsAfterFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	date1Str := params[0].AsString()
	date1, err := time.Parse(time.RFC3339, date1Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	date2Str := params[1].AsString()
	date2, err := time.Parse(time.RFC3339, date2Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 单位（可选）
	unit := "day"
	if len(params) >= 3 {
		unit = params[2].AsString()
	}

	isAfter := isDateAfter(date1, date2, unit)
	return NewTypedValue(isAfter, CellValueTypeBoolean), nil
}

// =========== IS_BEFORE 函数 ===========

type IsBeforeFunc struct {
	BaseDateTimeFunc
}

func NewIsBeforeFunc() *IsBeforeFunc {
	return &IsBeforeFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "IS_BEFORE",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *IsBeforeFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *IsBeforeFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeBoolean, false, nil
}

func (f *IsBeforeFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	date1Str := params[0].AsString()
	date1, err := time.Parse(time.RFC3339, date1Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	date2Str := params[1].AsString()
	date2, err := time.Parse(time.RFC3339, date2Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 单位（可选）
	unit := "day"
	if len(params) >= 3 {
		unit = params[2].AsString()
	}

	isBefore := isDateBefore(date1, date2, unit)
	return NewTypedValue(isBefore, CellValueTypeBoolean), nil
}

// =========== DATESTR 函数 ===========

type DatestrFunc struct {
	BaseDateTimeFunc
}

func NewDatestrFunc() *DatestrFunc {
	return &DatestrFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "DATESTR",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *DatestrFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *DatestrFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *DatestrFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 只返回日期部分（对齐原版）
	dateOnly := date.Format("2006-01-02")
	return NewTypedValue(dateOnly, CellValueTypeString), nil
}

// =========== TIMESTR 函数 ===========

type TimestrFunc struct {
	BaseDateTimeFunc
}

func NewTimestrFunc() *TimestrFunc {
	return &TimestrFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "TIMESTR",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *TimestrFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *TimestrFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *TimestrFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 只返回时间部分（对齐原版）
	timeOnly := date.Format("15:04:05")
	return NewTypedValue(timeOnly, CellValueTypeString), nil
}

// =========== 辅助函数 ===========

// areDatesSame 检查两个日期在指定单位下是否相同
func areDatesSame(date1, date2 time.Time, unit string) bool {
	switch unit {
	case "year", "years":
		return date1.Year() == date2.Year()
	case "month", "months":
		return date1.Year() == date2.Year() && date1.Month() == date2.Month()
	case "day", "days":
		return date1.Year() == date2.Year() && date1.Month() == date2.Month() && date1.Day() == date2.Day()
	case "hour", "hours":
		return date1.Truncate(time.Hour).Equal(date2.Truncate(time.Hour))
	case "minute", "minutes":
		return date1.Truncate(time.Minute).Equal(date2.Truncate(time.Minute))
	case "second", "seconds":
		return date1.Truncate(time.Second).Equal(date2.Truncate(time.Second))
	default:
		return date1.Equal(date2)
	}
}

// isDateAfter 检查date1是否在date2之后
func isDateAfter(date1, date2 time.Time, unit string) bool {
	switch unit {
	case "year", "years":
		return date1.Year() > date2.Year()
	case "month", "months":
		if date1.Year() != date2.Year() {
			return date1.Year() > date2.Year()
		}
		return date1.Month() > date2.Month()
	case "day", "days":
		d1 := time.Date(date1.Year(), date1.Month(), date1.Day(), 0, 0, 0, 0, date1.Location())
		d2 := time.Date(date2.Year(), date2.Month(), date2.Day(), 0, 0, 0, 0, date2.Location())
		return d1.After(d2)
	default:
		return date1.After(date2)
	}
}

// isDateBefore 检查date1是否在date2之前
func isDateBefore(date1, date2 time.Time, unit string) bool {
	switch unit {
	case "year", "years":
		return date1.Year() < date2.Year()
	case "month", "months":
		if date1.Year() != date2.Year() {
			return date1.Year() < date2.Year()
		}
		return date1.Month() < date2.Month()
	case "day", "days":
		d1 := time.Date(date1.Year(), date1.Month(), date1.Day(), 0, 0, 0, 0, date1.Location())
		d2 := time.Date(date2.Year(), date2.Month(), date2.Day(), 0, 0, 0, 0, date2.Location())
		return d1.Before(d2)
	default:
		return date1.Before(date2)
	}
}

// =========== FROMNOW 函数 ===========
// 对齐原版 FromNow（当前时间距离目标日期的时间差）

type FromNowFunc struct {
	BaseDateTimeFunc
}

func NewFromNowFunc() *FromNowFunc {
	return &FromNowFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "FROMNOW",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
				CellValueTypeBoolean:  true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *FromNowFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *FromNowFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *FromNowFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	targetDateStr := params[0].AsString()
	targetDate, err := time.Parse(time.RFC3339, targetDateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 获取时区
	loc, _ := time.LoadLocation(context.TimeZone)
	if loc == nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)

	// 单位（参数2）
	unit := "day"
	if len(params) >= 2 {
		unit = params[1].AsString()
	}

	// 是否返回小数（参数3）
	isFloat := false
	if len(params) >= 3 {
		isFloat = params[2].AsBoolean()
	}

	// 计算差值并取绝对值（对齐原版：Math.abs）
	diff := calculateDateDiff(now, targetDate, unit, isFloat)
	if diff < 0 {
		diff = -diff
	}

	return NewTypedValue(diff, CellValueTypeNumber), nil
}

// =========== TONOW 函数 ===========
// 对齐原版 ToNow（目标日期距离当前时间的时间差）

type ToNowFunc struct {
	BaseDateTimeFunc
}

func NewToNowFunc() *ToNowFunc {
	return &ToNowFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "TONOW",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
				CellValueTypeBoolean:  true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *ToNowFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *ToNowFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *ToNowFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// TONOW与FROMNOW相同（对齐原版：ToNow extends FromNow）
	fromNow := NewFromNowFunc()
	return fromNow.Eval(params, context)
}

// =========== DATETIME_FORMAT 函数 ===========

type DatetimeFormatFunc struct {
	BaseDateTimeFunc
}

func NewDatetimeFormatFunc() *DatetimeFormatFunc {
	return &DatetimeFormatFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "DATETIME_FORMAT",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *DatetimeFormatFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *DatetimeFormatFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *DatetimeFormatFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 格式字符串
	format := params[1].AsString()

	// 转换格式字符串（从dayjs格式到Go格式）
	goFormat := convertDateFormat(format)

	result := date.Format(goFormat)
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== DATETIME_PARSE 函数 ===========

type DatetimeParseFunc struct {
	BaseDateTimeFunc
}

func NewDatetimeParseFunc() *DatetimeParseFunc {
	return &DatetimeParseFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "DATETIME_PARSE",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *DatetimeParseFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *DatetimeParseFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeDateTime, false, nil
}

func (f *DatetimeParseFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	format := params[1].AsString()

	// 转换格式字符串
	goFormat := convertDateFormat(format)

	date, err := time.Parse(goFormat, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	return NewTypedValue(date.Format(time.RFC3339), CellValueTypeDateTime), nil
}

// =========== CREATED_TIME 函数 ===========

type CreatedTimeFunc struct {
	BaseDateTimeFunc
}

func NewCreatedTimeFunc() *CreatedTimeFunc {
	return &CreatedTimeFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "CREATED_TIME",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *CreatedTimeFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *CreatedTimeFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeDateTime, false, nil
}

func (f *CreatedTimeFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 从上下文记录中获取创建时间
	if context.Record != nil {
		if record, ok := context.Record.(map[string]interface{}); ok {
			if createdAt, ok := record["created_at"].(string); ok {
				return NewTypedValue(createdAt, CellValueTypeDateTime), nil
			}
			if createdAt, ok := record["createdTime"].(string); ok {
				return NewTypedValue(createdAt, CellValueTypeDateTime), nil
			}
		}
	}

	return NewTypedValue(nil, CellValueTypeNull), nil
}

// =========== LAST_MODIFIED_TIME 函数 ===========

type LastModifiedTimeFunc struct {
	BaseDateTimeFunc
}

func NewLastModifiedTimeFunc() *LastModifiedTimeFunc {
	return &LastModifiedTimeFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "LAST_MODIFIED_TIME",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *LastModifiedTimeFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 0 {
		return fmt.Errorf("%s needs 0 params", f.Name())
	}
	return nil
}

func (f *LastModifiedTimeFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	return CellValueTypeDateTime, false, nil
}

func (f *LastModifiedTimeFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 从上下文记录中获取最后修改时间
	if context.Record != nil {
		if record, ok := context.Record.(map[string]interface{}); ok {
			if updatedAt, ok := record["updated_at"].(string); ok {
				return NewTypedValue(updatedAt, CellValueTypeDateTime), nil
			}
			if updatedAt, ok := record["lastModifiedTime"].(string); ok {
				return NewTypedValue(updatedAt, CellValueTypeDateTime), nil
			}
		}
	}

	return NewTypedValue(nil, CellValueTypeNull), nil
}

// =========== WORKDAY 和 WORKDAY_DIFF 函数 ===========
// 简化实现（工作日计算较复杂，先实现基本版本）

type WorkdayFunc struct {
	BaseDateTimeFunc
}

func NewWorkdayFunc() *WorkdayFunc {
	return &WorkdayFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "WORKDAY",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
				CellValueTypeNumber:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *WorkdayFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *WorkdayFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeDateTime, false, nil
}

func (f *WorkdayFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	dateStr := params[0].AsString()
	date, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	days := int(params[1].AsNumber())

	// 简化实现：跳过周末
	result := date
	direction := 1
	if days < 0 {
		direction = -1
		days = -days
	}

	for days > 0 {
		result = result.AddDate(0, 0, direction)
		// 跳过周六周日
		if result.Weekday() != time.Saturday && result.Weekday() != time.Sunday {
			days--
		}
	}

	return NewTypedValue(result.Format(time.RFC3339), CellValueTypeDateTime), nil
}

type WorkdayDiffFunc struct {
	BaseDateTimeFunc
}

func NewWorkdayDiffFunc() *WorkdayDiffFunc {
	return &WorkdayDiffFunc{
		BaseDateTimeFunc: BaseDateTimeFunc{
			name: "WORKDAY_DIFF",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeDateTime: true,
				CellValueTypeString:   true,
				CellValueTypeNumber:   true,
			},
			acceptMultipleValue: false,
		},
	}
}

func (f *WorkdayDiffFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 2 {
		return fmt.Errorf("%s needs at least 2 params", f.Name())
	}
	return nil
}

func (f *WorkdayDiffFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *WorkdayDiffFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	date1Str := params[0].AsString()
	date1, err := time.Parse(time.RFC3339, date1Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	date2Str := params[1].AsString()
	date2, err := time.Parse(time.RFC3339, date2Str)
	if err != nil {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 简化实现：计算两个日期之间的工作日数量
	count := 0
	current := date1
	direction := 1
	if date2.Before(date1) {
		direction = -1
	}

	for {
		if direction == 1 && current.After(date2) {
			break
		}
		if direction == -1 && current.Before(date2) {
			break
		}

		// 如果是工作日（非周六周日），计数
		if current.Weekday() != time.Saturday && current.Weekday() != time.Sunday {
			count++
		}

		current = current.AddDate(0, 0, direction)
	}

	if direction == -1 {
		count = -count
	}

	return NewTypedValue(float64(count), CellValueTypeNumber), nil
}

// convertDateFormat 转换日期格式字符串（从dayjs格式到Go格式）
func convertDateFormat(format string) string {
	// 简化实现：支持常见格式
	replacements := map[string]string{
		"YYYY": "2006",
		"MM":   "01",
		"DD":   "02",
		"HH":   "15",
		"mm":   "04",
		"ss":   "05",
		"SSS":  "000",
	}

	result := format
	for old, new := range replacements {
		result = strings.Replace(result, old, new, -1)
	}

	return result
}
