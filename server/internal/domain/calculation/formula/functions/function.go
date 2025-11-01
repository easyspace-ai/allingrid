package functions

// FormulaFuncType 公式函数类型（对齐原版 FormulaFuncType）
type FormulaFuncType string

const (
	FuncTypeArray    FormulaFuncType = "Array"
	FuncTypeDateTime FormulaFuncType = "DateTime"
	FuncTypeLogical  FormulaFuncType = "Logical"
	FuncTypeNumeric  FormulaFuncType = "Numeric"
	FuncTypeText     FormulaFuncType = "Text"
	FuncTypeSystem   FormulaFuncType = "System"
)

// CellValueType 单元格值类型（从formula包复制，避免循环导入）
type CellValueType string

const (
	CellValueTypeString   CellValueType = "string"
	CellValueTypeNumber   CellValueType = "number"
	CellValueTypeBoolean  CellValueType = "boolean"
	CellValueTypeDateTime CellValueType = "dateTime"
	CellValueTypeNull     CellValueType = "null"
)

// TypedValue 类型化的值（从formula包复制，避免循环导入）
type TypedValue struct {
	Value      interface{}
	Type       CellValueType
	IsMultiple bool
	Field      interface{}
}

// FormulaFunc 公式函数接口（对齐原版）
type FormulaFunc interface {
	// Name 函数名称
	Name() string

	// Type 函数类型
	Type() FormulaFuncType

	// AcceptValueType 接受的值类型集合
	AcceptValueType() map[CellValueType]bool

	// AcceptMultipleValue 是否接受多值
	AcceptMultipleValue() bool

	// ValidateParams 验证参数
	ValidateParams(params []*TypedValue) error

	// GetReturnType 获取返回类型
	GetReturnType(params []*TypedValue) (CellValueType, bool, error)

	// Eval 执行函数
	Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error)
}

// TypedValue 辅助方法

func (tv *TypedValue) IsNull() bool {
	return tv.Value == nil || tv.Type == CellValueTypeNull
}

func (tv *TypedValue) AsString() string {
	if tv.IsNull() {
		return ""
	}
	if str, ok := tv.Value.(string); ok {
		return str
	}
	return ""
}

func (tv *TypedValue) AsNumber() float64 {
	if tv.IsNull() {
		return 0
	}
	switch v := tv.Value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}

func (tv *TypedValue) AsBoolean() bool {
	if tv.IsNull() {
		return false
	}
	if b, ok := tv.Value.(bool); ok {
		return b
	}
	return false
}

func NewTypedValue(value interface{}, valueType CellValueType) *TypedValue {
	return &TypedValue{
		Value:      value,
		Type:       valueType,
		IsMultiple: false,
		Field:      nil,
	}
}

// FormulaContext 公式上下文（对齐原版 IFormulaContext）
type FormulaContext struct {
	Record       interface{}            // 当前记录
	TimeZone     string                 // 时区
	Dependencies map[string]interface{} // 依赖的字段映射
}

// NewFormulaContext 创建公式上下文
func NewFormulaContext(record interface{}, timeZone string, dependencies map[string]interface{}) *FormulaContext {
	return &FormulaContext{
		Record:       record,
		TimeZone:     timeZone,
		Dependencies: dependencies,
	}
}

// FunctionName 函数名称枚举（对齐原版 FunctionName）
const (
	// Numeric 数值函数
	FuncSum       = "SUM"
	FuncAverage   = "AVERAGE"
	FuncMax       = "MAX"
	FuncMin       = "MIN"
	FuncRound     = "ROUND"
	FuncRoundUp   = "ROUNDUP"
	FuncRoundDown = "ROUNDDOWN"
	FuncCeiling   = "CEILING"
	FuncFloor     = "FLOOR"
	FuncAbs       = "ABS"
	FuncSqrt      = "SQRT"
	FuncPower     = "POWER"
	FuncMod       = "MOD"
	FuncValue     = "VALUE"

	// Text 文本函数
	FuncConcatenate        = "CONCATENATE"
	FuncFind               = "FIND"
	FuncSearch             = "SEARCH"
	FuncMid                = "MID"
	FuncLeft               = "LEFT"
	FuncRight              = "RIGHT"
	FuncReplace            = "REPLACE"
	FuncSubstitute         = "SUBSTITUTE"
	FuncLower              = "LOWER"
	FuncUpper              = "UPPER"
	FuncTrim               = "TRIM"
	FuncLen                = "LEN"
	FuncT                  = "T"
	FuncEncodeUrlComponent = "ENCODE_URL_COMPONENT"

	// Logical 逻辑函数
	FuncIf      = "IF"
	FuncSwitch  = "SWITCH"
	FuncAnd     = "AND"
	FuncOr      = "OR"
	FuncXor     = "XOR"
	FuncNot     = "NOT"
	FuncBlank   = "BLANK"
	FuncError   = "ERROR"
	FuncIsError = "IS_ERROR"

	// DateTime 日期时间函数
	FuncToday            = "TODAY"
	FuncNow              = "NOW"
	FuncYear             = "YEAR"
	FuncMonth            = "MONTH"
	FuncDay              = "DAY"
	FuncHour             = "HOUR"
	FuncMinute           = "MINUTE"
	FuncSecond           = "SECOND"
	FuncDatetimeDiff     = "DATETIME_DIFF"
	FuncDateAdd          = "DATE_ADD"
	FuncDatetimeFormat   = "DATETIME_FORMAT"
	FuncDatetimeParse    = "DATETIME_PARSE"
	FuncCreatedTime      = "CREATED_TIME"
	FuncLastModifiedTime = "LAST_MODIFIED_TIME"

	// Array 数组函数
	FuncCountAll     = "COUNTALL"
	FuncCountA       = "COUNTA"
	FuncCount        = "COUNT"
	FuncArrayJoin    = "ARRAY_JOIN"
	FuncArrayUnique  = "ARRAY_UNIQUE"
	FuncArrayFlatten = "ARRAY_FLATTEN"
	FuncArrayCompact = "ARRAY_COMPACT"

	// System 系统函数
	FuncRecordId   = "RECORD_ID"
	FuncAutoNumber = "AUTO_NUMBER"
)
