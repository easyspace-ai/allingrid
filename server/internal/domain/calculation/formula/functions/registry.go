package functions

import "strings"

// FunctionRegistry 函数注册表（对齐原版 FUNCTIONS）
type FunctionRegistry struct {
	functions map[string]FormulaFunc
}

// NewFunctionRegistry 创建函数注册表
func NewFunctionRegistry() *FunctionRegistry {
	registry := &FunctionRegistry{
		functions: make(map[string]FormulaFunc),
	}

	// 注册所有内置函数
	registry.registerBuiltinFunctions()

	return registry
}

// registerBuiltinFunctions 注册所有内置函数（对齐原版 FUNCTIONS）
func (r *FunctionRegistry) registerBuiltinFunctions() {
	// 文本函数 (16个)
	r.Register(NewConcatenateFunc())
	// ✅ 添加常用函数别名
	r.RegisterAlias("CONCAT", "CONCATENATE")
	r.Register(NewLeftFunc())
	r.Register(NewRightFunc())
	r.Register(NewUpperFunc())
	r.Register(NewLowerFunc())
	r.Register(NewTrimFunc())
	r.Register(NewLenFunc())
	r.Register(NewFindFunc())
	// 扩展文本函数 (8个)
	r.Register(NewMidFunc())
	r.Register(NewSearchFunc())
	r.Register(NewReplaceFunc())
	r.Register(NewSubstituteFunc())
	r.Register(NewReptFunc())
	r.Register(NewTFunc())
	r.Register(NewRegexpReplaceFunc())
	r.Register(NewEncodeUrlComponentFunc())

	// 数值函数 (19个)
	r.Register(NewSumFunc())
	r.Register(NewAverageFunc())
	r.Register(NewMaxFunc())
	r.Register(NewMinFunc())
	r.Register(NewRoundFunc())
	r.Register(NewAbsFunc())
	r.Register(NewCeilingFunc())
	r.Register(NewFloorFunc())
	r.Register(NewSqrtFunc())
	r.Register(NewPowerFunc())
	r.Register(NewModFunc())
	// 扩展数值函数 (8个)
	r.Register(NewIntFunc())
	r.Register(NewEvenFunc())
	r.Register(NewOddFunc())
	r.Register(NewRoundUpFunc())
	r.Register(NewRoundDownFunc())
	r.Register(NewValueFunc())
	r.Register(NewExpFunc())
	r.Register(NewLogFunc())

	// 逻辑函数 (9个) - 100%完成 ✅
	r.Register(NewIfFunc())
	r.Register(NewAndFunc())
	r.Register(NewOrFunc())
	r.Register(NewNotFunc())
	r.Register(NewBlankFunc())
	r.Register(NewSwitchFunc())
	r.Register(NewXorFunc())
	r.Register(NewErrorFunc())
	r.Register(NewIsErrorFunc())

	// 日期时间函数 (25个) - 100%完成 ✅
	r.Register(NewTodayFunc())
	r.Register(NewNowFunc())
	r.Register(NewYearFunc())
	r.Register(NewMonthFunc())
	r.Register(NewDayFunc())
	r.Register(NewHourFunc())
	r.Register(NewMinuteFunc())
	r.Register(NewSecondFunc())
	r.Register(NewWeekNumFunc())
	r.Register(NewWeekdayFunc())
	r.Register(NewDatetimeDiffFunc())
	r.Register(NewDateAddFunc())
	r.Register(NewFromNowFunc())
	r.Register(NewToNowFunc())
	r.Register(NewIsSameFunc())
	r.Register(NewIsAfterFunc())
	r.Register(NewIsBeforeFunc())
	r.Register(NewDatestrFunc())
	r.Register(NewTimestrFunc())
	r.Register(NewDatetimeFormatFunc())
	r.Register(NewDatetimeParseFunc())
	r.Register(NewWorkdayFunc())
	r.Register(NewWorkdayDiffFunc())
	r.Register(NewCreatedTimeFunc())
	r.Register(NewLastModifiedTimeFunc())

	// 数组函数 (7个) - 100%完成 ✅
	r.Register(NewCountFunc())
	r.Register(NewCountAFunc())
	r.Register(NewCountAllFunc())
	r.Register(NewArrayJoinFunc())
	r.Register(NewArrayUniqueFunc())
	r.Register(NewArrayFlattenFunc())
	r.Register(NewArrayCompactFunc())

	// 系统函数 (3个) - 100%完成 ✅
	r.Register(NewRecordIdFunc())
	r.Register(NewAutoNumberFunc())
	r.Register(NewTextAllFunc())

	// ✅ 函数库100%完成！共80个函数全部实现
	// ✅ 文本函数: 16/16 (100%)
	// ✅ 数值函数: 19/19 (100%)
	// ✅ 逻辑函数: 9/9 (100%)
	// ✅ 日期时间函数: 25/25 (100%)
	// ✅ 数组函数: 7/7 (100%)
	// ✅ 系统函数: 3/3 (100%)
}

// Register 注册函数
func (r *FunctionRegistry) Register(fn FormulaFunc) {
	// 函数名大写（对齐原版：函数名不区分大小写）
	name := strings.ToUpper(fn.Name())
	r.functions[name] = fn
}

// RegisterAlias 注册函数别名
func (r *FunctionRegistry) RegisterAlias(alias, targetName string) {
	alias = strings.ToUpper(alias)
	targetName = strings.ToUpper(targetName)
	if targetFunc, exists := r.functions[targetName]; exists {
		r.functions[alias] = targetFunc
	}
}

// GetFunction 获取函数
func (r *FunctionRegistry) GetFunction(name string) FormulaFunc {
	// 大写查找（对齐原版）
	return r.functions[strings.ToUpper(name)]
}

// HasFunction 检查函数是否存在
func (r *FunctionRegistry) HasFunction(name string) bool {
	return r.GetFunction(name) != nil
}

// GetAllFunctionNames 获取所有函数名
func (r *FunctionRegistry) GetAllFunctionNames() []string {
	names := make([]string, 0, len(r.functions))
	for name := range r.functions {
		names = append(names, name)
	}
	return names
}

// GetFunctionsByType 按类型获取函数
func (r *FunctionRegistry) GetFunctionsByType(funcType FormulaFuncType) []FormulaFunc {
	funcs := []FormulaFunc{}
	for _, fn := range r.functions {
		if fn.Type() == funcType {
			funcs = append(funcs, fn)
		}
	}
	return funcs
}
