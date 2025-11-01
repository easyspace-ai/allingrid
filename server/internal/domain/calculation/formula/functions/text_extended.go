package functions

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// =========== MID 函数 ===========
// 对齐原版 Mid

type MidFunc struct {
	BaseTextFunc
}

func NewMidFunc() *MidFunc {
	return &MidFunc{
		BaseTextFunc: BaseTextFunc{
			name: "MID",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *MidFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 3 {
		return fmt.Errorf("%s needs at least 3 params", f.Name())
	}
	return nil
}

func (f *MidFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *MidFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	targetStr := params[0].AsString()
	startPos := int(params[1].AsNumber())
	length := int(params[2].AsNumber())

	if startPos < 0 {
		startPos = 0
	}
	if length < 0 {
		length = 0
	}

	if startPos >= len(targetStr) {
		return NewTypedValue("", CellValueTypeString), nil
	}

	end := startPos + length
	if end > len(targetStr) {
		end = len(targetStr)
	}

	return NewTypedValue(targetStr[startPos:end], CellValueTypeString), nil
}

// =========== SEARCH 函数 ===========
// 对齐原版 Search（不区分大小写）

type SearchFunc struct {
	BaseTextFunc
}

func NewSearchFunc() *SearchFunc {
	return &SearchFunc{
		BaseTextFunc: BaseTextFunc{
			name: "SEARCH",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *SearchFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 1 {
		return fmt.Errorf("%s needs at least 1 param", f.Name())
	}
	return nil
}

func (f *SearchFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeNumber, false, nil
}

func (f *SearchFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	findStr := params[0].AsString()
	targetStr := ""
	if len(params) > 1 {
		targetStr = params[1].AsString()
	}

	// 起始位置（可选）
	startPos := 0
	if len(params) >= 3 {
		startPos = int(params[2].AsNumber())
		if startPos > 0 {
			startPos-- // 转为0基索引
		}
	}

	// SEARCH不区分大小写（对齐原版）
	lowerTarget := strings.ToLower(targetStr)
	lowerFind := strings.ToLower(findStr)

	index := strings.Index(lowerTarget[startPos:], lowerFind)
	if index == -1 {
		return NewTypedValue(nil, CellValueTypeNull), nil
	}

	// 返回1基索引位置
	return NewTypedValue(float64(startPos+index+1), CellValueTypeNumber), nil
}

// =========== REPLACE 函数 ===========
// 对齐原版 Replace

type ReplaceFunc struct {
	BaseTextFunc
}

func NewReplaceFunc() *ReplaceFunc {
	return &ReplaceFunc{
		BaseTextFunc: BaseTextFunc{
			name: "REPLACE",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *ReplaceFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 4 {
		return fmt.Errorf("%s needs 4 params", f.Name())
	}
	return nil
}

func (f *ReplaceFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *ReplaceFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	oldText := params[0].AsString()
	startPos := int(params[1].AsNumber()) - 1 // 1基索引转0基
	numChars := int(params[2].AsNumber())
	newText := params[3].AsString()

	if startPos < 0 || startPos >= len(oldText) {
		return NewTypedValue(oldText, CellValueTypeString), nil
	}

	end := startPos + numChars
	if end > len(oldText) {
		end = len(oldText)
	}

	result := oldText[:startPos] + newText + oldText[end:]
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== SUBSTITUTE 函数 ===========
// 对齐原版 Substitute

type SubstituteFunc struct {
	BaseTextFunc
}

func NewSubstituteFunc() *SubstituteFunc {
	return &SubstituteFunc{
		BaseTextFunc: BaseTextFunc{
			name: "SUBSTITUTE",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *SubstituteFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 3 {
		return fmt.Errorf("%s needs at least 3 params", f.Name())
	}
	return nil
}

func (f *SubstituteFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *SubstituteFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	text := params[0].AsString()
	oldText := params[1].AsString()
	newText := params[2].AsString()

	// 如果有第4个参数，只替换第N个匹配项
	if len(params) >= 4 {
		instanceNum := int(params[3].AsNumber())
		if instanceNum <= 0 {
			return NewTypedValue(text, CellValueTypeString), nil
		}

		count := 0
		index := 0
		for {
			pos := strings.Index(text[index:], oldText)
			if pos == -1 {
				break
			}
			count++
			if count == instanceNum {
				result := text[:index+pos] + newText + text[index+pos+len(oldText):]
				return NewTypedValue(result, CellValueTypeString), nil
			}
			index += pos + len(oldText)
		}
		return NewTypedValue(text, CellValueTypeString), nil
	}

	// 否则替换所有匹配项
	result := strings.ReplaceAll(text, oldText, newText)
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== REPT 函数 ===========
// 对齐原版 Rept

type ReptFunc struct {
	BaseTextFunc
}

func NewReptFunc() *ReptFunc {
	return &ReptFunc{
		BaseTextFunc: BaseTextFunc{
			name: "REPT",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
				CellValueTypeNumber: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *ReptFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 2 {
		return fmt.Errorf("%s needs exactly 2 params", f.Name())
	}
	return nil
}

func (f *ReptFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *ReptFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	text := params[0].AsString()
	times := int(params[1].AsNumber())

	if times < 0 {
		return NewTypedValue("", CellValueTypeString), nil
	}

	return NewTypedValue(strings.Repeat(text, times), CellValueTypeString), nil
}

// =========== T 函数 ===========
// 对齐原版 T

type TFunc struct {
	BaseTextFunc
}

func NewTFunc() *TFunc {
	return &TFunc{
		BaseTextFunc: BaseTextFunc{
			name: "T",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString:  true,
				CellValueTypeNumber:  true,
				CellValueTypeBoolean: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *TFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *TFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *TFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	// 如果是字符串，返回字符串；否则返回空字符串
	if params[0].Type == CellValueTypeString {
		return params[0], nil
	}
	return NewTypedValue("", CellValueTypeString), nil
}

// =========== REGEXP_REPLACE 函数 ===========
// 对齐原版 RegExpReplace

type RegexpReplaceFunc struct {
	BaseTextFunc
}

func NewRegexpReplaceFunc() *RegexpReplaceFunc {
	return &RegexpReplaceFunc{
		BaseTextFunc: BaseTextFunc{
			name: "REGEXP_REPLACE",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *RegexpReplaceFunc) ValidateParams(params []*TypedValue) error {
	if len(params) < 3 {
		return fmt.Errorf("%s needs at least 3 params", f.Name())
	}
	return nil
}

func (f *RegexpReplaceFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *RegexpReplaceFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	text := params[0].AsString()
	pattern := params[1].AsString()
	replacement := params[2].AsString()

	re, err := regexp.Compile(pattern)
	if err != nil {
		return NewTypedValue("#ERROR: Invalid regex pattern", CellValueTypeString), nil
	}

	result := re.ReplaceAllString(text, replacement)
	return NewTypedValue(result, CellValueTypeString), nil
}

// =========== ENCODE_URL_COMPONENT 函数 ===========
// 对齐原版 EncodeUrlComponent

type EncodeUrlComponentFunc struct {
	BaseTextFunc
}

func NewEncodeUrlComponentFunc() *EncodeUrlComponentFunc {
	return &EncodeUrlComponentFunc{
		BaseTextFunc: BaseTextFunc{
			name: "ENCODE_URL_COMPONENT",
			acceptValueType: map[CellValueType]bool{
				CellValueTypeString: true,
			},
			acceptMultipleValue: true,
		},
	}
}

func (f *EncodeUrlComponentFunc) ValidateParams(params []*TypedValue) error {
	if len(params) != 1 {
		return fmt.Errorf("%s needs exactly 1 param", f.Name())
	}
	return nil
}

func (f *EncodeUrlComponentFunc) GetReturnType(params []*TypedValue) (CellValueType, bool, error) {
	if err := f.ValidateParams(params); err != nil {
		return "", false, err
	}
	return CellValueTypeString, false, nil
}

func (f *EncodeUrlComponentFunc) Eval(params []*TypedValue, context *FormulaContext) (*TypedValue, error) {
	text := params[0].AsString()
	encoded := url.QueryEscape(text)
	return NewTypedValue(encoded, CellValueTypeString), nil
}
