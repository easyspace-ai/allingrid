package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// ==================== 文本类型验证器 ====================

// SingleLineTextValidator 单行文本验证器
// 参考: teable-develop/packages/core/src/models/field/derivate/single-line-text.field.ts
type SingleLineTextValidator struct{}

func NewSingleLineTextValidator() *SingleLineTextValidator {
	return &SingleLineTextValidator{}
}

func (v *SingleLineTextValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("singleLineText")
	return fieldType
}

func (v *SingleLineTextValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	str, ok := value.(string)
	if !ok {
		return Failure(NewValidationError(field.Name().String(), "必须是字符串类型", value))
	}

	// 空字符串转为 null
	if str == "" {
		return Success(nil)
	}

	return Success(str)
}

func (v *SingleLineTextValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if str, ok := value.(string); ok {
		return v.convertString(str)
	}

	// 尝试转换为字符串
	return v.convertString(fmt.Sprintf("%v", value))
}

func (v *SingleLineTextValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	return v.convertString(str), nil
}

func (v *SingleLineTextValidator) convertString(str string) string {
	if str == "" {
		return ""
	}

	// 移除换行符、回车符、制表符，替换为空格
	re := regexp.MustCompile(`[\n\r\t]+`)
	return strings.TrimSpace(re.ReplaceAllString(str, " "))
}

// LongTextValidator 长文本验证器
type LongTextValidator struct{}

func NewLongTextValidator() *LongTextValidator {
	return &LongTextValidator{}
}

func (v *LongTextValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("longText")
	return fieldType
}

func (v *LongTextValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	str, ok := value.(string)
	if !ok {
		return Failure(NewValidationError(field.Name().String(), "必须是字符串类型", value))
	}

	if str == "" {
		return Success(nil)
	}

	return Success(str)
}

func (v *LongTextValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if str, ok := value.(string); ok {
		return str
	}

	return fmt.Sprintf("%v", value)
}

func (v *LongTextValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	return str, nil
}

// ==================== 数字类型验证器 ====================

// NumberValidator 数字验证器
// 参考: teable-develop/packages/core/src/models/field/derivate/number.field.ts
type NumberValidator struct{}

func NewNumberValidator() *NumberValidator {
	return &NumberValidator{}
}

func (v *NumberValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("number")
	return fieldType
}

func (v *NumberValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	var num float64
	switch v := value.(type) {
	case float64:
		num = v
	case float32:
		num = float64(v)
	case int:
		num = float64(v)
	case int32:
		num = float64(v)
	case int64:
		num = float64(v)
	case string:
		// 尝试从字符串解析
		parsedNum, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return Failure(NewValidationError(field.Name().String(), "无效的数字格式", value))
		}
		num = parsedNum
	default:
		return Failure(NewValidationError(field.Name().String(), "必须是数字类型", value))
	}

	// ✅ 检查数字范围（从字段的 options 中获取）
	options := field.Options()
	if options != nil && options.Number != nil {
		// 优先使用 MinValue/MaxValue（camelCase），回退到 Min/Max
		minVal := options.Number.MinValue
		if minVal == nil {
			minVal = options.Number.Min
		}
		maxVal := options.Number.MaxValue
		if maxVal == nil {
			maxVal = options.Number.Max
		}

		if minVal != nil && num < float64(*minVal) {
			return Failure(NewValidationError(
				field.Name().String(),
				fmt.Sprintf("数字必须大于等于 %d", *minVal),
				value,
			))
		}
		if maxVal != nil && num > float64(*maxVal) {
			return Failure(NewValidationError(
				field.Name().String(),
				fmt.Sprintf("数字必须小于等于 %d", *maxVal),
				value,
			))
		}
	}

	return Success(num)
}

func (v *NumberValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case float64, float32, int, int32, int64:
		return v
	case string:
		num, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil
		}
		return num
	default:
		return nil
	}
}

func (v *NumberValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	num, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, fmt.Errorf("无效的数字格式: %s", str)
	}

	return num, nil
}

// RatingValidator 评分验证器
type RatingValidator struct {
	NumberValidator
}

func NewRatingValidator() *RatingValidator {
	return &RatingValidator{}
}

func (v *RatingValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("rating")
	return fieldType
}

func (v *RatingValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	// 先进行数字验证
	result := v.NumberValidator.ValidateCell(ctx, value, field)
	if !result.Success {
		return result
	}

	// 评分必须在 1-10 之间
	if result.Value != nil {
		rating, _ := result.Value.(float64)
		if rating < 0 || rating > 10 {
			return Failure(NewValidationError(field.Name().String(), "评分必须在 0-10 之间", value))
		}
	}

	return result
}

// PercentValidator 百分比验证器
type PercentValidator struct {
	NumberValidator
}

func NewPercentValidator() *PercentValidator {
	return &PercentValidator{}
}

func (v *PercentValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("percent")
	return fieldType
}

// CurrencyValidator 货币验证器
type CurrencyValidator struct {
	NumberValidator
}

func NewCurrencyValidator() *CurrencyValidator {
	return &CurrencyValidator{}
}

func (v *CurrencyValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("currency")
	return fieldType
}

// DurationValidator 时长验证器
type DurationValidator struct {
	NumberValidator
}

func NewDurationValidator() *DurationValidator {
	return &DurationValidator{}
}

func (v *DurationValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("duration")
	return fieldType
}

// ==================== 布尔类型验证器 ====================

// CheckboxValidator 复选框验证器
// 参考: teable-develop/apps/nestjs-backend/src/features/import/open-api/import.class.ts
type CheckboxValidator struct{}

func NewCheckboxValidator() *CheckboxValidator {
	return &CheckboxValidator{}
}

func (v *CheckboxValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("checkbox")
	return fieldType
}

func (v *CheckboxValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(false) // 默认为 false
	}

	switch v := value.(type) {
	case bool:
		return Success(v)
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		if lower == "true" || lower == "1" || lower == "yes" {
			return Success(true)
		}
		if lower == "false" || lower == "0" || lower == "no" || lower == "" {
			return Success(false)
		}
		return Failure(NewValidationError(field.Name().String(), "无效的布尔值", value))
	case int, int32, int64:
		return Success(v != 0)
	default:
		return Failure(NewValidationError(field.Name().String(), "必须是布尔类型", value))
	}
}

func (v *CheckboxValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case bool:
		return v
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		return lower == "true" || lower == "1" || lower == "yes"
	case int, int32, int64:
		return v != 0
	default:
		return false
	}
}

func (v *CheckboxValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	lower := strings.ToLower(strings.TrimSpace(str))
	return lower == "true" || lower == "1" || lower == "yes", nil
}

// ==================== 日期类型验证器 ====================

// DateValidator 日期验证器
// 参考: teable-develop/apps/nestjs-backend/src/features/import/open-api/import.class.ts
type DateValidator struct{}

func NewDateValidator() *DateValidator {
	return &DateValidator{}
}

func (v *DateValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("date")
	return fieldType
}

func (v *DateValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	switch val := value.(type) {
	case time.Time:
		return Success(val)
	case string:
		// 尝试多种日期格式
		t, err := v.parseDate(val)
		if err != nil {
			return Failure(NewValidationError(field.Name().String(), "无效的日期格式", value))
		}
		return Success(t)
	default:
		return Failure(NewValidationError(field.Name().String(), "必须是日期类型", value))
	}
}

func (v *DateValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	switch val := value.(type) {
	case time.Time:
		return val
	case string:
		t, err := v.parseDate(val)
		if err != nil {
			return nil
		}
		return t
	default:
		return nil
	}
}

func (v *DateValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	return v.parseDate(str)
}

func (v *DateValidator) parseDate(str string) (time.Time, error) {
	// 支持多种日期格式
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"02-01-2006",
	}

	for _, format := range formats {
		t, err := time.Parse(format, str)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("无法解析日期: %s", str)
}

// ==================== 选择类型验证器 ====================

// SingleSelectValidator 单选验证器
type SingleSelectValidator struct{}

func NewSingleSelectValidator() *SingleSelectValidator {
	return &SingleSelectValidator{}
}

func (v *SingleSelectValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("singleSelect")
	return fieldType
}

func (v *SingleSelectValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	str, ok := value.(string)
	if !ok {
		return Failure(NewValidationError(field.Name().String(), "必须是字符串类型", value))
	}

	if str == "" {
		return Success(nil)
	}

	// 验证选项是否在字段的 options 中
	if field.Options() != nil {
		// TODO: 解析 field.Options() 并检查选项是否有效
		// 这里需要根据具体的选项结构来实现
	}

	return Success(str)
}

func (v *SingleSelectValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if str, ok := value.(string); ok {
		return str
	}

	return fmt.Sprintf("%v", value)
}

func (v *SingleSelectValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}
	return str, nil
}

// MultipleSelectValidator 多选验证器
type MultipleSelectValidator struct{}

func NewMultipleSelectValidator() *MultipleSelectValidator {
	return &MultipleSelectValidator{}
}

func (v *MultipleSelectValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("multipleSelect")
	return fieldType
}

func (v *MultipleSelectValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	arr, ok := value.([]interface{})
	if !ok {
		// 尝试转换为数组
		if str, ok := value.(string); ok {
			arr = []interface{}{str}
		} else {
			return Failure(NewValidationError(field.Name().String(), "必须是数组类型", value))
		}
	}

	if len(arr) == 0 {
		return Success(nil)
	}

	// 验证每个选项是否在字段的 options 中
	if field.Options() != nil {
		// TODO: 解析 field.Options() 并检查每个选项是否有效
		// 这里需要根据具体的选项结构来实现
	}

	return Success(arr)
}

func (v *MultipleSelectValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if arr, ok := value.([]interface{}); ok {
		return arr
	}

	// 单个值转为数组
	if str, ok := value.(string); ok {
		if str == "" {
			return nil
		}
		return []string{str}
	}

	return nil
}

func (v *MultipleSelectValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	// 逗号分隔
	parts := strings.Split(str, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result, nil
}

// ==================== 链接类型验证器 ====================

// URLValidator URL验证器
type URLValidator struct{}

func NewURLValidator() *URLValidator {
	return &URLValidator{}
}

func (v *URLValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("url")
	return fieldType
}

func (v *URLValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	str, ok := value.(string)
	if !ok {
		return Failure(NewValidationError(field.Name().String(), "必须是字符串类型", value))
	}

	if str == "" {
		return Success(nil)
	}

	// 验证 URL 格式
	_, err := url.ParseRequestURI(str)
	if err != nil {
		return Failure(NewValidationError(field.Name().String(), "无效的URL格式", value))
	}

	return Success(str)
}

func (v *URLValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if str, ok := value.(string); ok {
		// 如果没有协议，添加 https://
		if !strings.HasPrefix(str, "http://") && !strings.HasPrefix(str, "https://") {
			str = "https://" + str
		}
		return str
	}

	return nil
}

func (v *URLValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	// 如果没有协议，添加 https://
	if !strings.HasPrefix(str, "http://") && !strings.HasPrefix(str, "https://") {
		str = "https://" + str
	}

	return str, nil
}

// EmailValidator 邮箱验证器
type EmailValidator struct{}

func NewEmailValidator() *EmailValidator {
	return &EmailValidator{}
}

func (v *EmailValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("email")
	return fieldType
}

func (v *EmailValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	str, ok := value.(string)
	if !ok {
		return Failure(NewValidationError(field.Name().String(), "必须是字符串类型", value))
	}

	if str == "" {
		return Success(nil)
	}

	// 验证邮箱格式
	_, err := mail.ParseAddress(str)
	if err != nil {
		return Failure(NewValidationError(field.Name().String(), "无效的邮箱格式", value))
	}

	return Success(str)
}

func (v *EmailValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if str, ok := value.(string); ok {
		return strings.TrimSpace(str)
	}

	return nil
}

func (v *EmailValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	str = strings.TrimSpace(str)
	_, err := mail.ParseAddress(str)
	if err != nil {
		return nil, fmt.Errorf("无效的邮箱格式: %s", str)
	}

	return str, nil
}

// PhoneValidator 电话号码验证器
type PhoneValidator struct{}

func NewPhoneValidator() *PhoneValidator {
	return &PhoneValidator{}
}

func (v *PhoneValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("phone")
	return fieldType
}

func (v *PhoneValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	str, ok := value.(string)
	if !ok {
		return Failure(NewValidationError(field.Name().String(), "必须是字符串类型", value))
	}

	if str == "" {
		return Success(nil)
	}

	// 简单验证：只允许数字、空格、+、-、()
	re := regexp.MustCompile(`^[\d\s\+\-\(\)]+$`)
	if !re.MatchString(str) {
		return Failure(NewValidationError(field.Name().String(), "无效的电话号码格式", value))
	}

	return Success(str)
}

func (v *PhoneValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	if value == nil {
		return nil
	}

	if str, ok := value.(string); ok {
		return strings.TrimSpace(str)
	}

	return fmt.Sprintf("%v", value)
}

func (v *PhoneValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}
	return strings.TrimSpace(str), nil
}

// ==================== 其他类型验证器 ====================

// AttachmentValidator 附件验证器
type AttachmentValidator struct{}

func NewAttachmentValidator() *AttachmentValidator {
	return &AttachmentValidator{}
}

func (v *AttachmentValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("attachment")
	return fieldType
}

func (v *AttachmentValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	// 附件应该是一个数组（JSON 格式）
	if arr, ok := value.([]interface{}); ok {
		// 基本验证：检查是否为数组
		for i, item := range arr {
			if item == nil {
				return Failure(NewValidationError(field.Name().String(), fmt.Sprintf("附件项[%d]不能为空", i), item))
			}
		}
	} else {
		return Failure(NewValidationError(field.Name().String(), "附件必须是数组格式", value))
	}

	return Success(value)
}

func (v *AttachmentValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	return value
}

func (v *AttachmentValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	// 尝试解析 JSON 字符串
	var result interface{}
	if err := json.Unmarshal([]byte(str), &result); err != nil {
		return nil, fmt.Errorf("无法解析附件JSON: %w", err)
	}

	return result, nil
}

// UserValidator 用户验证器
type UserValidator struct{}

func NewUserValidator() *UserValidator {
	return &UserValidator{}
}

func (v *UserValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("user")
	return fieldType
}

func (v *UserValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	if value == nil {
		return Success(nil)
	}

	// 用户应该是一个对象或数组（JSON 格式）
	if arr, ok := value.([]interface{}); ok {
		// 数组格式：多个用户
		for i, item := range arr {
			if item == nil {
				return Failure(NewValidationError(field.Name().String(), fmt.Sprintf("用户项[%d]不能为空", i), item))
			}
			if _, ok := item.(map[string]interface{}); !ok {
				return Failure(NewValidationError(field.Name().String(), fmt.Sprintf("用户项[%d]必须是对象格式", i), item))
			}
		}
	} else if obj, ok := value.(map[string]interface{}); ok {
		// 对象格式：单个用户
		if len(obj) == 0 {
			return Failure(NewValidationError(field.Name().String(), "用户对象不能为空", value))
		}
	} else {
		return Failure(NewValidationError(field.Name().String(), "用户必须是对象或数组格式", value))
	}

	return Success(value)
}

func (v *UserValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	return value
}

func (v *UserValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	if str == "" {
		return nil, nil
	}

	// 尝试解析 JSON 字符串
	var result interface{}
	if err := json.Unmarshal([]byte(str), &result); err != nil {
		return nil, fmt.Errorf("无法解析用户JSON: %w", err)
	}

	return result, nil
}

// AutoNumberValidator 自动编号验证器
type AutoNumberValidator struct{}

func NewAutoNumberValidator() *AutoNumberValidator {
	return &AutoNumberValidator{}
}

func (v *AutoNumberValidator) SupportedType() valueobject.FieldType {
	fieldType, _ := valueobject.NewFieldType("autoNumber")
	return fieldType
}

func (v *AutoNumberValidator) ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	// 自动编号是只读的，不允许用户设置
	return Failure(NewValidationError(field.Name().String(), "自动编号字段是只读的", value))
}

func (v *AutoNumberValidator) Repair(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	// 自动编号不能被修复
	return nil
}

func (v *AutoNumberValidator) ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error) {
	return nil, fmt.Errorf("自动编号字段不能设置值")
}
