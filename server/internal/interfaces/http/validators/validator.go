package validators

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

// Validator 统一验证器
type Validator struct {
	validate *validator.Validate
}

// NewValidator 创建验证器
func NewValidator() *Validator {
	v := validator.New()

	// 注册自定义验证器
	v.RegisterValidation("phone", validatePhone)
	v.RegisterValidation("password", validatePassword)
	v.RegisterValidation("username", validateUsername)
	v.RegisterValidation("nanoid", validateNanoID)
	v.RegisterValidation("url", validateURL)
	v.RegisterValidation("json", validateJSON)
	v.RegisterValidation("timezone", validateTimezone)
	v.RegisterValidation("language", validateLanguage)

	// 注册自定义标签名称
	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})

	return &Validator{validate: v}
}

// ValidateStruct 验证结构体
func (v *Validator) ValidateStruct(s interface{}) error {
	if err := v.validate.Struct(s); err != nil {
		return v.formatError(err)
	}
	return nil
}

// ValidateVar 验证单个变量
func (v *Validator) ValidateVar(field interface{}, tag string) error {
	if err := v.validate.Var(field, tag); err != nil {
		return v.formatError(err)
	}
	return nil
}

// formatError 格式化错误信息
func (v *Validator) formatError(err error) error {
	if _, ok := err.(*validator.InvalidValidationError); ok {
		return fmt.Errorf("invalid validation error")
	}

	validationErrors := err.(validator.ValidationErrors)
	var errorMessages []string

	for _, err := range validationErrors {
		errorMessages = append(errorMessages, v.getErrorMsg(err))
	}

	return fmt.Errorf(strings.Join(errorMessages, "; "))
}

// getErrorMsg 获取错误消息
func (v *Validator) getErrorMsg(err validator.FieldError) string {
	field := err.Field()
	tag := err.Tag()

	switch tag {
	case "required":
		return fmt.Sprintf("%s is required", field)
	case "min":
		return fmt.Sprintf("%s must be at least %s characters", field, err.Param())
	case "max":
		return fmt.Sprintf("%s must be at most %s characters", field, err.Param())
	case "email":
		return fmt.Sprintf("%s must be a valid email address", field)
	case "phone":
		return fmt.Sprintf("%s must be a valid phone number", field)
	case "password":
		return fmt.Sprintf("%s must be a strong password", field)
	case "username":
		return fmt.Sprintf("%s must be a valid username", field)
	case "url":
		return fmt.Sprintf("%s must be a valid URL", field)
	case "uuid":
		return fmt.Sprintf("%s must be a valid UUID", field)
	case "nanoid":
		return fmt.Sprintf("%s must be a valid nano ID", field)
	case "oneof":
		return fmt.Sprintf("%s must be one of: %s", field, err.Param())
	case "timezone":
		return fmt.Sprintf("%s must be a valid timezone", field)
	case "language":
		return fmt.Sprintf("%s must be a valid language code", field)
	default:
		return fmt.Sprintf("%s failed validation on tag '%s'", field, tag)
	}
}

// 自定义验证函数

// validatePhone 验证手机号
func validatePhone(fl validator.FieldLevel) bool {
	phone := fl.Field().String()
	if phone == "" {
		return true // 允许空值，使用 required 标签控制是否必填
	}

	// 支持国际格式
	phoneRegex := regexp.MustCompile(`^\+?[1-9]\d{1,14}$`)
	return phoneRegex.MatchString(phone)
}

// validatePassword 验证密码强度
func validatePassword(fl validator.FieldLevel) bool {
	password := fl.Field().String()
	if password == "" {
		return true
	}

	// 至少8个字符
	if len(password) < 8 {
		return false
	}

	// 至少包含一个大写字母、一个小写字母、一个数字
	hasUpper := regexp.MustCompile(`[A-Z]`).MatchString(password)
	hasLower := regexp.MustCompile(`[a-z]`).MatchString(password)
	hasDigit := regexp.MustCompile(`\d`).MatchString(password)

	return hasUpper && hasLower && hasDigit
}

// validateUsername 验证用户名
func validateUsername(fl validator.FieldLevel) bool {
	username := fl.Field().String()
	if username == "" {
		return true
	}

	// 3-20个字符，只能包含字母、数字、下划线和横线
	usernameRegex := regexp.MustCompile(`^[a-zA-Z0-9_-]{3,20}$`)
	return usernameRegex.MatchString(username)
}

// validateNanoID 验证 NanoID
func validateNanoID(fl validator.FieldLevel) bool {
	id := fl.Field().String()
	if id == "" {
		return true
	}

	// NanoID 默认长度为21，字符集为 A-Za-z0-9_-
	nanoIDRegex := regexp.MustCompile(`^[A-Za-z0-9_-]{21}$`)
	return nanoIDRegex.MatchString(id)
}

// validateURL 验证URL
func validateURL(fl validator.FieldLevel) bool {
	url := fl.Field().String()
	if url == "" {
		return true
	}

	urlRegex := regexp.MustCompile(`^https?://[^\s/$.?#].[^\s]*$`)
	return urlRegex.MatchString(url)
}

// validateJSON 验证JSON字符串
func validateJSON(fl validator.FieldLevel) bool {
	jsonStr := fl.Field().String()
	if jsonStr == "" {
		return true
	}

	var js interface{}
	return json.Unmarshal([]byte(jsonStr), &js) == nil
}

// validateTimezone 验证时区
func validateTimezone(fl validator.FieldLevel) bool {
	tz := fl.Field().String()
	if tz == "" {
		return true
	}

	_, err := time.LoadLocation(tz)
	return err == nil
}

// validateLanguage 验证语言代码
func validateLanguage(fl validator.FieldLevel) bool {
	lang := fl.Field().String()
	if lang == "" {
		return true
	}

	// 简化的语言代码验证
	validLanguages := map[string]bool{
		"zh-CN": true, "zh-TW": true, "en-US": true, "en-GB": true,
		"ja-JP": true, "ko-KR": true, "fr-FR": true, "de-DE": true,
		"es-ES": true, "pt-BR": true, "ru-RU": true, "it-IT": true,
	}

	return validLanguages[lang]
}

// 常用验证请求结构体

// PaginationRequest 分页请求
type PaginationRequest struct {
	Offset int `form:"offset" validate:"min=0"`
	Limit  int `form:"limit" validate:"min=1,max=100"`
}

// IDRequest ID请求
type IDRequest struct {
	ID string `uri:"id" validate:"required,nanoid"`
}

// BatchIDsRequest 批量ID请求
type BatchIDsRequest struct {
	IDs []string `json:"ids" validate:"required,min=1,dive,nanoid"`
}

// SearchRequest 搜索请求
type SearchRequest struct {
	Query  string `form:"q" validate:"max=200"`
	Offset int    `form:"offset" validate:"min=0"`
	Limit  int    `form:"limit" validate:"min=1,max=100"`
}

// SortRequest 排序请求
type SortRequest struct {
	SortBy    string `form:"sort_by" validate:"omitempty,oneof=created_time updated_time name"`
	SortOrder string `form:"sort_order" validate:"omitempty,oneof=asc desc"`
}

// DateRangeRequest 日期范围请求
type DateRangeRequest struct {
	StartDate string `form:"start_date" validate:"omitempty,datetime=2006-01-02"`
	EndDate   string `form:"end_date" validate:"omitempty,datetime=2006-01-02"`
}

// 验证辅助函数

// ValidateEmail 验证邮箱
func ValidateEmail(email string) error {
	v := NewValidator()
	return v.ValidateVar(email, "required,email")
}

// ValidatePassword 验证密码
func ValidatePassword(password string) error {
	v := NewValidator()
	return v.ValidateVar(password, "required,password")
}

// ValidatePhone 验证手机号
func ValidatePhone(phone string) error {
	v := NewValidator()
	return v.ValidateVar(phone, "required,phone")
}

// ValidateID 验证ID
func ValidateID(id string) error {
	v := NewValidator()
	return v.ValidateVar(id, "required,nanoid")
}

// ValidateUsername 验证用户名
func ValidateUsername(username string) error {
	v := NewValidator()
	return v.ValidateVar(username, "required,username")
}

// 全局验证器实例
var defaultValidator = NewValidator()

// Validate 使用默认验证器验证
func Validate(s interface{}) error {
	return defaultValidator.ValidateStruct(s)
}

// ValidateField 验证单个字段
func ValidateField(field interface{}, tag string) error {
	return defaultValidator.ValidateVar(field, tag)
}
