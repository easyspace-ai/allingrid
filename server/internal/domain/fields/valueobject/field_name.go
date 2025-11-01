package valueobject

import (
	"strings"
	"unicode"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

const (
	MaxFieldNameLength = 64
	MinFieldNameLength = 1
)

// FieldName 字段名称值对象
type FieldName struct {
	value string
}

// NewFieldName 创建字段名称
func NewFieldName(value string) (FieldName, error) {
	// 去除首尾空格
	trimmed := strings.TrimSpace(value)

	// 验证
	if err := validateFieldName(trimmed); err != nil {
		return FieldName{}, err
	}

	return FieldName{value: trimmed}, nil
}

// String 获取字符串值
func (fn FieldName) String() string {
	return fn.value
}

// Equals 比较两个字段名称是否相等
func (fn FieldName) Equals(other FieldName) bool {
	return fn.value == other.value
}

// EqualsIgnoreCase 忽略大小写比较
func (fn FieldName) EqualsIgnoreCase(other FieldName) bool {
	return strings.EqualFold(fn.value, other.value)
}

// Length 获取名称长度
func (fn FieldName) Length() int {
	return len(fn.value)
}

// IsEmpty 检查是否为空
func (fn FieldName) IsEmpty() bool {
	return fn.value == ""
}

// validateFieldName 验证字段名称
func validateFieldName(name string) error {
	// 检查是否为空
	if name == "" {
		return fields.ErrFieldNameEmpty
	}

	// 检查长度
	if len(name) > MaxFieldNameLength {
		return fields.ErrFieldNameTooLong
	}

	// 检查 SQL 注入
	if utils.ContainsSQLInjection(name) {
		return fields.NewDomainError(
			"INVALID_FIELD_NAME",
			"field name contains invalid characters",
			nil,
		)
	}

	// 检查是否包含有效字符（字母、数字、下划线、中文）
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' && r != ' ' && r != '-' {
			return fields.NewDomainError(
				"INVALID_FIELD_NAME_CHAR",
				"field name contains invalid characters",
				nil,
			)
		}
	}

	// 检查是否以数字开头
	if len(name) > 0 && unicode.IsNumber(rune(name[0])) {
		return fields.NewDomainError(
			"FIELD_NAME_STARTS_WITH_NUMBER",
			"field name cannot start with a number",
			nil,
		)
	}

	return nil
}
