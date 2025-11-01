package utils

import (
	"regexp"
	"strings"
)

// 危险的 SQL 关键字和字符
var (
	// SQL 注入检测模式
	sqlInjectionPatterns = []*regexp.Regexp{
		// SQL 关键字（不区分大小写）
		regexp.MustCompile(`(?i)\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|UNION|DECLARE)\b`),
		// SQL 注释
		regexp.MustCompile(`--`),
		regexp.MustCompile(`/\*`),
		regexp.MustCompile(`\*/`),
		// 多语句分隔符
		regexp.MustCompile(`;`),
		// XSS 基本模式
		regexp.MustCompile(`(?i)<script`),
		regexp.MustCompile(`(?i)javascript:`),
		regexp.MustCompile(`(?i)onerror=`),
		regexp.MustCompile(`(?i)onload=`),
	}
)

// ContainsSQLInjection 检查字符串是否包含潜在的 SQL 注入
func ContainsSQLInjection(input string) bool {
	if input == "" {
		return false
	}

	for _, pattern := range sqlInjectionPatterns {
		if pattern.MatchString(input) {
			return true
		}
	}

	return false
}

// IsValidName 验证名称字段（空间名、Base名、表名、字段名等）
// 名称应该只包含字母、数字、中文、下划线、连字符、空格和emoji
func IsValidName(name string) bool {
	if name == "" {
		return false
	}

	// 检查长度（最大255字符）
	if len(name) > 255 {
		return false
	}

	// 检查 SQL 注入
	if ContainsSQLInjection(name) {
		return false
	}

	return true
}

// SanitizeName 清洗名称字段，移除危险字符
func SanitizeName(name string) string {
	// 移除前后空格
	name = strings.TrimSpace(name)

	// 替换多个连续空格为单个空格
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")

	return name
}

// IsValidDescription 验证描述字段
func IsValidDescription(description string) bool {
	if description == "" {
		return true // 描述可以为空
	}

	// 检查长度（最大1000字符）
	if len(description) > 1000 {
		return false
	}

	// 检查 SQL 注入
	if ContainsSQLInjection(description) {
		return false
	}

	return true
}

// SanitizeDescription 清洗描述字段
func SanitizeDescription(description string) string {
	// 移除前后空格
	description = strings.TrimSpace(description)

	return description
}

// ValidateStringLength 验证字符串长度
func ValidateStringLength(s string, minLen, maxLen int) bool {
	length := len(s)
	return length >= minLen && length <= maxLen
}
