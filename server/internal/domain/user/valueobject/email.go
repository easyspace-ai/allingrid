package valueobject

import (
	"regexp"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user"
)

// Email 邮箱值对象（不可变）
type Email struct {
	value string
}

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

// NewEmail 创建邮箱值对象
func NewEmail(value string) (Email, error) {
	// 标准化处理
	normalized := strings.ToLower(strings.TrimSpace(value))

	// 验证
	if err := validateEmail(normalized); err != nil {
		return Email{}, err
	}

	return Email{value: normalized}, nil
}

// String 获取邮箱字符串值
func (e Email) String() string {
	return e.value
}

// Domain 获取邮箱域名
func (e Email) Domain() string {
	parts := strings.Split(e.value, "@")
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

// LocalPart 获取邮箱本地部分
func (e Email) LocalPart() string {
	parts := strings.Split(e.value, "@")
	if len(parts) >= 1 {
		return parts[0]
	}
	return ""
}

// Equals 比较两个邮箱是否相等
func (e Email) Equals(other Email) bool {
	return e.value == other.value
}

// IsEmpty 检查是否为空
func (e Email) IsEmpty() bool {
	return e.value == ""
}

// validateEmail 验证邮箱格式
func validateEmail(email string) error {
	if email == "" {
		return user.ErrEmailEmpty
	}

	if len(email) > 255 {
		return user.ErrEmailTooLong
	}

	if !emailRegex.MatchString(email) {
		return user.ErrEmailInvalid
	}

	return nil
}
