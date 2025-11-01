package valueobject

import (
	"regexp"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user"
)

// Phone 手机号值对象
type Phone struct {
	value string
}

// 支持多种手机号格式
var phoneRegexes = []*regexp.Regexp{
	regexp.MustCompile(`^1[3-9]\d{9}$`),            // 中国大陆
	regexp.MustCompile(`^\+\d{1,3}\d{6,14}$`),      // 国际格式
	regexp.MustCompile(`^(\d{3}-)?\d{3,4}-\d{4}$`), // 带连字符
}

// NewPhone 创建手机号值对象
func NewPhone(value string) (Phone, error) {
	// 去除空格和特殊字符
	normalized := normalizePhone(value)

	// 验证
	if err := validatePhone(normalized); err != nil {
		return Phone{}, err
	}

	return Phone{value: normalized}, nil
}

// String 获取手机号字符串值
func (p Phone) String() string {
	return p.value
}

// Equals 比较两个手机号是否相等
func (p Phone) Equals(other Phone) bool {
	return p.value == other.value
}

// IsEmpty 检查是否为空
func (p Phone) IsEmpty() bool {
	return p.value == ""
}

// Masked 获取掩码后的手机号（用于显示）
func (p Phone) Masked() string {
	if len(p.value) < 7 {
		return "***"
	}

	// 保留前3位和后4位
	return p.value[:3] + "****" + p.value[len(p.value)-4:]
}

// normalizePhone 标准化手机号
func normalizePhone(phone string) string {
	// 去除空格
	phone = strings.ReplaceAll(phone, " ", "")
	// 去除短横线
	phone = strings.ReplaceAll(phone, "-", "")
	// 去除括号
	phone = strings.ReplaceAll(phone, "(", "")
	phone = strings.ReplaceAll(phone, ")", "")

	return phone
}

// validatePhone 验证手机号
func validatePhone(phone string) error {
	if phone == "" {
		// 手机号是可选的，空值是允许的
		return nil
	}

	if len(phone) > 20 {
		return user.ErrPhoneTooLong
	}

	// 尝试匹配任一格式
	for _, regex := range phoneRegexes {
		if regex.MatchString(phone) {
			return nil
		}
	}

	return user.ErrPhoneInvalid
}
