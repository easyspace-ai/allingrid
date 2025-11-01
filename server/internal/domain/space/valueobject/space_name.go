package valueobject

import (
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

const (
	MaxSpaceNameLength = 100
	MinSpaceNameLength = 1
)

// SpaceName 空间名称值对象
type SpaceName struct {
	value string
}

// NewSpaceName 创建空间名称
func NewSpaceName(value string) (SpaceName, error) {
	// 去除首尾空格
	trimmed := strings.TrimSpace(value)

	// 验证
	if err := validateSpaceName(trimmed); err != nil {
		return SpaceName{}, err
	}

	return SpaceName{value: trimmed}, nil
}

// String 获取字符串值
func (sn SpaceName) String() string {
	return sn.value
}

// Equals 比较两个空间名称是否相等
func (sn SpaceName) Equals(other SpaceName) bool {
	return sn.value == other.value
}

// EqualsIgnoreCase 忽略大小写比较
func (sn SpaceName) EqualsIgnoreCase(other SpaceName) bool {
	return strings.EqualFold(sn.value, other.value)
}

// Length 获取名称长度
func (sn SpaceName) Length() int {
	return len(sn.value)
}

// IsEmpty 检查是否为空
func (sn SpaceName) IsEmpty() bool {
	return sn.value == ""
}

// validateSpaceName 验证空间名称
func validateSpaceName(name string) error {
	// 检查是否为空
	if name == "" {
		return space.ErrSpaceNameEmpty
	}

	// 检查长度
	if len(name) > MaxSpaceNameLength {
		return space.ErrSpaceNameTooLong
	}

	// 检查 SQL 注入
	if utils.ContainsSQLInjection(name) {
		return space.ErrSpaceNameInvalid
	}

	return nil
}
