package valueobject

import (
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/table"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

const (
	MaxTableNameLength = 100
	MinTableNameLength = 1
)

// TableName 表格名称值对象
type TableName struct {
	value string
}

// NewTableName 创建表格名称
func NewTableName(value string) (TableName, error) {
	// 去除首尾空格
	trimmed := strings.TrimSpace(value)

	// 验证
	if err := validateTableName(trimmed); err != nil {
		return TableName{}, err
	}

	return TableName{value: trimmed}, nil
}

// String 获取字符串值
func (tn TableName) String() string {
	return tn.value
}

// Equals 比较两个表格名称是否相等
func (tn TableName) Equals(other TableName) bool {
	return tn.value == other.value
}

// EqualsIgnoreCase 忽略大小写比较
func (tn TableName) EqualsIgnoreCase(other TableName) bool {
	return strings.EqualFold(tn.value, other.value)
}

// Length 获取名称长度
func (tn TableName) Length() int {
	return len(tn.value)
}

// IsEmpty 检查是否为空
func (tn TableName) IsEmpty() bool {
	return tn.value == ""
}

// validateTableName 验证表格名称
func validateTableName(name string) error {
	// 检查是否为空
	if name == "" {
		return table.ErrTableNameEmpty
	}

	// 检查长度
	if len(name) > MaxTableNameLength {
		return table.ErrTableNameTooLong
	}

	// 检查 SQL 注入
	if utils.ContainsSQLInjection(name) {
		return table.ErrTableNameInvalid
	}

	return nil
}
