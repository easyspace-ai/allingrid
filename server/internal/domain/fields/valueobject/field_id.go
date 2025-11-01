package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// FieldID 字段ID值对象
type FieldID struct {
	value string
}

// NewFieldID 创建字段ID
func NewFieldID(value string) FieldID {
	if value == "" {
		value = generateFieldID()
	}
	return FieldID{value: value}
}

// GenerateNew 生成新的字段ID
func (id FieldID) GenerateNew() FieldID {
	return FieldID{value: generateFieldID()}
}

// String 获取字符串值
func (id FieldID) String() string {
	return id.value
}

// Equals 比较两个字段ID是否相等
func (id FieldID) Equals(other FieldID) bool {
	return id.value == other.value
}

// IsEmpty 检查是否为空
func (id FieldID) IsEmpty() bool {
	return id.value == ""
}

// generateFieldID 生成字段ID（使用项目统一的ID生成器，符合varchar(30)限制）
func generateFieldID() string {
	return utils.GenerateFieldID()
}
