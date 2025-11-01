package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// TableID 表格ID值对象
type TableID struct {
	value string
}

// NewTableID 创建表格ID
func NewTableID(value string) TableID {
	if value == "" {
		value = generateTableID()
	}
	return TableID{value: value}
}

// GenerateNew 生成新的表格ID
func (id TableID) GenerateNew() TableID {
	return TableID{value: generateTableID()}
}

// String 获取字符串值
func (id TableID) String() string {
	return id.value
}

// Equals 比较两个表格ID是否相等
func (id TableID) Equals(other TableID) bool {
	return id.value == other.value
}

// IsEmpty 检查是否为空
func (id TableID) IsEmpty() bool {
	return id.value == ""
}

// generateTableID 生成表格ID（使用项目统一的ID生成器，符合varchar(30)限制）
func generateTableID() string {
	return utils.GenerateTableID()
}
