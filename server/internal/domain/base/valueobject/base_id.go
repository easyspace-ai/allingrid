package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// BaseID Base ID值对象
type BaseID struct {
	value string
}

// NewBaseID 创建Base ID
func NewBaseID(value string) BaseID {
	if value == "" {
		value = generateBaseID()
	}
	return BaseID{value: value}
}

// GenerateNew 生成新的Base ID
func (id BaseID) GenerateNew() BaseID {
	return BaseID{value: generateBaseID()}
}

// String 获取字符串值
func (id BaseID) String() string {
	return id.value
}

// Equals 比较两个Base ID是否相等
func (id BaseID) Equals(other BaseID) bool {
	return id.value == other.value
}

// IsEmpty 检查是否为空
func (id BaseID) IsEmpty() bool {
	return id.value == ""
}

// generateBaseID 生成Base ID（使用项目统一的ID生成器，符合varchar(30)限制）
func generateBaseID() string {
	return utils.GenerateBaseID()
}
