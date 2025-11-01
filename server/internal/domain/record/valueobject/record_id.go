package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// RecordID 记录ID值对象
type RecordID struct {
	value string
}

// NewRecordID 创建记录ID
func NewRecordID(value string) RecordID {
	if value == "" {
		value = generateRecordID()
	}
	return RecordID{value: value}
}

// GenerateNew 生成新的记录ID
func (id RecordID) GenerateNew() RecordID {
	return RecordID{value: generateRecordID()}
}

// String 获取字符串值
func (id RecordID) String() string {
	return id.value
}

// Equals 比较两个记录ID是否相等
func (id RecordID) Equals(other RecordID) bool {
	return id.value == other.value
}

// IsEmpty 检查是否为空
func (id RecordID) IsEmpty() bool {
	return id.value == ""
}

// generateRecordID 生成记录ID（使用项目统一的ID生成器，符合varchar(30)限制）
func generateRecordID() string {
	return utils.GenerateRecordID()
}
