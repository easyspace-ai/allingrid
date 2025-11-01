package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// SpaceID 空间ID值对象
type SpaceID struct {
	value string
}

// NewSpaceID 创建空间ID
func NewSpaceID(value string) SpaceID {
	if value == "" {
		value = generateSpaceID()
	}
	return SpaceID{value: value}
}

// GenerateNew 生成新的空间ID
func (id SpaceID) GenerateNew() SpaceID {
	return SpaceID{value: generateSpaceID()}
}

// String 获取字符串值
func (id SpaceID) String() string {
	return id.value
}

// Equals 比较两个空间ID是否相等
func (id SpaceID) Equals(other SpaceID) bool {
	return id.value == other.value
}

// IsEmpty 检查是否为空
func (id SpaceID) IsEmpty() bool {
	return id.value == ""
}

// generateSpaceID 生成空间ID（使用项目统一的ID生成器，符合varchar(30)限制）
func generateSpaceID() string {
	return utils.GenerateSpaceID() // 生成格式: spc_xxxxxxxxxxxxxxxxxxxxx (总长度25字符)
}
