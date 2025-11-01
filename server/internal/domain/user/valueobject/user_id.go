package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// UserID 用户ID值对象
type UserID struct {
	value string
}

// NewUserID 创建用户ID
func NewUserID(value string) UserID {
	if value == "" {
		value = generateUserID()
	}
	return UserID{value: value}
}

// GenerateNew 生成新的用户ID
func (id UserID) GenerateNew() UserID {
	return UserID{value: generateUserID()}
}

// String 获取字符串值
func (id UserID) String() string {
	return id.value
}

// Equals 比较两个用户ID是否相等
func (id UserID) Equals(other UserID) bool {
	return id.value == other.value
}

// IsEmpty 检查是否为空
func (id UserID) IsEmpty() bool {
	return id.value == ""
}

// generateUserID 生成用户ID（使用项目统一的ID生成器，符合varchar(30)限制）
func generateUserID() string {
	return utils.GenerateUserID() // 生成格式: usr_xxxxxxxxxxxxxxxxxxxxx (总长度25字符)
}
