package valueobject

import "github.com/easyspace-ai/luckdb/server/internal/domain/record"

// RecordVersion 记录版本值对象
// 用于乐观锁控制
type RecordVersion struct {
	value int64
}

// NewRecordVersion 创建记录版本
func NewRecordVersion(value int64) (RecordVersion, error) {
	if value < 0 {
		return RecordVersion{}, record.ErrInvalidVersion
	}

	return RecordVersion{value: value}, nil
}

// InitialVersion 创建初始版本
func InitialVersion() RecordVersion {
	return RecordVersion{value: 1}
}

// Value 获取版本号
func (rv RecordVersion) Value() int64 {
	return rv.value
}

// Increment 递增版本号（返回新版本）
func (rv RecordVersion) Increment() RecordVersion {
	return RecordVersion{value: rv.value + 1}
}

// Equals 比较两个版本是否相等
func (rv RecordVersion) Equals(other RecordVersion) bool {
	return rv.value == other.value
}

// IsGreaterThan 是否大于另一个版本
func (rv RecordVersion) IsGreaterThan(other RecordVersion) bool {
	return rv.value > other.value
}

// IsLessThan 是否小于另一个版本
func (rv RecordVersion) IsLessThan(other RecordVersion) bool {
	return rv.value < other.value
}
