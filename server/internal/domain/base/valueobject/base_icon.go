package valueobject

// BaseIcon Base图标值对象
type BaseIcon struct {
	value string
}

// NewBaseIcon 创建Base图标
func NewBaseIcon(icon string) *BaseIcon {
	return &BaseIcon{value: icon}
}

// String 获取图标值
func (i *BaseIcon) String() string {
	return i.value
}

// IsEmpty 检查是否为空
func (i *BaseIcon) IsEmpty() bool {
	return i.value == ""
}

// Equals 比较是否相等
func (i *BaseIcon) Equals(other *BaseIcon) bool {
	if other == nil {
		return false
	}
	return i.value == other.value
}
