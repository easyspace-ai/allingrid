package valueobject

import "github.com/easyspace-ai/luckdb/server/internal/domain/user"

// UserStatus 用户状态值对象
type UserStatus struct {
	value string
}

const (
	StatusActive      = "active"
	StatusDeactivated = "deactivated"
	StatusDeleted     = "deleted"
	StatusPending     = "pending"
)

// NewUserStatus 创建用户状态
func NewUserStatus(value string) (UserStatus, error) {
	if !isValidUserStatus(value) {
		return UserStatus{}, user.NewDomainError(
			"INVALID_USER_STATUS",
			"invalid user status: "+value,
			nil,
		)
	}

	return UserStatus{value: value}, nil
}

// ActiveStatus 返回激活状态
func ActiveStatus() UserStatus {
	return UserStatus{value: StatusActive}
}

// PendingStatus 返回待激活状态
func PendingStatus() UserStatus {
	return UserStatus{value: StatusPending}
}

// DeactivatedStatus 返回停用状态
func DeactivatedStatus() UserStatus {
	return UserStatus{value: StatusDeactivated}
}

// DeletedStatus 返回删除状态
func DeletedStatus() UserStatus {
	return UserStatus{value: StatusDeleted}
}

// String 获取字符串值
func (us UserStatus) String() string {
	return us.value
}

// Equals 比较两个用户状态是否相等
func (us UserStatus) Equals(other UserStatus) bool {
	return us.value == other.value
}

// IsActive 是否激活状态
func (us UserStatus) IsActive() bool {
	return us.value == StatusActive
}

// IsDeactivated 是否停用状态
func (us UserStatus) IsDeactivated() bool {
	return us.value == StatusDeactivated
}

// IsDeleted 是否删除状态
func (us UserStatus) IsDeleted() bool {
	return us.value == StatusDeleted
}

// IsPending 是否待激活状态
func (us UserStatus) IsPending() bool {
	return us.value == StatusPending
}

// CanActivate 是否可以激活
func (us UserStatus) CanActivate() bool {
	return us.value == StatusPending || us.value == StatusDeactivated
}

// CanDeactivate 是否可以停用
func (us UserStatus) CanDeactivate() bool {
	return us.value == StatusActive
}

// CanDelete 是否可以删除
func (us UserStatus) CanDelete() bool {
	return us.value != StatusDeleted
}

// isValidUserStatus 检查用户状态是否有效
func isValidUserStatus(value string) bool {
	validStatuses := map[string]bool{
		StatusActive:      true,
		StatusDeactivated: true,
		StatusDeleted:     true,
		StatusPending:     true,
	}

	return validStatuses[value]
}
