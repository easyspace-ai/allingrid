package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
)

// User 用户实体（充血模型）
type User struct {
	// 基础信息（私有）
	id       valueobject.UserID
	name     string
	email    valueobject.Email
	password valueobject.HashedPassword
	phone    *valueobject.Phone
	avatar   *string

	// 用户标识
	isSystem    bool
	isAdmin     bool
	isTrialUsed bool

	// 状态
	status valueobject.UserStatus

	// 审计字段
	createdBy     string
	createdAt     time.Time
	updatedAt     time.Time
	lastSignAt    *time.Time
	deactivatedAt *time.Time
	deletedAt     *time.Time

	// 元数据
	notifyMeta *string
	refMeta    *string

	// 版本控制
	version int
}

// NewUser 创建新用户（工厂方法）
func NewUser(
	email valueobject.Email,
	name string,
	password valueobject.Password,
	createdBy string,
) (*User, error) {
	// 验证名称
	if err := validateUserName(name); err != nil {
		return nil, err
	}

	// 哈希密码
	hashedPassword, err := password.Hash()
	if err != nil {
		return nil, err
	}

	now := time.Now()

	return &User{
		id:        valueobject.NewUserID(""),
		name:      name,
		email:     email,
		password:  hashedPassword,
		status:    valueobject.PendingStatus(),
		isSystem:  false,
		isAdmin:   false,
		createdBy: createdBy,
		createdAt: now,
		updatedAt: now,
		version:   1,
	}, nil
}

// ReconstructUser 重建用户（从数据库加载）
func ReconstructUser(
	id valueobject.UserID,
	name string,
	email valueobject.Email,
	password valueobject.HashedPassword,
	phone *valueobject.Phone,
	avatar *string,
	status valueobject.UserStatus,
	isSystem, isAdmin, isTrialUsed bool,
	createdBy string,
	createdAt, updatedAt time.Time,
	lastSignAt, deactivatedAt, deletedAt *time.Time,
	version int,
) *User {
	return &User{
		id:            id,
		name:          name,
		email:         email,
		password:      password,
		phone:         phone,
		avatar:        avatar,
		status:        status,
		isSystem:      isSystem,
		isAdmin:       isAdmin,
		isTrialUsed:   isTrialUsed,
		createdBy:     createdBy,
		createdAt:     createdAt,
		updatedAt:     updatedAt,
		lastSignAt:    lastSignAt,
		deactivatedAt: deactivatedAt,
		deletedAt:     deletedAt,
		version:       version,
	}
}

// ==================== 访问器方法 ====================

func (u *User) ID() valueobject.UserID               { return u.id }
func (u *User) Name() string                         { return u.name }
func (u *User) Email() valueobject.Email             { return u.email }
func (u *User) Password() valueobject.HashedPassword { return u.password }
func (u *User) Phone() *valueobject.Phone            { return u.phone }
func (u *User) Avatar() *string                      { return u.avatar }
func (u *User) Status() valueobject.UserStatus       { return u.status }
func (u *User) IsSystem() bool                       { return u.isSystem }
func (u *User) IsAdmin() bool                        { return u.isAdmin }
func (u *User) IsTrialUsed() bool                    { return u.isTrialUsed }
func (u *User) CreatedBy() string                    { return u.createdBy }
func (u *User) CreatedAt() time.Time                 { return u.createdAt }
func (u *User) UpdatedAt() time.Time                 { return u.updatedAt }
func (u *User) LastSignAt() *time.Time               { return u.lastSignAt }
func (u *User) DeactivatedAt() *time.Time            { return u.deactivatedAt }
func (u *User) DeletedAt() *time.Time                { return u.deletedAt }
func (u *User) Version() int                         { return u.version }

// IsActive 是否激活状态
func (u *User) IsActive() bool {
	return u.status.IsActive()
}

// IsDeleted 是否已删除
func (u *User) IsDeleted() bool {
	return u.deletedAt != nil
}

// ==================== 业务方法 ====================

// UpdateName 更新用户名
func (u *User) UpdateName(name string) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	if err := validateUserName(name); err != nil {
		return err
	}

	u.name = name
	u.updatedAt = time.Now()
	u.incrementVersion()

	return nil
}

// UpdateEmail 更新邮箱
func (u *User) UpdateEmail(email valueobject.Email) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	u.email = email
	u.updatedAt = time.Now()
	u.incrementVersion()

	return nil
}

// UpdatePassword 更新密码
func (u *User) UpdatePassword(oldPassword, newPassword valueobject.Password) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	// 验证旧密码
	if !u.password.Verify(oldPassword) {
		return user.ErrPasswordMismatch
	}

	// 哈希新密码
	hashedPassword, err := newPassword.Hash()
	if err != nil {
		return err
	}

	u.password = hashedPassword
	u.updatedAt = time.Now()
	u.incrementVersion()

	return nil
}

// SetPassword 设置密码（管理员操作）
func (u *User) SetPassword(password valueobject.Password) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	hashedPassword, err := password.Hash()
	if err != nil {
		return err
	}

	u.password = hashedPassword
	u.updatedAt = time.Now()
	u.incrementVersion()

	return nil
}

// VerifyPassword 验证密码
func (u *User) VerifyPassword(password valueobject.Password) bool {
	return u.password.Verify(password)
}

// UpdatePhone 更新手机号
func (u *User) UpdatePhone(phone valueobject.Phone) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	u.phone = &phone
	u.updatedAt = time.Now()

	return nil
}

// UpdateAvatar 更新头像
func (u *User) UpdateAvatar(avatar string) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	u.avatar = &avatar
	u.updatedAt = time.Now()

	return nil
}

// Activate 激活用户
func (u *User) Activate() error {
	if u.IsDeleted() {
		return user.ErrCannotActivateDeletedUser
	}

	if u.status.IsActive() {
		return user.ErrUserAlreadyActive
	}

	if !u.status.CanActivate() {
		return user.NewDomainError(
			"CANNOT_ACTIVATE_USER",
			"user in current status cannot be activated",
			nil,
		)
	}

	u.status = valueobject.ActiveStatus()
	u.updatedAt = time.Now()
	u.incrementVersion()

	return nil
}

// Deactivate 停用用户
func (u *User) Deactivate(reason string) error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	if !u.status.CanDeactivate() {
		return user.NewDomainError(
			"CANNOT_DEACTIVATE_USER",
			"user is not active",
			nil,
		)
	}

	u.status = valueobject.DeactivatedStatus()
	now := time.Now()
	u.deactivatedAt = &now
	u.updatedAt = now
	u.incrementVersion()

	return nil
}

// SoftDelete 软删除用户
func (u *User) SoftDelete() error {
	if u.IsDeleted() {
		return user.ErrUserAlreadyDeleted
	}

	u.status = valueobject.DeletedStatus()
	now := time.Now()
	u.deletedAt = &now
	u.updatedAt = now

	return nil
}

// Restore 恢复已删除的用户
func (u *User) Restore() error {
	if !u.IsDeleted() {
		return user.NewDomainError(
			"USER_NOT_DELETED",
			"user is not deleted",
			nil,
		)
	}

	u.status = valueobject.PendingStatus()
	u.deletedAt = nil
	u.updatedAt = time.Now()

	return nil
}

// RecordSignIn 记录登录时间
func (u *User) RecordSignIn() {
	now := time.Now()
	u.lastSignAt = &now
	u.updatedAt = now
}

// PromoteToAdmin 提升为管理员
func (u *User) PromoteToAdmin() error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	if !u.IsActive() {
		return user.ErrUserNotActive
	}

	u.isAdmin = true
	u.updatedAt = time.Now()

	return nil
}

// DemoteFromAdmin 取消管理员
func (u *User) DemoteFromAdmin() error {
	if u.IsDeleted() {
		return user.ErrCannotModifyDeletedUser
	}

	// 系统用户不能撤销管理员权限
	if u.isSystem {
		return user.ErrCannotModifySystemUser
	}

	u.isAdmin = false
	u.updatedAt = time.Now()

	return nil
}

// MarkTrialUsed 标记试用已使用
func (u *User) MarkTrialUsed() {
	u.isTrialUsed = true
	u.updatedAt = time.Now()
}

// ==================== 私有辅助方法 ====================

// incrementVersion 递增版本号
func (u *User) incrementVersion() {
	u.version++
}

// validateUserName 验证用户名
func validateUserName(name string) error {
	if name == "" {
		return user.ErrUserNameEmpty
	}

	if len(name) > 100 {
		return user.ErrUserNameTooLong
	}

	// 验证规则：禁止特殊字符、长度限制等
	// 参考 teable-develop: 用户名可以包含字母、数字、中文等
	// 简化实现：基本验证已够用

	return nil
}
