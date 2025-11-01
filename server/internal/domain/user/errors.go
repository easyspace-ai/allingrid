package user

import "errors"

// 用户相关领域错误
var (
	// 用户基础错误
	ErrUserNotFound            = errors.New("user not found")
	ErrUserAlreadyExists       = errors.New("user already exists")
	ErrCannotModifyDeletedUser = errors.New("cannot modify deleted user")
	ErrUserAlreadyDeleted      = errors.New("user is already deleted")

	// 用户名错误
	ErrUserNameEmpty   = errors.New("user name cannot be empty")
	ErrUserNameTooLong = errors.New("user name too long (max 100 characters)")
	ErrUserNameInvalid = errors.New("user name contains invalid characters")

	// 邮箱错误
	ErrEmailEmpty         = errors.New("email cannot be empty")
	ErrEmailInvalid       = errors.New("email format is invalid")
	ErrEmailTooLong       = errors.New("email too long (max 255 characters)")
	ErrEmailAlreadyExists = errors.New("email already exists")

	// 密码错误
	ErrPasswordEmpty       = errors.New("password cannot be empty")
	ErrPasswordTooShort    = errors.New("password too short (min 8 characters)")
	ErrPasswordTooLong     = errors.New("password too long (max 128 characters)")
	ErrPasswordTooWeak     = errors.New("password too weak")
	ErrPasswordMismatch    = errors.New("password does not match")
	ErrInvalidPasswordHash = errors.New("invalid password hash")

	// 手机号错误
	ErrPhoneInvalid = errors.New("phone number format is invalid")
	ErrPhoneTooLong = errors.New("phone number too long")

	// 用户状态错误
	ErrUserNotActive             = errors.New("user is not active")
	ErrUserDeactivated           = errors.New("user is deactivated")
	ErrUserAlreadyActive         = errors.New("user is already active")
	ErrCannotActivateDeletedUser = errors.New("cannot activate deleted user")

	// 账户错误
	ErrAccountNotFound      = errors.New("account not found")
	ErrAccountAlreadyLinked = errors.New("account already linked")
	ErrInvalidProvider      = errors.New("invalid provider")

	// 权限错误
	ErrInsufficientPrivilege  = errors.New("insufficient privilege")
	ErrNotSystemUser          = errors.New("not a system user")
	ErrNotAdminUser           = errors.New("not an admin user")
	ErrCannotModifySystemUser = errors.New("cannot modify system user")
)

// DomainError 结构化领域错误
type DomainError struct {
	Code    string
	Message string
	Err     error
}

func (e *DomainError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *DomainError) Unwrap() error {
	return e.Err
}

// NewDomainError 创建领域错误
func NewDomainError(code, message string, err error) *DomainError {
	return &DomainError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}
