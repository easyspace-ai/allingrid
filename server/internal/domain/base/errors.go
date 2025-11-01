package base

import "errors"

// Base相关领域错误
var (
	// Base基础错误
	ErrBaseNotFound            = errors.New("base not found")
	ErrBaseAlreadyExists       = errors.New("base already exists")
	ErrCannotModifyDeletedBase = errors.New("cannot modify deleted base")
	ErrBaseAlreadyDeleted      = errors.New("base is already deleted")

	// Base名称错误
	ErrBaseNameEmpty   = errors.New("base name cannot be empty")
	ErrBaseNameTooLong = errors.New("base name too long (max 100 characters)")

	// 权限错误
	ErrInsufficientPermission = errors.New("insufficient permission")
	ErrNotBaseOwner           = errors.New("not base owner")
)

// DomainError 领域错误类型
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
