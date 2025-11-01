package space

import "errors"

// 空间相关领域错误
var (
	// 空间基础错误
	ErrSpaceNotFound            = errors.New("space not found")
	ErrSpaceAlreadyExists       = errors.New("space already exists")
	ErrCannotModifyDeletedSpace = errors.New("cannot modify deleted space")
	ErrSpaceAlreadyDeleted      = errors.New("space is already deleted")

	// 空间名称错误
	ErrSpaceNameEmpty   = errors.New("space name cannot be empty")
	ErrSpaceNameTooLong = errors.New("space name too long (max 100 characters)")
	ErrSpaceNameInvalid = errors.New("space name contains invalid characters")

	// 成员错误
	ErrMemberNotFound         = errors.New("member not found in space")
	ErrMemberAlreadyExists    = errors.New("member already exists in space")
	ErrCannotRemoveOwner      = errors.New("cannot remove space owner")
	ErrCannotRemoveLastMember = errors.New("cannot remove last member from space")
	ErrInvalidRole            = errors.New("invalid collaborator role")

	// 权限错误
	ErrInsufficientPermission = errors.New("insufficient permission")
	ErrNotSpaceOwner          = errors.New("not space owner")
	ErrNotSpaceMember         = errors.New("not a member of this space")

	// 空间限制错误
	ErrSpaceLimitReached  = errors.New("space limit reached")
	ErrMemberLimitReached = errors.New("member limit reached")
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
