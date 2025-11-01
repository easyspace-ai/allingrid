package record

import "errors"

// 记录相关领域错误
var (
	// 记录基础错误
	ErrRecordNotFound            = errors.New("record not found")
	ErrCannotModifyDeletedRecord = errors.New("cannot modify deleted record")
	ErrRecordAlreadyDeleted      = errors.New("record is already deleted")

	// 记录数据错误
	ErrInvalidRecordData    = errors.New("invalid record data")
	ErrEmptyRecordData      = errors.New("record data cannot be empty")
	ErrMissingRequiredField = errors.New("missing required field")

	// 记录验证错误
	ErrValidationFailed          = errors.New("record validation failed")
	ErrFieldValueInvalid         = errors.New("field value is invalid")
	ErrUniqueConstraintViolation = errors.New("unique constraint violation")
	ErrForeignKeyViolation       = errors.New("foreign key constraint violation")

	// 记录版本错误
	ErrVersionConflict = errors.New("version conflict (optimistic lock failed)")
	ErrInvalidVersion  = errors.New("invalid record version")

	// 批量操作错误
	ErrBatchOperationFailed = errors.New("batch operation failed")
	ErrPartialSuccess       = errors.New("partial success in batch operation")
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
