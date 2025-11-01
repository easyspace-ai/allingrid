package table

import "errors"

// 表格相关领域错误
var (
	// 表格基础错误
	ErrTableNotFound            = errors.New("table not found")
	ErrTableNameEmpty           = errors.New("table name cannot be empty")
	ErrTableNameTooLong         = errors.New("table name too long (max 100 characters)")
	ErrTableNameInvalid         = errors.New("table name contains invalid characters")
	ErrTableNameNotUnique       = errors.New("table name already exists in this base")
	ErrCannotModifyDeletedTable = errors.New("cannot modify deleted table")
	ErrTableAlreadyDeleted      = errors.New("table is already deleted")

	// 表格内容错误
	ErrTableMustHaveFields     = errors.New("table must have at least one field")
	ErrTableMustHavePrimaryKey = errors.New("table must have a primary key field")

	// 字段相关错误（表格聚合层面）
	ErrFieldNameAlreadyExists = errors.New("field name already exists in this table")
	ErrFieldNotFound          = errors.New("field not found in table")
	ErrCannotDeletePrimaryKey = errors.New("cannot delete primary key field")
	ErrCannotDeleteLastField  = errors.New("cannot delete the last field")

	// 权限错误
	ErrInsufficientPermission = errors.New("insufficient permission to perform this operation")

	// 依赖错误
	ErrTableHasDependencies = errors.New("table has dependencies and cannot be deleted")
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
