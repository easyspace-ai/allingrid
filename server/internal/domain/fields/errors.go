package fields

import "errors"

// 字段相关领域错误
var (
	// 字段基础错误
	ErrFieldNotFound            = errors.New("field not found")
	ErrFieldNameEmpty           = errors.New("field name cannot be empty")
	ErrFieldNameTooLong         = errors.New("field name too long (max 64 characters)")
	ErrFieldNameNotUnique       = errors.New("field name already exists in this table")
	ErrCannotModifyDeletedField = errors.New("cannot modify deleted field")
	ErrFieldAlreadyDeleted      = errors.New("field is already deleted")

	// 字段类型错误
	ErrInvalidFieldType       = errors.New("invalid field type")
	ErrIncompatibleFieldType  = errors.New("incompatible field type")
	ErrIncompatibleTypeChange = errors.New("field type change is not compatible")
	ErrCannotMigrateData      = errors.New("cannot migrate existing data to new type")
	ErrUnsupportedFieldType   = errors.New("field type is not supported")

	// 字段选项错误
	ErrInvalidFieldOptions   = errors.New("invalid field options")
	ErrMissingRequiredOption = errors.New("missing required option for this field type")
	ErrInvalidOptionValue    = errors.New("invalid option value")
	ErrOptionsNotSupported   = errors.New("this field type does not support options")

	// 字段值错误
	ErrInvalidFieldValue         = errors.New("invalid field value")
	ErrValueTypeMismatch         = errors.New("value type does not match field type")
	ErrRequiredFieldEmpty        = errors.New("required field cannot be empty")
	ErrUniqueConstraintViolation = errors.New("unique constraint violation")

	// 虚拟字段错误
	ErrInvalidFormula      = errors.New("invalid formula expression")
	ErrCircularDependency  = errors.New("circular dependency detected")
	ErrMissingDependency   = errors.New("missing field dependency")
	ErrInvalidRollupConfig = errors.New("invalid rollup configuration")
	ErrInvalidLookupConfig = errors.New("invalid lookup configuration")

	// 字段关系错误
	ErrInvalidLinkField      = errors.New("invalid link field configuration")
	ErrLinkToSameTable       = errors.New("cannot link to the same table")
	ErrMissingTargetTable    = errors.New("missing target table for link field")
	ErrMissingSymmetricField = errors.New("missing symmetric field for bidirectional link")

	// 字段顺序错误
	ErrInvalidFieldOrder = errors.New("invalid field order")

	// 字段数据库映射错误
	ErrInvalidDBFieldName  = errors.New("invalid database field name")
	ErrDBFieldNameConflict = errors.New("database field name conflicts with reserved words")
)

// DomainError 领域错误类型（结构化错误）
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
