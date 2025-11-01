package validation

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// ValidationResult 验证结果
type ValidationResult struct {
	Success bool
	Value   interface{}
	Error   error
}

// FieldValidator 字段验证器接口
// 参考旧系统：FieldCore.validateCellValue
type FieldValidator interface {
	// ValidateCell 验证单元格值
	ValidateCell(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult

	// Repair 尝试修复/转换值
	Repair(ctx context.Context, value interface{}, field *entity.Field) interface{}

	// ConvertStringToValue 将字符串转换为字段值
	ConvertStringToValue(ctx context.Context, str string, field *entity.Field) (interface{}, error)

	// SupportedType 返回支持的字段类型
	SupportedType() valueobject.FieldType
}

// ValidationError 验证错误
type ValidationError struct {
	Field   string
	Message string
	Value   interface{}
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("字段 %s 验证失败: %s (值: %v)", e.Field, e.Message, e.Value)
}

// NewValidationError 创建验证错误
func NewValidationError(field, message string, value interface{}) *ValidationError {
	return &ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
	}
}

// Success 创建成功的验证结果
func Success(value interface{}) *ValidationResult {
	return &ValidationResult{
		Success: true,
		Value:   value,
		Error:   nil,
	}
}

// Failure 创建失败的验证结果
func Failure(err error) *ValidationResult {
	return &ValidationResult{
		Success: false,
		Value:   nil,
		Error:   err,
	}
}
