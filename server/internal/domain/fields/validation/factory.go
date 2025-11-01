package validation

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// ValidatorFactory 验证器工厂
type ValidatorFactory struct {
	validators map[string]FieldValidator
}

// NewValidatorFactory 创建验证器工厂
func NewValidatorFactory() *ValidatorFactory {
	factory := &ValidatorFactory{
		validators: make(map[string]FieldValidator),
	}

	// 注册所有验证器
	factory.registerValidators()

	return factory
}

// registerValidators 注册所有验证器
func (f *ValidatorFactory) registerValidators() {
	// 文本类型
	singleLineValidator := NewSingleLineTextValidator()
	f.Register(singleLineValidator)
	// 向后兼容：text 作为 singleLineText 的别名
	f.validators["text"] = singleLineValidator

	f.Register(NewLongTextValidator())

	// 数字类型
	f.Register(NewNumberValidator())
	f.Register(NewRatingValidator())
	f.Register(NewPercentValidator())
	f.Register(NewCurrencyValidator())
	f.Register(NewDurationValidator())

	// 布尔类型
	f.Register(NewCheckboxValidator())

	// 日期类型
	f.Register(NewDateValidator())

	// 选择类型
	singleSelectValidator := NewSingleSelectValidator()
	f.Register(singleSelectValidator)
	// 向后兼容：select 作为 singleSelect 的别名
	f.validators["select"] = singleSelectValidator

	f.Register(NewMultipleSelectValidator())

	// 链接类型
	f.Register(NewURLValidator())
	f.Register(NewEmailValidator())
	f.Register(NewPhoneValidator())

	// 附件类型
	f.Register(NewAttachmentValidator())

	// 用户类型
	f.Register(NewUserValidator())

	// 自动编号
	f.Register(NewAutoNumberValidator())
}

// Register 注册验证器
func (f *ValidatorFactory) Register(validator FieldValidator) {
	f.validators[validator.SupportedType().String()] = validator
}

// GetValidator 获取验证器
func (f *ValidatorFactory) GetValidator(fieldType valueobject.FieldType) (FieldValidator, error) {
	validator, exists := f.validators[fieldType.String()]
	if !exists {
		return nil, fmt.Errorf("不支持的字段类型: %s", fieldType.String())
	}
	return validator, nil
}

// ValidateField 验证字段值
func (f *ValidatorFactory) ValidateField(ctx context.Context, value interface{}, field *entity.Field) *ValidationResult {
	validator, err := f.GetValidator(field.Type())
	if err != nil {
		return Failure(err)
	}

	return validator.ValidateCell(ctx, value, field)
}

// RepairValue 修复值
func (f *ValidatorFactory) RepairValue(ctx context.Context, value interface{}, field *entity.Field) interface{} {
	validator, err := f.GetValidator(field.Type())
	if err != nil {
		return nil
	}

	return validator.Repair(ctx, value, field)
}
