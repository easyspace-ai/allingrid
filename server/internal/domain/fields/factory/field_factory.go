package factory

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// FieldFactory 字段工厂
type FieldFactory struct{}

// NewFieldFactory 创建字段工厂
func NewFieldFactory() *FieldFactory {
	return &FieldFactory{}
}

// CreateFieldWithType 创建指定类型的字段（通用方法）
func (f *FieldFactory) CreateFieldWithType(
	tableID string,
	name string,
	fieldTypeStr string,
	createdBy string,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(fieldTypeStr)
	if err != nil {
		return nil, err
	}

	return entity.NewField(tableID, fieldName, fieldType, createdBy)
}

// CreateTextField 创建文本字段
func (f *FieldFactory) CreateTextField(
	tableID string,
	name string,
	createdBy string,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(valueobject.TypeText)
	if err != nil {
		return nil, err
	}

	return entity.NewField(tableID, fieldName, fieldType, createdBy)
}

// CreateNumberField 创建数字字段
func (f *FieldFactory) CreateNumberField(
	tableID string,
	name string,
	createdBy string,
	precision *int,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(valueobject.TypeNumber)
	if err != nil {
		return nil, err
	}

	field, err := entity.NewField(tableID, fieldName, fieldType, createdBy)
	if err != nil {
		return nil, err
	}

	// 设置数字选项
	if precision != nil {
		options := valueobject.NewFieldOptions()
		options.Number = &valueobject.NumberOptions{
			Precision: precision,
		}
		field.UpdateOptions(options)
	}

	return field, nil
}

// CreateDateField 创建日期字段
func (f *FieldFactory) CreateDateField(
	tableID string,
	name string,
	createdBy string,
	includeTime bool,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	var fieldTypeStr string
	if includeTime {
		fieldTypeStr = valueobject.TypeDateTime
	} else {
		fieldTypeStr = valueobject.TypeDate
	}

	fieldType, err := valueobject.NewFieldType(fieldTypeStr)
	if err != nil {
		return nil, err
	}

	return entity.NewField(tableID, fieldName, fieldType, createdBy)
}

// CreateSelectField 创建选择字段
func (f *FieldFactory) CreateSelectField(
	tableID string,
	name string,
	createdBy string,
	choices []valueobject.SelectChoice,
	multiple bool,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	var fieldTypeStr string
	if multiple {
		fieldTypeStr = valueobject.TypeMultipleSelect
	} else {
		fieldTypeStr = valueobject.TypeSelect
	}

	fieldType, err := valueobject.NewFieldType(fieldTypeStr)
	if err != nil {
		return nil, err
	}

	field, err := entity.NewField(tableID, fieldName, fieldType, createdBy)
	if err != nil {
		return nil, err
	}

	// 设置选择选项
	options := valueobject.NewFieldOptions().WithSelect(choices)
	field.UpdateOptions(options)

	return field, nil
}

// CreateLinkField 创建链接字段
func (f *FieldFactory) CreateLinkField(
	tableID string,
	name string,
	createdBy string,
	linkedTableID string,
	relationship string,
	isSymmetric bool,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(valueobject.TypeLink)
	if err != nil {
		return nil, err
	}

	field, err := entity.NewField(tableID, fieldName, fieldType, createdBy)
	if err != nil {
		return nil, err
	}

	// 设置链接选项
	options := valueobject.NewFieldOptions().WithLink(linkedTableID, relationship, isSymmetric)
	field.UpdateOptions(options)

	return field, nil
}

// CreateFormulaField 创建公式字段
func (f *FieldFactory) CreateFormulaField(
	tableID string,
	name string,
	createdBy string,
	expression string,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(valueobject.TypeFormula)
	if err != nil {
		return nil, err
	}

	field, err := entity.NewField(tableID, fieldName, fieldType, createdBy)
	if err != nil {
		return nil, err
	}

	// 设置公式选项
	options := valueobject.NewFieldOptions().WithFormula(expression)
	field.UpdateOptions(options)

	return field, nil
}

// CreateRollupField 创建Rollup字段
func (f *FieldFactory) CreateRollupField(
	tableID string,
	name string,
	createdBy string,
	linkFieldID string,
	rollupFieldID string,
	aggregationFunc string,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(valueobject.TypeRollup)
	if err != nil {
		return nil, err
	}

	field, err := entity.NewField(tableID, fieldName, fieldType, createdBy)
	if err != nil {
		return nil, err
	}

	// 设置Rollup选项
	options := valueobject.NewFieldOptions().WithRollup(linkFieldID, rollupFieldID, aggregationFunc)
	field.UpdateOptions(options)

	return field, nil
}

// CreateLookupField 创建Lookup字段
func (f *FieldFactory) CreateLookupField(
	tableID string,
	name string,
	createdBy string,
	linkFieldID string,
	lookupFieldID string,
) (*entity.Field, error) {
	fieldName, err := valueobject.NewFieldName(name)
	if err != nil {
		return nil, err
	}

	fieldType, err := valueobject.NewFieldType(valueobject.TypeLookup)
	if err != nil {
		return nil, err
	}

	field, err := entity.NewField(tableID, fieldName, fieldType, createdBy)
	if err != nil {
		return nil, err
	}

	// 设置Lookup选项
	options := valueobject.NewFieldOptions().WithLookup(linkFieldID, lookupFieldID)
	field.UpdateOptions(options)

	return field, nil
}
