package valueobject

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
)

// FieldType 字段类型值对象
type FieldType struct {
	value      string
	category   FieldCategory
	isVirtual  bool
	isComputed bool
}

// FieldCategory 字段类别
type FieldCategory string

const (
	CategoryBasic      FieldCategory = "basic"      // text, number, date, boolean
	CategoryRelational FieldCategory = "relational" // link
	CategoryComputed   FieldCategory = "computed"   // formula, rollup, lookup
	CategoryMedia      FieldCategory = "media"      // attachment
	CategorySelect     FieldCategory = "select"     // select, multipleSelect
	CategoryAI         FieldCategory = "ai"         // ai
	CategorySystem     FieldCategory = "system"     // createdTime, createdBy, etc.
)

// 字段类型常量（对齐原版FieldType枚举）
const (
	TypeText             = "text"
	TypeNumber           = "number"
	TypeDate             = "date"
	TypeDateTime         = "datetime"
	TypeBoolean          = "boolean"
	TypeSelect           = "select"
	TypeSingleSelect     = "singleSelect"   // 对齐原版FieldType.SingleSelect
	TypeMultipleSelect   = "multipleSelect" // 对齐原版FieldType.MultipleSelect
	TypeAttachment       = "attachment"
	TypeUser             = "user"
	TypeLink             = "link"
	TypeFormula          = "formula"
	TypeRollup           = "rollup"
	TypeLookup           = "lookup"
	TypeCount            = "count"
	TypeAutoNumber       = "autoNumber"
	TypeCreatedTime      = "createdTime"
	TypeCreatedBy        = "createdBy"
	TypeModifiedTime     = "lastModifiedTime"
	TypeLastModifiedTime = "lastModifiedTime" // 别名
	TypeModifiedBy       = "lastModifiedBy"
	TypeLastModifiedBy   = "lastModifiedBy" // 别名
	TypeAI               = "ai"
	TypeEmail            = "email"
	TypeURL              = "url"
	TypePhone            = "phone"
	TypeRating           = "rating"
	TypeCheckbox         = "checkbox"
	TypeDuration         = "duration"
	TypePercent          = "percent"
	TypeCurrency         = "currency"
	TypeButton           = "button"         // 对齐原版
	TypeSingleLineText   = "singleLineText" // 对齐原版
	TypeLongText         = "longText"       // 对齐原版
)

// NewFieldType 创建字段类型值对象
func NewFieldType(value string) (FieldType, error) {
	if !isValidFieldType(value) {
		return FieldType{}, fields.ErrInvalidFieldType
	}

	return FieldType{
		value:      value,
		category:   determineCategory(value),
		isVirtual:  isVirtualType(value),
		isComputed: isComputedType(value),
	}, nil
}

// String 获取字符串值
func (ft FieldType) String() string {
	return ft.value
}

// Category 获取字段类别
func (ft FieldType) Category() FieldCategory {
	return ft.category
}

// IsVirtual 是否为虚拟字段（不存储在数据库中）
func (ft FieldType) IsVirtual() bool {
	return ft.isVirtual
}

// IsComputed 是否为计算字段（需要计算才能得到值）
func (ft FieldType) IsComputed() bool {
	return ft.isComputed
}

// IsBasic 是否为基础字段
func (ft FieldType) IsBasic() bool {
	return ft.category == CategoryBasic
}

// IsRelational 是否为关系字段
func (ft FieldType) IsRelational() bool {
	return ft.category == CategoryRelational
}

// Equals 比较两个字段类型是否相等
func (ft FieldType) Equals(other FieldType) bool {
	return ft.value == other.value
}

// IsCompatibleWith 检查是否可以转换为另一种字段类型
func (ft FieldType) IsCompatibleWith(target FieldType) bool {
	// 相同类型总是兼容
	if ft.Equals(target) {
		return true
	}

	// 虚拟字段不能互相转换
	if ft.IsVirtual() || target.IsVirtual() {
		return false
	}

	// 使用兼容性矩阵
	return compatibilityMatrix[ft.value][target.value]
}

// isValidFieldType 检查字段类型是否有效
func isValidFieldType(value string) bool {
	validTypes := map[string]bool{
		TypeText:           true,
		TypeNumber:         true,
		TypeDate:           true,
		TypeDateTime:       true,
		TypeBoolean:        true,
		TypeSelect:         true,
		TypeSingleSelect:   true,
		TypeMultipleSelect: true,
		TypeAttachment:     true,
		TypeUser:           true,
		TypeLink:           true,
		TypeFormula:        true,
		TypeRollup:         true,
		TypeLookup:         true,
		TypeCount:          true,
		TypeAutoNumber:     true,
		TypeCreatedTime:    true,
		TypeCreatedBy:      true,
		TypeModifiedTime:   true,
		TypeModifiedBy:     true,
		TypeAI:             true,
		TypeEmail:          true,
		TypeURL:            true,
		TypePhone:          true,
		TypeRating:         true,
		TypeCheckbox:       true,
		TypeDuration:       true,
		TypePercent:        true,
		TypeCurrency:       true,
		TypeButton:         true,
		TypeSingleLineText: true,
		TypeLongText:       true,
	}

	return validTypes[value]
}

// determineCategory 确定字段类别
func determineCategory(fieldType string) FieldCategory {
	switch fieldType {
	case TypeText, TypeNumber, TypeDate, TypeDateTime, TypeBoolean,
		TypeEmail, TypeURL, TypePhone, TypeRating, TypeCheckbox,
		TypeDuration, TypePercent, TypeCurrency, TypeAutoNumber,
		TypeSingleLineText, TypeLongText:
		return CategoryBasic

	case TypeLink:
		return CategoryRelational

	case TypeFormula, TypeRollup, TypeLookup, TypeCount:
		return CategoryComputed

	case TypeAttachment:
		return CategoryMedia

	case TypeSelect, TypeSingleSelect, TypeMultipleSelect:
		return CategorySelect

	case TypeAI:
		return CategoryAI

	case TypeCreatedTime, TypeCreatedBy, TypeModifiedTime, TypeModifiedBy:
		return CategorySystem

	default:
		return CategoryBasic
	}
}

// isVirtualType 是否为虚拟字段类型
func isVirtualType(fieldType string) bool {
	virtualTypes := map[string]bool{
		TypeFormula:      true,
		TypeRollup:       true,
		TypeLookup:       true,
		TypeAI:           true,
		TypeCount:        true,
		TypeCreatedTime:  true,
		TypeCreatedBy:    true,
		TypeModifiedTime: true,
		TypeModifiedBy:   true,
	}

	return virtualTypes[fieldType]
}

// isComputedType 是否为计算字段类型
func isComputedType(fieldType string) bool {
	computedTypes := map[string]bool{
		TypeFormula: true,
		TypeRollup:  true,
		TypeLookup:  true,
		TypeAI:      true,
		TypeCount:   true,
	}

	return computedTypes[fieldType]
}

// compatibilityMatrix 字段类型兼容性矩阵
// true 表示可以从源类型转换为目标类型
var compatibilityMatrix = map[string]map[string]bool{
	TypeText: {
		TypeNumber:   true,
		TypeEmail:    true,
		TypeURL:      true,
		TypePhone:    true,
		TypeDate:     true,
		TypeDateTime: true,
	},
	TypeNumber: {
		TypeText:     true,
		TypePercent:  true,
		TypeCurrency: true,
		TypeRating:   true,
		TypeDuration: true,
	},
	TypeDate: {
		TypeText:     true,
		TypeDateTime: true,
	},
	TypeDateTime: {
		TypeText: true,
		TypeDate: true,
	},
	TypeBoolean: {
		TypeText:     true,
		TypeCheckbox: true,
	},
	TypeCheckbox: {
		TypeBoolean: true,
		TypeText:    true,
	},
	TypeSelect: {
		TypeText:           true,
		TypeMultipleSelect: false, // 单选转多选需要特殊处理
	},
	TypeSingleSelect: {
		TypeText:           true,
		TypeMultipleSelect: false, // 单选转多选需要特殊处理
	},
	TypeMultipleSelect: {
		TypeText:   true,
		TypeSelect: false, // 多选转单选可能丢失数据
	},
	TypeEmail: {
		TypeText: true,
		TypeURL:  true,
	},
	TypeURL: {
		TypeText: true,
	},
	TypePhone: {
		TypeText: true,
	},
	TypeRating: {
		TypeNumber: true,
		TypeText:   true,
	},
	TypePercent: {
		TypeNumber: true,
		TypeText:   true,
	},
	TypeCurrency: {
		TypeNumber: true,
		TypeText:   true,
	},
	TypeDuration: {
		TypeNumber: true,
		TypeText:   true,
	},
}
