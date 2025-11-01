package opbuilder

import (
	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
)

// FieldOpBuilder 字段操作构建器
type FieldOpBuilder struct{}

// NewFieldOpBuilder 创建字段操作构建器
func NewFieldOpBuilder() *FieldOpBuilder {
	return &FieldOpBuilder{}
}

// SetFieldProperty 设置字段属性
func (b *FieldOpBuilder) SetFieldProperty(key string, oldValue, newValue interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{key},
		"oi": newValue,
		"od": oldValue,
	}
}

// CreateField 创建字段
func (b *FieldOpBuilder) CreateField(fieldID string, properties map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(properties))

	for key, value := range properties {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{key},
			"oi": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"fields", fieldID},
		"oi": ops,
	}
}

// DeleteField 删除字段
func (b *FieldOpBuilder) DeleteField(fieldID string, properties map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(properties))

	for key, value := range properties {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{key},
			"od": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"fields", fieldID},
		"od": ops,
	}
}

// UpdateFieldType 更新字段类型
func (b *FieldOpBuilder) UpdateFieldType(fieldID string, oldType, newType string) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"fields", fieldID, "type"},
		"oi": newType,
		"od": oldType,
	}
}

// UpdateFieldName 更新字段名称
func (b *FieldOpBuilder) UpdateFieldName(fieldID string, oldName, newName string) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"fields", fieldID, "name"},
		"oi": newName,
		"od": oldName,
	}
}
