package opbuilder

import (
	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
)

// TableOpBuilder 表操作构建器
type TableOpBuilder struct{}

// NewTableOpBuilder 创建表操作构建器
func NewTableOpBuilder() *TableOpBuilder {
	return &TableOpBuilder{}
}

// SetTableProperty 设置表属性
func (b *TableOpBuilder) SetTableProperty(key string, oldValue, newValue interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{key},
		"oi": newValue,
		"od": oldValue,
	}
}

// CreateTable 创建表
func (b *TableOpBuilder) CreateTable(tableID string, properties map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(properties))

	for key, value := range properties {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{key},
			"oi": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"tables", tableID},
		"oi": ops,
	}
}

// DeleteTable 删除表
func (b *TableOpBuilder) DeleteTable(tableID string, properties map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(properties))

	for key, value := range properties {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{key},
			"od": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"tables", tableID},
		"od": ops,
	}
}

// UpdateTableName 更新表名称
func (b *TableOpBuilder) UpdateTableName(tableID string, oldName, newName string) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"tables", tableID, "name"},
		"oi": newName,
		"od": oldName,
	}
}

// UpdateTableDescription 更新表描述
func (b *TableOpBuilder) UpdateTableDescription(tableID string, oldDesc, newDesc string) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"tables", tableID, "description"},
		"oi": newDesc,
		"od": oldDesc,
	}
}
