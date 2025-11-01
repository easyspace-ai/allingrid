package opbuilder

import (
	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
)

// RecordOpBuilder 记录操作构建器
type RecordOpBuilder struct{}

// NewRecordOpBuilder 创建记录操作构建器
func NewRecordOpBuilder() *RecordOpBuilder {
	return &RecordOpBuilder{}
}

// SetRecord 设置记录字段值
func (b *RecordOpBuilder) SetRecord(fieldID string, oldValue, newValue interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"fields", fieldID},
		"oi": newValue,
		"od": oldValue,
	}
}

// DeleteRecord 删除记录字段
func (b *RecordOpBuilder) DeleteRecord(fieldID string, oldValue interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"fields", fieldID},
		"od": oldValue,
	}
}

// CreateRecord 创建记录
func (b *RecordOpBuilder) CreateRecord(recordID string, fields map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(fields))

	for fieldID, value := range fields {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{"fields", fieldID},
			"oi": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"records", recordID},
		"oi": ops,
	}
}

// DeleteRecordFromTable 从表中删除记录
func (b *RecordOpBuilder) DeleteRecordFromTable(recordID string, fields map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(fields))

	for fieldID, value := range fields {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{"fields", fieldID},
			"od": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"records", recordID},
		"od": ops,
	}
}
