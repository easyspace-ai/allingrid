package opbuilder

// RecordOpBuilder 记录操作构建器
type RecordOpBuilder struct{}

// SetRecord 构建设置记录字段操作
func (b *RecordOpBuilder) SetRecord(fieldID string, newValue, oldValue interface{}) Operation {
	return Operation{
		Path:     []interface{}{"fields", fieldID},
		OldValue: oldValue,
		NewValue: newValue,
		Type:     OpTypeSet,
	}
}

// SetRecords 批量设置记录字段
func (b *RecordOpBuilder) SetRecords(updates map[string]interface{}) []Operation {
	ops := make([]Operation, 0, len(updates))
	for fieldID, value := range updates {
		ops = append(ops, b.SetRecord(fieldID, value, nil))
	}
	return ops
}

// InsertRecord 构建插入记录操作
func (b *RecordOpBuilder) InsertRecord(recordID string, data map[string]interface{}) Operation {
	return Operation{
		Path:     []interface{}{"records", recordID},
		OldValue: nil,
		NewValue: data,
		Type:     OpTypeInsert,
	}
}

// DeleteRecord 构建删除记录操作
func (b *RecordOpBuilder) DeleteRecord(recordID string) Operation {
	return Operation{
		Path:     []interface{}{"records", recordID},
		OldValue: nil,
		NewValue: nil,
		Type:     OpTypeDelete,
	}
}

// MoveRecord 构建移动记录操作
func (b *RecordOpBuilder) MoveRecord(recordID string, fromIndex, toIndex int) Operation {
	return Operation{
		Path:     []interface{}{"records", recordID},
		OldValue: fromIndex,
		NewValue: toIndex,
		Type:     OpTypeMove,
	}
}

// UpdateRecordFields 更新记录字段（支持批量）
func (b *RecordOpBuilder) UpdateRecordFields(recordID string, fieldUpdates map[string]interface{}) []Operation {
	ops := make([]Operation, 0, len(fieldUpdates))
	for fieldID, newValue := range fieldUpdates {
		ops = append(ops, b.SetRecord(fieldID, newValue, nil))
	}
	return ops
}

// CreateRecordOperation 创建完整的记录操作
func (b *RecordOpBuilder) CreateRecordOperation(recordID string, fieldID string, newValue, oldValue interface{}) RecordOperation {
	return RecordOperation{
		FieldID:   fieldID,
		OldValue:  oldValue,
		NewValue:  newValue,
		Operation: b.SetRecord(fieldID, newValue, oldValue),
	}
}
