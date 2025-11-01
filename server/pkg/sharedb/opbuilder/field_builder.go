package opbuilder

// FieldOpBuilder 字段操作构建器
type FieldOpBuilder struct{}

// SetFieldProperty 设置字段属性
func (b *FieldOpBuilder) SetFieldProperty(key string, newValue, oldValue interface{}) Operation {
	return Operation{
		Path:     []interface{}{key},
		OldValue: oldValue,
		NewValue: newValue,
		Type:     OpTypeSet,
	}
}

// SetFieldName 设置字段名称
func (b *FieldOpBuilder) SetFieldName(fieldID string, newName, oldName string) Operation {
	return Operation{
		Path:     []interface{}{"name"},
		OldValue: oldName,
		NewValue: newName,
		Type:     OpTypeSet,
	}
}

// SetFieldType 设置字段类型
func (b *FieldOpBuilder) SetFieldType(fieldID string, newType, oldType string) Operation {
	return Operation{
		Path:     []interface{}{"type"},
		OldValue: oldType,
		NewValue: newType,
		Type:     OpTypeSet,
	}
}

// SetFieldOptions 设置字段选项
func (b *FieldOpBuilder) SetFieldOptions(fieldID string, newOptions, oldOptions interface{}) Operation {
	return Operation{
		Path:     []interface{}{"options"},
		OldValue: oldOptions,
		NewValue: newOptions,
		Type:     OpTypeSet,
	}
}

// SetFieldDescription 设置字段描述
func (b *FieldOpBuilder) SetFieldDescription(fieldID string, newDesc, oldDesc string) Operation {
	return Operation{
		Path:     []interface{}{"description"},
		OldValue: oldDesc,
		NewValue: newDesc,
		Type:     OpTypeSet,
	}
}

// SetFieldRequired 设置字段是否必填
func (b *FieldOpBuilder) SetFieldRequired(fieldID string, newRequired, oldRequired bool) Operation {
	return Operation{
		Path:     []interface{}{"required"},
		OldValue: oldRequired,
		NewValue: newRequired,
		Type:     OpTypeSet,
	}
}

// InsertField 插入新字段
func (b *FieldOpBuilder) InsertField(fieldID string, fieldData map[string]interface{}) Operation {
	return Operation{
		Path:     []interface{}{"fields", fieldID},
		OldValue: nil,
		NewValue: fieldData,
		Type:     OpTypeInsert,
	}
}

// DeleteField 删除字段
func (b *FieldOpBuilder) DeleteField(fieldID string) Operation {
	return Operation{
		Path:     []interface{}{"fields", fieldID},
		OldValue: nil,
		NewValue: nil,
		Type:     OpTypeDelete,
	}
}

// MoveField 移动字段位置
func (b *FieldOpBuilder) MoveField(fieldID string, fromIndex, toIndex int) Operation {
	return Operation{
		Path:     []interface{}{"fields", fieldID},
		OldValue: fromIndex,
		NewValue: toIndex,
		Type:     OpTypeMove,
	}
}

// UpdateFieldProperties 批量更新字段属性
func (b *FieldOpBuilder) UpdateFieldProperties(fieldID string, updates map[string]interface{}) []Operation {
	ops := make([]Operation, 0, len(updates))
	for key, newValue := range updates {
		ops = append(ops, b.SetFieldProperty(key, newValue, nil))
	}
	return ops
}

// CreateFieldOperation 创建完整的字段操作
func (b *FieldOpBuilder) CreateFieldOperation(fieldID string, key string, newValue, oldValue interface{}) FieldOperation {
	return FieldOperation{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  newValue,
		Operation: b.SetFieldProperty(key, newValue, oldValue),
	}
}
