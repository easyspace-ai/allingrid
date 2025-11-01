package opbuilder

// TableOpBuilder 表格操作构建器
type TableOpBuilder struct{}

// SetTableProperty 设置表格属性
func (b *TableOpBuilder) SetTableProperty(key string, newValue, oldValue interface{}) Operation {
	return Operation{
		Path:     []interface{}{key},
		OldValue: oldValue,
		NewValue: newValue,
		Type:     OpTypeSet,
	}
}

// SetTableName 设置表格名称
func (b *TableOpBuilder) SetTableName(tableID string, newName, oldName string) Operation {
	return Operation{
		Path:     []interface{}{"name"},
		OldValue: oldName,
		NewValue: newName,
		Type:     OpTypeSet,
	}
}

// SetTableDescription 设置表格描述
func (b *TableOpBuilder) SetTableDescription(tableID string, newDesc, oldDesc string) Operation {
	return Operation{
		Path:     []interface{}{"description"},
		OldValue: oldDesc,
		NewValue: newDesc,
		Type:     OpTypeSet,
	}
}

// SetTableIcon 设置表格图标
func (b *TableOpBuilder) SetTableIcon(tableID string, newIcon, oldIcon string) Operation {
	return Operation{
		Path:     []interface{}{"icon"},
		OldValue: oldIcon,
		NewValue: newIcon,
		Type:     OpTypeSet,
	}
}

// SetTableColor 设置表格颜色
func (b *TableOpBuilder) SetTableColor(tableID string, newColor, oldColor string) Operation {
	return Operation{
		Path:     []interface{}{"color"},
		OldValue: oldColor,
		NewValue: newColor,
		Type:     OpTypeSet,
	}
}

// SetTableSettings 设置表格设置
func (b *TableOpBuilder) SetTableSettings(tableID string, newSettings, oldSettings interface{}) Operation {
	return Operation{
		Path:     []interface{}{"settings"},
		OldValue: oldSettings,
		NewValue: newSettings,
		Type:     OpTypeSet,
	}
}

// SetTablePermissions 设置表格权限
func (b *TableOpBuilder) SetTablePermissions(tableID string, newPermissions, oldPermissions interface{}) Operation {
	return Operation{
		Path:     []interface{}{"permissions"},
		OldValue: oldPermissions,
		NewValue: newPermissions,
		Type:     OpTypeSet,
	}
}

// InsertTable 插入新表格
func (b *TableOpBuilder) InsertTable(tableID string, tableData map[string]interface{}) Operation {
	return Operation{
		Path:     []interface{}{"tables", tableID},
		OldValue: nil,
		NewValue: tableData,
		Type:     OpTypeInsert,
	}
}

// DeleteTable 删除表格
func (b *TableOpBuilder) DeleteTable(tableID string) Operation {
	return Operation{
		Path:     []interface{}{"tables", tableID},
		OldValue: nil,
		NewValue: nil,
		Type:     OpTypeDelete,
	}
}

// MoveTable 移动表格位置
func (b *TableOpBuilder) MoveTable(tableID string, fromIndex, toIndex int) Operation {
	return Operation{
		Path:     []interface{}{"tables", tableID},
		OldValue: fromIndex,
		NewValue: toIndex,
		Type:     OpTypeMove,
	}
}

// UpdateTableProperties 批量更新表格属性
func (b *TableOpBuilder) UpdateTableProperties(tableID string, updates map[string]interface{}) []Operation {
	ops := make([]Operation, 0, len(updates))
	for key, newValue := range updates {
		ops = append(ops, b.SetTableProperty(key, newValue, nil))
	}
	return ops
}

// CreateTableOperation 创建完整的表格操作
func (b *TableOpBuilder) CreateTableOperation(tableID string, key string, newValue, oldValue interface{}) TableOperation {
	return TableOperation{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  newValue,
		Operation: b.SetTableProperty(key, newValue, oldValue),
	}
}
