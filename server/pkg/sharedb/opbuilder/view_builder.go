package opbuilder

// ViewOpBuilder 视图操作构建器
type ViewOpBuilder struct{}

// SetViewProperty 设置视图属性
func (b *ViewOpBuilder) SetViewProperty(key string, newValue, oldValue interface{}) Operation {
	return Operation{
		Path:     []interface{}{key},
		OldValue: oldValue,
		NewValue: newValue,
		Type:     OpTypeSet,
	}
}

// SetViewName 设置视图名称
func (b *ViewOpBuilder) SetViewName(viewID string, newName, oldName string) Operation {
	return Operation{
		Path:     []interface{}{"name"},
		OldValue: oldName,
		NewValue: newName,
		Type:     OpTypeSet,
	}
}

// SetViewType 设置视图类型
func (b *ViewOpBuilder) SetViewType(viewID string, newType, oldType string) Operation {
	return Operation{
		Path:     []interface{}{"type"},
		OldValue: oldType,
		NewValue: newType,
		Type:     OpTypeSet,
	}
}

// SetViewFilter 设置视图过滤器
func (b *ViewOpBuilder) SetViewFilter(viewID string, newFilter, oldFilter interface{}) Operation {
	return Operation{
		Path:     []interface{}{"filter"},
		OldValue: oldFilter,
		NewValue: newFilter,
		Type:     OpTypeSet,
	}
}

// SetViewSort 设置视图排序
func (b *ViewOpBuilder) SetViewSort(viewID string, newSort, oldSort interface{}) Operation {
	return Operation{
		Path:     []interface{}{"sort"},
		OldValue: oldSort,
		NewValue: newSort,
		Type:     OpTypeSet,
	}
}

// SetViewGroupBy 设置视图分组
func (b *ViewOpBuilder) SetViewGroupBy(viewID string, newGroupBy, oldGroupBy interface{}) Operation {
	return Operation{
		Path:     []interface{}{"groupBy"},
		OldValue: oldGroupBy,
		NewValue: newGroupBy,
		Type:     OpTypeSet,
	}
}

// SetViewColumns 设置视图列
func (b *ViewOpBuilder) SetViewColumns(viewID string, newColumns, oldColumns interface{}) Operation {
	return Operation{
		Path:     []interface{}{"columns"},
		OldValue: oldColumns,
		NewValue: newColumns,
		Type:     OpTypeSet,
	}
}

// InsertView 插入新视图
func (b *ViewOpBuilder) InsertView(viewID string, viewData map[string]interface{}) Operation {
	return Operation{
		Path:     []interface{}{"views", viewID},
		OldValue: nil,
		NewValue: viewData,
		Type:     OpTypeInsert,
	}
}

// DeleteView 删除视图
func (b *ViewOpBuilder) DeleteView(viewID string) Operation {
	return Operation{
		Path:     []interface{}{"views", viewID},
		OldValue: nil,
		NewValue: nil,
		Type:     OpTypeDelete,
	}
}

// MoveView 移动视图位置
func (b *ViewOpBuilder) MoveView(viewID string, fromIndex, toIndex int) Operation {
	return Operation{
		Path:     []interface{}{"views", viewID},
		OldValue: fromIndex,
		NewValue: toIndex,
		Type:     OpTypeMove,
	}
}

// UpdateViewProperties 批量更新视图属性
func (b *ViewOpBuilder) UpdateViewProperties(viewID string, updates map[string]interface{}) []Operation {
	ops := make([]Operation, 0, len(updates))
	for key, newValue := range updates {
		ops = append(ops, b.SetViewProperty(key, newValue, nil))
	}
	return ops
}

// CreateViewOperation 创建完整的视图操作
func (b *ViewOpBuilder) CreateViewOperation(viewID string, key string, newValue, oldValue interface{}) ViewOperation {
	return ViewOperation{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  newValue,
		Operation: b.SetViewProperty(key, newValue, oldValue),
	}
}
