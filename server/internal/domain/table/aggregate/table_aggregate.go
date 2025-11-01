package aggregate

import (
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table"
	tableEntity "github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/event"
)

// TableAggregate 表格聚合根
// 管理表格和字段的关系，维护表格的完整性
type TableAggregate struct {
	// 聚合根
	table *tableEntity.Table

	// 聚合内实体（字段集合）
	fields []*fieldEntity.Field

	// 领域事件
	domainEvents []event.DomainEvent

	// 缓存的主键字段
	primaryField *fieldEntity.Field
}

// NewTableAggregate 创建表格聚合
func NewTableAggregate(table *tableEntity.Table) *TableAggregate {
	return &TableAggregate{
		table:        table,
		fields:       make([]*fieldEntity.Field, 0),
		domainEvents: make([]event.DomainEvent, 0),
	}
}

// NewTableAggregateWithFields 创建带字段的表格聚合
func NewTableAggregateWithFields(table *tableEntity.Table, fields []*fieldEntity.Field) *TableAggregate {
	agg := &TableAggregate{
		table:        table,
		fields:       fields,
		domainEvents: make([]event.DomainEvent, 0),
	}

	// 缓存主键字段
	agg.refreshPrimaryField()

	return agg
}

// ==================== 访问器方法 ====================

// Table 获取表格实体
func (agg *TableAggregate) Table() *tableEntity.Table {
	return agg.table
}

// Fields 获取字段列表（返回副本）
func (agg *TableAggregate) Fields() []*fieldEntity.Field {
	fields := make([]*fieldEntity.Field, len(agg.fields))
	copy(fields, agg.fields)
	return fields
}

// GetFieldCount 获取字段数量
func (agg *TableAggregate) GetFieldCount() int {
	count := 0
	for _, field := range agg.fields {
		if !field.IsDeleted() {
			count++
		}
	}
	return count
}

// GetFieldByID 根据ID获取字段
func (agg *TableAggregate) GetFieldByID(fieldID string) *fieldEntity.Field {
	for _, field := range agg.fields {
		if field.ID().String() == fieldID && !field.IsDeleted() {
			return field
		}
	}
	return nil
}

// GetFieldByName 根据名称获取字段
func (agg *TableAggregate) GetFieldByName(name string) *fieldEntity.Field {
	for _, field := range agg.fields {
		if field.Name().String() == name && !field.IsDeleted() {
			return field
		}
	}
	return nil
}

// GetPrimaryField 获取主键字段
func (agg *TableAggregate) GetPrimaryField() *fieldEntity.Field {
	return agg.primaryField
}

// GetVirtualFields 获取所有虚拟字段
func (agg *TableAggregate) GetVirtualFields() []*fieldEntity.Field {
	virtualFields := make([]*fieldEntity.Field, 0)
	for _, field := range agg.fields {
		if field.IsVirtual() && !field.IsDeleted() {
			virtualFields = append(virtualFields, field)
		}
	}
	return virtualFields
}

// DomainEvents 获取领域事件
func (agg *TableAggregate) DomainEvents() []event.DomainEvent {
	return agg.domainEvents
}

// ClearDomainEvents 清空领域事件
func (agg *TableAggregate) ClearDomainEvents() {
	agg.domainEvents = make([]event.DomainEvent, 0)
}

// ==================== 业务方法 ====================

// AddField 添加字段
func (agg *TableAggregate) AddField(field *fieldEntity.Field) error {
	// 验证字段名唯一性
	if agg.hasFieldWithName(field.Name().String()) {
		return table.ErrFieldNameAlreadyExists
	}

	// 如果是主键字段，检查是否已存在主键
	if field.IsPrimary() && agg.primaryField != nil {
		return table.NewDomainError(
			"PRIMARY_KEY_ALREADY_EXISTS",
			"table already has a primary key field",
			nil,
		)
	}

	// 添加字段
	agg.fields = append(agg.fields, field)

	// 如果是主键字段，更新缓存
	if field.IsPrimary() {
		agg.primaryField = field
	}

	// 发布领域事件
	agg.addDomainEvent(event.NewFieldAddedToTable(
		agg.table.ID(),
		field.ID(),
		field.Name(),
	))

	return nil
}

// RemoveField 移除字段
func (agg *TableAggregate) RemoveField(fieldID string) error {
	// 查找字段
	var fieldIndex int = -1
	var targetField *fieldEntity.Field

	for i, field := range agg.fields {
		if field.ID().String() == fieldID {
			fieldIndex = i
			targetField = field
			break
		}
	}

	if fieldIndex == -1 {
		return table.ErrFieldNotFound
	}

	// 不能删除主键字段
	if targetField.IsPrimary() {
		return table.ErrCannotDeletePrimaryKey
	}

	// 不能删除最后一个字段
	if agg.GetFieldCount() <= 1 {
		return table.ErrCannotDeleteLastField
	}

	// 软删除字段
	if err := targetField.SoftDelete(); err != nil {
		return err
	}

	// 从列表中移除
	agg.fields = append(agg.fields[:fieldIndex], agg.fields[fieldIndex+1:]...)

	// 发布领域事件
	agg.addDomainEvent(event.NewFieldRemovedFromTable(
		agg.table.ID(),
		targetField.ID(),
		targetField.Name(),
	))

	return nil
}

// UpdateField 更新字段
func (agg *TableAggregate) UpdateField(fieldID string, updater func(*fieldEntity.Field) error) error {
	field := agg.GetFieldByID(fieldID)
	if field == nil {
		return table.ErrFieldNotFound
	}

	// 执行更新
	if err := updater(field); err != nil {
		return err
	}

	// 如果更新了主键字段，刷新缓存
	if field.IsPrimary() {
		agg.primaryField = field
	}

	return nil
}

// ValidateSchema 验证表格Schema
func (agg *TableAggregate) ValidateSchema() error {
	// 必须至少有一个字段
	if agg.GetFieldCount() == 0 {
		return table.ErrTableMustHaveFields
	}

	// 必须有主键字段
	if agg.primaryField == nil {
		return table.ErrTableMustHavePrimaryKey
	}

	// 验证字段名唯一性
	nameMap := make(map[string]bool)
	for _, field := range agg.fields {
		if field.IsDeleted() {
			continue
		}

		name := field.Name().String()
		if nameMap[name] {
			return table.NewDomainError(
				"DUPLICATE_FIELD_NAME",
				"duplicate field name: "+name,
				nil,
			)
		}
		nameMap[name] = true
	}

	return nil
}

// ==================== 私有辅助方法 ====================

// hasFieldWithName 检查是否存在指定名称的字段
func (agg *TableAggregate) hasFieldWithName(name string) bool {
	for _, field := range agg.fields {
		if field.Name().String() == name && !field.IsDeleted() {
			return true
		}
	}
	return false
}

// refreshPrimaryField 刷新主键字段缓存
func (agg *TableAggregate) refreshPrimaryField() {
	for _, field := range agg.fields {
		if field.IsPrimary() && !field.IsDeleted() {
			agg.primaryField = field
			return
		}
	}
	agg.primaryField = nil
}

// addDomainEvent 添加领域事件
func (agg *TableAggregate) addDomainEvent(evt event.DomainEvent) {
	agg.domainEvents = append(agg.domainEvents, evt)
}
