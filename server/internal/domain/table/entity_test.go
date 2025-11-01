package table

import (
	"testing"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	tableID := valueobject.NewTableID()
	baseID := "test-base-id"
	name := "测试表格"
	description := "这是一个测试表格"
	createdBy := uuid.New().String()

	table, err := NewTable(tableID, baseID, name, description, createdBy)

	assert.NoError(t, err)
	assert.NotNil(t, table)
	assert.Equal(t, tableID, table.ID())
	assert.Equal(t, baseID, table.BaseID())
	assert.Equal(t, name, table.Name())
	assert.Equal(t, description, table.Description())
	assert.Equal(t, createdBy, table.CreatedBy())
	assert.Equal(t, TableStatusActive, table.Status())
	assert.NotZero(t, table.CreatedAt())
	assert.NotZero(t, table.UpdatedAt())
}

func TestTable_UpdateName(t *testing.T) {
	table := createTestTable()
	newName := "新表格名称"

	err := table.UpdateName(newName, table.CreatedBy())
	assert.NoError(t, err)
	assert.Equal(t, newName, table.Name())
}

func TestTable_UpdateDescription(t *testing.T) {
	table := createTestTable()
	newDescription := "新的表格描述"

	err := table.UpdateDescription(newDescription, table.CreatedBy())
	assert.NoError(t, err)
	assert.Equal(t, newDescription, table.Description())
}

func TestTable_AddField(t *testing.T) {
	table := createTestTable()
	field := createTestField()

	err := table.AddField(field, table.CreatedBy())
	assert.NoError(t, err)

	fields := table.GetFields()
	assert.Len(t, fields, 1)
	assert.Equal(t, field.ID(), fields[0].ID())
}

func TestTable_RemoveField(t *testing.T) {
	table := createTestTable()
	field := createTestField()
	table.AddField(field, table.CreatedBy())

	err := table.RemoveField(field.ID(), table.CreatedBy())
	assert.NoError(t, err)

	fields := table.GetFields()
	assert.Len(t, fields, 0)
}

func TestTable_GetField(t *testing.T) {
	table := createTestTable()
	field := createTestField()
	table.AddField(field, table.CreatedBy())

	// 测试获取存在的字段
	foundField, exists := table.GetField(field.ID())
	assert.True(t, exists)
	assert.Equal(t, field.ID(), foundField.ID())

	// 测试获取不存在的字段
	_, exists = table.GetField("nonexistent-field-id")
	assert.False(t, exists)
}

func TestTable_GetFields(t *testing.T) {
	table := createTestTable()
	field1 := createTestField()
	field2 := createTestField()

	table.AddField(field1, table.CreatedBy())
	table.AddField(field2, table.CreatedBy())

	fields := table.GetFields()
	assert.Len(t, fields, 2)
}

func TestTable_Activate(t *testing.T) {
	table := createTestTable()
	table.Deactivate(table.CreatedBy())

	err := table.Activate(table.CreatedBy())
	assert.NoError(t, err)
	assert.Equal(t, TableStatusActive, table.Status())
}

func TestTable_Deactivate(t *testing.T) {
	table := createTestTable()

	err := table.Deactivate(table.CreatedBy())
	assert.NoError(t, err)
	assert.Equal(t, TableStatusInactive, table.Status())
}

func TestTable_IsActive(t *testing.T) {
	table := createTestTable()
	assert.True(t, table.IsActive())

	table.Deactivate(table.CreatedBy())
	assert.False(t, table.IsActive())
}

// 辅助函数
func createTestTable() *Table {
	tableID := valueobject.NewTableID()
	baseID := "test-base-id"
	name := "测试表格"
	description := "测试描述"
	createdBy := uuid.New().String()

	table, _ := NewTable(tableID, baseID, name, description, createdBy)
	return table
}

func createTestField() *entity.Field {
	fieldID := "test-field-id"
	tableID := "test-table-id"
	name := "测试字段"
	fieldType := entity.FieldTypeText
	options := entity.FieldOptions{}
	createdBy := uuid.New().String()

	field, _ := entity.NewField(fieldID, tableID, name, fieldType, options, createdBy)
	return field
}
