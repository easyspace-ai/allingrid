package record

import (
	"testing"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	recordID := valueobject.NewRecordID()
	tableID := "test-table-id"
	createdBy := uuid.New().String()
	data := map[string]interface{}{
		"name": "测试记录",
		"age":  25,
	}

	record, err := entity.NewRecord(recordID, tableID, createdBy, data)

	assert.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, recordID, record.ID())
	assert.Equal(t, tableID, record.TableID())
	assert.Equal(t, createdBy, record.CreatedBy())
	assert.Equal(t, data, record.Data())
	assert.Equal(t, entity.RecordStatusActive, record.Status())
	assert.NotZero(t, record.CreatedAt())
	assert.NotZero(t, record.UpdatedAt())
}

func TestRecord_UpdateData(t *testing.T) {
	record := createTestRecord()
	newData := map[string]interface{}{
		"name": "更新后的记录",
		"age":  30,
		"city": "北京",
	}

	err := record.UpdateData(newData, record.CreatedBy())
	assert.NoError(t, err)
	assert.Equal(t, newData, record.Data())
}

func TestRecord_UpdateField(t *testing.T) {
	record := createTestRecord()
	fieldName := "age"
	newValue := 30

	err := record.UpdateField(fieldName, newValue, record.CreatedBy())
	assert.NoError(t, err)

	value, exists := record.GetField(fieldName)
	assert.True(t, exists)
	assert.Equal(t, newValue, value)
}

func TestRecord_GetField(t *testing.T) {
	record := createTestRecord()

	// 测试存在的字段
	value, exists := record.GetField("name")
	assert.True(t, exists)
	assert.Equal(t, "测试记录", value)

	// 测试不存在的字段
	_, exists = record.GetField("nonexistent")
	assert.False(t, exists)
}

func TestRecord_DeleteField(t *testing.T) {
	record := createTestRecord()

	err := record.DeleteField("age", record.CreatedBy())
	assert.NoError(t, err)

	_, exists := record.GetField("age")
	assert.False(t, exists)
}

func TestRecord_SoftDelete(t *testing.T) {
	record := createTestRecord()
	deletedBy := uuid.New().String()

	err := record.SoftDelete(deletedBy)
	assert.NoError(t, err)
	assert.Equal(t, entity.RecordStatusDeleted, record.Status())
	assert.NotZero(t, record.DeletedAt())
	assert.Equal(t, deletedBy, record.DeletedBy())
}

func TestRecord_Restore(t *testing.T) {
	record := createTestRecord()
	record.SoftDelete(record.CreatedBy())

	err := record.Restore(record.CreatedBy())
	assert.NoError(t, err)
	assert.Equal(t, entity.RecordStatusActive, record.Status())
	assert.Zero(t, record.DeletedAt())
	assert.Empty(t, record.DeletedBy())
}

func TestRecord_IsDeleted(t *testing.T) {
	record := createTestRecord()
	assert.False(t, record.IsDeleted())

	record.SoftDelete(record.CreatedBy())
	assert.True(t, record.IsDeleted())
}

func TestRecord_IsActive(t *testing.T) {
	record := createTestRecord()
	assert.True(t, record.IsActive())

	record.SoftDelete(record.CreatedBy())
	assert.False(t, record.IsActive())
}

// 辅助函数
func createTestRecord() *entity.Record {
	recordID := valueobject.NewRecordID()
	tableID := "test-table-id"
	createdBy := uuid.New().String()
	data := map[string]interface{}{
		"name": "测试记录",
		"age":  25,
	}

	record, _ := entity.NewRecord(recordID, tableID, createdBy, data)
	return record
}
