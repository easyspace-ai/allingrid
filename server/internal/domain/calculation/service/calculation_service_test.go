package service

import (
	"context"
	"testing"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	recordEntity "github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// MockFieldRepository 模拟字段仓储
type MockFieldRepository struct {
	mock.Mock
}

func (m *MockFieldRepository) FindByTableID(ctx context.Context, tableID string) ([]*entity.Field, error) {
	args := m.Called(ctx, tableID)
	return args.Get(0).([]*entity.Field), args.Error(1)
}

func (m *MockFieldRepository) FindByID(ctx context.Context, fieldID string) (*entity.Field, error) {
	args := m.Called(ctx, fieldID)
	return args.Get(0).(*entity.Field), args.Error(1)
}

func (m *MockFieldRepository) FindByIDs(ctx context.Context, fieldIDs []string) ([]*entity.Field, error) {
	args := m.Called(ctx, fieldIDs)
	return args.Get(0).([]*entity.Field), args.Error(1)
}

// MockRecordRepository 模拟记录仓储
type MockRecordRepository struct {
	mock.Mock
}

func (m *MockRecordRepository) Save(ctx context.Context, record *recordEntity.Record) error {
	args := m.Called(ctx, record)
	return args.Error(0)
}

func (m *MockRecordRepository) FindByID(ctx context.Context, id valueobject.RecordID) (*recordEntity.Record, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*recordEntity.Record), args.Error(1)
}

func (m *MockRecordRepository) FindByIDs(ctx context.Context, tableID string, ids []valueobject.RecordID) ([]*recordEntity.Record, error) {
	args := m.Called(ctx, tableID, ids)
	return args.Get(0).([]*recordEntity.Record), args.Error(1)
}

func (m *MockRecordRepository) FindByTableAndID(ctx context.Context, tableID string, id valueobject.RecordID) (*recordEntity.Record, error) {
	args := m.Called(ctx, tableID, id)
	return args.Get(0).(*recordEntity.Record), args.Error(1)
}

func (m *MockRecordRepository) FindByTableID(ctx context.Context, tableID string) ([]*recordEntity.Record, error) {
	args := m.Called(ctx, tableID)
	return args.Get(0).([]*recordEntity.Record), args.Error(1)
}

func (m *MockRecordRepository) Delete(ctx context.Context, id valueobject.RecordID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockRecordRepository) DeleteByTableAndID(ctx context.Context, tableID string, id valueobject.RecordID) error {
	args := m.Called(ctx, tableID, id)
	return args.Error(0)
}

func (m *MockRecordRepository) Exists(ctx context.Context, id valueobject.RecordID) (bool, error) {
	args := m.Called(ctx, id)
	return args.Bool(0), args.Error(1)
}

func (m *MockRecordRepository) List(ctx context.Context, filter RecordFilter) ([]*recordEntity.Record, int64, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).([]*recordEntity.Record), args.Get(1).(int64), args.Error(2)
}

func (m *MockRecordRepository) BatchSave(ctx context.Context, records []*recordEntity.Record) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}

func (m *MockRecordRepository) BatchDelete(ctx context.Context, ids []valueobject.RecordID) error {
	args := m.Called(ctx, ids)
	return args.Error(0)
}

func (m *MockRecordRepository) CountByTableID(ctx context.Context, tableID string) (int64, error) {
	args := m.Called(ctx, tableID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockRecordRepository) FindWithVersion(ctx context.Context, id valueobject.RecordID, version int64) (*recordEntity.Record, error) {
	args := m.Called(ctx, id, version)
	return args.Get(0).(*recordEntity.Record), args.Error(1)
}

// MockTableRepository 模拟表格仓储
type MockTableRepository struct {
	mock.Mock
}

func (m *MockTableRepository) GetDBTableName(ctx context.Context, tableID string) (string, error) {
	args := m.Called(ctx, tableID)
	return args.String(0), args.Error(1)
}

func TestNewCalculationService(t *testing.T) {
	logger := zap.NewNop()
	fieldRepo := &MockFieldRepository{}
	recordRepo := &MockRecordRepository{}
	tableRepo := &MockTableRepository{}

	service := NewCalculationService(logger, fieldRepo, recordRepo, tableRepo)

	assert.NotNil(t, service)
	assert.Equal(t, logger, service.logger)
	assert.Equal(t, fieldRepo, service.fieldRepo)
	assert.Equal(t, recordRepo, service.recordRepo)
	assert.Equal(t, tableRepo, service.tableRepo)
}

func TestCalculationService_CalculateFieldForRecords(t *testing.T) {
	logger := zap.NewNop()
	fieldRepo := &MockFieldRepository{}
	recordRepo := &MockRecordRepository{}
	tableRepo := &MockTableRepository{}

	service := NewCalculationService(logger, fieldRepo, recordRepo, tableRepo)

	ctx := context.Background()
	tableID := "test-table-id"
	fieldID := "test-field-id"
	recordIDs := []string{"record-1", "record-2"}

	// 设置模拟期望
	fieldRepo.On("FindByID", ctx, fieldID).Return(createTestField(), nil)
	fieldRepo.On("FindByTableID", ctx, tableID).Return([]*entity.Field{createTestField()}, nil)
	recordRepo.On("FindByIDs", ctx, tableID, mock.AnythingOfType("[]valueobject.RecordID")).Return([]*recordEntity.Record{}, nil)

	// 执行测试
	err := service.CalculateFieldForRecords(ctx, tableID, fieldID, recordIDs)

	// 验证结果
	assert.NoError(t, err)
	fieldRepo.AssertExpectations(t)
	recordRepo.AssertExpectations(t)
}

func TestCalculationService_RecalculateFieldForRecords(t *testing.T) {
	logger := zap.NewNop()
	fieldRepo := &MockFieldRepository{}
	recordRepo := &MockRecordRepository{}
	tableRepo := &MockTableRepository{}

	service := NewCalculationService(logger, fieldRepo, recordRepo, tableRepo)

	ctx := context.Background()
	tableID := "test-table-id"
	fieldID := "test-field-id"
	recordIDs := []string{"record-1", "record-2"}

	// 设置模拟期望
	fieldRepo.On("FindByID", ctx, fieldID).Return(createTestField(), nil)
	fieldRepo.On("FindByTableID", ctx, tableID).Return([]*entity.Field{createTestField()}, nil)
	recordRepo.On("FindByIDs", ctx, tableID, mock.AnythingOfType("[]valueobject.RecordID")).Return([]*recordEntity.Record{}, nil)

	// 执行测试
	err := service.RecalculateFieldForRecords(ctx, tableID, fieldID, recordIDs)

	// 验证结果
	assert.NoError(t, err)
	fieldRepo.AssertExpectations(t)
	recordRepo.AssertExpectations(t)
}

// 辅助函数
func createTestField() *entity.Field {
	fieldID := "test-field-id"
	tableID := "test-table-id"
	name := "测试字段"
	fieldType := entity.FieldTypeText
	options := entity.FieldOptions{}
	createdBy := "test-user"

	field, _ := entity.NewField(fieldID, tableID, name, fieldType, options, createdBy)
	return field
}
