package sharedb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// MockAdapter 模拟适配器
type MockAdapter struct {
	mock.Mock
}

func (m *MockAdapter) Query(ctx context.Context, collection string, query interface{}, projection map[string]bool) ([]string, error) {
	args := m.Called(ctx, collection, query, projection)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockAdapter) GetSnapshot(ctx context.Context, collection string, id string, projection map[string]bool) (*Snapshot, error) {
	args := m.Called(ctx, collection, id, projection)
	return args.Get(0).(*Snapshot), args.Error(1)
}

func (m *MockAdapter) GetSnapshotBulk(ctx context.Context, collection string, ids []string, projection map[string]bool) (map[string]*Snapshot, error) {
	args := m.Called(ctx, collection, ids, projection)
	return args.Get(0).(map[string]*Snapshot), args.Error(1)
}

func (m *MockAdapter) GetOps(ctx context.Context, collection string, id string, from, to int64) ([]*Operation, error) {
	args := m.Called(ctx, collection, id, from, to)
	return args.Get(0).([]*Operation), args.Error(1)
}

func (m *MockAdapter) SkipPoll(ctx context.Context, collection string, id string, op *Operation, query interface{}) bool {
	args := m.Called(ctx, collection, id, op, query)
	return args.Bool(0)
}

func (m *MockAdapter) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockPubSub 模拟发布订阅
type MockPubSub struct {
	mock.Mock
}

func (m *MockPubSub) Publish(ctx context.Context, channels []string, op *Operation) error {
	args := m.Called(ctx, channels, op)
	return args.Error(0)
}

func (m *MockPubSub) Subscribe(ctx context.Context, channel string, handler func(*Operation)) error {
	args := m.Called(ctx, channel, handler)
	return args.Error(0)
}

func (m *MockPubSub) Unsubscribe(ctx context.Context, channel string) error {
	args := m.Called(ctx, channel)
	return args.Error(0)
}

func (m *MockPubSub) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockPresenceManager 模拟在线状态管理器
type MockPresenceManager struct {
	mock.Mock
}

func (m *MockPresenceManager) Submit(ctx context.Context, channel, clientID string, data PresenceData) error {
	args := m.Called(ctx, channel, clientID, data)
	return args.Error(0)
}

func (m *MockPresenceManager) GetPresences(ctx context.Context, channel string) (map[string]PresenceData, error) {
	args := m.Called(ctx, channel)
	return args.Get(0).(map[string]PresenceData), args.Error(1)
}

func (m *MockPresenceManager) RemovePresence(ctx context.Context, channel, clientID string) error {
	args := m.Called(ctx, channel, clientID)
	return args.Error(0)
}

func (m *MockPresenceManager) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestShareDBService_SubmitOp(t *testing.T) {
	// 创建模拟对象
	mockAdapter := &MockAdapter{}
	mockPubSub := &MockPubSub{}
	mockPresence := &MockPresenceManager{}

	// 创建服务
	logger := zap.NewNop()
	service := NewService(mockAdapter, mockPubSub, mockPresence, logger)

	// 测试数据
	collection := "record_tbl123"
	docID := "rec456"
	op := &Operation{
		Type:    OpTypeEdit,
		Version: 1,
		Source:  "client123",
		Op: []OTOperation{
			{
				"p":  []interface{}{"fields", "field123"},
				"oi": "new value",
				"od": "old value",
			},
		},
	}

	// 设置模拟期望
	mockPubSub.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// 执行测试
	err := service.SubmitOp(context.Background(), collection, docID, op)

	// 验证结果
	assert.NoError(t, err)
	mockPubSub.AssertExpectations(t)
}

func TestShareDBService_PublishOp(t *testing.T) {
	// 创建模拟对象
	mockAdapter := &MockAdapter{}
	mockPubSub := &MockPubSub{}
	mockPresence := &MockPresenceManager{}

	// 创建服务
	logger := zap.NewNop()
	service := NewService(mockAdapter, mockPubSub, mockPresence, logger)

	// 测试数据
	collection := "record_tbl123"
	docID := "rec456"
	op := &Operation{
		Type:    OpTypeEdit,
		Version: 1,
		Source:  "client123",
	}

	// 设置模拟期望
	expectedChannels := []string{collection, collection + "." + docID}
	mockPubSub.On("Publish", mock.Anything, expectedChannels, op).Return(nil)

	// 执行测试
	err := service.PublishOp(context.Background(), collection, docID, op)

	// 验证结果
	assert.NoError(t, err)
	mockPubSub.AssertExpectations(t)
}

func TestShareDBService_GetStats(t *testing.T) {
	// 创建模拟对象
	mockAdapter := &MockAdapter{}
	mockPubSub := &MockPubSub{}
	mockPresence := &MockPresenceManager{}

	// 创建服务
	logger := zap.NewNop()
	service := NewService(mockAdapter, mockPubSub, mockPresence, logger)

	// 执行测试
	stats := service.GetStats()

	// 验证结果
	assert.NotNil(t, stats)
	assert.Contains(t, stats, "connections")
	assert.Contains(t, stats, "documents")
	assert.Contains(t, stats, "timestamp")
}

func TestShareDBService_Shutdown(t *testing.T) {
	// 创建模拟对象
	mockAdapter := &MockAdapter{}
	mockPubSub := &MockPubSub{}
	mockPresence := &MockPresenceManager{}

	// 创建服务
	logger := zap.NewNop()
	service := NewService(mockAdapter, mockPubSub, mockPresence, logger)

	// 设置模拟期望
	mockAdapter.On("Close").Return(nil)
	mockPubSub.On("Close").Return(nil)
	mockPresence.On("Close").Return(nil)

	// 执行测试
	err := service.Shutdown()

	// 验证结果
	assert.NoError(t, err)
	mockAdapter.AssertExpectations(t)
	mockPubSub.AssertExpectations(t)
	mockPresence.AssertExpectations(t)
}

func TestShareDBService_AddMiddleware(t *testing.T) {
	// 创建模拟对象
	mockAdapter := &MockAdapter{}
	mockPubSub := &MockPubSub{}
	mockPresence := &MockPresenceManager{}

	// 创建服务
	logger := zap.NewNop()
	service := NewService(mockAdapter, mockPubSub, mockPresence, logger)

	// 创建模拟中间件
	mockMiddleware := &MockMiddleware{}
	service.AddMiddleware(mockMiddleware)

	// 验证中间件已添加
	assert.Len(t, service.middleware, 1)
}

// MockMiddleware 模拟中间件
type MockMiddleware struct {
	mock.Mock
}

func (m *MockMiddleware) Handle(ctx context.Context, conn *Connection, msg *Message) error {
	args := m.Called(ctx, conn, msg)
	return args.Error(0)
}

func TestParseCollection(t *testing.T) {
	tests := []struct {
		name       string
		collection string
		expected   *CollectionInfo
	}{
		{
			name:       "record collection",
			collection: "record_tbl123",
			expected: &CollectionInfo{
				Type:       DocumentTypeRecord,
				TableID:    "tbl123",
				DocumentID: "record_tbl123",
			},
		},
		{
			name:       "field collection",
			collection: "field_tbl456",
			expected: &CollectionInfo{
				Type:       DocumentTypeField,
				TableID:    "tbl456",
				DocumentID: "field_tbl456",
			},
		},
		{
			name:       "view collection",
			collection: "view_tbl789",
			expected: &CollectionInfo{
				Type:       DocumentTypeView,
				TableID:    "tbl789",
				DocumentID: "view_tbl789",
			},
		},
		{
			name:       "table collection",
			collection: "table_tbl000",
			expected: &CollectionInfo{
				Type:       DocumentTypeTable,
				TableID:    "tbl000",
				DocumentID: "table_tbl000",
			},
		},
		{
			name:       "invalid collection",
			collection: "invalid",
			expected: &CollectionInfo{
				Type:       DocumentTypeRecord,
				TableID:    "default",
				DocumentID: "invalid",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseCollection(tt.collection)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOperation_JSONSerialization(t *testing.T) {
	op := &Operation{
		Type:    OpTypeEdit,
		Version: 1,
		Source:  "client123",
		Seq:     1,
		Op: []OTOperation{
			{
				"p":  []interface{}{"fields", "field123"},
				"oi": "new value",
				"od": "old value",
			},
		},
	}

	// 测试 JSON 序列化
	jsonData, err := json.Marshal(op)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	// 测试 JSON 反序列化
	var deserializedOp Operation
	err = json.Unmarshal(jsonData, &deserializedOp)
	assert.NoError(t, err)
	assert.Equal(t, op.Type, deserializedOp.Type)
	assert.Equal(t, op.Version, deserializedOp.Version)
	assert.Equal(t, op.Source, deserializedOp.Source)
	assert.Equal(t, op.Seq, deserializedOp.Seq)
}

func TestSnapshot_JSONSerialization(t *testing.T) {
	snapshot := &Snapshot{
		ID:      "doc123",
		Type:    "json0",
		Version: 1,
		Data: map[string]interface{}{
			"id":     "doc123",
			"fields": map[string]interface{}{},
		},
	}

	// 测试 JSON 序列化
	jsonData, err := json.Marshal(snapshot)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	// 测试 JSON 反序列化
	var deserializedSnapshot Snapshot
	err = json.Unmarshal(jsonData, &deserializedSnapshot)
	assert.NoError(t, err)
	assert.Equal(t, snapshot.ID, deserializedSnapshot.ID)
	assert.Equal(t, snapshot.Type, deserializedSnapshot.Type)
	assert.Equal(t, snapshot.Version, deserializedSnapshot.Version)
}
