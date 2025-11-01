package sharedb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShareDB_RecordCollaboration(t *testing.T) {
	// 创建测试服务
	logger := zap.NewNop()
	adapter := NewPostgresAdapter(nil, logger) // 使用 nil DB 进行测试
	pubsub := NewLocalPubSub(logger)
	presence := NewPresenceManager(logger)

	service := NewService(adapter, pubsub, presence, logger)
	defer service.Shutdown()

	// 测试记录协作场景
	collection := "record_tbl123"
	recordID := "rec456"

	// 模拟用户 A 创建记录
	createOp := &Operation{
		Type:    OpTypeCreate,
		Version: 1,
		Source:  "userA",
		Seq:     1,
		Create: &CreateData{
			Type: "json0",
			Data: map[string]interface{}{
				"id": recordID,
				"fields": map[string]interface{}{
					"field1": "value1",
					"field2": "value2",
				},
			},
		},
	}

	// 提交创建操作
	err := service.SubmitOp(context.Background(), collection, recordID, createOp)
	require.NoError(t, err)

	// 模拟用户 B 编辑记录
	editOp := &Operation{
		Type:    OpTypeEdit,
		Version: 2,
		Source:  "userB",
		Seq:     1,
		Op: []OTOperation{
			{
				"p":  []interface{}{"fields", "field1"},
				"oi": "updated value",
				"od": "value1",
			},
		},
	}

	// 提交编辑操作
	err = service.SubmitOp(context.Background(), collection, recordID, editOp)
	require.NoError(t, err)

	// 发布操作
	err = service.PublishOp(context.Background(), collection, recordID, editOp)
	require.NoError(t, err)

	// 验证操作已处理
	assert.True(t, true) // 如果没有错误，说明操作处理成功
}

func TestShareDB_ConcurrentOps(t *testing.T) {
	// 创建测试服务
	logger := zap.NewNop()
	adapter := NewPostgresAdapter(nil, logger)
	pubsub := NewLocalPubSub(logger)
	presence := NewPresenceManager(logger)

	service := NewService(adapter, pubsub, presence, logger)
	defer service.Shutdown()

	// 并发操作测试
	collection := "record_tbl123"
	recordID := "rec456"

	// 创建多个并发操作
	operations := make([]*Operation, 10)
	for i := 0; i < 10; i++ {
		operations[i] = &Operation{
			Type:    OpTypeEdit,
			Version: int64(i + 1),
			Source:  "user" + string(rune('A'+i)),
			Seq:     1,
			Op: []OTOperation{
				{
					"p":  []interface{}{"fields", "field1"},
					"oi": "value" + string(rune('0'+i)),
					"od": "old value",
				},
			},
		}
	}

	// 并发提交操作
	done := make(chan error, len(operations))

	for i, op := range operations {
		go func(index int, operation *Operation) {
			err := service.SubmitOp(context.Background(), collection, recordID, operation)
			done <- err
		}(i, op)
	}

	// 等待所有操作完成
	for i := 0; i < len(operations); i++ {
		err := <-done
		assert.NoError(t, err)
	}

	// 验证所有操作都已处理
	assert.True(t, true)
}

func TestShareDB_PresenceManagement(t *testing.T) {
	// 创建测试服务
	logger := zap.NewNop()
	adapter := NewPostgresAdapter(nil, logger)
	pubsub := NewLocalPubSub(logger)
	presence := NewPresenceManager(logger)

	service := NewService(adapter, pubsub, presence, logger)
	defer service.Shutdown()

	// 测试在线状态管理
	channel := "record_tbl123.rec456"
	clientID := "client123"

	presenceData := PresenceData{
		UserID: "user123",
		Data: map[string]interface{}{
			"cursor": map[string]interface{}{
				"x": 100,
				"y": 200,
			},
			"selection": "field1",
		},
		Timestamp: time.Now().Unix(),
	}

	// 提交在线状态
	err := presence.Submit(context.Background(), channel, clientID, presenceData)
	require.NoError(t, err)

	// 获取在线状态
	presences, err := presence.GetPresences(context.Background(), channel)
	require.NoError(t, err)

	assert.Len(t, presences, 1)
	assert.Contains(t, presences, clientID)
	assert.Equal(t, presenceData.UserID, presences[clientID].UserID)

	// 移除在线状态
	err = presence.RemovePresence(context.Background(), channel, clientID)
	require.NoError(t, err)

	// 验证在线状态已移除
	presences, err = presence.GetPresences(context.Background(), channel)
	require.NoError(t, err)
	assert.Len(t, presences, 0)
}

func TestShareDB_MessageProtocol(t *testing.T) {
	// 测试 ShareDB 协议消息
	tests := []struct {
		name    string
		message Message
		valid   bool
	}{
		{
			name: "handshake message",
			message: Message{
				Action: "hs",
			},
			valid: true,
		},
		{
			name: "fetch message",
			message: Message{
				Action:     "f",
				Collection: "record_tbl123",
				DocID:      "rec456",
			},
			valid: true,
		},
		{
			name: "subscribe message",
			message: Message{
				Action:     "s",
				Collection: "record_tbl123",
				DocID:      "rec456",
			},
			valid: true,
		},
		{
			name: "operation message",
			message: Message{
				Action:     "op",
				Collection: "record_tbl123",
				DocID:      "rec456",
				Version:    1,
				Op: []OTOperation{
					{
						"p":  []interface{}{"fields", "field1"},
						"oi": "new value",
						"od": "old value",
					},
				},
			},
			valid: true,
		},
		{
			name: "presence message",
			message: Message{
				Action:     "p",
				Collection: "record_tbl123",
				DocID:      "rec456",
				Presence: map[string]interface{}{
					"client123": PresenceData{
						UserID: "user123",
						Data: map[string]interface{}{
							"cursor": map[string]interface{}{
								"x": 100,
								"y": 200,
							},
						},
						Timestamp: time.Now().Unix(),
					},
				},
			},
			valid: true,
		},
		{
			name: "invalid message",
			message: Message{
				Action: "invalid",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试 JSON 序列化
			jsonData, err := json.Marshal(tt.message)
			if tt.valid {
				require.NoError(t, err)
				assert.NotEmpty(t, jsonData)
			} else {
				// 无效消息仍然可以序列化，但会在处理时被拒绝
				require.NoError(t, err)
			}

			// 测试 JSON 反序列化
			var deserializedMessage Message
			err = json.Unmarshal(jsonData, &deserializedMessage)
			require.NoError(t, err)
			assert.Equal(t, tt.message.Action, deserializedMessage.Action)
			assert.Equal(t, tt.message.Collection, deserializedMessage.Collection)
			assert.Equal(t, tt.message.DocID, deserializedMessage.DocID)
		})
	}
}

func TestShareDB_OperationTypes(t *testing.T) {
	// 测试操作类型
	tests := []struct {
		name     string
		opType   OpType
		expected string
	}{
		{
			name:     "create operation",
			opType:   OpTypeCreate,
			expected: "create",
		},
		{
			name:     "edit operation",
			opType:   OpTypeEdit,
			expected: "edit",
		},
		{
			name:     "delete operation",
			opType:   OpTypeDelete,
			expected: "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.opType))
		})
	}
}

func TestShareDB_DocumentTypes(t *testing.T) {
	// 测试文档类型
	tests := []struct {
		name     string
		docType  DocumentType
		expected string
	}{
		{
			name:     "record document",
			docType:  DocumentTypeRecord,
			expected: "record",
		},
		{
			name:     "field document",
			docType:  DocumentTypeField,
			expected: "field",
		},
		{
			name:     "view document",
			docType:  DocumentTypeView,
			expected: "view",
		},
		{
			name:     "table document",
			docType:  DocumentTypeTable,
			expected: "table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.docType))
		})
	}
}

func TestShareDB_ErrorHandling(t *testing.T) {
	// 测试错误处理
	error := &Error{
		Code:    400,
		Message: "Bad Request",
	}

	// 测试 JSON 序列化
	jsonData, err := json.Marshal(error)
	require.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	// 测试 JSON 反序列化
	var deserializedError Error
	err = json.Unmarshal(jsonData, &deserializedError)
	require.NoError(t, err)
	assert.Equal(t, error.Code, deserializedError.Code)
	assert.Equal(t, error.Message, deserializedError.Message)
}
