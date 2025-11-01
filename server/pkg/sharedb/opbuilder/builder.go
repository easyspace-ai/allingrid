package opbuilder

// 全局操作构建器实例
var (
	// RecordOp 记录操作构建器
	RecordOp = &RecordOpBuilder{}

	// FieldOp 字段操作构建器
	FieldOp = &FieldOpBuilder{}

	// ViewOp 视图操作构建器
	ViewOp = &ViewOpBuilder{}

	// TableOp 表格操作构建器
	TableOp = &TableOpBuilder{}
)

// BatchOperation 批量操作
type BatchOperation struct {
	Operations []Operation `json:"operations"`
	Collection string      `json:"collection"`
	DocID      string      `json:"docId"`
	Version    int64       `json:"version"`
}

// NewBatchOperation 创建批量操作
func NewBatchOperation(collection, docID string, version int64) *BatchOperation {
	return &BatchOperation{
		Operations: make([]Operation, 0),
		Collection: collection,
		DocID:      docID,
		Version:    version,
	}
}

// AddOperation 添加操作到批量操作中
func (b *BatchOperation) AddOperation(op Operation) *BatchOperation {
	b.Operations = append(b.Operations, op)
	return b
}

// AddOperations 添加多个操作到批量操作中
func (b *BatchOperation) AddOperations(ops []Operation) *BatchOperation {
	b.Operations = append(b.Operations, ops...)
	return b
}

// GetOperations 获取所有操作
func (b *BatchOperation) GetOperations() []Operation {
	return b.Operations
}

// IsEmpty 检查批量操作是否为空
func (b *BatchOperation) IsEmpty() bool {
	return len(b.Operations) == 0
}

// Clear 清空所有操作
func (b *BatchOperation) Clear() *BatchOperation {
	b.Operations = make([]Operation, 0)
	return b
}

// Count 获取操作数量
func (b *BatchOperation) Count() int {
	return len(b.Operations)
}
