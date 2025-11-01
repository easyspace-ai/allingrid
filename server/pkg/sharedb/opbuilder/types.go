package opbuilder

// Operation ShareDB 操作结构
type Operation struct {
	Path     []interface{} `json:"p"`    // 操作路径
	OldValue interface{}   `json:"od"`   // 旧值
	NewValue interface{}   `json:"oi"`   // 新值
	Type     string        `json:"type"` // 操作类型
}

// OpType 操作类型常量
const (
	OpTypeSet    = "set"
	OpTypeInsert = "insert"
	OpTypeDelete = "delete"
	OpTypeMove   = "move"
)

// RecordOperation 记录操作
type RecordOperation struct {
	FieldID   string      `json:"fieldId"`
	OldValue  interface{} `json:"oldValue"`
	NewValue  interface{} `json:"newValue"`
	Operation Operation   `json:"operation"`
}

// FieldOperation 字段操作
type FieldOperation struct {
	Key       string      `json:"key"`
	OldValue  interface{} `json:"oldValue"`
	NewValue  interface{} `json:"newValue"`
	Operation Operation   `json:"operation"`
}

// ViewOperation 视图操作
type ViewOperation struct {
	Key       string      `json:"key"`
	OldValue  interface{} `json:"oldValue"`
	NewValue  interface{} `json:"newValue"`
	Operation Operation   `json:"operation"`
}

// TableOperation 表格操作
type TableOperation struct {
	Key       string      `json:"key"`
	OldValue  interface{} `json:"oldValue"`
	NewValue  interface{} `json:"newValue"`
	Operation Operation   `json:"operation"`
}
