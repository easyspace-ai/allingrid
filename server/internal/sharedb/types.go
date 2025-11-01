package sharedb

import (
	"context"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// OpType 操作类型
type OpType string

const (
	OpTypeCreate OpType = "create"
	OpTypeEdit   OpType = "edit"
	OpTypeDelete OpType = "delete"
)

// OTOperation JSON0 操作类型
type OTOperation map[string]interface{}

// CreateData 创建操作数据
type CreateData struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// EditOp 编辑操作（对齐 Teable）
type EditOp struct {
	C   string        `json:"c"`             // collection
	D   string        `json:"d"`             // docId
	V   int64         `json:"v"`             // version
	Op  []OTOperation `json:"op"`            // operations
	Src string        `json:"src,omitempty"` // source
	Seq int           `json:"seq,omitempty"` // sequence
}

// CreateOp 创建操作
type CreateOp struct {
	C      string      `json:"c"`
	D      string      `json:"d"`
	V      int64       `json:"v"`
	Create *CreateData `json:"create"`
	Src    string      `json:"src,omitempty"`
	Seq    int         `json:"seq,omitempty"`
}

// DeleteOp 删除操作
type DeleteOp struct {
	C   string `json:"c"`
	D   string `json:"d"`
	V   int64  `json:"v"`
	Del bool   `json:"del"`
	Src string `json:"src,omitempty"`
	Seq int    `json:"seq,omitempty"`
}

// Snapshot 文档快照
type Snapshot struct {
	ID      string      `json:"id"`
	Type    string      `json:"type"`
	Version int64       `json:"v"`
	Data    interface{} `json:"data"`
	Meta    interface{} `json:"m,omitempty"`
}

// Message ShareDB 协议消息
type Message struct {
	Action     string               `json:"a"`
	Collection string               `json:"c,omitempty"`
	DocID      string               `json:"d,omitempty"`
	Version    int64                `json:"v,omitempty"`
	Op         []OTOperation        `json:"op,omitempty"`
	Create     *CreateData          `json:"create,omitempty"`
	Data       interface{}          `json:"data,omitempty"`
	Error      *errors.ShareDBError `json:"error,omitempty"`
	// Presence 相关
	Presence map[string]interface{} `json:"presence,omitempty"`
	// 握手相关字段
	Protocol int         `json:"protocol,omitempty"`
	Type     string      `json:"type,omitempty"`
	ID       interface{} `json:"id,omitempty"`
}

// Operation ShareDB 操作（通用操作类型）
type Operation struct {
	Type       OpType        `json:"type"`   // 操作类型
	Op         []OTOperation `json:"op"`     // OT 操作列表
	Version    int64         `json:"v"`      // 版本号
	Source     string        `json:"src"`    // 操作来源
	Collection string        `json:"c"`      // 集合名
	DocID      string        `json:"d"`      // 文档ID
	Create     *CreateData   `json:"create"` // 创建数据（仅创建操作）
	Del        bool          `json:"del"`    // 删除标记（仅删除操作）
	Seq        int           `json:"seq"`    // 序列号
}

// Error ShareDB 错误
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Connection 连接信息
type Connection struct {
	ID            string
	UserID        string
	LastSeen      time.Time
	IsActive      bool
	CreatedAt     time.Time
	mu            sync.RWMutex
	subCancelFuncs map[string]context.CancelFunc // channel -> cancelFunc
}

// PresenceData 在线状态数据
type PresenceData struct {
	UserID    string                 `json:"userId"`
	Data      map[string]interface{} `json:"data"`
	Timestamp int64                  `json:"timestamp"`
}

// QueryOptions 查询选项
type QueryOptions struct {
	Projection map[string]bool `json:"projection,omitempty"`
	Limit      int             `json:"limit,omitempty"`
	Skip       int             `json:"skip,omitempty"`
	Sort       interface{}     `json:"sort,omitempty"`
}

// DocumentType 文档类型
type DocumentType string

const (
	DocumentTypeRecord DocumentType = "record"
	DocumentTypeField  DocumentType = "field"
	DocumentTypeView   DocumentType = "view"
	DocumentTypeTable  DocumentType = "table"
)

// CollectionInfo 集合信息
type CollectionInfo struct {
	Type       DocumentType
	TableID    string
	DocumentID string
}
