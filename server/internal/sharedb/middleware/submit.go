package middleware

import (
	"context"
	"errors"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
	"go.uber.org/zap"
)

// SubmitMiddleware 提交中间件
type SubmitMiddleware struct {
	logger *zap.Logger
}

// NewSubmitMiddleware 创建提交中间件
func NewSubmitMiddleware(logger *zap.Logger) sharedb.Middleware {
	return &SubmitMiddleware{
		logger: logger,
	}
}

// Handle 处理消息
func (m *SubmitMiddleware) Handle(ctx context.Context, conn *sharedb.Connection, msg *sharedb.Message) error {
	// 只允许 record 类型的操作提交
	if msg.Action == "op" && msg.Collection != "" {
		// 解析集合类型
		collectionInfo := parseCollection(msg.Collection)

		// 只允许 record 类型的操作
		if collectionInfo.Type != sharedb.DocumentTypeRecord {
			m.logger.Warn("Operation rejected: only record operations are allowed",
				zap.String("connection_id", conn.ID),
				zap.String("collection", msg.Collection),
				zap.String("doc_type", string(collectionInfo.Type)))
			return errors.New("only record op can be committed")
		}
	}

	// 记录操作日志
	m.logger.Debug("Operation validated",
		zap.String("connection_id", conn.ID),
		zap.String("action", msg.Action),
		zap.String("collection", msg.Collection),
		zap.String("doc_id", msg.DocID))

	return nil
}

// parseCollection 解析集合名称
func parseCollection(collection string) *sharedb.CollectionInfo {
	parts := strings.Split(collection, "_")
	if len(parts) < 2 {
		return &sharedb.CollectionInfo{
			Type:       sharedb.DocumentTypeRecord,
			TableID:    "default",
			DocumentID: collection,
		}
	}

	// 处理 "rec_" 前缀的集合名称
	docType := sharedb.DocumentType(parts[0])
	if string(docType) == "rec" {
		docType = sharedb.DocumentTypeRecord
	}

	tableID := parts[1]

	return &sharedb.CollectionInfo{
		Type:       docType,
		TableID:    tableID,
		DocumentID: collection,
	}
}
