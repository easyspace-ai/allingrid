package queries

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
)

// Query 查询接口
type Query interface {
	QueryType() string
	Validate() error
}

// QueryHandler 查询处理器接口
type QueryHandler interface {
	Handle(ctx context.Context, query Query) (interface{}, error)
	QueryType() string
}

// GetRecordQuery 获取记录查询
type GetRecordQuery struct {
	TableID  string               `json:"table_id" validate:"required"`
	RecordID valueobject.RecordID `json:"record_id" validate:"required"`
}

func (q *GetRecordQuery) QueryType() string {
	return "get_record"
}

func (q *GetRecordQuery) Validate() error {
	if q.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if q.RecordID.String() == "" {
		return fmt.Errorf("record_id is required")
	}
	return nil
}

// ListRecordsQuery 列出记录查询
type ListRecordsQuery struct {
	TableID string                 `json:"table_id" validate:"required"`
	Limit   int                    `json:"limit"`
	Offset  int                    `json:"offset"`
	Filter  map[string]interface{} `json:"filter"`
	Sort    []SortField            `json:"sort"`
}

type SortField struct {
	Field string `json:"field"`
	Order string `json:"order"` // "asc" or "desc"
}

func (q *ListRecordsQuery) QueryType() string {
	return "list_records"
}

func (q *ListRecordsQuery) Validate() error {
	if q.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if q.Limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if q.Offset < 0 {
		return fmt.Errorf("offset must be non-negative")
	}
	return nil
}

// SearchRecordsQuery 搜索记录查询
type SearchRecordsQuery struct {
	TableID string   `json:"table_id" validate:"required"`
	Query   string   `json:"query" validate:"required"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
	Fields  []string `json:"fields"`
}

func (q *SearchRecordsQuery) QueryType() string {
	return "search_records"
}

func (q *SearchRecordsQuery) Validate() error {
	if q.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if q.Query == "" {
		return fmt.Errorf("query is required")
	}
	if q.Limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if q.Offset < 0 {
		return fmt.Errorf("offset must be non-negative")
	}
	return nil
}

// GetRecordsByIDsQuery 根据ID列表获取记录查询
type GetRecordsByIDsQuery struct {
	TableID   string                 `json:"table_id" validate:"required"`
	RecordIDs []valueobject.RecordID `json:"record_ids" validate:"required"`
}

func (q *GetRecordsByIDsQuery) QueryType() string {
	return "get_records_by_ids"
}

func (q *GetRecordsByIDsQuery) Validate() error {
	if q.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if len(q.RecordIDs) == 0 {
		return fmt.Errorf("record_ids is required")
	}
	return nil
}

// CountRecordsQuery 统计记录数量查询
type CountRecordsQuery struct {
	TableID string                 `json:"table_id" validate:"required"`
	Filter  map[string]interface{} `json:"filter"`
}

func (q *CountRecordsQuery) QueryType() string {
	return "count_records"
}

func (q *CountRecordsQuery) Validate() error {
	if q.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	return nil
}

// GetRecordHistoryQuery 获取记录历史查询
type GetRecordHistoryQuery struct {
	TableID  string               `json:"table_id" validate:"required"`
	RecordID valueobject.RecordID `json:"record_id" validate:"required"`
	Limit    int                  `json:"limit"`
	Offset   int                  `json:"offset"`
}

func (q *GetRecordHistoryQuery) QueryType() string {
	return "get_record_history"
}

func (q *GetRecordHistoryQuery) Validate() error {
	if q.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if q.RecordID.String() == "" {
		return fmt.Errorf("record_id is required")
	}
	if q.Limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if q.Offset < 0 {
		return fmt.Errorf("offset must be non-negative")
	}
	return nil
}
