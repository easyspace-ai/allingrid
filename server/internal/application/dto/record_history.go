package dto

import "time"

// RecordHistoryResponse 记录历史响应
type RecordHistoryResponse struct {
	ID          string      `json:"id"`
	TableID     string      `json:"table_id"`
	RecordID    string      `json:"record_id"`
	FieldID     string      `json:"field_id"`
	Before      interface{} `json:"before"`
	After       interface{} `json:"after"`
	CreatedTime time.Time   `json:"created_time"`
	CreatedBy   string      `json:"created_by"`
}
