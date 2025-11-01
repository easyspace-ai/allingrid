package dto

import "time"

// PaginationRequest 分页请求
type PaginationRequest struct {
	Page     int `json:"page" form:"page"`
	PageSize int `json:"pageSize" form:"pageSize"` // ✅ form 标签也统一使用 camelCase
	Offset   int `json:"offset" form:"offset"`
	Limit    int `json:"limit" form:"limit"`
}

// PaginationResponse 分页响应
type PaginationResponse struct {
	Total       int64 `json:"total"`
	Page        int   `json:"page"`
	PageSize    int   `json:"pageSize"`
	TotalPages  int   `json:"totalPages"`
	HasNext     bool  `json:"hasNext"`
	HasPrevious bool  `json:"hasPrevious"`
}

// TimeRange 时间范围
type TimeRange struct {
	StartTime *time.Time `json:"startTime"`
	EndTime   *time.Time `json:"endTime"`
}

// SortOrder 排序顺序
type SortOrder string

const (
	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"
)

// OrderBy 排序字段
type OrderBy struct {
	Field string    `json:"field"`
	Order SortOrder `json:"order"`
}
