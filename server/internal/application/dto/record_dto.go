package dto

import (
	"time"

	recordEntity "github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
)

// CreateRecordRequest 创建记录请求
type CreateRecordRequest struct {
	TableID string                 `json:"tableId" binding:"required"` // ✅ 统一使用 camelCase
	Data    map[string]interface{} `json:"data" binding:"required"`
}

// UpdateRecordRequest 更新记录请求
type UpdateRecordRequest struct {
	// Teable 格式支持
	FieldKeyType string            `json:"fieldKeyType,omitempty"` // "id" or "name"
	Record       *UpdateRecordData `json:"record,omitempty"`       // 嵌套的 record 对象

	// 兼容格式
	Data    map[string]interface{} `json:"data,omitempty"`    // 直接的数据字段
	Version *int                   `json:"version,omitempty"` // 可选的版本号，用于乐观锁
}

// UpdateRecordData 更新记录数据（Teable 格式）
type UpdateRecordData struct {
	Fields map[string]interface{} `json:"fields" binding:"required"`
}

// BatchCreateRecordRequest 批量创建记录请求（对齐原版）
type BatchCreateRecordRequest struct {
	Records []RecordCreateItem `json:"records" binding:"required,max=1000"` // ✅ 移除 min=1，允许空数组
}

// RecordCreateItem 单条记录创建项
type RecordCreateItem struct {
	Fields map[string]interface{} `json:"fields" binding:"required"`
}

// BatchCreateRecordResponse 批量创建记录响应
type BatchCreateRecordResponse struct {
	Records      []*RecordResponse `json:"records"`
	SuccessCount int               `json:"successCount"`
	FailedCount  int               `json:"failedCount"`
	Errors       []string          `json:"errors,omitempty"`
}

// BatchUpdateRecordRequest 批量更新记录请求
type BatchUpdateRecordRequest struct {
	Records []RecordUpdateItem `json:"records" binding:"required,min=1,max=1000"`
}

// RecordUpdateItem 单条记录更新项
type RecordUpdateItem struct {
	ID     string                 `json:"id" binding:"required"`
	Fields map[string]interface{} `json:"fields" binding:"required"`
}

// BatchUpdateRecordResponse 批量更新记录响应
type BatchUpdateRecordResponse struct {
	Records      []*RecordResponse `json:"records"`
	SuccessCount int               `json:"successCount"`
	FailedCount  int               `json:"failedCount"`
	Errors       []string          `json:"errors,omitempty"`
}

// BatchDeleteRecordRequest 批量删除记录请求
type BatchDeleteRecordRequest struct {
	RecordIDs []string `json:"recordIds" binding:"required,min=1,max=1000"`
}

// BatchDeleteRecordResponse 批量删除记录响应
type BatchDeleteRecordResponse struct {
	SuccessCount int      `json:"successCount"`
	FailedCount  int      `json:"failedCount"`
	Errors       []string `json:"errors,omitempty"`
}

// ListRecordFilter 记录列表过滤器
type ListRecordFilter struct {
	TableID   *string                `json:"tableId"`
	Filters   map[string]interface{} `json:"filters"`
	OrderBy   string                 `json:"orderBy"`
	Offset    int                    `json:"offset"`
	Limit     int                    `json:"limit"`
	CreatedAt *time.Time             `json:"createdAt"`
}

// RecordResponse 记录响应
type RecordResponse struct {
	ID        string                 `json:"id"`
	TableID   string                 `json:"tableId"`
	Data      map[string]interface{} `json:"data"`
	CreatedBy string                 `json:"createdBy"`
	UpdatedBy string                 `json:"updatedBy"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
	Version   int                    `json:"version"`
}

// RecordListResponse 记录列表响应
type RecordListResponse struct {
	Records    []*RecordResponse   `json:"records"`
	Pagination *PaginationResponse `json:"pagination"`
}

// FromRecordEntity 从Domain实体转换为DTO
func FromRecordEntity(record *recordEntity.Record) *RecordResponse {
	if record == nil {
		return nil
	}

	dataMap := record.Data().ToMap()

	return &RecordResponse{
		ID:        record.ID().String(),
		TableID:   record.TableID(),
		Data:      dataMap,
		CreatedBy: record.CreatedBy(),
		UpdatedBy: record.UpdatedBy(),
		CreatedAt: record.CreatedAt(),
		UpdatedAt: record.UpdatedAt(),
		Version:   int(record.Version().Value()), // 从 RecordVersion 值对象获取实际版本号（转换为 int）
	}
}

// FromRecordEntities 批量转换
func FromRecordEntities(records []*recordEntity.Record) []*RecordResponse {
	result := make([]*RecordResponse, len(records))
	for i, record := range records {
		result[i] = FromRecordEntity(record)
	}
	return result
}
