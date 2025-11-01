package dto

import "time"

// CreateBaseRequest 创建Base请求（对齐原版）
type CreateBaseRequest struct {
	Name    string `json:"name" binding:"required,max=100"`
	Icon    string `json:"icon,omitempty"`
	SpaceID string `json:"spaceId"` // 从URL路径参数获取，不需要required验证
}

// UpdateBaseRequest 更新Base请求（对齐原版）
type UpdateBaseRequest struct {
	Name string `json:"name,omitempty" binding:"omitempty,max=100"`
	Icon string `json:"icon,omitempty"`
}

// DuplicateBaseRequest 复制Base请求
type DuplicateBaseRequest struct {
	Name        string `json:"name" binding:"required,max=100"`
	Description string `json:"description,omitempty"`
}

// BaseResponse Base响应（对齐原版）
type BaseResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Icon      string    `json:"icon,omitempty"`
	SpaceID   string    `json:"spaceId"`
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// BaseWithTablesResponse Base带Table列表响应
type BaseWithTablesResponse struct {
	BaseResponse
	Tables []*TableResponse `json:"tables,omitempty"`
}
