package dto

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/entity"
	"time"
)

// CreateViewRequest 创建视图请求
type CreateViewRequest struct {
	TableID     string                   `json:"tableId"` // ✅ 统一使用 camelCase，从URL路径获取，不需要required验证
	Name        string                   `json:"name" binding:"required"`
	Description string                   `json:"description"`
	Type        string                   `json:"type" binding:"required"` // grid, kanban, gallery, form, calendar
	Filter      map[string]interface{}   `json:"filter"`
	Sort        []map[string]interface{} `json:"sort"`
	Group       []map[string]interface{} `json:"group"`
	ColumnMeta  []map[string]interface{} `json:"columnMeta"`
	Options     map[string]interface{}   `json:"options"`
}

// UpdateViewRequest 更新视图请求
type UpdateViewRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	IsLocked    *bool   `json:"isLocked"`
}

// UpdateViewFilterRequest 更新过滤器请求
type UpdateViewFilterRequest struct {
	Filter map[string]interface{} `json:"filter"`
}

// UpdateViewSortRequest 更新排序请求
type UpdateViewSortRequest struct {
	Sort []map[string]interface{} `json:"sort"`
}

// UpdateViewGroupRequest 更新分组请求
type UpdateViewGroupRequest struct {
	Group []map[string]interface{} `json:"group"`
}

// UpdateViewColumnMetaRequest 更新列配置请求
type UpdateViewColumnMetaRequest struct {
	ColumnMeta []map[string]interface{} `json:"columnMeta"`
}

// UpdateViewOptionsRequest 更新选项请求
type UpdateViewOptionsRequest struct {
	Options map[string]interface{} `json:"options"`
}

// PatchViewOptionsRequest 部分更新选项请求
type PatchViewOptionsRequest struct {
	Options map[string]interface{} `json:"options"`
}

// UpdateViewOrderRequest 更新排序位置请求
type UpdateViewOrderRequest struct {
	Order float64 `json:"order" binding:"required"`
}

// UpdateShareMetaRequest 更新分享元数据请求
type UpdateShareMetaRequest struct {
	ShareMeta map[string]interface{} `json:"shareMeta"`
}

// DuplicateViewRequest 复制视图请求
type DuplicateViewRequest struct {
	Name string `json:"name" binding:"required"`
}

// ViewResponse 视图响应
type ViewResponse struct {
	ID          string                 `json:"id"`
	TableID     string                 `json:"tableId"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Type        string                 `json:"type"`
	Filter      interface{}            `json:"filter,omitempty"`
	Sort        interface{}            `json:"sort,omitempty"`
	Group       interface{}            `json:"group,omitempty"`
	ColumnMeta  interface{}            `json:"columnMeta"`
	Options     map[string]interface{} `json:"options,omitempty"`
	Order       float64                `json:"order"`
	Version     int                    `json:"version"`
	IsLocked    bool                   `json:"isLocked"`
	EnableShare bool                   `json:"enableShare"`
	ShareID     *string                `json:"shareId,omitempty"`
	ShareMeta   interface{}            `json:"shareMeta,omitempty"`
	CreatedBy   string                 `json:"createdBy"`
	CreatedAt   time.Time              `json:"createdAt"`
	UpdatedAt   time.Time              `json:"updatedAt"`
}

// EnableShareResponse 启用分享响应
type EnableShareResponse struct {
	ShareID string `json:"shareId"`
}

// RefreshShareIDResponse 刷新分享ID响应
type RefreshShareIDResponse struct {
	ShareID string `json:"shareId"`
}

// ViewCountResponse 视图数量响应
type ViewCountResponse struct {
	Count int64 `json:"count"`
}

// FromViewEntity 从视图实体转换为DTO
func FromViewEntity(view *entity.View) *ViewResponse {
	if view == nil {
		return nil
	}

	response := &ViewResponse{
		ID:          view.ID(),
		TableID:     view.TableID(),
		Name:        view.Name(),
		Description: view.Description(),
		Type:        string(view.ViewType()),
		Order:       view.Order(),
		Version:     view.Version(),
		IsLocked:    view.IsLocked(),
		EnableShare: view.EnableShare(),
		ShareID:     view.ShareID(),
		CreatedBy:   view.CreatedBy(),
		CreatedAt:   view.CreatedAt(),
		UpdatedAt:   view.UpdatedAt(),
	}

	// 转换过滤器
	if filter := view.Filter(); filter != nil {
		response.Filter = filter.ToMap()
	}

	// 转换排序
	if sort := view.Sort(); sort != nil {
		response.Sort = sort.ToSlice()
	}

	// 转换分组
	if group := view.Group(); group != nil {
		response.Group = group.ToSlice()
	}

	// 转换列配置
	if columnMeta := view.ColumnMeta(); columnMeta != nil {
		response.ColumnMeta = columnMeta.ToSlice()
	}

	// 转换选项
	if options := view.Options(); options != nil && len(options) > 0 {
		response.Options = options
	}

	// 转换分享元数据
	if shareMeta := view.ShareMeta(); shareMeta != nil && len(shareMeta) > 0 {
		response.ShareMeta = shareMeta
	}

	return response
}

// FromViewEntities 从视图实体列表转换为DTO列表
func FromViewEntities(views []*entity.View) []*ViewResponse {
	if views == nil {
		return []*ViewResponse{}
	}

	responses := make([]*ViewResponse, len(views))
	for i, view := range views {
		responses[i] = FromViewEntity(view)
	}

	return responses
}
