package dto

import (
	"time"

	spaceEntity "github.com/easyspace-ai/luckdb/server/internal/domain/space/entity"
)

// CreateSpaceRequest 创建空间请求
type CreateSpaceRequest struct {
	Name        string  `json:"name" binding:"required"`
	Description string  `json:"description"`
	Icon        *string `json:"icon"`
}

// UpdateSpaceRequest 更新空间请求
type UpdateSpaceRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Icon        *string `json:"icon"`
}

// SpaceListFilter 空间列表过滤器
type SpaceListFilter struct {
	Name      *string    `json:"name"`
	CreatedBy *string    `json:"createdBy"`
	CreatedAt *time.Time `json:"createdAt"`
	Page      int        `json:"page"`
	PageSize  int        `json:"pageSize"`
	Offset    int        `json:"offset"`
	Limit     int        `json:"limit"`
}

// SpaceResponse 空间响应
type SpaceResponse struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedBy   string    `json:"createdBy"`
	UpdatedBy   string    `json:"updatedBy"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// SpaceListResponse 空间列表响应
type SpaceListResponse struct {
	Spaces     []*SpaceResponse    `json:"spaces"`
	Pagination *PaginationResponse `json:"pagination"`
}

// 注意：AddCollaboratorRequest 已移至 collaborator.go 统一管理

// FromSpaceEntity 从Domain实体转换为DTO
func FromSpaceEntity(space *spaceEntity.Space) *SpaceResponse {
	if space == nil {
		return nil
	}

	desc := ""
	if space.Description() != nil {
		desc = *space.Description()
	}

	// UpdatedBy 通常应该从操作上下文中获取
	// 如果没有专门的 UpdatedBy 字段，可以使用 CreatedBy 作为默认值
	// 在实际应用中，应该从请求上下文中获取当前操作用户
	updatedBy := space.CreatedBy() // 默认使用创建者

	return &SpaceResponse{
		ID:          space.ID().String(),
		Name:        space.Name().String(),
		Description: desc,
		CreatedBy:   space.CreatedBy(),
		UpdatedBy:   updatedBy,
		CreatedAt:   space.CreatedAt(),
		UpdatedAt:   space.UpdatedAt(),
	}
}

// ListSpacesResponse 空间列表响应
type ListSpacesResponse struct {
	Spaces []*SpaceResponse `json:"spaces"`
	Total  int64            `json:"total"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
}
