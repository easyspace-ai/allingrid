package dto

import (
	"time"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	fieldVO "github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// CreateFieldRequest 创建字段请求
type CreateFieldRequest struct {
	TableID  string                 `json:"tableId" binding:"required"` // ✅ 统一使用 camelCase
	Name     string                 `json:"name" binding:"required"`
	Type     string                 `json:"type" binding:"required"`
	Options  map[string]interface{} `json:"options"`
	Required bool                   `json:"required"`
	Unique   bool                   `json:"unique"`
    // 顶层默认值，兼容 SDK 传参
    DefaultValue interface{}        `json:"defaultValue"`
}

// UpdateFieldRequest 更新字段请求
type UpdateFieldRequest struct {
	Name        *string                `json:"name"`
	Description *string                `json:"description"`
	Options     map[string]interface{} `json:"options"`
	Required    *bool                  `json:"required"`
	Unique      *bool                  `json:"unique"`
	// 顶层默认值，兼容 SDK 传参
	DefaultValue interface{} `json:"defaultValue"`
}

// FieldResponse 字段响应
type FieldResponse struct {
	ID          string                 `json:"id"`
	TableID     string                 `json:"tableId"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Options     map[string]interface{} `json:"options"`
	Required    bool                   `json:"required"`
	Unique      bool                   `json:"unique"`
	IsPrimary   bool                   `json:"isPrimary"`
	Description string                 `json:"description"`
	CreatedAt   time.Time              `json:"createdAt"`
	UpdatedAt   time.Time              `json:"updatedAt"`
}

// FieldListResponse 字段列表响应
type FieldListResponse struct {
	Fields []*FieldResponse `json:"fields"`
}

// FromFieldEntity 从Domain实体转换为DTO
func FromFieldEntity(field *fieldEntity.Field) *FieldResponse {
	if field == nil {
		return nil
	}

	desc := ""
	if field.Description() != nil {
		desc = *field.Description()
	}

	return &FieldResponse{
		ID:          field.ID().String(),
		TableID:     field.TableID(),
		Name:        field.Name().String(),
		Type:        field.Type().String(),
		Options:     fieldOptionsToMap(field.Options()),
		Required:    field.IsRequired(),
		Unique:      field.IsUnique(),
		IsPrimary:   field.IsPrimary(),
		Description: desc,
		CreatedAt:   field.CreatedAt(),
		UpdatedAt:   field.UpdatedAt(),
	}
}

// fieldOptionsToMap 将FieldOptions转换为map（简化版）
func fieldOptionsToMap(options *fieldVO.FieldOptions) map[string]interface{} {
	if options == nil {
		return nil
	}

	result := make(map[string]interface{})

	if options.Formula != nil {
		result["formula"] = map[string]interface{}{
			"expression": options.Formula.Expression,
		}
	}

	if options.Rollup != nil {
		result["rollup"] = map[string]interface{}{
			"link_field_id":        options.Rollup.LinkFieldID,
			"aggregation_function": options.Rollup.AggregationFunction,
		}
	}

	if options.Lookup != nil {
		result["lookup"] = map[string]interface{}{
			"link_field_id":   options.Lookup.LinkFieldID,
			"lookup_field_id": options.Lookup.LookupFieldID,
		}
	}

	return result
}

// FromFieldEntities 批量转换
func FromFieldEntities(fields []*fieldEntity.Field) []*FieldResponse {
	result := make([]*FieldResponse, len(fields))
	for i, field := range fields {
		result[i] = FromFieldEntity(field)
	}
	return result
}
