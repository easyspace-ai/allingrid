package handler

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// LookupFieldHandler Lookup字段处理器
type LookupFieldHandler struct {
	*BaseFieldHandler
}

// NewLookupFieldHandler 创建Lookup字段处理器
func NewLookupFieldHandler() *LookupFieldHandler {
	return &LookupFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeLookup),
	}
}

// ValidateValue 验证Lookup字段值（Lookup字段值由系统计算）
func (h *LookupFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// Lookup字段值由系统计算，不允许手动设置
	return fields.NewDomainError(
		"CANNOT_SET_LOOKUP_VALUE",
		"lookup field value is computed automatically",
		nil,
	)
}

// TransformValue 转换Lookup字段值
func (h *LookupFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	// Lookup字段不存储值
	return nil, nil
}

// ComputeValue 计算Lookup字段值
func (h *LookupFieldHandler) ComputeValue(
	ctx context.Context,
	field *entity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	// 获取Lookup配置
	if field.Options() == nil || field.Options().Lookup == nil {
		return nil, fields.ErrInvalidLookupConfig
	}

	lookupOpts := field.Options().Lookup

	// 获取关联记录
	linkedRecords, exists := relatedRecords[lookupOpts.LinkFieldID]
	if !exists || len(linkedRecords) == 0 {
		// 没有关联记录，返回nil
		return nil, nil
	}

	// 提取lookup字段的值
	values := make([]interface{}, 0, len(linkedRecords))
	for _, linkedRecord := range linkedRecords {
		if val, exists := linkedRecord[lookupOpts.LookupFieldID]; exists && val != nil {
			values = append(values, val)
		}
	}

	// 如果没有值，返回nil
	if len(values) == 0 {
		return nil, nil
	}

	// 如果只有一个值，返回单个值；否则返回数组
	if len(values) == 1 {
		return values[0], nil
	}

	return values, nil
}

// GetDependencies 获取Lookup依赖的字段
func (h *LookupFieldHandler) GetDependencies(ctx context.Context, field *entity.Field) ([]string, error) {
	if field.Options() == nil || field.Options().Lookup == nil {
		return []string{}, nil
	}

	lookupOpts := field.Options().Lookup

	// Lookup依赖link字段和被lookup的字段
	dependencies := []string{
		lookupOpts.LinkFieldID,
		lookupOpts.LookupFieldID,
	}

	return dependencies, nil
}

// IsAsync Lookup计算相对简单，可以同步
func (h *LookupFieldHandler) IsAsync() bool {
	return false
}

// SupportsOptions Lookup字段支持选项
func (h *LookupFieldHandler) SupportsOptions() bool {
	return true
}

// ValidateOptions 验证Lookup选项
func (h *LookupFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	if field.Options() == nil || field.Options().Lookup == nil {
		return fields.ErrMissingRequiredOption
	}

	lookupOpts := field.Options().Lookup

	// 验证必填字段
	if lookupOpts.LinkFieldID == "" {
		return fields.NewDomainError(
			"MISSING_LINK_FIELD_ID",
			"lookup must specify link field",
			nil,
		)
	}

	if lookupOpts.LookupFieldID == "" {
		return fields.NewDomainError(
			"MISSING_LOOKUP_FIELD_ID",
			"lookup must specify target field",
			nil,
		)
	}

	return nil
}
