package handler

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
)

// FieldHandler 字段处理器接口
// 每种字段类型都有对应的处理器
type FieldHandler interface {
	// ValidateValue 验证字段值
	ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error

	// TransformValue 转换字段值（用于存储前的转换）
	TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error)

	// FormatValue 格式化字段值（用于显示）
	FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error)

	// GetDefaultValue 获取字段默认值
	GetDefaultValue(ctx context.Context, field *entity.Field) (interface{}, error)

	// SupportsOptions 是否支持选项配置
	SupportsOptions() bool

	// ValidateOptions 验证字段选项
	ValidateOptions(ctx context.Context, field *entity.Field) error

	// GetFieldType 获取处理器对应的字段类型
	GetFieldType() string
}

// VirtualFieldHandler 虚拟字段处理器接口
// 虚拟字段需要额外的计算能力
type VirtualFieldHandler interface {
	FieldHandler

	// ComputeValue 计算字段值
	ComputeValue(ctx context.Context, field *entity.Field, record map[string]interface{}, relatedRecords map[string][]map[string]interface{}) (interface{}, error)

	// GetDependencies 获取依赖的字段ID列表
	GetDependencies(ctx context.Context, field *entity.Field) ([]string, error)

	// IsAsync 是否异步计算
	IsAsync() bool
}

// HandlerRegistry 字段处理器注册表
type HandlerRegistry interface {
	// Register 注册字段处理器
	Register(fieldType string, handler FieldHandler) error

	// Get 获取字段处理器
	Get(fieldType string) (FieldHandler, error)

	// Has 检查是否有对应的处理器
	Has(fieldType string) bool

	// GetAll 获取所有处理器
	GetAll() map[string]FieldHandler
}

// BaseFieldHandler 基础字段处理器
// 提供默认实现，具体处理器可以继承
type BaseFieldHandler struct {
	fieldType string
}

// NewBaseFieldHandler 创建基础字段处理器
func NewBaseFieldHandler(fieldType string) *BaseFieldHandler {
	return &BaseFieldHandler{
		fieldType: fieldType,
	}
}

// GetFieldType 获取字段类型
func (h *BaseFieldHandler) GetFieldType() string {
	return h.fieldType
}

// SupportsOptions 默认不支持选项
func (h *BaseFieldHandler) SupportsOptions() bool {
	return false
}

// ValidateOptions 默认选项验证（无操作）
func (h *BaseFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	return nil
}

// GetDefaultValue 默认返回nil
func (h *BaseFieldHandler) GetDefaultValue(ctx context.Context, field *entity.Field) (interface{}, error) {
	if field.DefaultValue() != nil {
		return *field.DefaultValue(), nil
	}
	return nil, nil
}

// FormatValue 默认格式化（返回原值）
func (h *BaseFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return value, nil
}
