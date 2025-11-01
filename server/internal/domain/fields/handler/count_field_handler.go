package handler

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// CountFieldHandler Count字段处理器（对齐原版CountFieldCore）
//
// 设计哲学：
//   - 自动计算：Count是虚拟字段，值由系统自动计算
//   - 只读性：用户不能手动修改值
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - Count字段是虚拟字段（isComputed=true）
//   - 值由系统自动计算（基于Link字段）
//   - 计算逻辑：统计关联记录的数量
//   - 用户不能手动设置值
//
// 配置选项：
//   - linkFieldId: 要统计的Link字段ID
//
// 计算逻辑：
//  1. 获取Link字段的值（关联记录ID数组）
//  2. 返回数组长度
//
// 对齐原版：
//   - 虚拟字段特性
//   - 自动计数逻辑
//   - 依赖Link字段
type CountFieldHandler struct {
	*BaseFieldHandler
}

// NewCountFieldHandler 创建Count字段处理器
func NewCountFieldHandler() *CountFieldHandler {
	return &CountFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeCount),
	}
}

// ValidateValue 验证字段值的合法性
// Count字段是虚拟字段，值由系统计算，用户不能设置
//
// 设计考量：
//   - 拒绝用户设置值
//   - 保持只读语义
//   - 清晰的错误信息
func (h *CountFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// Count字段是只读的，不接受用户输入
	if value != nil {
		return fields.NewDomainError(
			"READONLY_FIELD",
			"count field is read-only and cannot be set manually",
			nil,
		)
	}
	return nil
}

// TransformValue 转换字段值
// Count字段不存储用户输入的值
func (h *CountFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	// Count字段值由系统计算，忽略输入
	return nil, nil
}

// FormatValue 格式化字段值
// 返回计数值（整数）
func (h *CountFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return value, nil
}

// SupportsOptions 是否支持选项配置
func (h *CountFieldHandler) SupportsOptions() bool {
	return true // Count字段需要配置linkFieldId
}

// ValidateOptions 验证字段选项配置
// 验证规则：
//   - linkFieldId不能为空
//   - linkFieldId必须指向有效的Link字段
//
// 设计考量：
//   - 确保Count字段有数据源
//   - 验证依赖字段的存在性
func (h *CountFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	options := field.Options()
	if options == nil || options.Count == nil {
		return fields.NewDomainError(
			"INVALID_COUNT_OPTIONS",
			"Count字段必须配置Count选项",
			nil,
		)
	}

	if options.Count.LinkFieldID == "" {
		return fields.NewDomainError(
			"INVALID_COUNT_OPTIONS",
			"Count字段的linkFieldId不能为空",
			nil,
		)
	}

	// TODO: 可选的验证增强
	// 1. 验证LinkFieldID指向的字段是否存在
	// 2. 验证该字段是否为Link类型
	// 这需要在Handler中注入FieldRepository

	return nil
}

// ComputeValue 计算字段值（实现VirtualFieldHandler接口）
// 参数:
//   - ctx: 上下文
//   - field: Count字段实体
//   - record: 当前记录的数据
//   - relatedRecords: 关联记录数据（key为字段ID）
//
// 返回:
//   - interface{}: 计算后的值（int）
//   - error: 计算失败时的错误
//
// 计算逻辑：
//  1. 从field.Options获取linkFieldId
//  2. 从record中获取Link字段的值（关联记录ID数组）
//  3. 返回数组长度
//
// 设计考量：
//   - 健壮的错误处理
//   - 性能优化：直接计数，不查询完整记录
//   - 符合虚拟字段的计算模式
func (h *CountFieldHandler) ComputeValue(ctx context.Context, field *entity.Field, record map[string]interface{}, relatedRecords map[string][]map[string]interface{}) (interface{}, error) {
	// 1. 获取Count配置
	options := field.Options()
	if options == nil || options.Count == nil {
		return 0, fields.NewDomainError(
			"MISSING_COUNT_OPTIONS",
			"Count配置缺失",
			nil,
		)
	}
	linkFieldID := options.Count.LinkFieldID

	// 2. 从record中获取Link字段的值
	linkValue := record[linkFieldID]
	if linkValue == nil {
		return 0, nil // 无关联记录
	}

	// 3. 解析关联记录ID并计数
	switch v := linkValue.(type) {
	case []interface{}:
		return len(v), nil
	case []string:
		return len(v), nil
	case string:
		if v == "" {
			return 0, nil
		}
		return 1, nil // 单个关联
	default:
		return 0, nil
	}
}

// GetDependencies 获取依赖的字段ID列表
// Count字段依赖于Link字段
//
// 设计考量：
//   - 明确依赖关系，用于计算顺序排序
//   - 支持依赖图构建
func (h *CountFieldHandler) GetDependencies(ctx context.Context, field *entity.Field) ([]string, error) {
	// Count字段依赖于Link字段
	options := field.Options()
	if options != nil && options.Count != nil {
		linkFieldID := options.Count.LinkFieldID
		if linkFieldID != "" {
			return []string{linkFieldID}, nil
		}
	}

	// 如果没有配置，返回空数组
	return []string{}, nil
}

// IsAsync Count字段同步计算
func (h *CountFieldHandler) IsAsync() bool {
	return false
}
