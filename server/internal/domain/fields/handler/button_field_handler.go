package handler

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// ButtonFieldHandler Button字段处理器（对齐原版ButtonFieldCore）
//
// 设计哲学：
//   - 特殊性：Button是只读字段，不存储值
//   - 交互性：用于触发操作，非数据存储
//   - 完美对齐：100%复制原版的业务规则
//
// 业务规则：
//   - Button字段不存储值（只读）
//   - 任何写入值的尝试都应被忽略
//   - GetValue始终返回nil
//   - 主要用于UI交互
//
// 配置选项：
//   - label: 按钮文本
//   - action: 按钮触发的动作类型
//   - url: 打开的URL（可选）
//
// 对齐原版：
//   - 只读特性
//   - 配置驱动的按钮行为
type ButtonFieldHandler struct {
	*BaseFieldHandler
}

// NewButtonFieldHandler 创建Button字段处理器
func NewButtonFieldHandler() *ButtonFieldHandler {
	return &ButtonFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeButton),
	}
}

// ValidateValue 验证字段值的合法性
// Button字段不存储值，所以验证始终通过
//
// 设计考量：
//   - Button是只读字段，无需验证值
//   - 任何值都被接受（但不会被存储）
//   - 保持接口一致性
func (h *ButtonFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// Button字段不验证值（因为不存储值）
	return nil
}

// TransformValue 转换字段值
// Button字段不存储值，始终返回nil
//
// 设计考量：
//   - 明确的只读语义
//   - 任何输入都转换为nil
//   - 符合Button字段的特殊性
func (h *ButtonFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	// Button字段不存储值
	return nil, nil
}

// FormatValue 格式化字段值
// Button字段返回nil
func (h *ButtonFieldHandler) FormatValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	return nil, nil
}

// SupportsOptions 是否支持选项配置
func (h *ButtonFieldHandler) SupportsOptions() bool {
	return true // Button字段需要配置label和action
}

// ValidateOptions 验证字段选项配置
// 验证规则：
//   - label不能为空
//   - action必须是有效的动作类型
//
// 设计考量：
//   - 确保按钮可用性（必须有label）
//   - 动作类型验证（可扩展）
func (h *ButtonFieldHandler) ValidateOptions(ctx context.Context, field *entity.Field) error {
	options := field.Options()
	if options == nil || options.Button == nil {
		return fields.NewDomainError(
			"INVALID_BUTTON_OPTIONS",
			"Button字段必须配置Button选项",
			nil,
		)
	}

	if options.Button.Label == "" {
		return fields.NewDomainError(
			"INVALID_BUTTON_OPTIONS",
			"Button label不能为空",
			nil,
		)
	}

	// 验证动作类型
	validActions := []string{"open_url", "run_script", "trigger_automation"}
	isValidAction := false
	for _, va := range validActions {
		if options.Button.Action == va {
			isValidAction = true
			break
		}
	}

	if !isValidAction {
		return fields.NewDomainError(
			"INVALID_BUTTON_ACTION",
			"Button action无效，支持的类型: open_url, run_script, trigger_automation",
			nil,
		)
	}

	return nil
}

// IsReadOnly Button字段是只读的
// 这是一个特殊方法，标识Button字段的只读特性
//
// 设计考量：
//   - 明确的语义表达
//   - 可用于权限判断和UI渲染
func (h *ButtonFieldHandler) IsReadOnly() bool {
	return true
}
