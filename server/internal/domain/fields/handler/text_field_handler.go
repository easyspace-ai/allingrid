package handler

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// TextFieldHandler 文本字段处理器
type TextFieldHandler struct {
	*BaseFieldHandler
}

// NewTextFieldHandler 创建文本字段处理器
func NewTextFieldHandler() *TextFieldHandler {
	return &TextFieldHandler{
		BaseFieldHandler: NewBaseFieldHandler(valueobject.TypeText),
	}
}

// ValidateValue 验证文本值
func (h *TextFieldHandler) ValidateValue(ctx context.Context, field *entity.Field, value interface{}) error {
	// nil值检查
	if value == nil {
		if field.IsRequired() {
			return fields.ErrRequiredFieldEmpty
		}
		return nil
	}

	// 类型检查
	text, ok := value.(string)
	if !ok {
		return fields.NewDomainError(
			"INVALID_TEXT_VALUE",
			fmt.Sprintf("text field expects string, got %T", value),
			nil,
		)
	}

	// 长度限制（可选）
	// 可以根据配置添加最大长度限制
	maxLength := 65535 // TEXT类型默认最大长度
	if len(text) > maxLength {
		return fields.NewDomainError(
			"TEXT_TOO_LONG",
			fmt.Sprintf("text exceeds maximum length of %d characters", maxLength),
			nil,
		)
	}

	return nil
}

// TransformValue 转换文本值
func (h *TextFieldHandler) TransformValue(ctx context.Context, field *entity.Field, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	text, ok := value.(string)
	if !ok {
		// 尝试转换为字符串
		return fmt.Sprintf("%v", value), nil
	}

	return text, nil
}
