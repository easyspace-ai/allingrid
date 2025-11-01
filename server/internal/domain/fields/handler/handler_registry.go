package handler

import (
	"fmt"
	"sync"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"
)

// handlerRegistry 字段处理器注册表实现
type handlerRegistry struct {
	handlers map[string]FieldHandler
	mu       sync.RWMutex
}

// NewHandlerRegistry 创建处理器注册表
func NewHandlerRegistry() HandlerRegistry {
	return &handlerRegistry{
		handlers: make(map[string]FieldHandler),
	}
}

// Register 注册字段处理器
func (r *handlerRegistry) Register(fieldType string, handler FieldHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fieldType == "" {
		return fmt.Errorf("field type cannot be empty")
	}

	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// 检查是否已注册
	if _, exists := r.handlers[fieldType]; exists {
		return fmt.Errorf("handler for field type %s is already registered", fieldType)
	}

	r.handlers[fieldType] = handler
	return nil
}

// Get 获取字段处理器
func (r *handlerRegistry) Get(fieldType string) (FieldHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[fieldType]
	if !exists {
		return nil, fields.NewDomainError(
			"HANDLER_NOT_FOUND",
			fmt.Sprintf("no handler registered for field type: %s", fieldType),
			nil,
		)
	}

	return handler, nil
}

// Has 检查是否有对应的处理器
func (r *handlerRegistry) Has(fieldType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.handlers[fieldType]
	return exists
}

// GetAll 获取所有处理器
func (r *handlerRegistry) GetAll() map[string]FieldHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 返回副本，避免外部修改
	handlers := make(map[string]FieldHandler)
	for k, v := range r.handlers {
		handlers[k] = v
	}

	return handlers
}

// RegisterDefaultHandlers 注册默认处理器
func RegisterDefaultHandlers(registry HandlerRegistry, evaluator FormulaEvaluator) error {
	// 注册基础字段处理器
	if err := registry.Register("text", NewTextFieldHandler()); err != nil {
		return err
	}

	if err := registry.Register("number", NewNumberFieldHandler()); err != nil {
		return err
	}

	// 注册虚拟字段处理器
	if err := registry.Register("formula", NewFormulaFieldHandler(evaluator)); err != nil {
		return err
	}

	// 注册其他字段处理器（参考 teable-develop，可逐步扩展）
	// - date
	// - select
	// - link
	// - rollup
	// - lookup
	// - ai
	// 等

	return nil
}
