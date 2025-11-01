package protocol

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
)

// Handler MCP 消息处理器接口
type Handler interface {
	Handle(ctx context.Context, req *MCPRequest) (*MCPResponse, error)
}

// MethodHandler 方法处理器
type MethodHandler func(ctx context.Context, params interface{}) (interface{}, error)

// Router MCP 路由器
type Router struct {
	handlers map[string]MethodHandler
}

// NewRouter 创建新的路由器
func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]MethodHandler),
	}
}

// Register 注册方法处理器
func (r *Router) Register(method string, handler MethodHandler) {
	r.handlers[method] = handler
}

// GetHandlers 获取所有注册的处理器
func (r *Router) GetHandlers() map[string]MethodHandler {
	return r.handlers
}

// Handle 处理 MCP 请求
func (r *Router) Handle(ctx context.Context, req *MCPRequest) (*MCPResponse, error) {
	// 验证请求格式
	if req.JSONRPC != "2.0" {
		return NewMCPErrorResponse(req.ID, ErrorCodeInvalidRequest, "Invalid JSON-RPC version", nil), nil
	}

	if req.Method == "" {
		return NewMCPErrorResponse(req.ID, ErrorCodeInvalidRequest, "Method is required", nil), nil
	}

	// 查找处理器
	handler, exists := r.handlers[req.Method]
	if !exists {
		return NewMCPErrorResponse(req.ID, ErrorCodeMethodNotFound, fmt.Sprintf("Method '%s' not found", req.Method), nil), nil
	}

	// 调用处理器
	result, err := handler(ctx, req.Params)
	if err != nil {
		// 如果是 MCPError，直接返回
		if mcpErr, ok := err.(*MCPError); ok {
			return NewMCPErrorResponse(req.ID, mcpErr.Code, mcpErr.Message, mcpErr.Data), nil
		}

		// 其他错误转换为内部错误
		// TODO: 添加结构化日志记录器
		// logger.Error("Handler error", zap.String("method", req.Method), zap.Error(err))
		return NewMCPErrorResponse(req.ID, ErrorCodeInternalError, "Internal server error", map[string]string{
			"method": req.Method,
			"error":  err.Error(),
		}), nil
	}

	// 返回成功响应
	return NewMCPResponse(req.ID, result), nil
}

// GetRegisteredMethods 获取已注册的方法列表
func (r *Router) GetRegisteredMethods() []string {
	methods := make([]string, 0, len(r.handlers))
	for method := range r.handlers {
		methods = append(methods, method)
	}
	return methods
}

// Middleware 中间件函数类型
type Middleware func(ctx context.Context, req *MCPRequest, next Handler) (*MCPResponse, error)

// MiddlewareHandler 带中间件的处理器
type MiddlewareHandler struct {
	router     *Router
	middleware []Middleware
}

// NewMiddlewareHandler 创建带中间件的处理器
func NewMiddlewareHandler(router *Router, middleware ...Middleware) *MiddlewareHandler {
	return &MiddlewareHandler{
		router:     router,
		middleware: middleware,
	}
}

// Handle 处理请求（带中间件）
func (h *MiddlewareHandler) Handle(ctx context.Context, req *MCPRequest) (*MCPResponse, error) {
	// 构建中间件链
	var handler Handler = h.router
	for i := len(h.middleware) - 1; i >= 0; i-- {
		middleware := h.middleware[i]
		next := handler
		handler = &middlewareWrapper{
			middleware: middleware,
			next:       next,
		}
	}

	return handler.Handle(ctx, req)
}

// middlewareWrapper 中间件包装器
type middlewareWrapper struct {
	middleware Middleware
	next       Handler
}

func (w *middlewareWrapper) Handle(ctx context.Context, req *MCPRequest) (*MCPResponse, error) {
	return w.middleware(ctx, req, w.next)
}

// RequestValidator 请求验证器
type RequestValidator struct {
	requiredFields map[string][]string
}

// NewRequestValidator 创建请求验证器
func NewRequestValidator() *RequestValidator {
	return &RequestValidator{
		requiredFields: make(map[string][]string),
	}
}

// AddRequiredFields 添加必需字段
func (v *RequestValidator) AddRequiredFields(method string, fields []string) {
	v.requiredFields[method] = fields
}

// Validate 验证请求参数
func (v *RequestValidator) Validate(method string, params interface{}) error {
	requiredFields, exists := v.requiredFields[method]
	if !exists {
		return nil // 没有验证规则
	}

	if params == nil {
		return NewValidationFailedError("params", "Parameters are required")
	}

	// 将参数转换为 map
	paramsMap, ok := params.(map[string]interface{})
	if !ok {
		// 尝试通过反射获取字段
		paramsValue := reflect.ValueOf(params)
		if paramsValue.Kind() == reflect.Ptr {
			paramsValue = paramsValue.Elem()
		}

		if paramsValue.Kind() != reflect.Struct {
			return NewValidationFailedError("params", "Parameters must be an object")
		}

		paramsMap = make(map[string]interface{})
		paramsType := paramsValue.Type()
		for i := 0; i < paramsValue.NumField(); i++ {
			field := paramsType.Field(i)
			value := paramsValue.Field(i)

			// 获取 JSON 标签
			jsonTag := field.Tag.Get("json")
			if jsonTag == "" || jsonTag == "-" {
				continue
			}

			// 处理 JSON 标签中的选项
			jsonName := strings.Split(jsonTag, ",")[0]
			if jsonName == "" {
				jsonName = field.Name
			}

			paramsMap[jsonName] = value.Interface()
		}
	}

	// 检查必需字段
	for _, field := range requiredFields {
		if _, exists := paramsMap[field]; !exists {
			return NewValidationFailedError(field, "Field is required")
		}
	}

	return nil
}

// LoggingMiddleware 日志中间件
func LoggingMiddleware(logger *log.Logger) Middleware {
	return func(ctx context.Context, req *MCPRequest, next Handler) (*MCPResponse, error) {
		start := time.Now()

		logger.Printf("MCP Request: %s", req.Method)

		resp, err := next.Handle(ctx, req)

		duration := time.Since(start)
		if err != nil {
			logger.Printf("MCP Request failed: %s (duration: %v)", req.Method, duration)
		} else {
			logger.Printf("MCP Request completed: %s (duration: %v)", req.Method, duration)
		}

		return resp, err
	}
}

// ValidationMiddleware 验证中间件
func ValidationMiddleware(validator *RequestValidator) Middleware {
	return func(ctx context.Context, req *MCPRequest, next Handler) (*MCPResponse, error) {
		if err := validator.Validate(req.Method, req.Params); err != nil {
			return NewMCPErrorResponse(req.ID, ErrorCodeInvalidParams, err.Error(), nil), nil
		}

		return next.Handle(ctx, req)
	}
}

// ContextMiddleware 上下文中间件
func ContextMiddleware() Middleware {
	return func(ctx context.Context, req *MCPRequest, next Handler) (*MCPResponse, error) {
		// 添加请求 ID 到上下文
		if req.ID != nil {
			ctx = context.WithValue(ctx, "request_id", req.ID)
		}

		// 添加方法名到上下文
		ctx = context.WithValue(ctx, "method", req.Method)

		return next.Handle(ctx, req)
	}
}

// ErrorHandler 错误处理器
type ErrorHandler struct {
	handler Handler
}

// NewErrorHandler 创建错误处理器
func NewErrorHandler(handler Handler) *ErrorHandler {
	return &ErrorHandler{
		handler: handler,
	}
}

// Handle 处理请求并捕获错误
func (h *ErrorHandler) Handle(ctx context.Context, req *MCPRequest) (*MCPResponse, error) {
	defer func() {
		if r := recover(); r != nil {
			// TODO: 添加结构化日志记录器
			// logger.Error("Panic in MCP handler", zap.Any("panic", r))
		}
	}()

	return h.handler.Handle(ctx, req)
}
