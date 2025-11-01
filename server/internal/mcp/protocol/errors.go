package protocol

// MCP 错误码定义
const (
	// JSON-RPC 标准错误码
	ErrorCodeParseError     = -32700
	ErrorCodeInvalidRequest = -32600
	ErrorCodeMethodNotFound = -32601
	ErrorCodeInvalidParams  = -32602
	ErrorCodeInternalError  = -32603

	// MCP 特定错误码
	ErrorCodeInvalidProtocolVersion = -32001
	ErrorCodeInitializationFailed   = -32002
	ErrorCodeAuthenticationFailed   = -32003
	ErrorCodeAuthorizationFailed    = -32004
	ErrorCodeResourceNotFound       = -32005
	ErrorCodeToolNotFound           = -32006
	ErrorCodePromptNotFound         = -32007
	ErrorCodeInvalidToolArguments   = -32008
	ErrorCodeToolExecutionFailed    = -32009
	ErrorCodeResourceAccessDenied   = -32010
	ErrorCodeRateLimitExceeded      = -32011
	ErrorCodeValidationFailed       = -32012
)

// 错误消息映射
var errorMessages = map[int]string{
	ErrorCodeParseError:             "Parse error",
	ErrorCodeInvalidRequest:         "Invalid Request",
	ErrorCodeMethodNotFound:         "Method not found",
	ErrorCodeInvalidParams:          "Invalid params",
	ErrorCodeInternalError:          "Internal error",
	ErrorCodeInvalidProtocolVersion: "Invalid protocol version",
	ErrorCodeInitializationFailed:   "Initialization failed",
	ErrorCodeAuthenticationFailed:   "Authentication failed",
	ErrorCodeAuthorizationFailed:    "Authorization failed",
	ErrorCodeResourceNotFound:       "Resource not found",
	ErrorCodeToolNotFound:           "Tool not found",
	ErrorCodePromptNotFound:         "Prompt not found",
	ErrorCodeInvalidToolArguments:   "Invalid tool arguments",
	ErrorCodeToolExecutionFailed:    "Tool execution failed",
	ErrorCodeResourceAccessDenied:   "Resource access denied",
	ErrorCodeRateLimitExceeded:      "Rate limit exceeded",
	ErrorCodeValidationFailed:       "Validation failed",
}

// MCPError 自定义错误类型
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Error 实现 error 接口
func (e *MCPError) Error() string {
	return e.Message
}

// NewMCPError 创建新的 MCP 错误
func NewMCPError(code int, message string, data interface{}) *MCPError {
	if message == "" {
		if msg, exists := errorMessages[code]; exists {
			message = msg
		} else {
			message = "Unknown error"
		}
	}
	return &MCPError{
		Code:    code,
		Message: message,
		Data:    data,
	}
}

// 预定义的错误
var (
	ErrParseError             = NewMCPError(ErrorCodeParseError, "", nil)
	ErrInvalidRequest         = NewMCPError(ErrorCodeInvalidRequest, "", nil)
	ErrMethodNotFound         = NewMCPError(ErrorCodeMethodNotFound, "", nil)
	ErrInvalidParams          = NewMCPError(ErrorCodeInvalidParams, "", nil)
	ErrInternalError          = NewMCPError(ErrorCodeInternalError, "", nil)
	ErrInvalidProtocolVersion = NewMCPError(ErrorCodeInvalidProtocolVersion, "", nil)
	ErrInitializationFailed   = NewMCPError(ErrorCodeInitializationFailed, "", nil)
	ErrAuthenticationFailed   = NewMCPError(ErrorCodeAuthenticationFailed, "", nil)
	ErrAuthorizationFailed    = NewMCPError(ErrorCodeAuthorizationFailed, "", nil)
	ErrResourceNotFound       = NewMCPError(ErrorCodeResourceNotFound, "", nil)
	ErrToolNotFound           = NewMCPError(ErrorCodeToolNotFound, "", nil)
	ErrPromptNotFound         = NewMCPError(ErrorCodePromptNotFound, "", nil)
	ErrInvalidToolArguments   = NewMCPError(ErrorCodeInvalidToolArguments, "", nil)
	ErrToolExecutionFailed    = NewMCPError(ErrorCodeToolExecutionFailed, "", nil)
	ErrResourceAccessDenied   = NewMCPError(ErrorCodeResourceAccessDenied, "", nil)
	ErrRateLimitExceeded      = NewMCPError(ErrorCodeRateLimitExceeded, "", nil)
	ErrValidationFailed       = NewMCPError(ErrorCodeValidationFailed, "", nil)
)

// NewParseError 创建解析错误
func NewParseError(data interface{}) *MCPError {
	return NewMCPError(ErrorCodeParseError, "", data)
}

// NewInvalidRequestError 创建无效请求错误
func NewInvalidRequestError(data interface{}) *MCPError {
	return NewMCPError(ErrorCodeInvalidRequest, "", data)
}

// NewMethodNotFoundError 创建方法未找到错误
func NewMethodNotFoundError(method string) *MCPError {
	return NewMCPError(ErrorCodeMethodNotFound, "", map[string]string{"method": method})
}

// NewInvalidParamsError 创建无效参数错误
func NewInvalidParamsError(data interface{}) *MCPError {
	return NewMCPError(ErrorCodeInvalidParams, "", data)
}

// NewInternalError 创建内部错误
func NewInternalError(data interface{}) *MCPError {
	return NewMCPError(ErrorCodeInternalError, "", data)
}

// NewInvalidProtocolVersionError 创建无效协议版本错误
func NewInvalidProtocolVersionError(version string) *MCPError {
	return NewMCPError(ErrorCodeInvalidProtocolVersion, "", map[string]string{"version": version})
}

// NewInitializationFailedError 创建初始化失败错误
func NewInitializationFailedError(reason string) *MCPError {
	return NewMCPError(ErrorCodeInitializationFailed, "", map[string]string{"reason": reason})
}

// NewAuthenticationFailedError 创建认证失败错误
func NewAuthenticationFailedError(reason string) *MCPError {
	return NewMCPError(ErrorCodeAuthenticationFailed, "", map[string]string{"reason": reason})
}

// NewAuthorizationFailedError 创建授权失败错误
func NewAuthorizationFailedError(resource, action string) *MCPError {
	return NewMCPError(ErrorCodeAuthorizationFailed, "", map[string]string{
		"resource": resource,
		"action":   action,
	})
}

// NewResourceNotFoundError 创建资源未找到错误
func NewResourceNotFoundError(uri string) *MCPError {
	return NewMCPError(ErrorCodeResourceNotFound, "", map[string]string{"uri": uri})
}

// NewToolNotFoundError 创建工具未找到错误
func NewToolNotFoundError(name string) *MCPError {
	return NewMCPError(ErrorCodeToolNotFound, "", map[string]string{"name": name})
}

// NewPromptNotFoundError 创建提示未找到错误
func NewPromptNotFoundError(name string) *MCPError {
	return NewMCPError(ErrorCodePromptNotFound, "", map[string]string{"name": name})
}

// NewInvalidToolArgumentsError 创建无效工具参数错误
func NewInvalidToolArgumentsError(toolName string, details interface{}) *MCPError {
	return NewMCPError(ErrorCodeInvalidToolArguments, "", map[string]interface{}{
		"tool":    toolName,
		"details": details,
	})
}

// NewToolExecutionFailedError 创建工具执行失败错误
func NewToolExecutionFailedError(toolName string, reason string) *MCPError {
	return NewMCPError(ErrorCodeToolExecutionFailed, "", map[string]string{
		"tool":   toolName,
		"reason": reason,
	})
}

// NewResourceAccessDeniedError 创建资源访问拒绝错误
func NewResourceAccessDeniedError(uri string) *MCPError {
	return NewMCPError(ErrorCodeResourceAccessDenied, "", map[string]string{"uri": uri})
}

// NewRateLimitExceededError 创建限流错误
func NewRateLimitExceededError(limit string) *MCPError {
	return NewMCPError(ErrorCodeRateLimitExceeded, "", map[string]string{"limit": limit})
}

// NewValidationFailedError 创建验证失败错误
func NewValidationFailedError(field string, reason string) *MCPError {
	return NewMCPError(ErrorCodeValidationFailed, "", map[string]string{
		"field":  field,
		"reason": reason,
	})
}
