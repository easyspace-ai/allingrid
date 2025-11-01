package errors

import (
	"fmt"
	"strings"
)

// ErrorHandler 错误处理器
type ErrorHandler struct {
	// 是否启用详细错误信息（生产环境建议关闭）
	Verbose bool
	// 是否记录错误日志
	LogErrors bool
}

// NewErrorHandler 创建新的错误处理器
func NewErrorHandler(verbose, logErrors bool) *ErrorHandler {
	return &ErrorHandler{
		Verbose:   verbose,
		LogErrors: logErrors,
	}
}

// HandleError 处理错误
func (h *ErrorHandler) HandleError(err error) *ShareDBError {
	if err == nil {
		return nil
	}

	// 如果已经是 ShareDBError，直接返回
	if shareDBErr, ok := err.(*ShareDBError); ok {
		return shareDBErr
	}

	// 根据错误消息判断错误类型
	errMsg := strings.ToLower(err.Error())

	switch {
	case strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "unauth"):
		return NewShareDBError("UNAUTHORIZED", "Authentication required")
	case strings.Contains(errMsg, "permission") || strings.Contains(errMsg, "forbidden"):
		return NewShareDBError("PERMISSION_DENIED", "Permission denied")
	case strings.Contains(errMsg, "not found"):
		return NewShareDBError(ErrDocumentNotFound, "Resource not found")
	case strings.Contains(errMsg, "already exists") || strings.Contains(errMsg, "duplicate"):
		return NewShareDBError(ErrDocumentExists, "Resource already exists")
	case strings.Contains(errMsg, "version") || strings.Contains(errMsg, "conflict"):
		return NewShareDBError(ErrVersionMismatch, "Version conflict")
	case strings.Contains(errMsg, "timeout"):
		return NewShareDBError(ErrOperationTimeout, "Operation timeout")
	case strings.Contains(errMsg, "network") || strings.Contains(errMsg, "connection"):
		return NewShareDBError(ErrNetworkError, "Network error")
	case strings.Contains(errMsg, "server") || strings.Contains(errMsg, "internal"):
		return NewShareDBError(ErrServerError, "Server error")
	case strings.Contains(errMsg, "quota") || strings.Contains(errMsg, "limit"):
		return NewShareDBError("QUOTA_EXCEEDED", "Quota exceeded")
	default:
		return NewShareDBError(ErrServerError, "Unknown error")
	}
}

// WrapError 包装错误
func (h *ErrorHandler) WrapError(err error, code, message string) *ShareDBError {
	if err == nil {
		return NewShareDBError(code, message)
	}

	details := ""
	if h.Verbose {
		details = err.Error()
	}

	return &ShareDBError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

// WrapErrorWithContext 包装错误并添加上下文
func (h *ErrorHandler) WrapErrorWithContext(err error, code, message string, context map[string]interface{}) *ShareDBError {
	if err == nil {
		return NewShareDBErrorWithContext(code, message, context)
	}

	details := ""
	if h.Verbose {
		details = err.Error()
	}

	return &ShareDBError{
		Code:    code,
		Message: message,
		Details: details,
		Context: context,
	}
}

// ValidateError 验证错误
func (h *ErrorHandler) ValidateError(err *ShareDBError) error {
	if err == nil {
		return fmt.Errorf("error cannot be nil")
	}

	if err.Code == "" {
		return fmt.Errorf("error code cannot be empty")
	}

	if err.Message == "" {
		return fmt.Errorf("error message cannot be empty")
	}

	return nil
}

// ShareDBErrorResponse ShareDB 错误响应结构
type ShareDBErrorResponse struct {
	Error     *ShareDBError `json:"error"`
	Success   bool          `json:"success"`
	RequestID string        `json:"requestId,omitempty"`
}

// NewShareDBErrorResponse 创建 ShareDB 错误响应
func NewShareDBErrorResponse(err *ShareDBError, requestID string) *ShareDBErrorResponse {
	return &ShareDBErrorResponse{
		Error:     err,
		Success:   false,
		RequestID: requestID,
	}
}

// ShareDBErrorCodeMap ShareDB 错误代码映射
var ShareDBErrorCodeMap = map[string]string{
	"UNAUTHORIZED":             "认证失败",
	"UNAUTHORIZED_SHARE":       "分享链接认证失败",
	"TOKEN_EXPIRED":            "令牌已过期",
	"INVALID_TOKEN":            "无效令牌",
	"DOCUMENT_NOT_FOUND":       "文档不存在",
	"DOCUMENT_EXISTS":          "文档已存在",
	"DOCUMENT_LOCKED":          "文档已锁定",
	"DOCUMENT_CORRUPTED":       "文档损坏",
	"VIEW_NOT_FOUND":           "视图不存在",
	"VIEW_EXISTS":              "视图已存在",
	"VIEW_INVALID":             "视图无效",
	"VIEW_PERMISSION_DENIED":   "视图权限不足",
	"FIELD_NOT_FOUND":          "字段不存在",
	"FIELD_EXISTS":             "字段已存在",
	"FIELD_INVALID":            "字段无效",
	"FIELD_TYPE_MISMATCH":      "字段类型不匹配",
	"FIELD_REQUIRED":           "字段必填",
	"RECORD_NOT_FOUND":         "记录不存在",
	"RECORD_EXISTS":            "记录已存在",
	"RECORD_INVALID":           "记录无效",
	"RECORD_PERMISSION_DENIED": "记录权限不足",
	"TABLE_NOT_FOUND":          "表格不存在",
	"TABLE_EXISTS":             "表格已存在",
	"TABLE_INVALID":            "表格无效",
	"TABLE_PERMISSION_DENIED":  "表格权限不足",
	"VERSION_MISMATCH":         "版本不匹配",
	"OPERATION_REJECTED":       "操作被拒绝",
	"OPERATION_INVALID":        "操作无效",
	"OPERATION_CONFLICT":       "操作冲突",
	"OPERATION_TIMEOUT":        "操作超时",
	"NETWORK_ERROR":            "网络错误",
	"CONNECTION_LOST":          "连接丢失",
	"CONNECTION_TIMEOUT":       "连接超时",
	"CONNECTION_REFUSED":       "连接被拒绝",
	"SERVER_ERROR":             "服务器错误",
	"SERVER_OVERLOADED":        "服务器过载",
	"SERVER_MAINTENANCE":       "服务器维护中",
	"SERVER_UNAVAILABLE":       "服务器不可用",
	"DATA_CORRUPTED":           "数据损坏",
	"DATA_INVALID":             "数据无效",
	"DATA_TOO_LARGE":           "数据过大",
	"DATA_NOT_FOUND":           "数据不存在",
	"CACHE_MISS":               "缓存未命中",
	"CACHE_ERROR":              "缓存错误",
	"CACHE_TIMEOUT":            "缓存超时",
	"PERMISSION_DENIED":        "权限不足",
	"INSUFFICIENT_PERMISSIONS": "权限不足",
	"ACCESS_DENIED":            "访问被拒绝",
	"QUOTA_EXCEEDED":           "配额超限",
	"RATE_LIMIT_EXCEEDED":      "频率限制",
	"STORAGE_QUOTA_EXCEEDED":   "存储配额超限",
}

// GetShareDBLocalizedMessage 获取 ShareDB 本地化错误消息
func GetShareDBLocalizedMessage(code string) string {
	if msg, exists := ShareDBErrorCodeMap[code]; exists {
		return msg
	}
	return "未知错误"
}
