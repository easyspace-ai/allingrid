package errors

// ShareDB 错误代码常量
const (
	// 认证相关错误
	ErrUnauthorizedShare = "UNAUTHORIZED_SHARE"

	// 文档相关错误
	ErrDocumentNotFound  = "DOCUMENT_NOT_FOUND"
	ErrDocumentExists    = "DOCUMENT_EXISTS"
	ErrDocumentLocked    = "DOCUMENT_LOCKED"
	ErrDocumentCorrupted = "DOCUMENT_CORRUPTED"

	// 视图相关错误
	ErrViewInvalid          = "VIEW_INVALID"
	ErrViewPermissionDenied = "VIEW_PERMISSION_DENIED"

	// 字段相关错误
	ErrFieldInvalid = "FIELD_INVALID"

	// 记录相关错误
	ErrRecordInvalid          = "RECORD_INVALID"
	ErrRecordPermissionDenied = "RECORD_PERMISSION_DENIED"

	// 表格相关错误
	ErrTableInvalid          = "TABLE_INVALID"
	ErrTablePermissionDenied = "TABLE_PERMISSION_DENIED"

	// 操作相关错误
	ErrVersionMismatch   = "VERSION_MISMATCH"
	ErrOperationRejected = "OPERATION_REJECTED"
	ErrOperationInvalid  = "OPERATION_INVALID"
	ErrOperationConflict = "OPERATION_CONFLICT"
	ErrOperationTimeout  = "OPERATION_TIMEOUT"

	// 网络相关错误
	ErrNetworkError      = "NETWORK_ERROR"
	ErrConnectionLost    = "CONNECTION_LOST"
	ErrConnectionTimeout = "CONNECTION_TIMEOUT"
	ErrConnectionRefused = "CONNECTION_REFUSED"

	// 服务器相关错误
	ErrServerError       = "SERVER_ERROR"
	ErrServerOverloaded  = "SERVER_OVERLOADED"
	ErrServerMaintenance = "SERVER_MAINTENANCE"
	ErrServerUnavailable = "SERVER_UNAVAILABLE"

	// 数据相关错误
	ErrDataCorrupted = "DATA_CORRUPTED"
	ErrDataInvalid   = "DATA_INVALID"
	ErrDataTooLarge  = "DATA_TOO_LARGE"
	ErrDataNotFound  = "DATA_NOT_FOUND"

	// 缓存相关错误
	ErrCacheMiss    = "CACHE_MISS"
	ErrCacheError   = "CACHE_ERROR"
	ErrCacheTimeout = "CACHE_TIMEOUT"

	// 权限相关错误
	ErrPermissionDenied        = "PERMISSION_DENIED"
	ErrInsufficientPermissions = "INSUFFICIENT_PERMISSIONS"
	ErrAccessDenied            = "ACCESS_DENIED"

	// 配额相关错误
	ErrRateLimitExceeded    = "RATE_LIMIT_EXCEEDED"
	ErrStorageQuotaExceeded = "STORAGE_QUOTA_EXCEEDED"
)

// ShareDBError ShareDB 错误结构
type ShareDBError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details string                 `json:"details,omitempty"`
	Context map[string]interface{} `json:"context,omitempty"`
}

// Error 实现 error 接口
func (e *ShareDBError) Error() string {
	if e.Details != "" {
		return e.Message + ": " + e.Details
	}
	return e.Message
}

// NewShareDBError 创建新的 ShareDB 错误
func NewShareDBError(code, message string) *ShareDBError {
	return &ShareDBError{
		Code:    code,
		Message: message,
	}
}

// NewShareDBErrorWithDetails 创建带详情的 ShareDB 错误
func NewShareDBErrorWithDetails(code, message, details string) *ShareDBError {
	return &ShareDBError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

// NewShareDBErrorWithContext 创建带上下文的 ShareDB 错误
func NewShareDBErrorWithContext(code, message string, context map[string]interface{}) *ShareDBError {
	return &ShareDBError{
		Code:    code,
		Message: message,
		Context: context,
	}
}

// IsUnauthorized 检查是否为认证错误
func (e *ShareDBError) IsUnauthorized() bool {
	return e.Code == "UNAUTHORIZED" || e.Code == ErrUnauthorizedShare ||
		e.Code == "TOKEN_EXPIRED" || e.Code == "INVALID_TOKEN"
}

// IsDocumentError 检查是否为文档相关错误
func (e *ShareDBError) IsDocumentError() bool {
	return e.Code == ErrDocumentNotFound || e.Code == ErrDocumentExists ||
		e.Code == ErrDocumentLocked || e.Code == ErrDocumentCorrupted
}

// IsOperationError 检查是否为操作相关错误
func (e *ShareDBError) IsOperationError() bool {
	return e.Code == ErrVersionMismatch || e.Code == ErrOperationRejected ||
		e.Code == ErrOperationInvalid || e.Code == ErrOperationConflict ||
		e.Code == ErrOperationTimeout
}

// IsNetworkError 检查是否为网络相关错误
func (e *ShareDBError) IsNetworkError() bool {
	return e.Code == ErrNetworkError || e.Code == ErrConnectionLost ||
		e.Code == ErrConnectionTimeout || e.Code == ErrConnectionRefused
}

// IsServerError 检查是否为服务器相关错误
func (e *ShareDBError) IsServerError() bool {
	return e.Code == ErrServerError || e.Code == ErrServerOverloaded ||
		e.Code == ErrServerMaintenance || e.Code == ErrServerUnavailable
}

// IsPermissionError 检查是否为权限相关错误
func (e *ShareDBError) IsPermissionError() bool {
	return e.Code == ErrPermissionDenied || e.Code == ErrInsufficientPermissions ||
		e.Code == ErrAccessDenied || e.Code == ErrViewPermissionDenied ||
		e.Code == ErrRecordPermissionDenied || e.Code == ErrTablePermissionDenied
}

// IsQuotaError 检查是否为配额相关错误
func (e *ShareDBError) IsQuotaError() bool {
	return e.Code == "QUOTA_EXCEEDED" || e.Code == ErrRateLimitExceeded ||
		e.Code == ErrStorageQuotaExceeded
}

// ShouldRetry 检查是否应该重试
func (e *ShareDBError) ShouldRetry() bool {
	return e.Code == ErrNetworkError || e.Code == ErrConnectionLost ||
		e.Code == ErrConnectionTimeout || e.Code == ErrServerOverloaded ||
		e.Code == ErrOperationTimeout || e.Code == ErrCacheTimeout
}

// ShouldRefreshAuth 检查是否应该刷新认证
func (e *ShareDBError) ShouldRefreshAuth() bool {
	return e.Code == "TOKEN_EXPIRED" || e.Code == "INVALID_TOKEN"
}

// ShouldRedirectToLogin 检查是否应该重定向到登录页
func (e *ShareDBError) ShouldRedirectToLogin() bool {
	return e.Code == "UNAUTHORIZED" || e.Code == ErrUnauthorizedShare
}

// GetHTTPStatus 获取对应的 HTTP 状态码
func (e *ShareDBError) GetHTTPStatus() int {
	switch {
	case e.IsUnauthorized():
		return 401
	case e.IsPermissionError():
		return 403
	case e.Code == ErrDocumentNotFound || e.Code == "VIEW_NOT_FOUND" ||
		e.Code == "FIELD_NOT_FOUND" || e.Code == "RECORD_NOT_FOUND" ||
		e.Code == "TABLE_NOT_FOUND":
		return 404
	case e.Code == ErrDocumentExists || e.Code == "VIEW_EXISTS" ||
		e.Code == "FIELD_EXISTS" || e.Code == "RECORD_EXISTS" ||
		e.Code == "TABLE_EXISTS":
		return 409
	case e.Code == ErrVersionMismatch:
		return 409
	case e.IsQuotaError():
		return 429
	case e.IsServerError():
		return 500
	case e.IsNetworkError():
		return 502
	default:
		return 400
	}
}
