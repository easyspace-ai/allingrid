package errors

import (
	"fmt"
	"net/http"
)

// AppError 应用错误结构
type AppError struct {
	Code       string      `json:"code"`
	Message    string      `json:"message"`
	Details    interface{} `json:"details,omitempty"`
	HTTPStatus int         `json:"-"`
}

func (e *AppError) Error() string {
	return e.Message
}

// ErrorResponse HTTP错误响应结构
type ErrorResponse struct {
	Error   string      `json:"error"`
	Code    string      `json:"code,omitempty"`
	Details interface{} `json:"details,omitempty"`
	TraceID string      `json:"trace_id,omitempty"`
}

// New 创建新的应用错误
func New(code, message string, httpStatus int) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
	}
}

// Newf 创建格式化的应用错误
func Newf(code string, httpStatus int, format string, args ...interface{}) *AppError {
	return &AppError{
		Code:       code,
		Message:    fmt.Sprintf(format, args...),
		HTTPStatus: httpStatus,
	}
}

// WithDetails 添加错误详情
func (e *AppError) WithDetails(details interface{}) *AppError {
	e.Details = details
	return e
}

// WithMessage 添加错误消息
func (e *AppError) WithMessage(message string) *AppError {
	e.Message = message
	return e
}

// 预定义错误
var (
	// 通用错误
	ErrInternalServer  = New("INTERNAL_SERVER_ERROR", "服务器内部错误", http.StatusInternalServerError)
	ErrBadRequest      = New("BAD_REQUEST", "请求参数错误", http.StatusBadRequest)
	ErrUnauthorized    = New("UNAUTHORIZED", "未授权访问", http.StatusUnauthorized)
	ErrForbidden       = New("FORBIDDEN", "权限不足", http.StatusForbidden)
	ErrNotFound        = New("NOT_FOUND", "资源不存在", http.StatusNotFound)
	ErrConflict        = New("CONFLICT", "资源冲突", http.StatusConflict)
	ErrTooManyRequests = New("TOO_MANY_REQUESTS", "请求过于频繁", http.StatusTooManyRequests)

	// 用户相关错误
	ErrUserNotFound       = New("USER_NOT_FOUND", "用户不存在", http.StatusNotFound)
	ErrUserExists         = New("USER_EXISTS", "用户已存在", http.StatusConflict)
	ErrInvalidCredentials = New("INVALID_CREDENTIALS", "用户名或密码错误", http.StatusUnauthorized)
	ErrInvalidPassword    = New("INVALID_PASSWORD", "密码格式不正确", http.StatusBadRequest)
	ErrEmailExists        = New("EMAIL_EXISTS", "邮箱已存在", http.StatusConflict)
	ErrPhoneExists        = New("PHONE_EXISTS", "手机号已存在", http.StatusConflict)
	ErrUserDeactivated    = New("USER_DEACTIVATED", "用户已被禁用", http.StatusForbidden)
	ErrUserDeleted        = New("USER_DELETED", "用户已被删除", http.StatusForbidden)

	// 认证相关错误
	ErrInvalidToken        = New("INVALID_TOKEN", "无效的访问令牌", http.StatusUnauthorized)
	ErrTokenExpired        = New("TOKEN_EXPIRED", "访问令牌已过期", http.StatusUnauthorized)
	ErrRefreshTokenExpired = New("REFRESH_TOKEN_EXPIRED", "刷新令牌已过期", http.StatusUnauthorized)
	ErrInvalidRefreshToken = New("INVALID_REFRESH_TOKEN", "无效的刷新令牌", http.StatusUnauthorized)

	// 空间相关错误
	ErrSpaceNotFound      = New("SPACE_NOT_FOUND", "空间不存在", http.StatusNotFound)
	ErrSpaceExists        = New("SPACE_EXISTS", "空间已存在", http.StatusConflict)
	ErrSpaceNotAccessible = New("SPACE_NOT_ACCESSIBLE", "无权访问此空间", http.StatusForbidden)

	// 基础相关错误
	ErrBaseNotFound      = New("BASE_NOT_FOUND", "基础不存在", http.StatusNotFound)
	ErrBaseExists        = New("BASE_EXISTS", "基础已存在", http.StatusConflict)
	ErrBaseNotAccessible = New("BASE_NOT_ACCESSIBLE", "无权访问此基础", http.StatusForbidden)

	// 表格相关错误
	ErrTableNotFound      = New("TABLE_NOT_FOUND", "表格不存在", http.StatusNotFound)
	ErrTableExists        = New("TABLE_EXISTS", "表格已存在", http.StatusConflict)
	ErrTableNotAccessible = New("TABLE_NOT_ACCESSIBLE", "无权访问此表格", http.StatusForbidden)

	// 字段相关错误
	ErrFieldNotFound    = New("FIELD_NOT_FOUND", "字段不存在", http.StatusBadRequest) // ✅ 改为400，因为这是输入验证错误
	ErrFieldExists      = New("FIELD_EXISTS", "字段已存在", http.StatusConflict)
	ErrInvalidFieldType = New("INVALID_FIELD_TYPE", "无效的字段类型", http.StatusBadRequest)
	ErrFieldInUse       = New("FIELD_IN_USE", "字段正在使用中", http.StatusConflict)

	// 记录相关错误
	ErrRecordNotFound    = New("RECORD_NOT_FOUND", "记录不存在", http.StatusNotFound)
	ErrRecordExists      = New("RECORD_EXISTS", "记录已存在", http.StatusConflict)
	ErrInvalidRecordData = New("INVALID_RECORD_DATA", "记录数据格式错误", http.StatusBadRequest)

	// 视图相关错误
	ErrViewNotFound    = New("VIEW_NOT_FOUND", "视图不存在", http.StatusNotFound)
	ErrViewExists      = New("VIEW_EXISTS", "视图已存在", http.StatusConflict)
	ErrInvalidViewType = New("INVALID_VIEW_TYPE", "无效的视图类型", http.StatusBadRequest)

	// 文件相关错误
	ErrFileNotFound     = New("FILE_NOT_FOUND", "文件不存在", http.StatusNotFound)
	ErrFileTooLarge     = New("FILE_TOO_LARGE", "文件大小超出限制", http.StatusBadRequest)
	ErrInvalidFileType  = New("INVALID_FILE_TYPE", "不支持的文件类型", http.StatusBadRequest)
	ErrFileUploadFailed = New("FILE_UPLOAD_FAILED", "文件上传失败", http.StatusInternalServerError)

	// 导入导出错误
	ErrImportFailed      = New("IMPORT_FAILED", "数据导入失败", http.StatusBadRequest)
	ErrExportFailed      = New("EXPORT_FAILED", "数据导出失败", http.StatusInternalServerError)
	ErrInvalidFileFormat = New("INVALID_FILE_FORMAT", "不支持的文件格式", http.StatusBadRequest)

	// 数据库相关错误
	ErrDatabaseConnection  = New("DATABASE_CONNECTION_ERROR", "数据库连接错误", http.StatusInternalServerError)
	ErrDatabaseQuery       = New("DATABASE_QUERY_ERROR", "数据库查询错误", http.StatusInternalServerError)
	ErrDatabaseTransaction = New("DATABASE_TRANSACTION_ERROR", "数据库事务错误", http.StatusInternalServerError)
	ErrDatabaseOperation   = New("DATABASE_OPERATION_ERROR", "数据库操作错误", http.StatusInternalServerError)
	ErrTimeout             = New("TIMEOUT_ERROR", "操作超时", http.StatusRequestTimeout)

	// 缓存相关错误
	ErrCacheConnection = New("CACHE_CONNECTION_ERROR", "缓存连接错误", http.StatusInternalServerError)
	ErrCacheOperation  = New("CACHE_OPERATION_ERROR", "缓存操作错误", http.StatusInternalServerError)

	// 队列相关错误
	ErrQueueConnection = New("QUEUE_CONNECTION_ERROR", "队列连接错误", http.StatusInternalServerError)
	ErrQueueOperation  = New("QUEUE_OPERATION_ERROR", "队列操作错误", http.StatusInternalServerError)
	ErrTaskFailed      = New("TASK_FAILED", "任务执行失败", http.StatusInternalServerError)

	// 验证相关错误
	ErrValidationFailed = New("VALIDATION_FAILED", "数据验证失败", http.StatusBadRequest)
	ErrRequiredField    = New("REQUIRED_FIELD", "必填字段不能为空", http.StatusBadRequest)
	ErrInvalidFormat    = New("INVALID_FORMAT", "数据格式不正确", http.StatusBadRequest)
	ErrInvalidValue     = New("INVALID_VALUE", "数据值不正确", http.StatusBadRequest)
	ErrInvalidRequest   = New("INVALID_REQUEST", "请求参数错误", http.StatusBadRequest)
	ErrResourceExists   = New("RESOURCE_EXISTS", "资源已存在", http.StatusConflict)

	// 新增: 字段验证错误
	ErrFieldRequired     = New("FIELD_REQUIRED", "必填字段不能为空", http.StatusBadRequest)
	ErrInvalidFieldValue = New("INVALID_FIELD_VALUE", "字段值无效", http.StatusBadRequest)
	ErrFieldTypeMismatch = New("FIELD_TYPE_MISMATCH", "字段值类型不匹配", http.StatusBadRequest)
	ErrFieldTooLong      = New("FIELD_TOO_LONG", "字段长度超出限制", http.StatusBadRequest)
	ErrFieldOutOfRange   = New("FIELD_OUT_OF_RANGE", "字段值超出范围", http.StatusBadRequest)
	ErrInvalidEmail      = New("INVALID_EMAIL", "邮箱格式不正确", http.StatusBadRequest)
	ErrInvalidURL        = New("INVALID_URL", "URL格式不正确", http.StatusBadRequest)
	ErrInvalidPhone      = New("INVALID_PHONE", "手机号格式不正确", http.StatusBadRequest)
	ErrDuplicateValue    = New("DUPLICATE_VALUE", "字段值重复", http.StatusBadRequest)
	ErrInvalidPattern    = New("INVALID_PATTERN", "格式不匹配", http.StatusBadRequest)
	ErrFieldNotExists    = New("FIELD_NOT_EXISTS", "字段不存在", http.StatusBadRequest)

	// 新增: 资源冲突错误
	ErrDuplicateField  = New("DUPLICATE_FIELD", "字段名已存在", http.StatusConflict)
	ErrDuplicateRecord = New("DUPLICATE_RECORD", "记录已存在", http.StatusConflict)
	ErrDuplicateView   = New("DUPLICATE_VIEW", "视图名已存在", http.StatusConflict)

	// 业务逻辑错误
	ErrOperationNotAllowed = New("OPERATION_NOT_ALLOWED", "不允许此操作", http.StatusForbidden)
	ErrResourceInUse       = New("RESOURCE_IN_USE", "资源正在使用中", http.StatusConflict)
	ErrQuotaExceeded       = New("QUOTA_EXCEEDED", "配额已超出限制", http.StatusForbidden)
	ErrFeatureNotAvailable = New("FEATURE_NOT_AVAILABLE", "功能不可用", http.StatusServiceUnavailable)
	ErrNotImplemented      = New("NOT_IMPLEMENTED", "功能暂未实现", http.StatusNotImplemented)
)

// ValidationError 验证错误结构
type ValidationError struct {
	Field   string      `json:"field"`
	Message string      `json:"message"`
	Value   interface{} `json:"value,omitempty"`
}

// NewValidationError 创建验证错误
func NewValidationError(field, message string, value interface{}) *ValidationError {
	return &ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
	}
}

// Wrap 包装错误
func Wrap(err error, code, message string, httpStatus int) *AppError {
	appErr := &AppError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
	}

	if err != nil {
		appErr.Details = err.Error()
	}

	return appErr
}

// IsAppError 检查是否为应用错误
func IsAppError(err error) (*AppError, bool) {
	if appErr, ok := err.(*AppError); ok {
		return appErr, true
	}
	return nil, false
}

// GetHTTPStatus 获取HTTP状态码
func GetHTTPStatus(err error) int {
	if appErr, ok := IsAppError(err); ok {
		return appErr.HTTPStatus
	}
	return http.StatusInternalServerError
}

// 数字业务码常量
