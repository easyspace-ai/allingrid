package errors

import "net/http"

// 数字错误码规范：code = http_status * 1000 + subcode(0-999)
// 例如：
// 200000 = OK
// 400001 = VALIDATION_FAILED
// 401001 = INVALID_TOKEN
// 404001 = NOT_FOUND
// 500001 = INTERNAL_SERVER_ERROR

const (
	CodeOK = 200000

	// 4xx 客户端错误
	CodeBadRequest       = 400002
	CodeValidationFailed = 400001

	// 新增: 数据验证相关 (400xxx)
	CodeInvalidFieldValue = 400101 // 字段值无效
	CodeInvalidFieldType  = 400102 // 字段类型无效
	CodeFieldRequired     = 400103 // 必填字段缺失
	CodeFieldTooLong      = 400104 // 字段过长
	CodeFieldOutOfRange   = 400105 // 字段超出范围
	CodeInvalidEmail      = 400106 // 无效邮箱
	CodeInvalidURL        = 400107 // 无效URL
	CodeInvalidPhone      = 400108 // 无效手机号
	CodeDuplicateValue    = 400109 // 重复值
	CodeInvalidPattern    = 400110 // 格式不匹配
	CodeFieldNotExists    = 400111 // 字段不存在于表中

	CodeUnauthorized       = 401000
	CodeInvalidToken       = 401001
	CodeTokenExpired       = 401002
	CodeInvalidCredentials = 401003

	CodeForbidden = 403001

	CodeNotFound = 404001
	// 领域细分（示例）
	CodeUserNotFound   = 404101
	CodeSpaceNotFound  = 404201
	CodeBaseNotFound   = 404301
	CodeTableNotFound  = 404401
	CodeFieldNotFound  = 404501
	CodeRecordNotFound = 404601
	CodeViewNotFound   = 404701

	CodeConflict = 409001
	// 新增: 资源冲突细分 (409xxx)
	CodeDuplicateField  = 409101 // 字段名重复
	CodeDuplicateRecord = 409102 // 记录重复
	CodeDuplicateView   = 409103 // 视图名重复

	CodeTooManyReq = 429001
)

const (
	// 5xx 服务端错误
	CodeInternalError       = 500001
	CodeDatabaseOperation   = 500101
	CodeDatabaseQuery       = 500102
	CodeDatabaseTransaction = 500103
	CodeCacheOperation      = 500201
	CodeQueueOperation      = 500301
	CodeFileUploadFailed    = 500401
	CodeTaskFailed          = 500901

	CodeNotImplemented     = 501001
	CodeServiceUnavailable = 503001
	CodeTimeout            = 504001
)

// string 业务码到数字码映射
var stringToNumeric = map[string]int{
	// 通用
	"OK":                    CodeOK,
	"BAD_REQUEST":           CodeBadRequest,
	"INVALID_REQUEST":       CodeBadRequest,
	"VALIDATION_FAILED":     CodeValidationFailed,
	"UNAUTHORIZED":          CodeUnauthorized,
	"FORBIDDEN":             CodeForbidden,
	"NOT_FOUND":             CodeNotFound,
	"CONFLICT":              CodeConflict,
	"TOO_MANY_REQUESTS":     CodeTooManyReq,
	"INTERNAL_SERVER_ERROR": CodeInternalError,

	// 用户
	"USER_NOT_FOUND":      CodeUserNotFound,
	"USER_EXISTS":         CodeConflict,
	"INVALID_CREDENTIALS": CodeInvalidCredentials,
	"INVALID_PASSWORD":    CodeBadRequest,
	"EMAIL_EXISTS":        CodeConflict,
	"PHONE_EXISTS":        CodeConflict,
	"USER_DEACTIVATED":    CodeForbidden,
	"USER_DELETED":        CodeForbidden,

	// 认证
	"INVALID_TOKEN":         CodeInvalidToken,
	"TOKEN_EXPIRED":         CodeTokenExpired,
	"REFRESH_TOKEN_EXPIRED": CodeTokenExpired,
	"INVALID_REFRESH_TOKEN": CodeInvalidToken,

	// 空间
	"SPACE_NOT_FOUND":      CodeSpaceNotFound,
	"SPACE_EXISTS":         CodeConflict,
	"SPACE_NOT_ACCESSIBLE": CodeForbidden,

	// 基础
	"BASE_NOT_FOUND":      CodeBaseNotFound,
	"BASE_EXISTS":         CodeConflict,
	"BASE_NOT_ACCESSIBLE": CodeForbidden,

	// 表格
	"TABLE_NOT_FOUND":      CodeTableNotFound,
	"TABLE_EXISTS":         CodeConflict,
	"TABLE_NOT_ACCESSIBLE": CodeForbidden,

	// 字段
	"FIELD_NOT_FOUND":    CodeFieldNotFound,
	"FIELD_EXISTS":       CodeConflict,
	"INVALID_FIELD_TYPE": CodeBadRequest,
	"FIELD_IN_USE":       409501,

	// 记录
	"RECORD_NOT_FOUND":    CodeRecordNotFound,
	"RECORD_EXISTS":       CodeConflict,
	"INVALID_RECORD_DATA": CodeBadRequest,

	// 视图
	"VIEW_NOT_FOUND":    CodeViewNotFound,
	"VIEW_EXISTS":       CodeConflict,
	"INVALID_VIEW_TYPE": CodeBadRequest,

	// 文件
	"FILE_NOT_FOUND":     CodeNotFound,
	"FILE_TOO_LARGE":     CodeBadRequest,
	"INVALID_FILE_TYPE":  CodeBadRequest,
	"FILE_UPLOAD_FAILED": CodeFileUploadFailed,

	// 导入导出
	"IMPORT_FAILED":       CodeBadRequest,
	"EXPORT_FAILED":       CodeInternalError,
	"INVALID_FILE_FORMAT": CodeBadRequest,

	// 数据库
	"DATABASE_CONNECTION_ERROR":  CodeInternalError,
	"DATABASE_QUERY_ERROR":       CodeDatabaseQuery,
	"DATABASE_TRANSACTION_ERROR": CodeDatabaseTransaction,
	"DATABASE_OPERATION_ERROR":   CodeDatabaseOperation,
	"TIMEOUT_ERROR":              CodeTimeout,

	// 缓存
	"CACHE_CONNECTION_ERROR": CodeInternalError,
	"CACHE_OPERATION_ERROR":  CodeCacheOperation,

	// 队列
	"QUEUE_CONNECTION_ERROR": CodeInternalError,
	"QUEUE_OPERATION_ERROR":  CodeQueueOperation,
	"TASK_FAILED":            CodeTaskFailed,

	// 验证
	"REQUIRED_FIELD":  CodeBadRequest,
	"INVALID_FORMAT":  CodeBadRequest,
	"INVALID_VALUE":   CodeBadRequest,
	"RESOURCE_EXISTS": CodeConflict,

	// 新增: 验证错误
	"FIELD_REQUIRED":      CodeFieldRequired,
	"INVALID_FIELD_VALUE": CodeInvalidFieldValue,
	"FIELD_TYPE_MISMATCH": CodeInvalidFieldType,
	"FIELD_TOO_LONG":      CodeFieldTooLong,
	"FIELD_OUT_OF_RANGE":  CodeFieldOutOfRange,
	"INVALID_EMAIL":       CodeInvalidEmail,
	"INVALID_URL":         CodeInvalidURL,
	"INVALID_PHONE":       CodeInvalidPhone,
	"DUPLICATE_VALUE":     CodeDuplicateValue,
	"INVALID_PATTERN":     CodeInvalidPattern,
	"FIELD_NOT_EXISTS":    CodeFieldNotExists,

	// 新增: 资源冲突
	"DUPLICATE_FIELD":  CodeDuplicateField,
	"DUPLICATE_RECORD": CodeDuplicateRecord,
	"DUPLICATE_VIEW":   CodeDuplicateView,

	// 业务
	"OPERATION_NOT_ALLOWED": CodeForbidden,
	"RESOURCE_IN_USE":       CodeConflict,
	"QUOTA_EXCEEDED":        CodeForbidden,
	"FEATURE_NOT_AVAILABLE": CodeServiceUnavailable,
	"NOT_IMPLEMENTED":       CodeNotImplemented,
}

// NumericCodeFromString 返回字符串业务码对应的数字码。
// 若未命中映射，则回退为 httpStatus*1000（subcode=000）。
func NumericCodeFromString(code string, httpStatus int) int {
	if v, ok := stringToNumeric[code]; ok {
		return v
	}
	if httpStatus == 0 {
		httpStatus = http.StatusInternalServerError
	}
	return httpStatus * 1000
}
