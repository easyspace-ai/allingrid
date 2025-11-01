package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mattn/go-sqlite3"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	fieldRepo "github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// PostgreSQL 错误码
const (
	PgUniqueViolation  = "23505" // 唯一性约束违反
	PgNotNullViolation = "23502" // 非空约束违反
	PgForeignKey       = "23503" // 外键约束违反
	PgCheckViolation   = "23514" // 检查约束违反
	PgDeadlock         = "40P01" // 死锁
)

// SQLite 错误码
const (
	SqliteConstraint        = 19   // 约束违反
	SqliteConstraintUnique  = 2067 // 唯一性约束
	SqliteConstraintNotNull = 1299 // 非空约束
	SqliteBusy              = 5    // 数据库锁定
)

// ConstraintError 约束错误详情
type ConstraintError struct {
	Type       string                 // unique, notNull, foreignKey, check
	Message    string                 // 友好的错误消息
	FieldName  string                 // 字段名称
	FieldID    string                 // 字段ID
	TableName  string                 // 表名
	Value      interface{}            // 违规值（如果有）
	Constraint string                 // 约束名称
	RawError   error                  // 原始错误
	Details    map[string]interface{} // 额外详情
}

// Error 实现 error 接口
func (e *ConstraintError) Error() string {
	return e.Message
}

// HandleDBConstraintError 处理数据库约束错误
// 将底层数据库错误转换为友好的应用层错误
func HandleDBConstraintError(err error, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) error {
	if err == nil {
		return nil
	}

	// 尝试解析 PostgreSQL 错误
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return handlePostgresError(pgErr, tableID, fieldRepo, ctx)
	}

	// 尝试解析 SQLite 错误
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		return handleSQLiteError(sqliteErr, tableID, fieldRepo, ctx)
	}

	// 如果是 GORM 错误，尝试提取底层错误
	if gormErr, ok := err.(interface{ Unwrap() error }); ok {
		return HandleDBConstraintError(gormErr.Unwrap(), tableID, fieldRepo, ctx)
	}

	// 无法识别的错误，直接返回
	return err
}

// handlePostgresError 处理 PostgreSQL 错误
func handlePostgresError(pgErr *pgconn.PgError, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) error {
	switch pgErr.Code {
	case PgUniqueViolation:
		return handleUniqueViolation(pgErr, tableID, fieldRepo, ctx)

	case PgNotNullViolation:
		return handleNotNullViolation(pgErr, tableID, fieldRepo, ctx)

	case PgForeignKey:
		return handleForeignKeyViolation(pgErr)

	case PgCheckViolation:
		return handleCheckViolation(pgErr)

	case PgDeadlock:
		return errors.ErrDatabaseOperation.WithDetails(map[string]interface{}{
			"type":    "deadlock",
			"message": "检测到死锁，请重试",
			"detail":  pgErr.Detail,
		})

	default:
		// 其他 PostgreSQL 错误
		logger.Error("未处理的PostgreSQL错误",
			logger.String("code", pgErr.Code),
			logger.String("message", pgErr.Message),
			logger.String("detail", pgErr.Detail))
		return errors.ErrDatabaseOperation.WithDetails(pgErr.Message)
	}
}

// handleSQLiteError 处理 SQLite 错误
func handleSQLiteError(sqliteErr sqlite3.Error, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) error {
	switch sqliteErr.ExtendedCode {
	case SqliteConstraintUnique:
		// SQLite 唯一性约束错误
		fieldName := extractFieldNameFromSQLiteError(sqliteErr.Error())
		field := findFieldByDBName(fieldName, tableID, fieldRepo, ctx)

		if field != nil {
			return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
				"type":       "unique",
				"message":    fmt.Sprintf("字段【%s】的值重复，请修改后重试", field.Name().String()),
				"field_id":   field.ID().String(),
				"field_name": field.Name().String(),
			})
		}

		return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
			"type":    "unique",
			"message": "字段值重复",
			"detail":  sqliteErr.Error(),
		})

	case SqliteConstraintNotNull:
		// SQLite 非空约束错误
		fieldName := extractFieldNameFromSQLiteError(sqliteErr.Error())
		field := findFieldByDBName(fieldName, tableID, fieldRepo, ctx)

		if field != nil {
			return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
				"type":       "notNull",
				"message":    fmt.Sprintf("字段【%s】不能为空", field.Name().String()),
				"field_id":   field.ID().String(),
				"field_name": field.Name().String(),
			})
		}

		return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
			"type":    "notNull",
			"message": "必填字段不能为空",
			"detail":  sqliteErr.Error(),
		})

	case SqliteBusy:
		return errors.ErrDatabaseOperation.WithDetails(map[string]interface{}{
			"type":    "locked",
			"message": "数据库被锁定，请稍后重试",
		})

	default:
		logger.Error("未处理的SQLite错误",
			logger.Int("code", int(sqliteErr.Code)),
			logger.Int("extended_code", int(sqliteErr.ExtendedCode)),
			logger.String("message", sqliteErr.Error()))
		return errors.ErrDatabaseOperation.WithDetails(sqliteErr.Error())
	}
}

// handleUniqueViolation 处理唯一性约束违反
func handleUniqueViolation(pgErr *pgconn.PgError, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) error {
	// 从约束名称中提取字段信息
	fieldName := extractFieldNameFromConstraint(pgErr.ConstraintName)

	// 查询字段信息
	field := findFieldByDBName(fieldName, tableID, fieldRepo, ctx)

	if field != nil {
		return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
			"type":       "unique",
			"message":    fmt.Sprintf("字段【%s】的值重复，请修改后重试", field.Name().String()),
			"field_id":   field.ID().String(),
			"field_name": field.Name().String(),
			"constraint": pgErr.ConstraintName,
			"detail":     pgErr.Detail,
		})
	}

	// 无法识别字段，返回通用错误
	return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
		"type":       "unique",
		"message":    "字段值重复，请检查并修改",
		"constraint": pgErr.ConstraintName,
		"detail":     pgErr.Detail,
	})
}

// handleNotNullViolation 处理非空约束违反
func handleNotNullViolation(pgErr *pgconn.PgError, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) error {
	// 从错误消息中提取字段名称
	fieldName := extractFieldNameFromMessage(pgErr.Message)

	// 查询字段信息
	field := findFieldByDBName(fieldName, tableID, fieldRepo, ctx)

	if field != nil {
		return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
			"type":       "notNull",
			"message":    fmt.Sprintf("字段【%s】不能为空", field.Name().String()),
			"field_id":   field.ID().String(),
			"field_name": field.Name().String(),
			"detail":     pgErr.Detail,
		})
	}

	// 无法识别字段，返回通用错误
	return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
		"type":    "notNull",
		"message": "必填字段不能为空",
		"detail":  pgErr.Message,
	})
}

// handleForeignKeyViolation 处理外键约束违反
func handleForeignKeyViolation(pgErr *pgconn.PgError) error {
	return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
		"type":       "foreignKey",
		"message":    "关联的记录不存在或已被删除",
		"constraint": pgErr.ConstraintName,
		"detail":     pgErr.Detail,
	})
}

// handleCheckViolation 处理检查约束违反
func handleCheckViolation(pgErr *pgconn.PgError) error {
	return errors.ErrValidationFailed.WithDetails(map[string]interface{}{
		"type":       "check",
		"message":    "字段值不符合约束条件",
		"constraint": pgErr.ConstraintName,
		"detail":     pgErr.Detail,
	})
}

// extractFieldNameFromConstraint 从约束名称中提取字段名
// 约束名称格式: {table_name}_{field_name}_key 或 {table_name}_{field_name}_unique
func extractFieldNameFromConstraint(constraintName string) string {
	if constraintName == "" {
		return ""
	}

	// 移除后缀 _key, _unique, _idx 等
	parts := strings.Split(constraintName, "_")
	if len(parts) < 2 {
		return ""
	}

	// 最后一个部分通常是 key/unique/idx
	// 倒数第二个部分可能是字段 ID
	if len(parts) >= 2 {
		// 查找可能是字段名的部分（通常包含 fld_ 前缀）
		for i := len(parts) - 2; i >= 0; i-- {
			if strings.HasPrefix(parts[i], "fld_") {
				return parts[i]
			}
		}

		// 如果没有找到 fld_ 前缀，返回倒数第二个部分
		return parts[len(parts)-2]
	}

	return ""
}

// extractFieldNameFromMessage 从错误消息中提取字段名
// PostgreSQL 格式: column "field_name" violates not-null constraint
func extractFieldNameFromMessage(message string) string {
	// 使用正则表达式提取字段名
	re := regexp.MustCompile(`column "([^"]+)"`)
	matches := re.FindStringSubmatch(message)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// extractFieldNameFromSQLiteError 从 SQLite 错误消息中提取字段名
func extractFieldNameFromSQLiteError(errMsg string) string {
	// SQLite 格式可能有多种，尝试常见模式
	// 例如: "UNIQUE constraint failed: table.field"
	// 或: "NOT NULL constraint failed: table.field"

	re := regexp.MustCompile(`constraint failed: [^.]+\.([^\s]+)`)
	matches := re.FindStringSubmatch(errMsg)
	if len(matches) >= 2 {
		return matches[1]
	}

	return ""
}

// findFieldByDBName 根据数据库字段名查询字段信息
func findFieldByDBName(dbFieldName string, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) *fieldEntity.Field {
	if dbFieldName == "" || tableID == "" || fieldRepo == nil {
		return nil
	}

	// 查询表的所有字段
	fields, err := fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		logger.Warn("查询字段失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
		return nil
	}

	// 查找匹配的字段
	for _, field := range fields {
		if field.DBFieldName().String() == dbFieldName {
			return field
		}
	}

	return nil
}

// BatchConstraintErrors 批量约束错误
type BatchConstraintErrors struct {
	Errors []ConstraintError
}

// Error 实现 error 接口
func (e *BatchConstraintErrors) Error() string {
	if len(e.Errors) == 0 {
		return "批量操作约束错误"
	}

	messages := make([]string, len(e.Errors))
	for i, err := range e.Errors {
		messages[i] = err.Message
	}

	return fmt.Sprintf("批量操作失败：%s", strings.Join(messages, "; "))
}

// HandleBatchConstraintError 处理批量操作的约束错误
func HandleBatchConstraintError(err error, tableID string, fieldRepo fieldRepo.FieldRepository, ctx context.Context) error {
	if err == nil {
		return nil
	}

	// 批量操作通常只会抛出第一个错误
	// 我们将其转换为友好的错误消息
	return HandleDBConstraintError(err, tableID, fieldRepo, ctx)
}

// IsDeadlock 检查是否为死锁错误
func IsDeadlock(err error) bool {
	if err == nil {
		return false
	}

	// PostgreSQL 死锁
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == PgDeadlock
	}

	// SQLite 锁定
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		return sqliteErr.Code == SqliteBusy
	}

	// 尝试从嵌套错误中检查
	if gormErr, ok := err.(interface{ Unwrap() error }); ok {
		return IsDeadlock(gormErr.Unwrap())
	}

	return false
}

// IsConstraintError 检查是否为约束错误
func IsConstraintError(err error) bool {
	if err == nil {
		return false
	}

	// PostgreSQL 约束错误
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == PgUniqueViolation ||
			pgErr.Code == PgNotNullViolation ||
			pgErr.Code == PgForeignKey ||
			pgErr.Code == PgCheckViolation
	}

	// SQLite 约束错误
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		return sqliteErr.Code == SqliteConstraint
	}

	// 尝试从嵌套错误中检查
	if gormErr, ok := err.(interface{ Unwrap() error }); ok {
		return IsConstraintError(gormErr.Unwrap())
	}

	return false
}
