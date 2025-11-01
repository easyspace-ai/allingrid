package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm/logger"

	appLogger "github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// SQLLogger 自定义SQL日志记录器
type SQLLogger struct {
	zapLogger *zap.Logger
	config    logger.Config
}

// NewSQLLogger 创建新的SQL日志记录器
func NewSQLLogger(zapLogger *zap.Logger, config logger.Config) *SQLLogger {
	return &SQLLogger{
		zapLogger: zapLogger,
		config:    config,
	}
}

// LogMode 设置日志模式
func (l *SQLLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.config.LogLevel = level
	return &newLogger
}

// Info 记录信息日志
func (l *SQLLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Info {
		l.zapLogger.Info(fmt.Sprintf(msg, data...))
	}
}

// Warn 记录警告日志
func (l *SQLLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Warn {
		l.zapLogger.Warn(fmt.Sprintf(msg, data...))
	}
}

// Error 记录错误日志
func (l *SQLLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Error {
		l.zapLogger.Error(fmt.Sprintf(msg, data...))
	}
}

// Trace 记录SQL跟踪日志
func (l *SQLLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.config.LogLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	switch {
	case err != nil && l.config.LogLevel >= logger.Error:
		sql, rows := fc()
		l.logSQL("❌ SQL Error", sql, rows, elapsed, err)
		// 同时写入独立的SQL日志文件
		if appLogger.SQLLogger != nil {
			appLogger.SQLLogger.LogSQL(sql, nil, elapsed, rows, err)
		}
	case elapsed > l.config.SlowThreshold && l.config.SlowThreshold != 0 && l.config.LogLevel >= logger.Warn:
		sql, rows := fc()
		l.logSQL("🐌 Slow Query", sql, rows, elapsed, nil)
		// 同时写入独立的SQL日志文件
		if appLogger.SQLLogger != nil {
			appLogger.SQLLogger.LogSQL(sql, nil, elapsed, rows, nil)
		}
	case l.config.LogLevel == logger.Info:
		sql, rows := fc()
		l.logSQL("🔍 SQL Query", sql, rows, elapsed, nil)
		// 同时写入独立的SQL日志文件
		if appLogger.SQLLogger != nil {
			appLogger.SQLLogger.LogSQL(sql, nil, elapsed, rows, nil)
		}
	}
}

// logSQL 格式化并记录SQL查询
func (l *SQLLogger) logSQL(level, sql string, rows int64, elapsed time.Duration, err error) {
	// 清理和格式化SQL
	formattedSQL := l.formatSQL(sql)

	// 构建日志字段
	fields := []zap.Field{
		zap.String("sql", formattedSQL),
		zap.Int64("rows", rows),
		zap.Duration("duration", elapsed),
	}

	if err != nil {
		fields = append(fields, zap.Error(err))
	}

	// 根据查询类型使用不同的日志级别
	if strings.Contains(strings.ToUpper(sql), "SELECT") {
		l.zapLogger.Info(level, fields...)
	} else if strings.Contains(strings.ToUpper(sql), "INSERT") {
		l.zapLogger.Info("➕ "+level, fields...)
	} else if strings.Contains(strings.ToUpper(sql), "UPDATE") {
		l.zapLogger.Info("✏️ "+level, fields...)
	} else if strings.Contains(strings.ToUpper(sql), "DELETE") {
		l.zapLogger.Info("🗑️ "+level, fields...)
	} else {
		l.zapLogger.Info(level, fields...)
	}
}

// formatSQL 格式化SQL查询，使其更易读
func (l *SQLLogger) formatSQL(sql string) string {
	// 移除多余的空白字符
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")
	sql = strings.TrimSpace(sql)

	// 如果SQL太长，截断并添加省略号
	if len(sql) > 200 {
		sql = sql[:200] + "..."
	}

	return sql
}
