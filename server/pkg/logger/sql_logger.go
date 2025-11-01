package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	// SQLLogger SQL日志实例
	SQLLogger *SQLFileLogger
	sqlOnce   sync.Once
)

// SQLFileLogger SQL文件日志记录器
type SQLFileLogger struct {
	writer io.Writer
	mu     sync.Mutex
}

// SQLLoggerConfig SQL日志配置
type SQLLoggerConfig struct {
	Enabled    bool
	OutputPath string
	MaxSize    int  // 单个文件最大大小(MB)
	MaxBackups int  // 保留的旧日志文件数量
	MaxAge     int  // 保留的最大天数
	Compress   bool // 是否压缩
}

// InitSQLLogger 初始化SQL日志记录器
func InitSQLLogger(config SQLLoggerConfig) error {
	var err error
	sqlOnce.Do(func() {
		if !config.Enabled {
			// 如果禁用，使用丢弃写入器
			SQLLogger = &SQLFileLogger{
				writer: io.Discard,
			}
			return
		}

		// 确保日志目录存在
		dir := filepath.Dir(config.OutputPath)
		if err = os.MkdirAll(dir, 0755); err != nil {
			return
		}

		// 使用 lumberjack 实现日志轮转
		writer := &lumberjack.Logger{
			Filename:   config.OutputPath,
			MaxSize:    config.MaxSize,    // MB
			MaxBackups: config.MaxBackups, // 保留的旧文件数量
			MaxAge:     config.MaxAge,     // 保留天数
			Compress:   config.Compress,   // 是否压缩
			LocalTime:  true,              // 使用本地时间
		}

		SQLLogger = &SQLFileLogger{
			writer: writer,
		}

		// 写入初始化标记
		SQLLogger.LogInfo("========================================")
		SQLLogger.LogInfo(fmt.Sprintf("SQL日志初始化完成 - %s", time.Now().Format("2006-01-02 15:04:05")))
		SQLLogger.LogInfo("========================================")
		SQLLogger.LogInfo("")
	})

	return err
}

// LogSQL 记录SQL查询
func (l *SQLFileLogger) LogSQL(sql string, args []interface{}, duration time.Duration, rows int64, err error) {
	if l == nil || l.writer == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// 构建日志内容
	var logContent string

	if err != nil {
		logContent = fmt.Sprintf("[%s] ❌ SQL执行失败 (耗时: %v)\n", timestamp, duration)
		logContent += fmt.Sprintf("SQL: %s\n", sql)
		if len(args) > 0 {
			logContent += fmt.Sprintf("参数: %v\n", args)
		}
		logContent += fmt.Sprintf("错误: %v\n", err)
	} else {
		icon := "✅"
		if duration > 200*time.Millisecond {
			icon = "🐌" // 慢查询
		}
		logContent = fmt.Sprintf("[%s] %s SQL执行成功 (耗时: %v, 影响行数: %d)\n", timestamp, icon, duration, rows)
		logContent += fmt.Sprintf("%s\n", sql)
		if len(args) > 0 {
			logContent += fmt.Sprintf("-- 参数: %v\n", args)
		}
	}

	logContent += "\n"

	// 写入日志
	_, _ = l.writer.Write([]byte(logContent))
}

// LogInfo 记录信息日志
func (l *SQLFileLogger) LogInfo(msg string) {
	if l == nil || l.writer == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logContent := fmt.Sprintf("[%s] %s\n", timestamp, msg)
	_, _ = l.writer.Write([]byte(logContent))
}

// LogWarn 记录警告日志
func (l *SQLFileLogger) LogWarn(msg string) {
	if l == nil || l.writer == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logContent := fmt.Sprintf("[%s] ⚠️  %s\n", timestamp, msg)
	_, _ = l.writer.Write([]byte(logContent))
}

// LogError 记录错误日志
func (l *SQLFileLogger) LogError(msg string, err error) {
	if l == nil || l.writer == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logContent := fmt.Sprintf("[%s] ❌ %s: %v\n", timestamp, msg, err)
	_, _ = l.writer.Write([]byte(logContent))
}

// Close 关闭日志记录器
func (l *SQLFileLogger) Close() error {
	if l == nil || l.writer == nil {
		return nil
	}

	if closer, ok := l.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
