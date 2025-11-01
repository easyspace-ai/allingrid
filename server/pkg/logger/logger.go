package logger

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Logger 全局日志实例
	Logger *zap.Logger
	// Sugar 语法糖日志实例
	Sugar *zap.SugaredLogger
)

// LoggerConfig 日志配置
type LoggerConfig struct {
	Level      string `json:"level"`      // debug, info, warn, error
	Format     string `json:"format"`     // json, console
	OutputPath string `json:"outputPath"` // stdout, stderr, file path
}

// Init 初始化日志
func Init(config LoggerConfig) error {
	// 解析日志级别
	level, err := zapcore.ParseLevel(config.Level)
	if err != nil {
		level = zapcore.InfoLevel
	}

	// 编码器配置
	var encoderConfig zapcore.EncoderConfig
	if config.Format == "console" {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	// 创建编码器
	var encoder zapcore.Encoder
	if config.Format == "console" {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	// 输出配置 - 同时输出到控制台和文件
	var writers []zapcore.WriteSyncer

	// 始终添加控制台输出
	writers = append(writers, zapcore.AddSync(os.Stdout))

	// 如果指定了文件路径且不是stdout/stderr，则同时输出到文件
	if config.OutputPath != "" && config.OutputPath != "stdout" && config.OutputPath != "stderr" {
		// 确保日志目录存在
		if err := os.MkdirAll("logs", 0755); err != nil {
			// 目录创建失败时输出警告到控制台，但不影响启动
			os.Stderr.WriteString("警告: 无法创建日志目录: " + err.Error() + "\n")
		} else {
			file, err := os.OpenFile(config.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				// 文件打开失败时输出警告到控制台，但不影响启动
				os.Stderr.WriteString("警告: 无法打开日志文件 " + config.OutputPath + ": " + err.Error() + "\n")
			} else {
				writers = append(writers, zapcore.AddSync(file))
			}
		}
	}

	writer := zapcore.NewMultiWriteSyncer(writers...)

	// 创建核心
	core := zapcore.NewCore(encoder, writer, level)

	// 创建日志实例
	Logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	Sugar = Logger.Sugar()

	return nil
}

// Debug 调试日志
func Debug(msg string, fields ...zap.Field) {
	Logger.Debug(msg, fields...)
}

// Info 信息日志
func Info(msg string, fields ...zap.Field) {
	Logger.Info(msg, fields...)
}

// Warn 警告日志
func Warn(msg string, fields ...zap.Field) {
	Logger.Warn(msg, fields...)
}

// Error 错误日志
func Error(msg string, fields ...zap.Field) {
	Logger.Error(msg, fields...)
}

// Fatal 致命错误日志
func Fatal(msg string, fields ...zap.Field) {
	Logger.Fatal(msg, fields...)
}

// Debugf 格式化调试日志
func Debugf(template string, args ...interface{}) {
	Sugar.Debugf(template, args...)
}

// Infof 格式化信息日志
func Infof(template string, args ...interface{}) {
	Sugar.Infof(template, args...)
}

// Warnf 格式化警告日志
func Warnf(template string, args ...interface{}) {
	Sugar.Warnf(template, args...)
}

// Errorf 格式化错误日志
func Errorf(template string, args ...interface{}) {
	Sugar.Errorf(template, args...)
}

// Fatalf 格式化致命错误日志
func Fatalf(template string, args ...interface{}) {
	Sugar.Fatalf(template, args...)
}

// Sync 同步日志缓冲区
func Sync() error {
	if Logger != nil {
		return Logger.Sync()
	}
	return nil
}

// WithFields 创建带字段的日志实例
func WithFields(fields ...zap.Field) *zap.Logger {
	return Logger.With(fields...)
}

// WithContext 创建带上下文的日志实例
func WithContext(ctx map[string]interface{}) *zap.SugaredLogger {
	fields := make([]interface{}, 0, len(ctx)*2)
	for k, v := range ctx {
		fields = append(fields, k, v)
	}
	return Sugar.With(fields...)
}

// Field 日志字段构造器
func String(key, val string) zap.Field {
	return zap.String(key, val)
}

func Int(key string, val int) zap.Field {
	return zap.Int(key, val)
}

func Int64(key string, val int64) zap.Field {
	return zap.Int64(key, val)
}

func Float64(key string, val float64) zap.Field {
	return zap.Float64(key, val)
}

func Bool(key string, val bool) zap.Field {
	return zap.Bool(key, val)
}

func Any(key string, val interface{}) zap.Field {
	return zap.Any(key, val)
}

func ErrorField(err error) zap.Field {
	return zap.Error(err)
}

func Duration(key string, val time.Duration) zap.Field {
	return zap.Duration(key, val)
}

func Strings(key string, val []string) zap.Field {
	return zap.Strings(key, val)
}
