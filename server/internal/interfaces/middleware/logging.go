package middleware

import (
	"bytes"
	"io"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// LoggingConfig 日志中间件配置
type LoggingConfig struct {
	SkipPaths       []string
	SkipPathPrefix  []string
	LogRequestBody  bool
	LogResponseBody bool
	MaxBodySize     int64
}

// DefaultLoggingConfig 默认日志配置
func DefaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		SkipPaths: []string{
			"/health",
			"/ping",
			"/metrics",
		},
		SkipPathPrefix: []string{
			"/static",
			"/assets",
		},
		LogRequestBody:  false,
		LogResponseBody: false,
		MaxBodySize:     1024 * 1024, // 1MB
	}
}

// RequestIDMiddleware 请求ID中间件
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

// LoggingMiddleware 日志中间件
func LoggingMiddleware(config ...LoggingConfig) gin.HandlerFunc {
	var cfg LoggingConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultLoggingConfig()
	}

	return gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		// 检查是否跳过日志记录
		if shouldSkipLogging(param.Path, cfg) {
			return ""
		}

		// 记录请求日志
		fields := []zap.Field{
			logger.String("method", param.Method),
			logger.String("path", param.Path),
			logger.String("query", param.Request.URL.RawQuery),
			logger.String("ip", param.ClientIP),
			logger.String("user_agent", param.Request.UserAgent()),
			logger.Int("status", param.StatusCode),
			logger.Int64("latency_ms", param.Latency.Milliseconds()),
			logger.Int("body_size", param.BodySize),
		}

		// 添加请求ID
		if requestID := param.Request.Header.Get("X-Request-ID"); requestID != "" {
			fields = append(fields, logger.String("request_id", requestID))
		}

		// 添加用户信息
		if userID := param.Request.Header.Get("X-User-ID"); userID != "" {
			fields = append(fields, logger.String("user_id", userID))
		}

		// 添加错误信息
		if param.ErrorMessage != "" {
			fields = append(fields, logger.String("error", param.ErrorMessage))
		}

		// 根据状态码选择日志级别
		message := "HTTP Request"
		if param.StatusCode >= 500 {
			logger.Error(message, fields...)
		} else if param.StatusCode >= 400 {
			logger.Warn(message, fields...)
		} else {
			logger.Info(message, fields...)
		}

		return ""
	})
}

// DetailedLoggingMiddleware 详细日志中间件(包含请求体和响应体)
func DetailedLoggingMiddleware(config ...LoggingConfig) gin.HandlerFunc {
	var cfg LoggingConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultLoggingConfig()
	}

	return func(c *gin.Context) {
		// 检查是否跳过日志记录
		if shouldSkipLogging(c.Request.URL.Path, cfg) {
			c.Next()
			return
		}

		start := time.Now()

		// 读取请求体
		var requestBody []byte
		if cfg.LogRequestBody && c.Request.Body != nil {
			requestBody, _ = io.ReadAll(io.LimitReader(c.Request.Body, cfg.MaxBodySize))
			c.Request.Body = io.NopCloser(bytes.NewReader(requestBody))
		}

		// 包装响应写入器以捕获响应体
		var responseBody bytes.Buffer
		bodyWriter := &bodyLogWriter{
			ResponseWriter: c.Writer,
			body:           &responseBody,
			logBody:        cfg.LogResponseBody,
			maxSize:        cfg.MaxBodySize,
		}
		c.Writer = bodyWriter

		// 处理请求
		c.Next()

		// 计算延迟
		latency := time.Since(start)

		// 准备日志字段
		fields := []zap.Field{
			logger.String("method", c.Request.Method),
			logger.String("path", c.Request.URL.Path),
			logger.String("query", c.Request.URL.RawQuery),
			logger.String("ip", c.ClientIP()),
			logger.String("user_agent", c.Request.UserAgent()),
			logger.Int("status", c.Writer.Status()),
			logger.Int64("latency_ms", latency.Milliseconds()),
			logger.Int("response_size", c.Writer.Size()),
		}

		// 添加请求ID
		if requestID, exists := c.Get("request_id"); exists {
			fields = append(fields, logger.String("request_id", requestID.(string)))
		}

		// 添加用户ID
		if userID, exists := c.Get("user_id"); exists {
			fields = append(fields, logger.String("user_id", userID.(string)))
		}

		// 添加请求体
		if cfg.LogRequestBody && len(requestBody) > 0 {
			fields = append(fields, logger.String("request_body", string(requestBody)))
		}

		// 添加响应体
		if cfg.LogResponseBody && responseBody.Len() > 0 {
			fields = append(fields, logger.String("response_body", responseBody.String()))
		}

		// 添加错误信息
		if len(c.Errors) > 0 {
			errorMsgs := make([]string, len(c.Errors))
			for i, err := range c.Errors {
				errorMsgs[i] = err.Error()
			}
			fields = append(fields, logger.Any("errors", errorMsgs))
		}

		// 根据状态码选择日志级别
		message := "HTTP Request Completed"
		status := c.Writer.Status()
		if status >= 500 {
			logger.Error(message, fields...)
		} else if status >= 400 {
			logger.Warn(message, fields...)
		} else {
			logger.Info(message, fields...)
		}
	}
}

// bodyLogWriter 包装响应写入器以捕获响应体
type bodyLogWriter struct {
	gin.ResponseWriter
	body    *bytes.Buffer
	logBody bool
	maxSize int64
}

func (w *bodyLogWriter) Write(data []byte) (int, error) {
	// 写入响应体到缓冲区
	if w.logBody && w.body.Len() < int(w.maxSize) {
		remaining := int(w.maxSize) - w.body.Len()
		if len(data) <= remaining {
			w.body.Write(data)
		} else {
			w.body.Write(data[:remaining])
		}
	}

	// 写入实际响应
	return w.ResponseWriter.Write(data)
}

// shouldSkipLogging 检查是否应该跳过日志记录
func shouldSkipLogging(path string, config LoggingConfig) bool {
	// 检查完整路径匹配
	for _, skipPath := range config.SkipPaths {
		if path == skipPath {
			return true
		}
	}

	// 检查路径前缀匹配
	for _, prefix := range config.SkipPathPrefix {
		if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
			return true
		}
	}

	return false
}
