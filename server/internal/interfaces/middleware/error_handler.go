package middleware

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	pkgErrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// ErrorHandlerConfig 错误处理中间件配置
type ErrorHandlerConfig struct {
	// 是否记录堆栈信息
	LogStackTrace bool
	// 是否记录请求详情
	LogRequestDetails bool
	// 是否在生产环境中隐藏内部错误详情
	HideInternalErrors bool
	// 错误监控回调函数
	ErrorMonitor func(error, *gin.Context)
	// 错误告警回调函数
	ErrorAlert func(error, *gin.Context)
}

// DefaultErrorHandlerConfig 默认错误处理配置
func DefaultErrorHandlerConfig() ErrorHandlerConfig {
	return ErrorHandlerConfig{
		LogStackTrace:      false, // 默认不记录堆栈信息，减少日志冗余
		LogRequestDetails:  false, // 默认不记录详细请求信息
		HideInternalErrors: true,
		ErrorMonitor:       nil,
		ErrorAlert:         nil,
	}
}

// ErrorHandler 统一错误处理中间件
func ErrorHandler(config ...ErrorHandlerConfig) gin.HandlerFunc {
	var cfg ErrorHandlerConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultErrorHandlerConfig()
	}

	return func(c *gin.Context) {
		// 处理请求
		c.Next()

		// 处理错误
		if len(c.Errors) > 0 {
			handleErrors(c, c.Errors, cfg)
		}
	}
}

// handleErrors 处理错误列表
func handleErrors(c *gin.Context, ginErrors []*gin.Error, config ErrorHandlerConfig) {
	if len(ginErrors) == 0 {
		return
	}

	// 获取最后一个错误（通常是主要错误）
	lastError := ginErrors[len(ginErrors)-1]

	// 获取请求ID
	requestID, _ := c.Get("request_id")

	// 获取用户ID
	userID, _ := c.Get("user_id")

	// 检查是否为应用错误
	if appErr, ok := pkgErrors.IsAppError(lastError.Err); ok {
		handleAppError(c, appErr, requestID, userID, config)
	} else {
		handleGenericError(c, lastError.Err, requestID, userID, config)
	}
}

// handleAppError 处理应用错误
func handleAppError(c *gin.Context, appErr *pkgErrors.AppError, requestID, userID interface{}, config ErrorHandlerConfig) {
	// 构建错误响应
	response := pkgErrors.ErrorResponse{
		Error:   appErr.Message,
		Code:    appErr.Code,
		Details: appErr.Details,
		TraceID: getStringValue(requestID),
	}

	// 记录错误日志
	logError(c, appErr, requestID, userID, config)

	// 监控错误
	if config.ErrorMonitor != nil {
		config.ErrorMonitor(appErr, c)
	}

	// 告警错误（5xx错误）
	if appErr.HTTPStatus >= 500 && config.ErrorAlert != nil {
		config.ErrorAlert(appErr, c)
	}

	// 返回错误响应
	c.JSON(appErr.HTTPStatus, response)
}

// handleGenericError 处理通用错误
func handleGenericError(c *gin.Context, err error, requestID, userID interface{}, config ErrorHandlerConfig) {

	// 构建错误响应
	response := pkgErrors.ErrorResponse{
		Error:   "服务器内部错误",
		Code:    "INTERNAL_SERVER_ERROR",
		TraceID: getStringValue(requestID),
	}

	// 如果不在生产环境或配置允许，显示详细错误信息
	if !config.HideInternalErrors {
		response.Details = err.Error()
	}

	// 记录错误日志
	logError(c, err, requestID, userID, config)

	// 监控错误
	if config.ErrorMonitor != nil {
		config.ErrorMonitor(err, c)
	}

	// 告警错误
	if config.ErrorAlert != nil {
		config.ErrorAlert(err, c)
	}

	// 返回错误响应
	c.JSON(http.StatusInternalServerError, response)
}

// logError 记录错误日志
func logError(c *gin.Context, err error, requestID, userID interface{}, config ErrorHandlerConfig) {
	// 准备基础日志字段
	fields := []zap.Field{
		zap.String("error", err.Error()),
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("ip", c.ClientIP()),
	}

	// 添加请求ID
	if requestID != nil {
		fields = append(fields, zap.String("request_id", getStringValue(requestID)))
	}

	// 添加用户ID
	if userID != nil {
		fields = append(fields, zap.String("user_id", getStringValue(userID)))
	}

	// 根据错误类型选择日志级别和详细信息
	if appErr, ok := pkgErrors.IsAppError(err); ok {
		// 添加错误代码
		fields = append(fields, zap.String("error_code", appErr.Code))

		// 对于4xx错误，只记录基本信息
		if appErr.HTTPStatus < 500 {
			zap.L().Warn("Client Error", fields...)
		} else {
			// 对于5xx错误，记录更多信息
			if config.LogRequestDetails {
				fields = append(fields,
					zap.String("user_agent", c.Request.UserAgent()),
					zap.String("query", c.Request.URL.RawQuery),
					zap.String("content_type", c.Request.Header.Get("Content-Type")),
				)
			}

			// 只在开发环境或明确配置时记录堆栈信息
			if config.LogStackTrace {
				stack := getStackTrace()
				fields = append(fields, zap.String("stack", stack))
			}

			zap.L().Error("Server Error", fields...)
		}
	} else {
		// 内部错误，记录详细信息
		if config.LogRequestDetails {
			fields = append(fields,
				zap.String("user_agent", c.Request.UserAgent()),
				zap.String("query", c.Request.URL.RawQuery),
				zap.String("content_type", c.Request.Header.Get("Content-Type")),
			)
		}

		if config.LogStackTrace {
			stack := getStackTrace()
			fields = append(fields, zap.String("stack", stack))
		}

		zap.L().Error("Internal Error", fields...)
	}
}

// getStringValue 安全获取字符串值
func getStringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", value)
}

// getStackTrace 获取堆栈跟踪
func getStackTrace() string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// PanicRecovery 恐慌恢复中间件
func PanicRecovery(config ...ErrorHandlerConfig) gin.HandlerFunc {
	var cfg ErrorHandlerConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultErrorHandlerConfig()
	}

	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// 记录恐慌日志
				fields := []zap.Field{
					zap.Any("panic", err),
					zap.String("method", c.Request.Method),
					zap.String("path", c.Request.URL.Path),
					zap.String("ip", c.ClientIP()),
				}

				if requestID, exists := c.Get("request_id"); exists {
					fields = append(fields, zap.String("request_id", getStringValue(requestID)))
				}

				if userID, exists := c.Get("user_id"); exists {
					fields = append(fields, zap.String("user_id", getStringValue(userID)))
				}

				if cfg.LogStackTrace {
					stack := getStackTrace()
					fields = append(fields, zap.String("stack", stack))
				}

				zap.L().Error("Panic Recovered", fields...)

				// 监控恐慌
				if cfg.ErrorMonitor != nil {
					cfg.ErrorMonitor(fmt.Errorf("panic: %v", err), c)
				}

				// 告警恐慌
				if cfg.ErrorAlert != nil {
					cfg.ErrorAlert(fmt.Errorf("panic: %v", err), c)
				}

				// 返回错误响应
				requestID, _ := c.Get("request_id")
				response := pkgErrors.ErrorResponse{
					Error:   "服务器内部错误",
					Code:    "INTERNAL_SERVER_ERROR",
					TraceID: getStringValue(requestID),
				}

				c.JSON(http.StatusInternalServerError, response)
			}
		}()

		c.Next()
	}
}

// TimeoutHandler 超时处理中间件
func TimeoutHandler(timeout time.Duration, config ...ErrorHandlerConfig) gin.HandlerFunc {
	var cfg ErrorHandlerConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultErrorHandlerConfig()
	}

	return func(c *gin.Context) {
		// 创建带超时的上下文
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		// 替换请求上下文
		c.Request = c.Request.WithContext(ctx)

		// 创建错误通道
		errChan := make(chan error, 1)

		// 在goroutine中处理请求
		go func() {
			defer func() {
				if err := recover(); err != nil {
					errChan <- fmt.Errorf("panic: %v", err)
				}
			}()

			c.Next()

			// 检查是否有错误
			if len(c.Errors) > 0 {
				errChan <- c.Errors[len(c.Errors)-1].Err
			} else {
				errChan <- nil
			}
		}()

		// 等待结果或超时
		select {
		case err := <-errChan:
			if err != nil {
				requestID, _ := c.Get("request_id")
				userID, _ := c.Get("user_id")
				handleGenericError(c, err, requestID, userID, cfg)
			}
		case <-ctx.Done():
			// 超时处理
			timeoutErr := fmt.Errorf("request timeout")
			requestID, _ := c.Get("request_id")
			userID, _ := c.Get("user_id")
			handleGenericError(c, timeoutErr, requestID, userID, cfg)
		}
	}
}

// RateLimitError 限流错误处理
func RateLimitError(c *gin.Context, limit int, window time.Duration) {
	requestID, _ := c.Get("request_id")

	response := pkgErrors.ErrorResponse{
		Error: "请求过于频繁",
		Code:  "RATE_LIMIT_EXCEEDED",
		Details: map[string]interface{}{
			"limit":  limit,
			"window": window.String(),
		},
		TraceID: getStringValue(requestID),
	}

	// 记录限流日志
	zap.L().Warn("Rate limit exceeded",
		zap.String("request_id", getStringValue(requestID)),
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("ip", c.ClientIP()),
		zap.Int("limit", limit),
		zap.Duration("window", window),
	)

	c.JSON(http.StatusTooManyRequests, response)
}
