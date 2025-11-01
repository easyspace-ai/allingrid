package monitoring

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// PerformanceMiddleware 性能监控中间件
type PerformanceMiddleware struct {
	monitor *PerformanceMonitor
	logger  *zap.Logger
}

// NewPerformanceMiddleware 创建性能监控中间件
func NewPerformanceMiddleware(monitor *PerformanceMonitor, logger *zap.Logger) *PerformanceMiddleware {
	return &PerformanceMiddleware{
		monitor: monitor,
		logger:  logger,
	}
}

// WrapOperation 包装操作以进行性能监控
func (pm *PerformanceMiddleware) WrapOperation(operationType string, operation func() error) error {
	start := time.Now()

	err := operation()

	duration := time.Since(start)
	success := err == nil

	var errorCode string
	if err != nil {
		errorCode = pm.extractErrorCode(err)
	}

	// 记录操作指标
	pm.monitor.RecordOperation(operationType, duration, success, errorCode, 0)

	// 记录详细日志
	if pm.logger != nil {
		if success {
			pm.logger.Debug("Operation completed",
				zap.String("operationType", operationType),
				zap.Duration("duration", duration),
			)
		} else {
			pm.logger.Warn("Operation failed",
				zap.String("operationType", operationType),
				zap.Duration("duration", duration),
				zap.String("errorCode", errorCode),
				zap.Error(err),
			)
		}
	}

	return err
}

// WrapOperationWithBytes 包装带字节数的操作
func (pm *PerformanceMiddleware) WrapOperationWithBytes(operationType string, bytes int64, operation func() error) error {
	start := time.Now()

	err := operation()

	duration := time.Since(start)
	success := err == nil

	var errorCode string
	if err != nil {
		errorCode = pm.extractErrorCode(err)
	}

	// 记录操作指标
	pm.monitor.RecordOperation(operationType, duration, success, errorCode, bytes)

	// 记录详细日志
	if pm.logger != nil {
		if success {
			pm.logger.Debug("Operation completed",
				zap.String("operationType", operationType),
				zap.Duration("duration", duration),
				zap.Int64("bytes", bytes),
			)
		} else {
			pm.logger.Warn("Operation failed",
				zap.String("operationType", operationType),
				zap.Duration("duration", duration),
				zap.Int64("bytes", bytes),
				zap.String("errorCode", errorCode),
				zap.Error(err),
			)
		}
	}

	return err
}

// WrapAsyncOperation 包装异步操作
func (pm *PerformanceMiddleware) WrapAsyncOperation(operationType string, operation func() error) func() {
	return func() {
		pm.WrapOperation(operationType, operation)
	}
}

// extractErrorCode 提取错误代码
func (pm *PerformanceMiddleware) extractErrorCode(err error) string {
	if err == nil {
		return ""
	}

	// 尝试从错误中提取错误代码
	// 这里可以根据实际的错误类型进行扩展
	errStr := err.Error()

	// 简单的错误代码提取逻辑
	if len(errStr) > 0 {
		// 假设错误代码是错误消息的前几个字符
		if len(errStr) > 10 {
			return errStr[:10]
		}
		return errStr
	}

	return "UNKNOWN_ERROR"
}

// MonitorConnection 监控连接
func (pm *PerformanceMiddleware) MonitorConnection(connectionID string, operation func()) {
	start := time.Now()

	operation()

	duration := time.Since(start)

	if pm.logger != nil {
		pm.logger.Debug("Connection operation completed",
			zap.String("connectionID", connectionID),
			zap.Duration("duration", duration),
		)
	}
}

// MonitorWebSocketMessage 监控 WebSocket 消息
func (pm *PerformanceMiddleware) MonitorWebSocketMessage(messageType string, messageSize int, operation func() error) error {
	return pm.WrapOperationWithBytes("websocket_"+messageType, int64(messageSize), operation)
}

// MonitorDatabaseOperation 监控数据库操作
func (pm *PerformanceMiddleware) MonitorDatabaseOperation(operationType string, operation func() error) error {
	return pm.WrapOperation("database_"+operationType, operation)
}

// MonitorCacheOperation 监控缓存操作
func (pm *PerformanceMiddleware) MonitorCacheOperation(operationType string, operation func() error) error {
	return pm.WrapOperation("cache_"+operationType, operation)
}

// MonitorPubSubOperation 监控发布订阅操作
func (pm *PerformanceMiddleware) MonitorPubSubOperation(operationType string, operation func() error) error {
	return pm.WrapOperation("pubsub_"+operationType, operation)
}

// GetMonitor 获取性能监控器
func (pm *PerformanceMiddleware) GetMonitor() *PerformanceMonitor {
	return pm.monitor
}

// ContextKey 上下文键类型
type ContextKey string

const (
	// PerformanceContextKey 性能监控上下文键
	PerformanceContextKey ContextKey = "performance_monitor"
)

// WithPerformanceMonitor 将性能监控器添加到上下文中
func WithPerformanceMonitor(ctx context.Context, monitor *PerformanceMonitor) context.Context {
	return context.WithValue(ctx, PerformanceContextKey, monitor)
}

// GetPerformanceMonitor 从上下文中获取性能监控器
func GetPerformanceMonitor(ctx context.Context) *PerformanceMonitor {
	if monitor, ok := ctx.Value(PerformanceContextKey).(*PerformanceMonitor); ok {
		return monitor
	}
	return nil
}

// RecordOperationFromContext 从上下文记录操作
func RecordOperationFromContext(ctx context.Context, operationType string, duration time.Duration, success bool, errorCode string, bytes int64) {
	if monitor := GetPerformanceMonitor(ctx); monitor != nil {
		monitor.RecordOperation(operationType, duration, success, errorCode, bytes)
	}
}
