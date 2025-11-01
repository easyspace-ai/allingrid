package monitoring

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// PerformanceMetrics 性能指标
type PerformanceMetrics struct {
	// 连接相关指标
	ActiveConnections int64 `json:"activeConnections"`
	TotalConnections  int64 `json:"totalConnections"`
	DisconnectedCount int64 `json:"disconnectedCount"`

	// 操作相关指标
	TotalOperations      int64 `json:"totalOperations"`
	SuccessfulOperations int64 `json:"successfulOperations"`
	FailedOperations     int64 `json:"failedOperations"`

	// 延迟指标
	AvgOperationLatency time.Duration `json:"avgOperationLatency"`
	MaxOperationLatency time.Duration `json:"maxOperationLatency"`
	MinOperationLatency time.Duration `json:"minOperationLatency"`

	// 吞吐量指标
	OperationsPerSecond float64 `json:"operationsPerSecond"`
	BytesPerSecond      float64 `json:"bytesPerSecond"`

	// 错误指标
	ErrorRate float64 `json:"errorRate"`

	// 内存指标
	MemoryUsage int64 `json:"memoryUsage"`

	// 时间戳
	LastUpdated time.Time `json:"lastUpdated"`
}

// OperationMetrics 操作指标
type OperationMetrics struct {
	OperationType string        `json:"operationType"`
	Duration      time.Duration `json:"duration"`
	Success       bool          `json:"success"`
	ErrorCode     string        `json:"errorCode,omitempty"`
	Bytes         int64         `json:"bytes"`
	Timestamp     time.Time     `json:"timestamp"`
}

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	logger  *zap.Logger
	metrics *PerformanceMetrics
	mu      sync.RWMutex

	// 历史数据
	operationHistory []OperationMetrics
	maxHistorySize   int

	// 统计窗口
	windowSize      time.Duration
	lastWindowReset time.Time

	// 回调函数
	onMetricsUpdate func(*PerformanceMetrics)
}

// NewPerformanceMonitor 创建性能监控器
func NewPerformanceMonitor(logger *zap.Logger) *PerformanceMonitor {
	return &PerformanceMonitor{
		logger: logger,
		metrics: &PerformanceMetrics{
			MinOperationLatency: time.Hour, // 初始化为很大的值
		},
		operationHistory: make([]OperationMetrics, 0),
		maxHistorySize:   1000,
		windowSize:       60 * time.Second, // 1分钟窗口
		lastWindowReset:  time.Now(),
	}
}

// RecordOperation 记录操作指标
func (pm *PerformanceMonitor) RecordOperation(opType string, duration time.Duration, success bool, errorCode string, bytes int64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 记录操作历史
	operation := OperationMetrics{
		OperationType: opType,
		Duration:      duration,
		Success:       success,
		ErrorCode:     errorCode,
		Bytes:         bytes,
		Timestamp:     time.Now(),
	}

	pm.operationHistory = append(pm.operationHistory, operation)

	// 限制历史记录大小
	if len(pm.operationHistory) > pm.maxHistorySize {
		pm.operationHistory = pm.operationHistory[1:]
	}

	// 更新指标
	pm.updateMetrics(operation)

	// 检查是否需要重置窗口
	if time.Since(pm.lastWindowReset) > pm.windowSize {
		pm.resetWindow()
	}
}

// updateMetrics 更新性能指标
func (pm *PerformanceMonitor) updateMetrics(operation OperationMetrics) {
	metrics := pm.metrics

	// 更新操作计数
	metrics.TotalOperations++
	if operation.Success {
		metrics.SuccessfulOperations++
	} else {
		metrics.FailedOperations++
	}

	// 更新延迟指标
	if operation.Duration < metrics.MinOperationLatency {
		metrics.MinOperationLatency = operation.Duration
	}
	if operation.Duration > metrics.MaxOperationLatency {
		metrics.MaxOperationLatency = operation.Duration
	}

	// 计算平均延迟
	if metrics.TotalOperations > 0 {
		totalLatency := metrics.AvgOperationLatency * time.Duration(metrics.TotalOperations-1)
		metrics.AvgOperationLatency = (totalLatency + operation.Duration) / time.Duration(metrics.TotalOperations)
	}

	// 更新错误率
	if metrics.TotalOperations > 0 {
		metrics.ErrorRate = float64(metrics.FailedOperations) / float64(metrics.TotalOperations)
	}

	// 更新吞吐量（基于窗口）
	windowOps := pm.getOperationsInWindow()
	if windowOps > 0 {
		metrics.OperationsPerSecond = float64(windowOps) / pm.windowSize.Seconds()
	}

	// 更新字节吞吐量
	windowBytes := pm.getBytesInWindow()
	if windowBytes > 0 {
		metrics.BytesPerSecond = float64(windowBytes) / pm.windowSize.Seconds()
	}

	metrics.LastUpdated = time.Now()

	// 触发回调
	if pm.onMetricsUpdate != nil {
		pm.onMetricsUpdate(metrics)
	}
}

// getOperationsInWindow 获取窗口内的操作数
func (pm *PerformanceMonitor) getOperationsInWindow() int64 {
	cutoff := time.Now().Add(-pm.windowSize)
	count := int64(0)

	for _, op := range pm.operationHistory {
		if op.Timestamp.After(cutoff) {
			count++
		}
	}

	return count
}

// getBytesInWindow 获取窗口内的字节数
func (pm *PerformanceMonitor) getBytesInWindow() int64 {
	cutoff := time.Now().Add(-pm.windowSize)
	bytes := int64(0)

	for _, op := range pm.operationHistory {
		if op.Timestamp.After(cutoff) {
			bytes += op.Bytes
		}
	}

	return bytes
}

// resetWindow 重置统计窗口
func (pm *PerformanceMonitor) resetWindow() {
	pm.lastWindowReset = time.Now()

	// 清理过期历史记录
	cutoff := time.Now().Add(-pm.windowSize * 2) // 保留2个窗口的数据
	filtered := make([]OperationMetrics, 0)

	for _, op := range pm.operationHistory {
		if op.Timestamp.After(cutoff) {
			filtered = append(filtered, op)
		}
	}

	pm.operationHistory = filtered
}

// UpdateConnectionCount 更新连接数
func (pm *PerformanceMonitor) UpdateConnectionCount(active, total, disconnected int64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.metrics.ActiveConnections = active
	pm.metrics.TotalConnections = total
	pm.metrics.DisconnectedCount = disconnected
	pm.metrics.LastUpdated = time.Now()
}

// GetMetrics 获取当前指标
func (pm *PerformanceMonitor) GetMetrics() *PerformanceMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 返回副本以避免并发修改
	metrics := *pm.metrics
	return &metrics
}

// GetOperationHistory 获取操作历史
func (pm *PerformanceMonitor) GetOperationHistory() []OperationMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 返回副本
	history := make([]OperationMetrics, len(pm.operationHistory))
	copy(history, pm.operationHistory)
	return history
}

// SetMetricsUpdateCallback 设置指标更新回调
func (pm *PerformanceMonitor) SetMetricsUpdateCallback(callback func(*PerformanceMetrics)) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.onMetricsUpdate = callback
}

// LogMetrics 记录指标到日志
func (pm *PerformanceMonitor) LogMetrics() {
	metrics := pm.GetMetrics()

	pm.logger.Info("Performance Metrics",
		zap.Int64("activeConnections", metrics.ActiveConnections),
		zap.Int64("totalConnections", metrics.TotalConnections),
		zap.Int64("totalOperations", metrics.TotalOperations),
		zap.Int64("successfulOperations", metrics.SuccessfulOperations),
		zap.Int64("failedOperations", metrics.FailedOperations),
		zap.Duration("avgOperationLatency", metrics.AvgOperationLatency),
		zap.Duration("maxOperationLatency", metrics.MaxOperationLatency),
		zap.Duration("minOperationLatency", metrics.MinOperationLatency),
		zap.Float64("operationsPerSecond", metrics.OperationsPerSecond),
		zap.Float64("bytesPerSecond", metrics.BytesPerSecond),
		zap.Float64("errorRate", metrics.ErrorRate),
		zap.Int64("memoryUsage", metrics.MemoryUsage),
	)
}

// StartPeriodicLogging 启动定期日志记录
func (pm *PerformanceMonitor) StartPeriodicLogging(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			pm.LogMetrics()
		}
	}()
}

// GetOperationStats 获取操作统计
func (pm *PerformanceMonitor) GetOperationStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	stats := make(map[string]interface{})
	operationCounts := make(map[string]int64)
	operationDurations := make(map[string][]time.Duration)

	for _, op := range pm.operationHistory {
		operationCounts[op.OperationType]++
		operationDurations[op.OperationType] = append(operationDurations[op.OperationType], op.Duration)
	}

	for opType, count := range operationCounts {
		durations := operationDurations[opType]
		if len(durations) > 0 {
			var total time.Duration
			for _, d := range durations {
				total += d
			}
			avgDuration := total / time.Duration(len(durations))

			stats[opType] = map[string]interface{}{
				"count":       count,
				"avgDuration": avgDuration.String(),
			}
		}
	}

	return stats
}
