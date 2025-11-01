package middleware

import (
	"bytes"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// PerformanceMonitor 性能监控中间件
type PerformanceMonitor struct {
	logger        *zap.Logger
	slowThreshold time.Duration
	metrics       *PerformanceMetrics
}

// PerformanceMetrics 性能指标
type PerformanceMetrics struct {
	mu            sync.RWMutex
	requests      map[string]*EndpointMetrics
	totalRequests uint64
	totalDuration time.Duration
}

// EndpointMetrics 端点指标
type EndpointMetrics struct {
	Count       uint64
	TotalTime   time.Duration
	MinTime     time.Duration
	MaxTime     time.Duration
	AvgTime     time.Duration
	LastTime    time.Duration
	Errors      uint64
	StatusCodes map[int]uint64
}

// NewPerformanceMonitor 创建性能监控中间件
func NewPerformanceMonitor(logger *zap.Logger, slowThreshold time.Duration) *PerformanceMonitor {
	return &PerformanceMonitor{
		logger:        logger,
		slowThreshold: slowThreshold,
		metrics: &PerformanceMetrics{
			requests: make(map[string]*EndpointMetrics),
		},
	}
}

// Middleware 返回 Gin 中间件
func (pm *PerformanceMonitor) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 记录请求开始
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		// 获取请求体大小
		requestSize := c.Request.ContentLength

		// 内存使用前
		var memStatsBefore runtime.MemStats
		runtime.ReadMemStats(&memStatsBefore)

		// 创建响应写入器包装器来捕获响应大小
		blw := &perfBodyLogWriter{
			ResponseWriter: c.Writer,
			body:           bytes.NewBufferString(""),
		}
		c.Writer = blw

		// 处理请求
		c.Next()

		// 计算耗时
		duration := time.Since(start)

		// 内存使用后
		var memStatsAfter runtime.MemStats
		runtime.ReadMemStats(&memStatsAfter)

		// 计算内存增长
		memoryUsed := int64(memStatsAfter.Alloc - memStatsBefore.Alloc)

		// 获取响应信息
		statusCode := c.Writer.Status()
		responseSize := blw.body.Len()

		// 更新指标
		endpoint := method + " " + path
		pm.updateMetrics(endpoint, duration, statusCode, len(c.Errors) > 0)

		// 慢请求检测
		if duration > pm.slowThreshold {
			pm.logger.Warn("Slow request detected",
				zap.String("method", method),
				zap.String("path", path),
				zap.Duration("duration", duration),
				zap.Int("status", statusCode),
				zap.Int64("request_size", requestSize),
				zap.Int("response_size", responseSize),
				zap.Int64("memory_used", memoryUsed),
				zap.String("client_ip", c.ClientIP()),
				zap.String("user_agent", c.Request.UserAgent()),
			)
		}

		// 记录性能日志
		pm.logger.Info("Request processed",
			zap.String("method", method),
			zap.String("path", path),
			zap.Duration("duration", duration),
			zap.Int("status", statusCode),
			zap.Int64("request_size", requestSize),
			zap.Int("response_size", responseSize),
			zap.Int64("memory_used", memoryUsed),
		)

		// 设置性能响应头
		c.Header("X-Response-Time", duration.String())
		c.Header("X-Server-Time", start.Format(time.RFC3339))
	}
}

// updateMetrics 更新指标
func (pm *PerformanceMonitor) updateMetrics(endpoint string, duration time.Duration, statusCode int, hasError bool) {
	pm.metrics.mu.Lock()
	defer pm.metrics.mu.Unlock()

	// 获取或创建端点指标
	metrics, exists := pm.metrics.requests[endpoint]
	if !exists {
		metrics = &EndpointMetrics{
			MinTime:     duration,
			MaxTime:     duration,
			StatusCodes: make(map[int]uint64),
		}
		pm.metrics.requests[endpoint] = metrics
	}

	// 更新指标
	metrics.Count++
	metrics.TotalTime += duration
	metrics.LastTime = duration
	metrics.AvgTime = metrics.TotalTime / time.Duration(metrics.Count)

	if duration < metrics.MinTime {
		metrics.MinTime = duration
	}
	if duration > metrics.MaxTime {
		metrics.MaxTime = duration
	}

	if hasError {
		metrics.Errors++
	}

	metrics.StatusCodes[statusCode]++

	// 更新总体指标
	pm.metrics.totalRequests++
	pm.metrics.totalDuration += duration
}

// GetMetrics 获取性能指标
func (pm *PerformanceMonitor) GetMetrics() map[string]interface{} {
	pm.metrics.mu.RLock()
	defer pm.metrics.mu.RUnlock()

	endpoints := make(map[string]map[string]interface{})

	for endpoint, metrics := range pm.metrics.requests {
		endpoints[endpoint] = map[string]interface{}{
			"count":        metrics.Count,
			"total_time":   metrics.TotalTime.String(),
			"avg_time":     metrics.AvgTime.String(),
			"min_time":     metrics.MinTime.String(),
			"max_time":     metrics.MaxTime.String(),
			"last_time":    metrics.LastTime.String(),
			"errors":       metrics.Errors,
			"error_rate":   float64(metrics.Errors) / float64(metrics.Count),
			"status_codes": metrics.StatusCodes,
		}
	}

	avgDuration := time.Duration(0)
	if pm.metrics.totalRequests > 0 {
		avgDuration = pm.metrics.totalDuration / time.Duration(pm.metrics.totalRequests)
	}

	return map[string]interface{}{
		"total_requests": pm.metrics.totalRequests,
		"total_duration": pm.metrics.totalDuration.String(),
		"avg_duration":   avgDuration.String(),
		"endpoints":      endpoints,
		"slow_threshold": pm.slowThreshold.String(),
	}
}

// GetTopSlowEndpoints 获取最慢的端点
func (pm *PerformanceMonitor) GetTopSlowEndpoints(limit int) []map[string]interface{} {
	pm.metrics.mu.RLock()
	defer pm.metrics.mu.RUnlock()

	// 收集所有端点
	type endpointStat struct {
		endpoint string
		avgTime  time.Duration
		count    uint64
	}

	stats := make([]endpointStat, 0, len(pm.metrics.requests))
	for endpoint, metrics := range pm.metrics.requests {
		stats = append(stats, endpointStat{
			endpoint: endpoint,
			avgTime:  metrics.AvgTime,
			count:    metrics.Count,
		})
	}

	// 按平均时间排序
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].avgTime > stats[j].avgTime
	})

	// 返回前N个
	result := make([]map[string]interface{}, 0, limit)
	for i := 0; i < limit && i < len(stats); i++ {
		result = append(result, map[string]interface{}{
			"endpoint": stats[i].endpoint,
			"avg_time": stats[i].avgTime.String(),
			"count":    stats[i].count,
		})
	}

	return result
}

// Reset 重置指标
func (pm *PerformanceMonitor) Reset() {
	pm.metrics.mu.Lock()
	defer pm.metrics.mu.Unlock()

	pm.metrics.requests = make(map[string]*EndpointMetrics)
	pm.metrics.totalRequests = 0
	pm.metrics.totalDuration = 0
}

// bodyLogWriter 响应写入器包装器
type perfBodyLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w *perfBodyLogWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

// MemoryMonitor 内存监控
type MemoryMonitor struct {
	logger    *zap.Logger
	threshold uint64 // 内存阈值（字节）
	interval  time.Duration
}

// NewMemoryMonitor 创建内存监控器
func NewMemoryMonitor(logger *zap.Logger, thresholdMB uint64, interval time.Duration) *MemoryMonitor {
	return &MemoryMonitor{
		logger:    logger,
		threshold: thresholdMB * 1024 * 1024,
		interval:  interval,
	}
}

// Start 启动内存监控
func (mm *MemoryMonitor) Start() {
	ticker := time.NewTicker(mm.interval)
	go func() {
		for range ticker.C {
			mm.check()
		}
	}()
}

// check 检查内存使用
func (mm *MemoryMonitor) check() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// 检查是否超过阈值
	if m.Alloc > mm.threshold {
		mm.logger.Warn("Memory usage exceeds threshold",
			zap.Uint64("allocated", m.Alloc),
			zap.Uint64("threshold", mm.threshold),
			zap.Uint64("total_alloc", m.TotalAlloc),
			zap.Uint64("sys", m.Sys),
			zap.Uint32("num_gc", m.NumGC),
		)

		// 触发 GC
		runtime.GC()

		// 再次读取内存统计
		runtime.ReadMemStats(&m)
		mm.logger.Info("After GC",
			zap.Uint64("allocated", m.Alloc),
			zap.Uint64("freed", mm.threshold-m.Alloc),
		)
	}
}

// CPUMonitor CPU 监控
type CPUMonitor struct {
	logger   *zap.Logger
	interval time.Duration
}

// NewCPUMonitor 创建 CPU 监控器
func NewCPUMonitor(logger *zap.Logger, interval time.Duration) *CPUMonitor {
	return &CPUMonitor{
		logger:   logger,
		interval: interval,
	}
}

// Start 启动 CPU 监控
func (cm *CPUMonitor) Start() {
	ticker := time.NewTicker(cm.interval)
	numCPU := runtime.NumCPU()

	go func() {
		for range ticker.C {
			cm.logger.Info("CPU stats",
				zap.Int("num_cpu", numCPU),
				zap.Int("num_goroutine", runtime.NumGoroutine()),
			)
		}
	}()
}
