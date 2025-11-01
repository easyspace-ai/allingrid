package monitoring

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BusinessMetrics 业务指标监控器
type BusinessMetrics struct {
	metrics map[string]*BusinessMetric
	mu      sync.RWMutex
	logger  *zap.Logger
}

// BusinessMetric 业务指标
type BusinessMetric struct {
	Name        string                 `json:"name"`
	Type        MetricType             `json:"type"`
	Value       interface{}            `json:"value"`
	Labels      map[string]string      `json:"labels"`
	Timestamp   time.Time              `json:"timestamp"`
	Description string                 `json:"description"`
	Tags        map[string]interface{} `json:"tags,omitempty"`
}

// MetricType 指标类型
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"   // 计数器
	MetricTypeGauge     MetricType = "gauge"     // 仪表盘
	MetricTypeHistogram MetricType = "histogram" // 直方图
	MetricTypeSummary   MetricType = "summary"   // 摘要
)

// NewBusinessMetrics 创建业务指标监控器
func NewBusinessMetrics() *BusinessMetrics {
	return &BusinessMetrics{
		metrics: make(map[string]*BusinessMetric),
		logger:  zap.L(),
	}
}

// IncrementCounter 增加计数器
func (bm *BusinessMetrics) IncrementCounter(name string, labels map[string]string, value int64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	key := bm.generateKey(name, labels)
	metric, exists := bm.metrics[key]

	if !exists {
		metric = &BusinessMetric{
			Name:      name,
			Type:      MetricTypeCounter,
			Value:     int64(0),
			Labels:    labels,
			Timestamp: time.Now(),
		}
		bm.metrics[key] = metric
	}

	metric.Value = metric.Value.(int64) + value
	metric.Timestamp = time.Now()
}

// SetGauge 设置仪表盘指标
func (bm *BusinessMetrics) SetGauge(name string, labels map[string]string, value float64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	key := bm.generateKey(name, labels)
	metric := &BusinessMetric{
		Name:      name,
		Type:      MetricTypeGauge,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	}
	bm.metrics[key] = metric
}

// ObserveHistogram 观察直方图指标
func (bm *BusinessMetrics) ObserveHistogram(name string, labels map[string]string, value float64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	key := bm.generateKey(name, labels)
	metric, exists := bm.metrics[key]

	if !exists {
		metric = &BusinessMetric{
			Name:      name,
			Type:      MetricTypeHistogram,
			Value:     []float64{},
			Labels:    labels,
			Timestamp: time.Now(),
		}
		bm.metrics[key] = metric
	}

	values := metric.Value.([]float64)
	values = append(values, value)
	metric.Value = values
	metric.Timestamp = time.Now()
}

// RecordSummary 记录摘要指标
func (bm *BusinessMetrics) RecordSummary(name string, labels map[string]string, value float64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	key := bm.generateKey(name, labels)
	metric, exists := bm.metrics[key]

	if !exists {
		metric = &BusinessMetric{
			Name:      name,
			Type:      MetricTypeSummary,
			Value:     []float64{},
			Labels:    labels,
			Timestamp: time.Now(),
		}
		bm.metrics[key] = metric
	}

	values := metric.Value.([]float64)
	values = append(values, value)
	metric.Value = values
	metric.Timestamp = time.Now()
}

// GetMetric 获取指标
func (bm *BusinessMetrics) GetMetric(name string, labels map[string]string) (*BusinessMetric, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	key := bm.generateKey(name, labels)
	metric, exists := bm.metrics[key]
	return metric, exists
}

// GetAllMetrics 获取所有指标
func (bm *BusinessMetrics) GetAllMetrics() map[string]*BusinessMetric {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	metrics := make(map[string]*BusinessMetric)
	for key, metric := range bm.metrics {
		metrics[key] = metric
	}
	return metrics
}

// GetMetricsByType 根据类型获取指标
func (bm *BusinessMetrics) GetMetricsByType(metricType MetricType) []*BusinessMetric {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var metrics []*BusinessMetric
	for _, metric := range bm.metrics {
		if metric.Type == metricType {
			metrics = append(metrics, metric)
		}
	}
	return metrics
}

// ResetMetric 重置指标
func (bm *BusinessMetrics) ResetMetric(name string, labels map[string]string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	key := bm.generateKey(name, labels)
	delete(bm.metrics, key)
}

// ResetAllMetrics 重置所有指标
func (bm *BusinessMetrics) ResetAllMetrics() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.metrics = make(map[string]*BusinessMetric)
}

// generateKey 生成指标键
func (bm *BusinessMetrics) generateKey(name string, labels map[string]string) string {
	key := name
	for k, v := range labels {
		key += ":" + k + "=" + v
	}
	return key
}

// BusinessMetricsCollector 业务指标收集器
type BusinessMetricsCollector struct {
	metrics    *BusinessMetrics
	collectors []MetricCollector
	logger     *zap.Logger
	interval   time.Duration
	stopChan   chan struct{}
}

// MetricCollector 指标收集器接口
type MetricCollector interface {
	Collect(ctx context.Context, metrics *BusinessMetrics) error
	Name() string
}

// NewBusinessMetricsCollector 创建业务指标收集器
func NewBusinessMetricsCollector(metrics *BusinessMetrics, interval time.Duration) *BusinessMetricsCollector {
	return &BusinessMetricsCollector{
		metrics:  metrics,
		logger:   zap.L(),
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// AddCollector 添加收集器
func (bmc *BusinessMetricsCollector) AddCollector(collector MetricCollector) {
	bmc.collectors = append(bmc.collectors, collector)
}

// Start 启动收集器
func (bmc *BusinessMetricsCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(bmc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bmc.collect(ctx)
		case <-bmc.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop 停止收集器
func (bmc *BusinessMetricsCollector) Stop() {
	close(bmc.stopChan)
}

// collect 收集指标
func (bmc *BusinessMetricsCollector) collect(ctx context.Context) {
	for _, collector := range bmc.collectors {
		if err := collector.Collect(ctx, bmc.metrics); err != nil {
			bmc.logger.Error("Failed to collect metrics",
				zap.String("collector", collector.Name()),
				zap.Error(err),
			)
		}
	}
}

// UserMetricsCollector 用户指标收集器
type UserMetricsCollector struct {
	userService interface{} // 这里应该是用户服务接口
}

// NewUserMetricsCollector 创建用户指标收集器
func NewUserMetricsCollector(userService interface{}) *UserMetricsCollector {
	return &UserMetricsCollector{
		userService: userService,
	}
}

// Collect 收集用户指标
func (umc *UserMetricsCollector) Collect(ctx context.Context, metrics *BusinessMetrics) error {
	// 这里应该实现具体的用户指标收集逻辑
	// 例如：活跃用户数、注册用户数、在线用户数等

	// 示例：记录活跃用户数
	metrics.SetGauge("active_users", map[string]string{
		"type": "total",
	}, 1000)

	// 示例：记录注册用户数
	metrics.SetGauge("registered_users", map[string]string{
		"type": "total",
	}, 5000)

	return nil
}

// Name 返回收集器名称
func (umc *UserMetricsCollector) Name() string {
	return "user_metrics"
}

// SpaceMetricsCollector 空间指标收集器
type SpaceMetricsCollector struct {
	spaceService interface{} // 这里应该是空间服务接口
}

// NewSpaceMetricsCollector 创建空间指标收集器
func NewSpaceMetricsCollector(spaceService interface{}) *SpaceMetricsCollector {
	return &SpaceMetricsCollector{
		spaceService: spaceService,
	}
}

// Collect 收集空间指标
func (smc *SpaceMetricsCollector) Collect(ctx context.Context, metrics *BusinessMetrics) error {
	// 这里应该实现具体的空间指标收集逻辑
	// 例如：空间总数、活跃空间数、空间使用率等

	// 示例：记录空间总数
	metrics.SetGauge("spaces_total", map[string]string{
		"type": "total",
	}, 500)

	// 示例：记录活跃空间数
	metrics.SetGauge("active_spaces", map[string]string{
		"type": "total",
	}, 300)

	return nil
}

// Name 返回收集器名称
func (smc *SpaceMetricsCollector) Name() string {
	return "space_metrics"
}

// TableMetricsCollector 表格指标收集器
type TableMetricsCollector struct {
	tableService interface{} // 这里应该是表格服务接口
}

// NewTableMetricsCollector 创建表格指标收集器
func NewTableMetricsCollector(tableService interface{}) *TableMetricsCollector {
	return &TableMetricsCollector{
		tableService: tableService,
	}
}

// Collect 收集表格指标
func (tmc *TableMetricsCollector) Collect(ctx context.Context, metrics *BusinessMetrics) error {
	// 这里应该实现具体的表格指标收集逻辑
	// 例如：表格总数、记录总数、字段总数等

	// 示例：记录表格总数
	metrics.SetGauge("tables_total", map[string]string{
		"type": "total",
	}, 2000)

	// 示例：记录记录总数
	metrics.SetGauge("records_total", map[string]string{
		"type": "total",
	}, 100000)

	return nil
}

// Name 返回收集器名称
func (tmc *TableMetricsCollector) Name() string {
	return "table_metrics"
}

// APIMetricsCollector API指标收集器
type APIMetricsCollector struct {
	// 这里可以注入API相关的服务
}

// NewAPIMetricsCollector 创建API指标收集器
func NewAPIMetricsCollector() *APIMetricsCollector {
	return &APIMetricsCollector{}
}

// Collect 收集API指标
func (amc *APIMetricsCollector) Collect(ctx context.Context, metrics *BusinessMetrics) error {
	// 这里应该实现具体的API指标收集逻辑
	// 例如：API调用次数、响应时间、错误率等

	// 示例：记录API调用次数
	metrics.IncrementCounter("api_calls_total", map[string]string{
		"endpoint": "all",
		"method":   "all",
	}, 1)

	// 示例：记录API响应时间
	metrics.ObserveHistogram("api_response_time_seconds", map[string]string{
		"endpoint": "all",
	}, 0.1)

	return nil
}

// Name 返回收集器名称
func (amc *APIMetricsCollector) Name() string {
	return "api_metrics"
}

// BusinessDatabaseMetricsCollector 业务数据库指标收集器
type BusinessDatabaseMetricsCollector struct {
	db interface{} // 这里应该是数据库连接接口
}

// NewBusinessDatabaseMetricsCollector 创建业务数据库指标收集器
func NewBusinessDatabaseMetricsCollector(db interface{}) *BusinessDatabaseMetricsCollector {
	return &BusinessDatabaseMetricsCollector{
		db: db,
	}
}

// Collect 收集数据库指标
func (dmc *BusinessDatabaseMetricsCollector) Collect(ctx context.Context, metrics *BusinessMetrics) error {
	// 这里应该实现具体的数据库指标收集逻辑
	// 例如：连接数、查询次数、慢查询数等

	// 示例：记录数据库连接数
	metrics.SetGauge("database_connections", map[string]string{
		"type": "active",
	}, 10)

	// 示例：记录数据库查询次数
	metrics.IncrementCounter("database_queries_total", map[string]string{
		"type": "select",
	}, 1)

	return nil
}

// Name 返回收集器名称
func (dmc *BusinessDatabaseMetricsCollector) Name() string {
	return "business_database_metrics"
}
