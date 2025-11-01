package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// MetricsCollector 监控数据收集器
type MetricsCollector struct {
	collectors map[string]Collector
	storage    MetricsStorage
	logger     *zap.Logger
	mu         sync.RWMutex
	interval   time.Duration
	stopChan   chan struct{}
}

// Collector 收集器接口
type Collector interface {
	Collect(ctx context.Context) (*MetricData, error)
	Name() string
	Type() string
}

// MetricData 监控数据
type MetricData struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Value       interface{}            `json:"value"`
	Labels      map[string]string      `json:"labels"`
	Timestamp   time.Time              `json:"timestamp"`
	Tags        map[string]interface{} `json:"tags,omitempty"`
	Description string                 `json:"description,omitempty"`
}

// MetricsStorage 监控数据存储接口
type MetricsStorage interface {
	Store(ctx context.Context, data *MetricData) error
	Query(ctx context.Context, query MetricsQuery) ([]*MetricData, error)
	Delete(ctx context.Context, name string, before time.Time) error
}

// MetricsQuery 监控数据查询
type MetricsQuery struct {
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	StartTime time.Time         `json:"start_time"`
	EndTime   time.Time         `json:"end_time"`
	Limit     int               `json:"limit"`
}

// NewMetricsCollector 创建监控数据收集器
func NewMetricsCollector(storage MetricsStorage, interval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		collectors: make(map[string]Collector),
		storage:    storage,
		logger:     zap.L(),
		interval:   interval,
		stopChan:   make(chan struct{}),
	}
}

// AddCollector 添加收集器
func (mc *MetricsCollector) AddCollector(collector Collector) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.collectors[collector.Name()] = collector
}

// RemoveCollector 移除收集器
func (mc *MetricsCollector) RemoveCollector(name string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.collectors, name)
}

// Start 启动收集器
func (mc *MetricsCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()

	mc.logger.Info("Starting metrics collector",
		zap.Duration("interval", mc.interval),
		zap.Int("collectors", len(mc.collectors)),
	)

	for {
		select {
		case <-ticker.C:
			mc.collect(ctx)
		case <-mc.stopChan:
			mc.logger.Info("Metrics collector stopped")
			return
		case <-ctx.Done():
			mc.logger.Info("Metrics collector context cancelled")
			return
		}
	}
}

// Stop 停止收集器
func (mc *MetricsCollector) Stop() {
	close(mc.stopChan)
}

// collect 收集所有指标
func (mc *MetricsCollector) collect(ctx context.Context) {
	mc.mu.RLock()
	collectors := make([]Collector, 0, len(mc.collectors))
	for _, collector := range mc.collectors {
		collectors = append(collectors, collector)
	}
	mc.mu.RUnlock()

	for _, collector := range collectors {
		go mc.collectFromCollector(ctx, collector)
	}
}

// collectFromCollector 从单个收集器收集数据
func (mc *MetricsCollector) collectFromCollector(ctx context.Context, collector Collector) {
	start := time.Now()

	data, err := collector.Collect(ctx)
	if err != nil {
		mc.logger.Error("Failed to collect metrics",
			zap.String("collector", collector.Name()),
			zap.Error(err),
		)
		return
	}

	// 存储数据
	if err := mc.storage.Store(ctx, data); err != nil {
		mc.logger.Error("Failed to store metrics",
			zap.String("collector", collector.Name()),
			zap.Error(err),
		)
		return
	}

	duration := time.Since(start)
	mc.logger.Debug("Metrics collected successfully",
		zap.String("collector", collector.Name()),
		zap.Duration("duration", duration),
	)
}

// Query 查询监控数据
func (mc *MetricsCollector) Query(ctx context.Context, query MetricsQuery) ([]*MetricData, error) {
	return mc.storage.Query(ctx, query)
}

// GetCollectors 获取所有收集器信息
func (mc *MetricsCollector) GetCollectors() []CollectorInfo {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var info []CollectorInfo
	for _, collector := range mc.collectors {
		info = append(info, CollectorInfo{
			Name: collector.Name(),
			Type: collector.Type(),
		})
	}
	return info
}

// CollectorInfo 收集器信息
type CollectorInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// InMemoryMetricsStorage 内存监控数据存储
type InMemoryMetricsStorage struct {
	data map[string][]*MetricData
	mu   sync.RWMutex
}

// NewInMemoryMetricsStorage 创建内存监控数据存储
func NewInMemoryMetricsStorage() *InMemoryMetricsStorage {
	return &InMemoryMetricsStorage{
		data: make(map[string][]*MetricData),
	}
}

// Store 存储监控数据
func (ims *InMemoryMetricsStorage) Store(ctx context.Context, data *MetricData) error {
	ims.mu.Lock()
	defer ims.mu.Unlock()

	ims.data[data.Name] = append(ims.data[data.Name], data)

	// 限制每个指标的数据量，保留最近1000条记录
	if len(ims.data[data.Name]) > 1000 {
		ims.data[data.Name] = ims.data[data.Name][len(ims.data[data.Name])-1000:]
	}

	return nil
}

// Query 查询监控数据
func (ims *InMemoryMetricsStorage) Query(ctx context.Context, query MetricsQuery) ([]*MetricData, error) {
	ims.mu.RLock()
	defer ims.mu.RUnlock()

	allData, exists := ims.data[query.Name]
	if !exists {
		return []*MetricData{}, nil
	}

	var results []*MetricData
	for _, data := range allData {
		// 时间过滤
		if !query.StartTime.IsZero() && data.Timestamp.Before(query.StartTime) {
			continue
		}
		if !query.EndTime.IsZero() && data.Timestamp.After(query.EndTime) {
			continue
		}

		// 标签过滤
		if query.Labels != nil {
			match := true
			for key, value := range query.Labels {
				if data.Labels[key] != value {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		results = append(results, data)

		// 限制结果数量
		if query.Limit > 0 && len(results) >= query.Limit {
			break
		}
	}

	return results, nil
}

// Delete 删除监控数据
func (ims *InMemoryMetricsStorage) Delete(ctx context.Context, name string, before time.Time) error {
	ims.mu.Lock()
	defer ims.mu.Unlock()

	data, exists := ims.data[name]
	if !exists {
		return nil
	}

	var filtered []*MetricData
	for _, item := range data {
		if item.Timestamp.After(before) {
			filtered = append(filtered, item)
		}
	}

	ims.data[name] = filtered
	return nil
}

// SystemMetricsCollector 系统指标收集器
type SystemMetricsCollector struct {
	startTime time.Time
}

// NewSystemMetricsCollector 创建系统指标收集器
func NewSystemMetricsCollector() *SystemMetricsCollector {
	return &SystemMetricsCollector{
		startTime: time.Now(),
	}
}

// Collect 收集系统指标
func (smc *SystemMetricsCollector) Collect(ctx context.Context) (*MetricData, error) {
	// 这里应该收集真实的系统指标
	// 例如：CPU使用率、内存使用率、磁盘使用率等

	uptime := time.Since(smc.startTime)

	return &MetricData{
		Name:  "system_uptime_seconds",
		Type:  "gauge",
		Value: uptime.Seconds(),
		Labels: map[string]string{
			"instance": "luckdb-backend",
		},
		Timestamp:   time.Now(),
		Description: "System uptime in seconds",
	}, nil
}

// Name 返回收集器名称
func (smc *SystemMetricsCollector) Name() string {
	return "system_metrics"
}

// Type 返回收集器类型
func (smc *SystemMetricsCollector) Type() string {
	return "system"
}

// ApplicationMetricsCollector 应用指标收集器
type ApplicationMetricsCollector struct {
	startTime time.Time
}

// NewApplicationMetricsCollector 创建应用指标收集器
func NewApplicationMetricsCollector() *ApplicationMetricsCollector {
	return &ApplicationMetricsCollector{
		startTime: time.Now(),
	}
}

// Collect 收集应用指标
func (amc *ApplicationMetricsCollector) Collect(ctx context.Context) (*MetricData, error) {
	// 这里应该收集真实的应用指标
	// 例如：请求数、错误数、响应时间等

	return &MetricData{
		Name:  "application_requests_total",
		Type:  "counter",
		Value: 1000,
		Labels: map[string]string{
			"method": "all",
			"status": "all",
		},
		Timestamp:   time.Now(),
		Description: "Total number of requests",
	}, nil
}

// Name 返回收集器名称
func (amc *ApplicationMetricsCollector) Name() string {
	return "application_metrics"
}

// Type 返回收集器类型
func (amc *ApplicationMetricsCollector) Type() string {
	return "application"
}

// DatabaseMetricsCollector 数据库指标收集器
type DatabaseMetricsCollector struct {
	db interface{} // 这里应该是数据库连接接口
}

// NewDatabaseMetricsCollector 创建数据库指标收集器
func NewDatabaseMetricsCollector(db interface{}) *DatabaseMetricsCollector {
	return &DatabaseMetricsCollector{
		db: db,
	}
}

// Collect 收集数据库指标
func (dmc *DatabaseMetricsCollector) Collect(ctx context.Context) (*MetricData, error) {
	// 这里应该收集真实的数据库指标
	// 例如：连接数、查询数、慢查询数等

	return &MetricData{
		Name:  "database_connections",
		Type:  "gauge",
		Value: 10,
		Labels: map[string]string{
			"state": "active",
		},
		Timestamp:   time.Now(),
		Description: "Number of active database connections",
	}, nil
}

// Name 返回收集器名称
func (dmc *DatabaseMetricsCollector) Name() string {
	return "database_metrics"
}

// Type 返回收集器类型
func (dmc *DatabaseMetricsCollector) Type() string {
	return "database"
}

// MetricsExporter 监控数据导出器
type MetricsExporter struct {
	storage MetricsStorage
	logger  *zap.Logger
}

// NewMetricsExporter 创建监控数据导出器
func NewMetricsExporter(storage MetricsStorage) *MetricsExporter {
	return &MetricsExporter{
		storage: storage,
		logger:  zap.L(),
	}
}

// ExportPrometheus 导出Prometheus格式数据
func (me *MetricsExporter) ExportPrometheus(ctx context.Context, query MetricsQuery) (string, error) {
	data, err := me.storage.Query(ctx, query)
	if err != nil {
		return "", err
	}

	var output string
	for _, metric := range data {
		line := fmt.Sprintf("# HELP %s %s\n", metric.Name, metric.Description)
		line += fmt.Sprintf("# TYPE %s %s\n", metric.Name, metric.Type)

		// 构建标签字符串
		labels := ""
		if len(metric.Labels) > 0 {
			labels = "{"
			first := true
			for key, value := range metric.Labels {
				if !first {
					labels += ","
				}
				labels += fmt.Sprintf("%s=\"%s\"", key, value)
				first = false
			}
			labels += "}"
		}

		line += fmt.Sprintf("%s%s %v %d\n", metric.Name, labels, metric.Value, metric.Timestamp.Unix()*1000)
		output += line
	}

	return output, nil
}

// ExportJSON 导出JSON格式数据
func (me *MetricsExporter) ExportJSON(ctx context.Context, query MetricsQuery) ([]byte, error) {
	data, err := me.storage.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

// GetMetricsSummary 获取指标摘要
func (me *MetricsExporter) GetMetricsSummary(ctx context.Context) (*MetricsSummary, error) {
	// 查询所有指标
	query := MetricsQuery{
		EndTime:   time.Now(),
		StartTime: time.Now().Add(-24 * time.Hour), // 最近24小时
	}

	allData, err := me.storage.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	summary := &MetricsSummary{
		TotalMetrics:  len(allData),
		MetricsByType: make(map[string]int),
		MetricsByName: make(map[string]int),
		LastUpdated:   time.Now(),
	}

	// 统计指标
	for _, metric := range allData {
		summary.MetricsByType[metric.Type]++
		summary.MetricsByName[metric.Name]++
	}

	return summary, nil
}

// MetricsSummary 指标摘要
type MetricsSummary struct {
	TotalMetrics  int            `json:"total_metrics"`
	MetricsByType map[string]int `json:"metrics_by_type"`
	MetricsByName map[string]int `json:"metrics_by_name"`
	LastUpdated   time.Time      `json:"last_updated"`
}
