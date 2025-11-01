package database

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// QueryOptimizer 查询优化器
type QueryOptimizer struct {
	db     *gorm.DB
	cache  QueryCache
	logger *zap.Logger
}

// QueryCache 查询缓存接口
type QueryCache interface {
	Get(ctx context.Context, key string, dest interface{}) error
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
}

// QueryOptimizerConfig 查询优化器配置
type QueryOptimizerConfig struct {
	// 是否启用查询缓存
	EnableCache bool
	// 默认缓存时间
	DefaultCacheTTL time.Duration
	// 慢查询阈值
	SlowQueryThreshold time.Duration
	// 是否启用查询分析
	EnableQueryAnalysis bool
	// 最大缓存大小
	MaxCacheSize int
}

// DefaultQueryOptimizerConfig 默认配置
func DefaultQueryOptimizerConfig() QueryOptimizerConfig {
	return QueryOptimizerConfig{
		EnableCache:         true,
		DefaultCacheTTL:     10 * time.Minute,
		SlowQueryThreshold:  100 * time.Millisecond,
		EnableQueryAnalysis: true,
		MaxCacheSize:        10000,
	}
}

// NewQueryOptimizer 创建查询优化器
func NewQueryOptimizer(db *gorm.DB, cache QueryCache) *QueryOptimizer {
	return &QueryOptimizer{
		db:     db,
		cache:  cache,
		logger: zap.L(),
	}
}

// OptimizedFind 优化的查询方法
func (qo *QueryOptimizer) OptimizedFind(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	start := time.Now()

	// 构建缓存键
	cacheKey := qo.buildCacheKey(query, args...)

	// 尝试从缓存获取
	if qo.cache != nil {
		if err := qo.cache.Get(ctx, cacheKey, dest); err == nil {
			qo.logger.Debug("Query cache hit",
				zap.String("query", query),
				zap.Duration("duration", time.Since(start)),
			)
			return nil
		}
	}

	// 执行查询
	err := qo.db.WithContext(ctx).Raw(query, args...).Scan(dest).Error
	duration := time.Since(start)

	// 记录慢查询
	if duration > 100*time.Millisecond {
		qo.logger.Warn("Slow query detected",
			zap.String("query", query),
			zap.Duration("duration", duration),
			zap.Any("args", args),
		)
	}

	// 缓存结果
	if err == nil && qo.cache != nil {
		go func() {
			cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			qo.cache.Set(cacheCtx, cacheKey, dest, 10*time.Minute)
		}()
	}

	return err
}

// OptimizedFirst 优化的单条查询
func (qo *QueryOptimizer) OptimizedFirst(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	start := time.Now()

	// 构建缓存键
	cacheKey := qo.buildCacheKey(query, args...)

	// 尝试从缓存获取
	if qo.cache != nil {
		if err := qo.cache.Get(ctx, cacheKey, dest); err == nil {
			qo.logger.Debug("Query cache hit",
				zap.String("query", query),
				zap.Duration("duration", time.Since(start)),
			)
			return nil
		}
	}

	// 执行查询
	err := qo.db.WithContext(ctx).Raw(query, args...).First(dest).Error
	duration := time.Since(start)

	// 记录慢查询
	if duration > 100*time.Millisecond {
		qo.logger.Warn("Slow query detected",
			zap.String("query", query),
			zap.Duration("duration", duration),
			zap.Any("args", args),
		)
	}

	// 缓存结果
	if err == nil && qo.cache != nil {
		go func() {
			cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			qo.cache.Set(cacheCtx, cacheKey, dest, 10*time.Minute)
		}()
	}

	return err
}

// OptimizedCount 优化的计数查询
func (qo *QueryOptimizer) OptimizedCount(ctx context.Context, query string, args ...interface{}) (int64, error) {
	start := time.Now()

	// 构建缓存键
	cacheKey := qo.buildCacheKey("count:"+query, args...)

	// 尝试从缓存获取
	if qo.cache != nil {
		var count int64
		if err := qo.cache.Get(ctx, cacheKey, &count); err == nil {
			// Count query cache hit - no logging for performance (high frequency)
			return count, nil
		}
	}

	// 执行查询
	var count int64
	err := qo.db.WithContext(ctx).Raw(query, args...).Count(&count).Error
	duration := time.Since(start)

	// 记录慢查询
	if duration > 100*time.Millisecond {
		qo.logger.Warn("Slow count query detected",
			zap.String("query", query),
			zap.Duration("duration", duration),
			zap.Any("args", args),
		)
	}

	// 缓存结果
	if err == nil && qo.cache != nil {
		go func() {
			cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			qo.cache.Set(cacheCtx, cacheKey, count, 5*time.Minute)
		}()
	}

	return count, err
}

// BatchInsert 批量插入优化
func (qo *QueryOptimizer) BatchInsert(ctx context.Context, table string, data []interface{}, batchSize int) error {
	if len(data) == 0 {
		return nil
	}

	if batchSize <= 0 {
		batchSize = 1000 // 默认批量大小
	}

	start := time.Now()

	// 分批处理
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]

		// 构建批量插入SQL
		query, args := qo.buildBatchInsertSQL(table, batch)

		// 执行批量插入
		if err := qo.db.WithContext(ctx).Exec(query, args...).Error; err != nil {
			qo.logger.Error("Batch insert failed",
				zap.String("table", table),
				zap.Int("batch_size", len(batch)),
				zap.Error(err),
			)
			return err
		}
	}

	duration := time.Since(start)
	qo.logger.Info("Batch insert completed",
		zap.String("table", table),
		zap.Int("total_records", len(data)),
		zap.Int("batch_size", batchSize),
		zap.Duration("duration", duration),
	)

	return nil
}

// buildBatchInsertSQL 构建批量插入SQL
func (qo *QueryOptimizer) buildBatchInsertSQL(table string, data []interface{}) (string, []interface{}) {
	if len(data) == 0 {
		return "", nil
	}

	// 获取第一个元素的字段信息
	firstItem := data[0]
	v := reflect.ValueOf(firstItem)
	t := reflect.TypeOf(firstItem)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// 构建字段列表
	var fields []string
	var placeholders []string
	var args []interface{}

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		dbTag := field.Tag.Get("gorm")
		if dbTag != "" && !strings.Contains(dbTag, "primaryKey") {
			fields = append(fields, field.Name)
		}
	}

	// 构建占位符
	for range data {
		var rowPlaceholders []string
		for range fields {
			rowPlaceholders = append(rowPlaceholders, "?")
		}
		placeholders = append(placeholders, "("+strings.Join(rowPlaceholders, ",")+")")
	}

	// 构建SQL
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(fields, ","),
		strings.Join(placeholders, ","),
	)

	// 构建参数
	for _, item := range data {
		v := reflect.ValueOf(item)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		for _, fieldName := range fields {
			field := v.FieldByName(fieldName)
			if field.IsValid() {
				args = append(args, field.Interface())
			} else {
				args = append(args, nil)
			}
		}
	}

	return query, args
}

// buildCacheKey 构建缓存键
func (qo *QueryOptimizer) buildCacheKey(query string, args ...interface{}) string {
	// 简化实现，实际项目中应该使用更复杂的哈希算法
	key := fmt.Sprintf("query:%s:%v", query, args)
	return key
}

// AnalyzeQuery 分析查询性能
func (qo *QueryOptimizer) AnalyzeQuery(ctx context.Context, query string) (*QueryAnalysis, error) {
	// 执行EXPLAIN ANALYZE
	var results []QueryPlanRow
	err := qo.db.WithContext(ctx).Raw("EXPLAIN ANALYZE " + query).Scan(&results).Error
	if err != nil {
		return nil, err
	}

	analysis := &QueryAnalysis{
		Query:      query,
		PlanRows:   results,
		AnalyzedAt: time.Now(),
	}

	// 分析查询计划
	analysis.analyze()

	return analysis, nil
}

// QueryAnalysis 查询分析结果
type QueryAnalysis struct {
	Query      string         `json:"query"`
	PlanRows   []QueryPlanRow `json:"plan_rows"`
	AnalyzedAt time.Time      `json:"analyzed_at"`

	// 分析结果
	TotalCost       float64  `json:"total_cost"`
	ExecutionTime   string   `json:"execution_time"`
	RowsExamined    int64    `json:"rows_examined"`
	RowsReturned    int64    `json:"rows_returned"`
	HasIndexScan    bool     `json:"has_index_scan"`
	HasSeqScan      bool     `json:"has_seq_scan"`
	HasNestedLoop   bool     `json:"has_nested_loop"`
	Recommendations []string `json:"recommendations"`
}

// QueryPlanRow 查询计划行
type QueryPlanRow struct {
	Plan string `json:"plan"`
}

// analyze 分析查询计划
func (qa *QueryAnalysis) analyze() {
	// 分析查询计划的文本
	planText := ""
	for _, row := range qa.PlanRows {
		planText += row.Plan + "\n"
	}

	// 提取执行时间
	if strings.Contains(planText, "Execution Time:") {
		// 解析执行时间
		// 这里需要根据实际的EXPLAIN ANALYZE输出格式来解析
	}

	// 检查是否有索引扫描
	qa.HasIndexScan = strings.Contains(planText, "Index Scan")
	qa.HasSeqScan = strings.Contains(planText, "Seq Scan")
	qa.HasNestedLoop = strings.Contains(planText, "Nested Loop")

	// 生成建议
	qa.generateRecommendations()
}

// generateRecommendations 生成优化建议
func (qa *QueryAnalysis) generateRecommendations() {
	qa.Recommendations = []string{}

	if qa.HasSeqScan {
		qa.Recommendations = append(qa.Recommendations, "Consider adding an index to avoid sequential scan")
	}

	if qa.HasNestedLoop {
		qa.Recommendations = append(qa.Recommendations, "Consider optimizing joins to reduce nested loops")
	}

	if qa.RowsExamined > qa.RowsReturned*10 {
		qa.Recommendations = append(qa.Recommendations, "High rows examined to rows returned ratio - consider better filtering")
	}
}

// InvalidateCache 使缓存失效
func (qo *QueryOptimizer) InvalidateCache(ctx context.Context, pattern string) error {
	if qo.cache == nil {
		return nil
	}

	// 这里需要根据实际的缓存实现来使特定模式的键失效
	// 简化实现
	return nil
}

// GetQueryStats 获取查询统计信息
func (qo *QueryOptimizer) GetQueryStats(ctx context.Context) (*QueryStats, error) {
	// 查询慢查询日志
	var slowQueries []SlowQuery
	err := qo.db.WithContext(ctx).Raw(`
		SELECT query, calls, total_time, mean_time, rows
		FROM pg_stat_statements
		WHERE mean_time > 100
		ORDER BY mean_time DESC
		LIMIT 10
	`).Scan(&slowQueries).Error

	if err != nil {
		// 如果pg_stat_statements扩展未启用，返回空统计
		return &QueryStats{
			SlowQueries: []SlowQuery{},
			GeneratedAt: time.Now(),
		}, nil
	}

	return &QueryStats{
		SlowQueries: slowQueries,
		GeneratedAt: time.Now(),
	}, nil
}

// QueryStats 查询统计信息
type QueryStats struct {
	SlowQueries []SlowQuery `json:"slow_queries"`
	GeneratedAt time.Time   `json:"generated_at"`
}

// SlowQuery 慢查询信息
type SlowQuery struct {
	Query     string  `json:"query"`
	Calls     int64   `json:"calls"`
	TotalTime float64 `json:"total_time"`
	MeanTime  float64 `json:"mean_time"`
	Rows      int64   `json:"rows"`
}
