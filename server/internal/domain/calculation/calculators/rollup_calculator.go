package calculators

import (
	"context"
	"fmt"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// RollupCalculator Rollup字段计算器
type RollupCalculator struct {
	BaseCalculator
	recordRepo RecordRepository
}

// NewRollupCalculator 创建Rollup计算器
func NewRollupCalculator(recordRepo RecordRepository) *RollupCalculator {
	return &RollupCalculator{
		recordRepo: recordRepo,
	}
}

// CanCalculate 判断是否可以计算该字段
func (c *RollupCalculator) CanCalculate(field *entity.Field) bool {
	return field.Type().String() == valueobject.TypeRollup
}

// Calculate 计算Rollup字段值
func (c *RollupCalculator) Calculate(
	ctx context.Context,
	field *entity.Field,
	record map[string]interface{},
	fieldMap map[string]*entity.Field,
) (interface{}, error) {
	options := field.Options()
	if options == nil || options.Rollup == nil {
		return nil, fmt.Errorf("rollup options not found")
	}

	// 获取Link字段
	linkFieldID := options.Rollup.LinkFieldID
	_, ok := fieldMap[linkFieldID]
	if !ok {
		return nil, fmt.Errorf("link field not found: %s", linkFieldID)
	}

	// 获取关联的记录ID
	linkedValue := c.GetFieldValue(record, linkFieldID)
	if linkedValue == nil {
		return 0, nil
	}

	linkedRecordIDs := c.extractRecordIDs(linkedValue)
	if len(linkedRecordIDs) == 0 {
		return 0, nil
	}

	// 获取要聚合的字段
	rollupFieldID := options.Rollup.RollupFieldID
	rollupField, ok := fieldMap[rollupFieldID]
	if !ok {
		return nil, fmt.Errorf("rollup field not found: %s", rollupFieldID)
	}

	// 从关联表查询记录
	linkedTableID := rollupField.TableID()
	linkedRecords, err := c.recordRepo.FindByIDs(ctx, linkedTableID, linkedRecordIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch linked records: %w", err)
	}

	// 提取要聚合的字段值
	values := make([]interface{}, 0, len(linkedRecords))
	for _, linkedRecord := range linkedRecords {
		if value := c.GetFieldValue(linkedRecord, rollupFieldID); value != nil {
			values = append(values, value)
		}
	}

	// 执行聚合函数
	aggregateFunc := options.Rollup.AggregationFunction
	return c.aggregate(aggregateFunc, values)
}

// aggregate 执行聚合函数
func (c *RollupCalculator) aggregate(funcName string, values []interface{}) (interface{}, error) {
	funcName = strings.ToUpper(strings.TrimSpace(funcName))

	switch funcName {
	case "SUM":
		return c.sum(values), nil
	case "COUNT":
		return len(values), nil
	case "AVG", "AVERAGE":
		return c.average(values), nil
	case "MIN":
		return c.min(values), nil
	case "MAX":
		return c.max(values), nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function: %s", funcName)
	}
}

// sum 求和
func (c *RollupCalculator) sum(values []interface{}) float64 {
	sum := 0.0
	for _, v := range values {
		sum += c.toFloat64(v)
	}
	return sum
}

// average 求平均值
func (c *RollupCalculator) average(values []interface{}) float64 {
	if len(values) == 0 {
		return 0
	}
	return c.sum(values) / float64(len(values))
}

// min 求最小值
func (c *RollupCalculator) min(values []interface{}) float64 {
	if len(values) == 0 {
		return 0
	}
	min := c.toFloat64(values[0])
	for _, v := range values[1:] {
		if val := c.toFloat64(v); val < min {
			min = val
		}
	}
	return min
}

// max 求最大值
func (c *RollupCalculator) max(values []interface{}) float64 {
	if len(values) == 0 {
		return 0
	}
	max := c.toFloat64(values[0])
	for _, v := range values[1:] {
		if val := c.toFloat64(v); val > max {
			max = val
		}
	}
	return max
}

// toFloat64 转换为float64
func (c *RollupCalculator) toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	default:
		return 0
	}
}

// extractRecordIDs 从Link字段值提取记录ID
func (c *RollupCalculator) extractRecordIDs(value interface{}) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		ids := make([]string, 0, len(v))
		for _, item := range v {
			if id, ok := item.(string); ok {
				ids = append(ids, id)
			}
		}
		return ids
	default:
		return []string{}
	}
}
