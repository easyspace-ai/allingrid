package batch

import (
	"context"
	"fmt"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula"
	"sync"
)

// FieldCalculation 字段计算结果
type FieldCalculation struct {
	FieldID  string
	RecordID string
	Value    interface{}
	Error    error
}

// BatchCalculator 批量计算器（对齐原版 BatchService + ReferenceService）
type BatchCalculator struct {
	timeZone      string
	maxConcurrent int
}

// NewBatchCalculator 创建批量计算器
func NewBatchCalculator(timeZone string, maxConcurrent int) *BatchCalculator {
	if maxConcurrent <= 0 {
		maxConcurrent = 10 // 默认并发数
	}
	return &BatchCalculator{
		timeZone:      timeZone,
		maxConcurrent: maxConcurrent,
	}
}

// CalculateRequest 批量计算请求
type CalculateRequest struct {
	Fields       []*FieldWithFormula      // 需要计算的字段
	Records      []map[string]interface{} // 记录数据
	Dependencies map[string]interface{}   // 字段依赖关系
}

// FieldWithFormula 带公式的字段
type FieldWithFormula struct {
	ID      string
	Formula string
	Type    string // formula, rollup, lookup
}

// Calculate 批量计算（对齐原版 calculate 方法）
func (c *BatchCalculator) Calculate(ctx context.Context, req *CalculateRequest) ([]*FieldCalculation, error) {
	// 1. 构建依赖图
	topoItems := c.buildTopoItems(req.Fields)

	// 2. 拓扑排序（对齐原版）
	orders, err := dependency.GetTopoOrders(topoItems)
	if err != nil {
		return nil, fmt.Errorf("topological sort failed: %w", err)
	}

	// 3. 按拓扑顺序计算所有字段
	var results []*FieldCalculation
	var mu sync.Mutex

	for _, order := range orders {
		fieldID := order.ID

		// 查找字段定义
		var field *FieldWithFormula
		for _, f := range req.Fields {
			if f.ID == fieldID {
				field = f
				break
			}
		}

		if field == nil {
			continue
		}

		// 4. 为每条记录计算该字段
		for _, record := range req.Records {
			recordID := ""
			if id, ok := record["id"].(string); ok {
				recordID = id
			}

			// 计算字段值
			value, err := c.calculateField(ctx, field, record, req.Dependencies)

			result := &FieldCalculation{
				FieldID:  fieldID,
				RecordID: recordID,
				Value:    value,
				Error:    err,
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}
	}

	return results, nil
}

// CalculateConcurrent 并发批量计算（性能优化版本）
func (c *BatchCalculator) CalculateConcurrent(
	ctx context.Context,
	req *CalculateRequest,
) ([]*FieldCalculation, error) {
	// 1. 构建依赖图并排序
	topoItems := c.buildTopoItems(req.Fields)
	orders, err := dependency.GetTopoOrders(topoItems)
	if err != nil {
		return nil, fmt.Errorf("topological sort failed: %w", err)
	}

	// 2. 按拓扑层级分组（同一层级可以并发计算）
	levels := c.groupByLevel(orders)

	var results []*FieldCalculation
	var mu sync.Mutex

	// 3. 按层级顺序处理，每层内并发
	for _, levelFields := range levels {
		// 使用goroutine池进行并发计算
		var wg sync.WaitGroup
		sem := make(chan struct{}, c.maxConcurrent)

		for _, fieldID := range levelFields {
			// 查找字段定义
			var field *FieldWithFormula
			for _, f := range req.Fields {
				if f.ID == fieldID {
					field = f
					break
				}
			}

			if field == nil {
				continue
			}

			// 为每条记录并发计算
			for _, record := range req.Records {
				wg.Add(1)
				sem <- struct{}{} // 获取信号量

				go func(f *FieldWithFormula, rec map[string]interface{}) {
					defer wg.Done()
					defer func() { <-sem }() // 释放信号量

					recordID := ""
					if id, ok := rec["id"].(string); ok {
						recordID = id
					}

					value, err := c.calculateField(ctx, f, rec, req.Dependencies)

					result := &FieldCalculation{
						FieldID:  f.ID,
						RecordID: recordID,
						Value:    value,
						Error:    err,
					}

					mu.Lock()
					results = append(results, result)
					mu.Unlock()
				}(field, record)
			}
		}

		wg.Wait() // 等待当前层级全部完成
	}

	return results, nil
}

// calculateField 计算单个字段值
func (c *BatchCalculator) calculateField(
	ctx context.Context,
	field *FieldWithFormula,
	record map[string]interface{},
	dependencies map[string]interface{},
) (interface{}, error) {
	// 使用公式引擎求值
	result, err := formula.Evaluate(field.Formula, dependencies, record, c.timeZone)
	if err != nil {
		return nil, err
	}

	return result.Value, nil
}

// buildTopoItems 构建拓扑排序项
func (c *BatchCalculator) buildTopoItems(fields []*FieldWithFormula) []dependency.GraphItem {
	items := make([]dependency.GraphItem, len(fields))
	for i, field := range fields {
		items[i] = dependency.GraphItem{
			FromFieldID: field.ID,
			ToFieldID:   field.ID,
		}
	}
	return items
}

// groupByLevel 按依赖层级分组
func (c *BatchCalculator) groupByLevel(orders []dependency.TopoItem) [][]string {
	// 简化实现：所有字段按拓扑顺序串行
	levels := [][]string{}
	for _, order := range orders {
		levels = append(levels, []string{order.ID})
	}
	return levels
}
