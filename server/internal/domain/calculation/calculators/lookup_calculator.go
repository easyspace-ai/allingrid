package calculators

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// RecordRepository 记录仓储接口
type RecordRepository interface {
	FindByIDs(ctx context.Context, tableID string, recordIDs []string) ([]map[string]interface{}, error)
}

// LookupCalculator Lookup字段计算器
type LookupCalculator struct {
	BaseCalculator
	recordRepo RecordRepository
}

// NewLookupCalculator 创建Lookup计算器
func NewLookupCalculator(recordRepo RecordRepository) *LookupCalculator {
	return &LookupCalculator{
		recordRepo: recordRepo,
	}
}

// CanCalculate 判断是否可以计算该字段
func (c *LookupCalculator) CanCalculate(field *entity.Field) bool {
	return field.Type().String() == valueobject.TypeLookup
}

// Calculate 计算Lookup字段值
func (c *LookupCalculator) Calculate(
	ctx context.Context,
	field *entity.Field,
	record map[string]interface{},
	fieldMap map[string]*entity.Field,
) (interface{}, error) {
	options := field.Options()
	if options == nil || options.Lookup == nil {
		return nil, fmt.Errorf("lookup options not found")
	}

	// 获取Link字段
	linkFieldID := options.Lookup.LinkFieldID
	linkField, ok := fieldMap[linkFieldID]
	if !ok {
		return nil, fmt.Errorf("link field not found: %s", linkFieldID)
	}

	// 获取关联的记录ID
	linkedValue := c.GetFieldValue(record, linkFieldID)
	if linkedValue == nil {
		return nil, nil
	}

	// 转换为记录ID数组
	linkedRecordIDs := c.extractRecordIDs(linkedValue)
	if len(linkedRecordIDs) == 0 {
		return nil, nil
	}

	// 获取要查找的字段
	lookupFieldID := options.Lookup.LookupFieldID
	lookupField, ok := fieldMap[lookupFieldID]
	if !ok {
		return nil, fmt.Errorf("lookup field not found: %s", lookupFieldID)
	}

	// 从关联表查询记录
	linkedTableID := lookupField.TableID()
	linkedRecords, err := c.recordRepo.FindByIDs(ctx, linkedTableID, linkedRecordIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch linked records: %w", err)
	}

	// 提取Lookup字段的值
	values := make([]interface{}, 0, len(linkedRecords))
	for _, linkedRecord := range linkedRecords {
		if value := c.GetFieldValue(linkedRecord, lookupFieldID); value != nil {
			values = append(values, value)
		}
	}

	// 根据Link字段的多值设置返回单值或数组
	if c.isMultipleLink(linkField) {
		return values, nil
	}

	if len(values) > 0 {
		return values[0], nil
	}

	return nil, nil
}

// extractRecordIDs 从Link字段值提取记录ID
func (c *LookupCalculator) extractRecordIDs(value interface{}) []string {
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

// isMultipleLink 判断Link字段是否允许多值
func (c *LookupCalculator) isMultipleLink(field *entity.Field) bool {
	options := field.Options()
	if options != nil && options.Link != nil {
		return options.Link.AllowMultiple
	}
	return false
}
