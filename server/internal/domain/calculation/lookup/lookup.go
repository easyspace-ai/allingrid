package lookup

import (
	"fmt"
)

// LookupOptions Lookup字段配置（对齐原版 ILookupOptionsVo）
type LookupOptions struct {
	LinkFieldId   string `json:"linkFieldId"`   // 关联字段ID
	LookupFieldId string `json:"lookupFieldId"` // 查找字段ID
	Relationship  string `json:"relationship"`  // 关系类型：oneMany, manyOne, manyMany, oneOne
}

// LookupCalculator Lookup查找计算器
type LookupCalculator struct{}

// NewLookupCalculator 创建Lookup计算器
func NewLookupCalculator() *LookupCalculator {
	return &LookupCalculator{}
}

// Calculate 计算Lookup字段值（对齐原版）
// linkFieldValue: 当前记录的link字段值（关联记录ID或ID数组）
// lookedRecords: 关联表的记录数据
// lookupFieldId: 要查找的字段ID
// returns: 查找到的值或值数组
func (c *LookupCalculator) Calculate(
	linkFieldValue interface{},
	lookedRecords map[string]interface{},
	lookupFieldId string,
) (interface{}, error) {
	if linkFieldValue == nil {
		return nil, nil
	}

	// 处理单个关联ID
	if linkId, ok := linkFieldValue.(string); ok {
		if record, exists := lookedRecords[linkId]; exists {
			if recordMap, ok := record.(map[string]interface{}); ok {
				return recordMap[lookupFieldId], nil
			}
		}
		return nil, nil
	}

	// 处理多个关联ID（数组）
	if linkIds, ok := linkFieldValue.([]interface{}); ok {
		var values []interface{}
		for _, id := range linkIds {
			if linkId, ok := id.(string); ok {
				if record, exists := lookedRecords[linkId]; exists {
					if recordMap, ok := record.(map[string]interface{}); ok {
						if value := recordMap[lookupFieldId]; value != nil {
							values = append(values, value)
						}
					}
				}
			}
		}
		return values, nil
	}

	// 处理Link单元格值格式（对齐原版 ILinkCellValue）
	if linkCell, ok := linkFieldValue.(map[string]interface{}); ok {
		if linkIds, exists := linkCell["id"]; exists {
			if ids, ok := linkIds.([]interface{}); ok {
				var values []interface{}
				for _, id := range ids {
					if linkId, ok := id.(string); ok {
						if record, exists := lookedRecords[linkId]; exists {
							if recordMap, ok := record.(map[string]interface{}); ok {
								if value := recordMap[lookupFieldId]; value != nil {
									values = append(values, value)
								}
							}
						}
					}
				}
				return values, nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported link field value type")
}

// FilterArrayNull 过滤数组中的null值（对齐原版 filterArrayNull）
func FilterArrayNull(values interface{}) interface{} {
	if values == nil {
		return nil
	}

	if arr, ok := values.([]interface{}); ok {
		var filtered []interface{}
		for _, v := range arr {
			if v != nil {
				// 递归处理嵌套数组
				if nested, ok := v.([]interface{}); ok {
					if filteredNested := FilterArrayNull(nested); filteredNested != nil {
						filtered = append(filtered, filteredNested)
					}
				} else {
					filtered = append(filtered, v)
				}
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		return filtered
	}

	return values
}
