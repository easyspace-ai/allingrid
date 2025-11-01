package opbuilder

import (
	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
)

// ViewOpBuilder 视图操作构建器
type ViewOpBuilder struct{}

// NewViewOpBuilder 创建视图操作构建器
func NewViewOpBuilder() *ViewOpBuilder {
	return &ViewOpBuilder{}
}

// SetViewProperty 设置视图属性
func (b *ViewOpBuilder) SetViewProperty(key string, oldValue, newValue interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{key},
		"oi": newValue,
		"od": oldValue,
	}
}

// CreateView 创建视图
func (b *ViewOpBuilder) CreateView(viewID string, properties map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(properties))

	for key, value := range properties {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{key},
			"oi": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"views", viewID},
		"oi": ops,
	}
}

// DeleteView 删除视图
func (b *ViewOpBuilder) DeleteView(viewID string, properties map[string]interface{}) sharedb.OTOperation {
	ops := make([]sharedb.OTOperation, 0, len(properties))

	for key, value := range properties {
		ops = append(ops, sharedb.OTOperation{
			"p":  []interface{}{key},
			"od": value,
		})
	}

	return sharedb.OTOperation{
		"p":  []interface{}{"views", viewID},
		"od": ops,
	}
}

// UpdateViewName 更新视图名称
func (b *ViewOpBuilder) UpdateViewName(viewID string, oldName, newName string) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"views", viewID, "name"},
		"oi": newName,
		"od": oldName,
	}
}

// UpdateViewFilter 更新视图过滤器
func (b *ViewOpBuilder) UpdateViewFilter(viewID string, oldFilter, newFilter interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"views", viewID, "filter"},
		"oi": newFilter,
		"od": oldFilter,
	}
}

// UpdateViewSort 更新视图排序
func (b *ViewOpBuilder) UpdateViewSort(viewID string, oldSort, newSort interface{}) sharedb.OTOperation {
	return sharedb.OTOperation{
		"p":  []interface{}{"views", viewID, "sort"},
		"oi": newSort,
		"od": oldSort,
	}
}
