package dependency

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// DependencyGraphBuilder 依赖图构建器
type DependencyGraphBuilder struct {
	fieldRepo FieldRepository
}

// FieldRepository 字段仓储接口
type FieldRepository interface {
	// FindByTableID 根据表ID查询所有字段
	FindByTableID(ctx context.Context, tableID string) ([]*entity.Field, error)
	// FindByID 根据字段ID查询字段
	FindByID(ctx context.Context, fieldID string) (*entity.Field, error)
	// FindLinkFieldsToTable 查找指向指定表的Link字段
	FindLinkFieldsToTable(ctx context.Context, tableID string) ([]*entity.Field, error)
}

// NewDependencyGraphBuilder 创建依赖图构建器
func NewDependencyGraphBuilder(fieldRepo FieldRepository) *DependencyGraphBuilder {
	return &DependencyGraphBuilder{
		fieldRepo: fieldRepo,
	}
}

// BuildDependencyGraph 为指定表构建字段依赖图
// 参考 teable-develop 的实现逻辑
func (b *DependencyGraphBuilder) BuildDependencyGraph(ctx context.Context, tableID string) ([]GraphItem, error) {
	// 获取表的所有字段
	fields, err := b.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get fields for table %s: %w", tableID, err)
	}

	var edges []GraphItem

	// 遍历每个字段，提取依赖关系
	for _, field := range fields {
		// 跳过已删除的字段
		if field.IsDeleted() {
			continue
		}

		// 根据字段类型提取依赖
		fieldEdges := b.extractFieldDependencies(field)
		edges = append(edges, fieldEdges...)
	}

	return edges, nil
}

// extractFieldDependencies 提取单个字段的依赖关系
func (b *DependencyGraphBuilder) extractFieldDependencies(field *entity.Field) []GraphItem {
	var edges []GraphItem

	fieldType := field.Type().String()
	options := field.Options()

	switch fieldType {
	case valueobject.TypeFormula:
		// 公式字段：依赖公式中引用的字段
		edges = b.extractFormulaDependencies(field, options)

	case valueobject.TypeLookup:
		// Lookup字段：依赖Link字段和目标字段
		edges = b.extractLookupDependencies(field, options)

	case valueobject.TypeRollup:
		// Rollup字段：依赖Link字段和聚合字段
		edges = b.extractRollupDependencies(field, options)

	case valueobject.TypeLink:
		// Link字段：可能作为Lookup/Rollup的依赖
		// Link字段本身不产生依赖边，但会被其他字段依赖

	case valueobject.TypeCount:
		// Count字段：可能依赖Link字段
		// TODO: 实现Count字段依赖提取
	}

	return edges
}

// extractFormulaDependencies 提取公式字段的依赖
func (b *DependencyGraphBuilder) extractFormulaDependencies(field *entity.Field, options *valueobject.FieldOptions) []GraphItem {
	var edges []GraphItem

	// 从options中提取公式表达式
	// TODO: 实现从公式表达式中解析依赖字段的逻辑
	// 目前FormulaOptions只有Expression字段，没有预先计算的Dependencies
	// 需要解析表达式中的字段引用（如 {fieldName} 或 {fieldId}）
	if options != nil && options.Formula != nil {
		// 这里暂时留空，等实现公式解析器后再填充
		// dependencies := parseFormulaDependencies(options.Formula.Expression)
		// for _, depFieldID := range dependencies {
		// 	edges = append(edges, GraphItem{
		// 		FromFieldID: field.ID().String(),
		// 		ToFieldID:   depFieldID,
		// 	})
		// }
	}

	return edges
}

// extractLookupDependencies 提取Lookup字段的依赖
func (b *DependencyGraphBuilder) extractLookupDependencies(field *entity.Field, options *valueobject.FieldOptions) []GraphItem {
	var edges []GraphItem

	if options != nil && options.Lookup != nil {
		// Lookup字段依赖Link字段
		if options.Lookup.LinkFieldID != "" {
			edges = append(edges, GraphItem{
				FromFieldID: field.ID().String(),
				ToFieldID:   options.Lookup.LinkFieldID,
			})
		}

		// Lookup字段也依赖目标表的被查找字段
		// 这形成了跨表依赖
		if options.Lookup.LookupFieldID != "" {
			edges = append(edges, GraphItem{
				FromFieldID: field.ID().String(),
				ToFieldID:   options.Lookup.LookupFieldID,
			})
		}
	}

	return edges
}

// extractRollupDependencies 提取Rollup字段的依赖
func (b *DependencyGraphBuilder) extractRollupDependencies(field *entity.Field, options *valueobject.FieldOptions) []GraphItem {
	var edges []GraphItem

	if options != nil && options.Rollup != nil {
		// Rollup字段依赖Link字段
		if options.Rollup.LinkFieldID != "" {
			edges = append(edges, GraphItem{
				FromFieldID: field.ID().String(),
				ToFieldID:   options.Rollup.LinkFieldID,
			})
		}

		// Rollup字段也依赖目标表的聚合字段
		if options.Rollup.RollupFieldID != "" {
			edges = append(edges, GraphItem{
				FromFieldID: field.ID().String(),
				ToFieldID:   options.Rollup.RollupFieldID,
			})
		}
	}

	return edges
}

// extractCountDependencies 提取Count字段的依赖
func (b *DependencyGraphBuilder) extractCountDependencies(field *entity.Field, options *valueobject.FieldOptions) []GraphItem {
	var edges []GraphItem

	// TODO: Count字段的options结构需要定义
	// 暂时返回空边集
	// if options != nil && options.Count != nil {
	// 	if options.Count.LinkFieldID != "" {
	// 		edges = append(edges, GraphItem{
	// 			FromFieldID: field.ID().String(),
	// 			ToFieldID:   options.Count.LinkFieldID,
	// 		})
	// 	}
	// }

	return edges
}

// BuildDependencyGraphForFields 为指定字段列表构建依赖图
// 用于增量更新场景
func (b *DependencyGraphBuilder) BuildDependencyGraphForFields(ctx context.Context, fieldIDs []string) ([]GraphItem, error) {
	var edges []GraphItem

	for _, fieldID := range fieldIDs {
		field, err := b.fieldRepo.FindByID(ctx, fieldID)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %s: %w", fieldID, err)
		}

		if field == nil || field.IsDeleted() {
			continue
		}

		fieldEdges := b.extractFieldDependencies(field)
		edges = append(edges, fieldEdges...)
	}

	return edges, nil
}

// GetAffectedFields 获取受指定字段变更影响的字段列表
// 返回所有直接或间接依赖于指定字段的字段ID
func (b *DependencyGraphBuilder) GetAffectedFields(ctx context.Context, tableID string, changedFieldIDs []string) ([]string, error) {
	// 构建完整的依赖图
	graph, err := b.BuildDependencyGraph(ctx, tableID)
	if err != nil {
		return nil, err
	}

	// 构建反向邻接表：被依赖字段 -> 依赖它的字段列表
	dependentsMap := make(map[string][]string)
	for _, edge := range graph {
		dependentsMap[edge.ToFieldID] = append(dependentsMap[edge.ToFieldID], edge.FromFieldID)
	}

	// BFS 遍历所有受影响的字段
	visited := make(map[string]bool)
	queue := make([]string, len(changedFieldIDs))
	copy(queue, changedFieldIDs)

	// 标记初始字段为已访问
	for _, fieldID := range changedFieldIDs {
		visited[fieldID] = true
	}

	// 收集所有受影响的字段
	affectedFields := []string{}

	for len(queue) > 0 {
		currentFieldID := queue[0]
		queue = queue[1:]

		// 获取依赖当前字段的所有字段
		dependents := dependentsMap[currentFieldID]
		for _, dependentFieldID := range dependents {
			if !visited[dependentFieldID] {
				visited[dependentFieldID] = true
				affectedFields = append(affectedFields, dependentFieldID)
				queue = append(queue, dependentFieldID)
			}
		}
	}

	return affectedFields, nil
}

// ValidateNoCycles 验证依赖图中没有循环依赖
func (b *DependencyGraphBuilder) ValidateNoCycles(graph []GraphItem) error {
	_, err := GetTopoOrders(graph)
	if err != nil {
		return fmt.Errorf("circular dependency detected: %w", err)
	}
	return nil
}

// SerializeGraph 序列化依赖图为JSON（用于缓存）
func (b *DependencyGraphBuilder) SerializeGraph(graph []GraphItem) (string, error) {
	data, err := json.Marshal(graph)
	if err != nil {
		return "", fmt.Errorf("failed to serialize graph: %w", err)
	}
	return string(data), nil
}

// DeserializeGraph 从JSON反序列化依赖图
func (b *DependencyGraphBuilder) DeserializeGraph(data string) ([]GraphItem, error) {
	var graph []GraphItem
	if err := json.Unmarshal([]byte(data), &graph); err != nil {
		return nil, fmt.Errorf("failed to deserialize graph: %w", err)
	}
	return graph, nil
}
