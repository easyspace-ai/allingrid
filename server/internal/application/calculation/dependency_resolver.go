package calculation

import (
	"context"
	"fmt"
	"sort"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	fieldRepo "github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// DependencyResolver 依赖解析器
// 负责分析字段间的依赖关系，确定计算顺序
type DependencyResolver struct {
	fieldRepo fieldRepo.FieldRepository
}

// NewDependencyResolver 创建依赖解析器
func NewDependencyResolver(fieldRepo fieldRepo.FieldRepository) *DependencyResolver {
	return &DependencyResolver{
		fieldRepo: fieldRepo,
	}
}

// FieldDependency 字段依赖关系
type FieldDependency struct {
	FieldID      string   `json:"field_id"`
	Dependencies []string `json:"dependencies"` // 依赖的字段ID列表
	Dependents   []string `json:"dependents"`   // 依赖此字段的字段ID列表
	FieldType    string   `json:"field_type"`
	Priority     int      `json:"priority"`
}

// DependencyGraph 依赖图
type DependencyGraph struct {
	Fields           map[string]*FieldDependency `json:"fields"`
	Topology         []string                    `json:"topology"`          // 拓扑排序结果
	Cycles           [][]string                  `json:"cycles"`            // 循环依赖
	IsValid          bool                        `json:"is_valid"`          // 是否有循环依赖
	CalculationOrder []string                    `json:"calculation_order"` // 计算顺序
}

// ResolveDependencies 解析字段依赖关系
func (r *DependencyResolver) ResolveDependencies(ctx context.Context, tableID string, fieldIDs []string) (*DependencyGraph, error) {
	logger.Info("开始解析字段依赖关系",
		logger.String("table_id", tableID),
		logger.Strings("field_ids", fieldIDs))

	// 1. 获取所有字段信息
	allFields, err := r.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("获取字段信息失败: %v", err)
	}

	// 2. 构建字段映射
	fieldMap := make(map[string]*fieldEntity.Field)
	for _, field := range allFields {
		fieldMap[field.ID().String()] = field
	}

	// 3. 构建依赖图
	graph := &DependencyGraph{
		Fields: make(map[string]*FieldDependency),
	}

	// 4. 分析每个字段的依赖关系
	for _, fieldID := range fieldIDs {
		field, exists := fieldMap[fieldID]
		if !exists {
			logger.Warn("字段不存在",
				logger.String("field_id", fieldID))
			continue
		}

		dependency := r.analyzeFieldDependency(field, fieldMap)
		graph.Fields[fieldID] = dependency
	}

	// 5. 拓扑排序
	topology, cycles := r.topologicalSort(graph)
	graph.Topology = topology
	graph.Cycles = cycles
	graph.IsValid = len(cycles) == 0

	// 6. 确定计算顺序
	if graph.IsValid {
		graph.CalculationOrder = topology
	} else {
		// 如果有循环依赖，使用优先级排序
		graph.CalculationOrder = r.prioritySort(graph)
	}

	logger.Info("字段依赖关系解析完成",
		logger.String("table_id", tableID),
		logger.Int("total_fields", len(graph.Fields)),
		logger.Int("cycles", len(graph.Cycles)),
		logger.Bool("is_valid", graph.IsValid))

	return graph, nil
}

// analyzeFieldDependency 分析单个字段的依赖关系
func (r *DependencyResolver) analyzeFieldDependency(field *fieldEntity.Field, fieldMap map[string]*fieldEntity.Field) *FieldDependency {
	dependency := &FieldDependency{
		FieldID:      field.ID().String(),
		Dependencies: make([]string, 0),
		Dependents:   make([]string, 0),
		FieldType:    field.Type().String(),
		Priority:     r.getFieldPriority(field),
	}

	// 根据字段类型分析依赖关系
	switch field.Type().String() {
	case "formula":
		// 公式字段依赖其引用的字段
		deps := r.extractFormulaDependencies(field)
		dependency.Dependencies = deps

	case "lookup":
		// 查找字段依赖源字段
		deps := r.extractLookupDependencies(field)
		dependency.Dependencies = deps

	case "rollup":
		// 汇总字段依赖源字段
		deps := r.extractRollupDependencies(field)
		dependency.Dependencies = deps

	case "count":
		// 计数字段依赖关联字段
		deps := r.extractCountDependencies(field)
		dependency.Dependencies = deps

	default:
		// 其他字段类型通常没有依赖
		dependency.Dependencies = []string{}
	}

	return dependency
}

// extractFormulaDependencies 提取公式字段的依赖
func (r *DependencyResolver) extractFormulaDependencies(field *fieldEntity.Field) []string {
	// 这里应该解析公式表达式，提取字段引用
	// 为了演示，我们返回空列表
	return []string{}
}

// extractLookupDependencies 提取查找字段的依赖
func (r *DependencyResolver) extractLookupDependencies(field *fieldEntity.Field) []string {
	// 这里应该从字段选项中提取源字段ID
	// 为了演示，我们返回空列表
	return []string{}
}

// extractRollupDependencies 提取汇总字段的依赖
func (r *DependencyResolver) extractRollupDependencies(field *fieldEntity.Field) []string {
	// 这里应该从字段选项中提取源字段ID
	// 为了演示，我们返回空列表
	return []string{}
}

// extractCountDependencies 提取计数字段的依赖
func (r *DependencyResolver) extractCountDependencies(field *fieldEntity.Field) []string {
	// 这里应该从字段选项中提取关联字段ID
	// 为了演示，我们返回空列表
	return []string{}
}

// getFieldPriority 获取字段优先级
func (r *DependencyResolver) getFieldPriority(field *fieldEntity.Field) int {
	// 根据字段类型确定优先级
	switch field.Type().String() {
	case "formula":
		return 1 // 最高优先级
	case "lookup":
		return 2
	case "rollup":
		return 3
	case "count":
		return 4
	default:
		return 5 // 最低优先级
	}
}

// topologicalSort 拓扑排序
func (r *DependencyResolver) topologicalSort(graph *DependencyGraph) ([]string, [][]string) {
	// 计算入度
	inDegree := make(map[string]int)
	for fieldID := range graph.Fields {
		inDegree[fieldID] = 0
	}

	// 计算每个字段的入度
	for _, dependency := range graph.Fields {
		for _, dep := range dependency.Dependencies {
			if _, exists := graph.Fields[dep]; exists {
				inDegree[dependency.FieldID]++
			}
		}
	}

	// 找到入度为0的字段
	queue := make([]string, 0)
	for fieldID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, fieldID)
		}
	}

	// 拓扑排序
	result := make([]string, 0)
	for len(queue) > 0 {
		// 取出一个入度为0的字段
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// 更新依赖此字段的其他字段的入度
		for _, dependency := range graph.Fields {
			for _, dep := range dependency.Dependencies {
				if dep == current {
					inDegree[dependency.FieldID]--
					if inDegree[dependency.FieldID] == 0 {
						queue = append(queue, dependency.FieldID)
					}
				}
			}
		}
	}

	// 检查是否有循环依赖
	cycles := make([][]string, 0)
	if len(result) < len(graph.Fields) {
		// 存在循环依赖，找出循环
		cycles = r.findCycles(graph, inDegree)
	}

	return result, cycles
}

// findCycles 查找循环依赖
func (r *DependencyResolver) findCycles(graph *DependencyGraph, inDegree map[string]int) [][]string {
	cycles := make([][]string, 0)
	visited := make(map[string]bool)

	for fieldID, degree := range inDegree {
		if degree > 0 && !visited[fieldID] {
			cycle := r.dfsFindCycle(graph, fieldID, visited, make(map[string]bool))
			if len(cycle) > 0 {
				cycles = append(cycles, cycle)
			}
		}
	}

	return cycles
}

// dfsFindCycle 深度优先搜索查找循环
func (r *DependencyResolver) dfsFindCycle(graph *DependencyGraph, fieldID string, visited, recStack map[string]bool) []string {
	visited[fieldID] = true
	recStack[fieldID] = true

	dependency := graph.Fields[fieldID]
	for _, dep := range dependency.Dependencies {
		if _, exists := graph.Fields[dep]; !exists {
			continue
		}

		if !visited[dep] {
			cycle := r.dfsFindCycle(graph, dep, visited, recStack)
			if len(cycle) > 0 {
				return append(cycle, fieldID)
			}
		} else if recStack[dep] {
			// 找到循环
			return []string{dep, fieldID}
		}
	}

	recStack[fieldID] = false
	return []string{}
}

// prioritySort 按优先级排序
func (r *DependencyResolver) prioritySort(graph *DependencyGraph) []string {
	fields := make([]*FieldDependency, 0, len(graph.Fields))
	for _, dependency := range graph.Fields {
		fields = append(fields, dependency)
	}

	// 按优先级排序
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Priority < fields[j].Priority
	})

	result := make([]string, 0, len(fields))
	for _, field := range fields {
		result = append(result, field.FieldID)
	}

	return result
}

// GetAffectedFields 获取受影响的字段
func (r *DependencyResolver) GetAffectedFields(ctx context.Context, tableID string, changedFieldIDs []string) ([]string, error) {
	logger.Info("分析受影响的字段",
		logger.String("table_id", tableID),
		logger.Strings("changed_field_ids", changedFieldIDs))

	// 1. 获取所有字段信息
	allFields, err := r.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("获取字段信息失败: %v", err)
	}

	// 2. 构建字段映射
	fieldMap := make(map[string]*fieldEntity.Field)
	for _, field := range allFields {
		fieldMap[field.ID().String()] = field
	}

	// 3. 分析受影响的字段
	affectedFields := make(map[string]bool)
	queue := make([]string, 0, len(changedFieldIDs))

	// 初始化队列
	for _, fieldID := range changedFieldIDs {
		affectedFields[fieldID] = true
		queue = append(queue, fieldID)
	}

	// 广度优先搜索
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// 查找依赖此字段的其他字段
		for _, field := range allFields {
			deps := r.getFieldDependencies(field, fieldMap)
			for _, dep := range deps {
				if dep == current && !affectedFields[field.ID().String()] {
					affectedFields[field.ID().String()] = true
					queue = append(queue, field.ID().String())
				}
			}
		}
	}

	// 4. 转换为列表
	result := make([]string, 0, len(affectedFields))
	for fieldID := range affectedFields {
		result = append(result, fieldID)
	}

	logger.Info("受影响的字段分析完成",
		logger.String("table_id", tableID),
		logger.Int("affected_count", len(result)))

	return result, nil
}

// getFieldDependencies 获取字段的依赖列表
func (r *DependencyResolver) getFieldDependencies(field *fieldEntity.Field, fieldMap map[string]*fieldEntity.Field) []string {
	switch field.Type().String() {
	case "formula":
		return r.extractFormulaDependencies(field)
	case "lookup":
		return r.extractLookupDependencies(field)
	case "rollup":
		return r.extractRollupDependencies(field)
	case "count":
		return r.extractCountDependencies(field)
	default:
		return []string{}
	}
}
