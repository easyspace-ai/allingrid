package service

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// DependencyGraph 字段依赖图
// 参考旧系统: teable-develop/apps/nestjs-backend/src/features/field/field-calculate/
type DependencyGraph struct {
	// adjacencyList 邻接表: fieldID -> []dependentFieldID
	// 如果 B 依赖 A，则 adjacencyList[A] 包含 B
	adjacencyList map[string][]string

	// reverseList 反向邻接表: fieldID -> []requiredFieldID
	// 如果 B 依赖 A，则 reverseList[B] 包含 A
	reverseList map[string][]string

	// fieldMap 字段映射
	fieldMap map[string]*entity.Field

	fieldRepo repository.FieldRepository
}

// NewDependencyGraph 创建依赖图
func NewDependencyGraph(fieldRepo repository.FieldRepository) *DependencyGraph {
	return &DependencyGraph{
		adjacencyList: make(map[string][]string),
		reverseList:   make(map[string][]string),
		fieldMap:      make(map[string]*entity.Field),
		fieldRepo:     fieldRepo,
	}
}

// Build 构建依赖图
func (g *DependencyGraph) Build(ctx context.Context, tableID string) error {
	logger.Info("构建字段依赖图", logger.String("table_id", tableID))

	// 获取所有字段
	fields, err := g.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return fmt.Errorf("获取字段列表失败: %w", err)
	}

	// 初始化
	g.adjacencyList = make(map[string][]string)
	g.reverseList = make(map[string][]string)
	g.fieldMap = make(map[string]*entity.Field)

	for _, field := range fields {
		g.fieldMap[field.ID().String()] = field
	}

	// 构建依赖关系
	for _, field := range fields {
		if !field.IsComputed() {
			continue
		}

		fieldID := field.ID().String()
		dependencies := g.extractDependencies(field)

		logger.Debug("字段依赖关系",
			logger.String("field_id", fieldID),
			logger.String("field_name", field.Name().String()),
			logger.String("field_type", field.Type().String()),
			logger.Int("dependencies", len(dependencies)))

		// 构建邻接表和反向邻接表
		for _, depFieldID := range dependencies {
			// A -> B (B 依赖 A)
			g.adjacencyList[depFieldID] = append(g.adjacencyList[depFieldID], fieldID)
			g.reverseList[fieldID] = append(g.reverseList[fieldID], depFieldID)
		}
	}

	logger.Info("✅ 依赖图构建完成",
		logger.Int("total_fields", len(fields)),
		logger.Int("computed_fields", len(g.adjacencyList)))

	return nil
}

// extractDependencies 提取字段依赖
func (g *DependencyGraph) extractDependencies(field *entity.Field) []string {
	dependencies := []string{}

	// 根据字段类型和 options 提取依赖
	switch field.Type().String() {
	case "formula":
		// 解析 formula 表达式，提取字段引用
		dependencies = g.extractFormulaDependencies(field)

	case "lookup":
		// 从 options 中提取 lookupFieldId
		dependencies = g.extractLookupDependencies(field)

	case "rollup":
		// 从 options 中提取 linkFieldId 和 rollupFieldId
		dependencies = g.extractRollupDependencies(field)

	case "count":
		// 从 options 中提取 linkFieldId
		dependencies = g.extractCountDependencies(field)

	default:
		// 其他字段类型通常没有依赖
		return []string{}
	}

	return dependencies
}

// extractFormulaDependencies 提取公式字段的依赖
func (g *DependencyGraph) extractFormulaDependencies(field *entity.Field) []string {
	// TODO: 实现公式表达式解析
	// 需要解析表达式中的字段引用，如 {fieldName} 或 {fieldId}
	// 暂时返回空列表
	return []string{}
}

// extractLookupDependencies 提取Lookup字段的依赖
func (g *DependencyGraph) extractLookupDependencies(field *entity.Field) []string {
	// TODO: 从 options 中提取 lookupFieldId
	// 需要解析 LookupOptions 结构
	// 暂时返回空列表
	return []string{}
}

// extractRollupDependencies 提取Rollup字段的依赖
func (g *DependencyGraph) extractRollupDependencies(field *entity.Field) []string {
	// TODO: 从 options 中提取 linkFieldId 和 rollupFieldId
	// 需要解析 RollupOptions 结构
	// 暂时返回空列表
	return []string{}
}

// extractCountDependencies 提取Count字段的依赖
func (g *DependencyGraph) extractCountDependencies(field *entity.Field) []string {
	// TODO: 从 options 中提取 linkFieldId
	// 需要解析 CountOptions 结构
	// 暂时返回空列表
	return []string{}
}

// GetAffectedFields 获取受影响的字段（拓扑排序）
// 当 changedFieldIDs 变更时，返回需要重新计算的字段（按计算顺序）
func (g *DependencyGraph) GetAffectedFields(changedFieldIDs []string) ([]string, error) {
	visited := make(map[string]bool)
	stack := make([]string, 0)

	// DFS 遍历所有受影响的字段
	var dfs func(string)
	dfs = func(fieldID string) {
		if visited[fieldID] {
			return
		}
		visited[fieldID] = true

		// 访问所有依赖此字段的字段
		for _, depFieldID := range g.adjacencyList[fieldID] {
			dfs(depFieldID)
		}

		stack = append(stack, fieldID)
	}

	// 从变更的字段开始 DFS
	for _, fieldID := range changedFieldIDs {
		dfs(fieldID)
	}

	// 反转栈以获得正确的计算顺序
	result := make([]string, 0, len(stack))
	for i := len(stack) - 1; i >= 0; i-- {
		// 只返回计算字段
		if field, exists := g.fieldMap[stack[i]]; exists && field.IsComputed() {
			result = append(result, stack[i])
		}
	}

	logger.Info("受影响的字段",
		logger.Int("changed_fields", len(changedFieldIDs)),
		logger.Int("affected_fields", len(result)))

	return result, nil
}

// TopologicalSort 拓扑排序所有计算字段
func (g *DependencyGraph) TopologicalSort() ([]string, error) {
	inDegree := make(map[string]int)
	queue := make([]string, 0)
	result := make([]string, 0)

	// 计算所有字段的入度
	for fieldID := range g.fieldMap {
		inDegree[fieldID] = len(g.reverseList[fieldID])
		if inDegree[fieldID] == 0 && g.fieldMap[fieldID].IsComputed() {
			queue = append(queue, fieldID)
		}
	}

	// Kahn 算法拓扑排序
	for len(queue) > 0 {
		fieldID := queue[0]
		queue = queue[1:]
		result = append(result, fieldID)

		// 处理所有依赖此字段的字段
		for _, depFieldID := range g.adjacencyList[fieldID] {
			inDegree[depFieldID]--
			if inDegree[depFieldID] == 0 {
				queue = append(queue, depFieldID)
			}
		}
	}

	// 检测循环依赖
	for fieldID, degree := range inDegree {
		if degree > 0 && g.fieldMap[fieldID].IsComputed() {
			return nil, fmt.Errorf("检测到循环依赖: field=%s", fieldID)
		}
	}

	logger.Info("拓扑排序完成", logger.Int("fields", len(result)))

	return result, nil
}
