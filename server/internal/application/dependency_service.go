package application

import (
	"regexp"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// DependencyService 依赖管理服务
// 负责管理字段之间的依赖关系
type DependencyService struct {
	errorService *ErrorService
}

// NewDependencyService 创建依赖管理服务
func NewDependencyService(errorService *ErrorService) *DependencyService {
	return &DependencyService{
		errorService: errorService,
	}
}

// BuildDependencyGraph 构建依赖图
func (s *DependencyService) BuildDependencyGraph(fields []*fieldEntity.Field) []dependency.GraphItem {
	items := make([]dependency.GraphItem, 0)

	for _, field := range fields {
		if field.IsDeleted() {
			continue
		}

		dependencies := s.extractFieldDependencies(field, fields)
		for _, dep := range dependencies {
			items = append(items, dependency.GraphItem{
				FromFieldID: dep.FromFieldID,
				ToFieldID:   dep.ToFieldID,
			})
		}
	}

	logger.Debug("dependency graph built",
		logger.Int("total_fields", len(fields)),
		logger.Int("dependency_items", len(items)))

	return items
}

// PropagateDependencies 传播依赖：找出所有受影响的字段
func (s *DependencyService) PropagateDependencies(
	changedFieldIDs []string,
	depGraph []dependency.GraphItem,
	allFields []*fieldEntity.Field,
) []string {
	affected := make(map[string]bool)
	queue := make([]string, len(changedFieldIDs))
	copy(queue, changedFieldIDs)

	// BFS遍历依赖图
	for len(queue) > 0 {
		currentFieldID := queue[0]
		queue = queue[1:]

		// 找出依赖于currentFieldID的所有字段
		for _, item := range depGraph {
			if item.FromFieldID == currentFieldID {
				// 检查ToField是否是虚拟字段
				toField := s.getFieldByID(allFields, item.ToFieldID)
				if toField != nil && s.isVirtualField(toField) {
					if !affected[item.ToFieldID] {
						affected[item.ToFieldID] = true
						queue = append(queue, item.ToFieldID) // 继续传播
					}
				}
			}
		}
	}

	// 转换为数组
	result := make([]string, 0, len(affected))
	for fieldID := range affected {
		result = append(result, fieldID)
	}

	logger.Debug("dependencies propagated",
		logger.Int("changed_fields", len(changedFieldIDs)),
		logger.Int("affected_fields", len(result)))

	return result
}

// extractFieldDependencies 提取字段依赖关系
func (s *DependencyService) extractFieldDependencies(field *fieldEntity.Field, allFields []*fieldEntity.Field) []DependencyItem {
	var items []DependencyItem

	switch field.Type().String() {
	case "formula":
		items = s.extractFormulaDependencies(field, allFields)
	case "rollup":
		items = s.extractRollupDependencies(field)
	case "lookup":
		items = s.extractLookupDependencies(field)
	case "count":
		items = s.extractCountDependencies(field)
	}

	return items
}

// extractFormulaDependencies 提取公式字段的依赖
func (s *DependencyService) extractFormulaDependencies(field *fieldEntity.Field, allFields []*fieldEntity.Field) []DependencyItem {
	options := field.Options()
	if options == nil || options.Formula == nil {
		return []DependencyItem{}
	}

	expression := options.Formula.Expression
	if expression == "" {
		return []DependencyItem{}
	}

	// 使用正则表达式提取 {fieldName} 引用
	re := regexp.MustCompile(`\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(expression, -1)

	if len(matches) == 0 {
		return []DependencyItem{}
	}

	// 提取字段名称/ID
	dependencies := make([]DependencyItem, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			fieldRef := match[1] // 提取括号内的内容

			// 查找对应的字段
			depField := s.findFieldByRef(allFields, fieldRef)
			if depField != nil {
				dependencies = append(dependencies, DependencyItem{
					FromFieldID: depField.ID().String(),
					ToFieldID:   field.ID().String(),
				})
			}
		}
	}

	return dependencies
}

// extractRollupDependencies 提取Rollup字段的依赖
func (s *DependencyService) extractRollupDependencies(field *fieldEntity.Field) []DependencyItem {
	options := field.Options()
	if options == nil || options.Rollup == nil {
		return []DependencyItem{}
	}

	linkFieldID := options.Rollup.LinkFieldID
	rollupFieldID := options.Rollup.RollupFieldID

	return []DependencyItem{
		{
			FromFieldID: linkFieldID,
			ToFieldID:   field.ID().String(),
		},
		{
			FromFieldID: rollupFieldID,
			ToFieldID:   field.ID().String(),
		},
	}
}

// extractLookupDependencies 提取Lookup字段的依赖
func (s *DependencyService) extractLookupDependencies(field *fieldEntity.Field) []DependencyItem {
	options := field.Options()
	if options == nil || options.Lookup == nil {
		return []DependencyItem{}
	}

	linkFieldID := options.Lookup.LinkFieldID
	lookupFieldID := options.Lookup.LookupFieldID

	return []DependencyItem{
		{
			FromFieldID: linkFieldID,
			ToFieldID:   field.ID().String(),
		},
		{
			FromFieldID: lookupFieldID,
			ToFieldID:   field.ID().String(),
		},
	}
}

// extractCountDependencies 提取Count字段的依赖
func (s *DependencyService) extractCountDependencies(field *fieldEntity.Field) []DependencyItem {
	options := field.Options()
	if options == nil || options.Link == nil {
		return []DependencyItem{}
	}

	// 当前实现：使用Link配置作为workaround
	linkFieldID := options.Link.LinkedTableID

	return []DependencyItem{
		{
			FromFieldID: linkFieldID,
			ToFieldID:   field.ID().String(),
		},
	}
}

// findFieldByRef 根据引用查找字段
func (s *DependencyService) findFieldByRef(fields []*fieldEntity.Field, ref string) *fieldEntity.Field {
	for _, field := range fields {
		// 先尝试按ID匹配
		if field.ID().String() == ref {
			return field
		}
		// 再尝试按名称匹配
		if field.Name().String() == ref {
			return field
		}
	}
	return nil
}

// getFieldByID 根据ID获取字段
func (s *DependencyService) getFieldByID(fields []*fieldEntity.Field, fieldID string) *fieldEntity.Field {
	for _, field := range fields {
		if field.ID().String() == fieldID {
			return field
		}
	}
	return nil
}

// isVirtualField 检查是否为虚拟字段
func (s *DependencyService) isVirtualField(field *fieldEntity.Field) bool {
	virtualTypes := map[string]bool{
		"formula": true,
		"rollup":  true,
		"lookup":  true,
		"count":   true,
	}
	return virtualTypes[field.Type().String()]
}

// DependencyItem 依赖项
type DependencyItem struct {
	FromFieldID string
	ToFieldID   string
}
