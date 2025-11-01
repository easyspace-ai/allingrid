package service

import (
	"context"
	"fmt"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
)

// FieldRepository 字段仓储接口
type FieldRepository interface {
	GetByID(ctx context.Context, id string) (*fieldEntity.Field, error)
	GetByTableID(ctx context.Context, tableID string) ([]*fieldEntity.Field, error)
}

// RecordUpdate 记录更新结构
type RecordUpdate struct {
	TableID  string
	RecordID string
	FieldID  string
	OldValue interface{}
	NewValue interface{}
}

// BatchService 批量服务接口
type BatchService interface {
	MergeUpdatesByRecord(updates []RecordUpdate) map[string]map[string]RecordUpdate
	BatchUpdateRecords(ctx context.Context, updates []RecordUpdate) error
	BatchQueryRecords(ctx context.Context, tableID string, recordIDs []string, fields []string) ([]map[string]interface{}, error)
}

// CrossTableCalculationService 跨表计算服务
// 处理Link字段触发的级联计算和记录裂变
type CrossTableCalculationService struct {
	fieldRepo    FieldRepository
	batchService BatchService
	evaluator    FormulaEvaluator
}

// NewCrossTableCalculationService 创建跨表计算服务
func NewCrossTableCalculationService(
	fieldRepo FieldRepository,
	batchService BatchService,
	evaluator FormulaEvaluator,
) *CrossTableCalculationService {
	return &CrossTableCalculationService{
		fieldRepo:    fieldRepo,
		batchService: batchService,
		evaluator:    evaluator,
	}
}

// RecordSplitContext 记录裂变上下文
// 描述一个记录变更如何裂变到多个关联记录
type RecordSplitContext struct {
	SourceTableID   string   // 来源表ID
	SourceRecordIDs []string // 来源记录IDs
	TargetTableID   string   // 目标表ID (包含Link字段的表)
	TargetRecordIDs []string // 目标记录IDs (需要重算)
	LinkFieldID     string   // 关联字段ID
	CalculateFields []string // 需要重算的字段IDs
}

// FindReferencingRecords 查找所有引用指定记录的记录
// 这是记录裂变的核心：找出一个记录变更会影响哪些其他记录
func (s *CrossTableCalculationService) FindReferencingRecords(
	ctx context.Context,
	sourceTableID string,
	sourceRecordIDs []string,
) ([]RecordSplitContext, error) {
	var splits []RecordSplitContext

	// 1. 查找所有可能引用sourceTableID的Link字段
	linkFields, err := s.findLinkFieldsPointingTo(ctx, sourceTableID)
	if err != nil {
		return nil, fmt.Errorf("failed to find link fields: %w", err)
	}

	// 2. 对每个Link字段，查找包含sourceRecordIDs的记录
	for _, linkField := range linkFields {
		targetRecordIDs, err := s.findRecordsContainingLinkValue(
			ctx,
			linkField.TableID(),
			linkField.ID().String(),
			sourceRecordIDs,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to find records with link value: %w", err)
		}

		if len(targetRecordIDs) == 0 {
			continue
		}

		// 3. 找出依赖这个Link字段的计算字段
		dependentFields, err := s.findFieldsDependingOnLink(ctx, linkField)
		if err != nil {
			return nil, fmt.Errorf("failed to find dependent fields: %w", err)
		}

		split := RecordSplitContext{
			SourceTableID:   sourceTableID,
			SourceRecordIDs: sourceRecordIDs,
			TargetTableID:   linkField.TableID(),
			TargetRecordIDs: targetRecordIDs,
			LinkFieldID:     linkField.ID().String(),
			CalculateFields: extractFieldIDs(dependentFields),
		}

		splits = append(splits, split)
	}

	return splits, nil
}

// findLinkFieldsPointingTo 查找所有指向指定表的Link字段
func (s *CrossTableCalculationService) findLinkFieldsPointingTo(
	ctx context.Context,
	targetTableID string,
) ([]*fieldEntity.Field, error) {
	// 通过FieldRepository实现（简化实现：直接返回）
	// 需要添加方法：fieldRepo.GetLinkFieldsPointingTo(ctx, targetTableID)
	return nil, fmt.Errorf("not implemented: should use FieldRepository.GetLinkFieldsPointingTo")
}

// findRecordsContainingLinkValue 查找哪些记录的Link字段包含指定值
func (s *CrossTableCalculationService) findRecordsContainingLinkValue(
	ctx context.Context,
	tableID string,
	linkFieldID string,
	sourceRecordIDs []string,
) ([]string, error) {
	// 通过RecordRepository实现（简化实现：直接返回）
	// 需要添加方法：recordRepo.GetRecordsByLinkValue(ctx, tableID, linkFieldID, sourceRecordIDs)
	return nil, fmt.Errorf("not implemented: should use RecordRepository.GetRecordsByLinkValue")
}

// findFieldsDependingOnLink 查找依赖指定Link字段的计算字段
func (s *CrossTableCalculationService) findFieldsDependingOnLink(
	ctx context.Context,
	linkField *fieldEntity.Field,
) ([]*fieldEntity.Field, error) {
	// 查找同一个表中所有Rollup和Lookup字段
	allFields, err := s.fieldRepo.GetByTableID(ctx, linkField.TableID())
	if err != nil {
		return nil, err
	}

	var dependentFields []*fieldEntity.Field
	for _, field := range allFields {
		// 检查是否依赖linkField
		if s.fieldDependsOnLink(field, linkField.ID().String()) {
			dependentFields = append(dependentFields, field)
		}
	}

	return dependentFields, nil
}

// fieldDependsOnLink 检查字段是否依赖指定的Link字段
func (s *CrossTableCalculationService) fieldDependsOnLink(
	field *fieldEntity.Field,
	linkFieldID string,
) bool {
	if field.Options == nil {
		return false
	}

	switch field.Type().String() {
	case "rollup":
		// Rollup字段依赖Link字段
		rollupLinkID := field.Options().Rollup.LinkFieldID
		if rollupLinkID == "" {
			rollupLinkID = field.Options().Rollup.LinkFieldID
		}
		return rollupLinkID == linkFieldID

	case "lookup":
		// Lookup字段依赖Link字段
		lookupLinkID := field.Options().Lookup.LinkFieldID
		if lookupLinkID == "" {
			lookupLinkID = field.Options().Rollup.LinkFieldID
		}
		return lookupLinkID == linkFieldID

	default:
		return false
	}
}

// CalculateCrossTable 执行跨表计算
// 根据记录裂变上下文，批量重算受影响的记录
func (s *CrossTableCalculationService) CalculateCrossTable(
	ctx context.Context,
	split RecordSplitContext,
) error {
	if len(split.TargetRecordIDs) == 0 || len(split.CalculateFields) == 0 {
		return nil
	}

	// 1. 批量查询来源记录（提供依赖数据）
	sourceRecords, err := s.batchService.BatchQueryRecords(
		ctx,
		split.SourceTableID,
		split.SourceRecordIDs,
		nil, // 查询所有字段
	)
	if err != nil {
		return fmt.Errorf("failed to query source records: %w", err)
	}

	// 2. 批量查询目标记录（需要重算）
	targetRecords, err := s.batchService.BatchQueryRecords(
		ctx,
		split.TargetTableID,
		split.TargetRecordIDs,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to query target records: %w", err)
	}

	// 3. 获取要计算的字段定义
	var calculateFields []*fieldEntity.Field
	for _, fieldID := range split.CalculateFields {
		field, err := s.fieldRepo.GetByID(ctx, fieldID)
		if err != nil {
			return fmt.Errorf("failed to get field %s: %w", fieldID, err)
		}
		calculateFields = append(calculateFields, field)
	}

	// 4. 准备批量更新
	var updates []RecordUpdate

	for _, targetData := range targetRecords {
		for _, field := range calculateFields {
			targetRecordID, _ := targetData["id"].(string)
			// 获取Link字段的值（关联的记录IDs）
			linkValue := targetData[split.LinkFieldID]
			linkedRecordIDs := s.extractRecordIDsFromLinkValue(linkValue)

			// 提取依赖的来源记录
			dependencies := make(map[string]interface{})
			for _, linkedID := range linkedRecordIDs {
				linkedRecordData := findRecordByID(sourceRecords, linkedID)
				if linkedRecordData != nil {
				}
			}

			// 计算字段值
			value, err := s.calculateFieldWithDependencies(
				ctx, field, targetData, dependencies,
			)
			if err != nil {
				return fmt.Errorf("failed to calculate field %s for record %s: %w",
					field.ID().String(), targetRecordID, err)
			}

			updates = append(updates, RecordUpdate{
				TableID:  split.TargetTableID,
				RecordID: targetRecordID,
				FieldID:  field.ID().String(),
				NewValue: value,
			})
		}
	}

	// 5. 批量更新
	if len(updates) > 0 {
		if err := s.batchService.BatchUpdateRecords(ctx, updates); err != nil {
			return fmt.Errorf("failed to batch update records: %w", err)
		}
	}

	return nil
}

// extractRecordIDsFromLinkValue 从Link字段值中提取记录IDs
func (s *CrossTableCalculationService) extractRecordIDsFromLinkValue(
	linkValue interface{},
) []string {
	if linkValue == nil {
		return []string{}
	}

	switch v := linkValue.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		ids := make([]string, 0, len(v))
		for _, item := range v {
			if id, ok := item.(string); ok {
				ids = append(ids, id)
			} else if m, ok := item.(map[string]interface{}); ok {
				// Link value可能是 [{id: "xxx", title: "yyy"}]
				if id, ok := m["id"].(string); ok {
					ids = append(ids, id)
				}
			}
		}
		return ids
	default:
		return []string{}
	}
}

// calculateFieldWithDependencies 使用依赖数据计算字段值
func (s *CrossTableCalculationService) calculateFieldWithDependencies(
	ctx context.Context,
	field *fieldEntity.Field,
	recordData map[string]interface{},
	dependencies map[string]interface{},
) (interface{}, error) {
	switch field.Type().String() {
	case "rollup":
		return s.calculateRollupWithDependencies(field, dependencies)

	case "lookup":
		return s.calculateLookupWithDependencies(field, dependencies)

	case string("formula"):
		// Formula字段也可能引用Link字段的数据
		return s.calculateFormulaWithDependencies(field, recordData, dependencies)

	default:
		return nil, fmt.Errorf("unsupported field type for cross-table calculation: %s", field.Type().String())
	}
}

// calculateRollupWithDependencies 计算Rollup字段值
func (s *CrossTableCalculationService) calculateRollupWithDependencies(
	field *fieldEntity.Field,
	dependencies map[string]interface{},
) (interface{}, error) {
	if field.Options == nil {
		return nil, fmt.Errorf("rollup field options is nil")
	}

	// 获取要汇总的字段ID
	rollupFieldID := field.Options().Rollup.LinkFieldID
	if rollupFieldID == "" && field.Options().Rollup != nil {
		rollupFieldID = field.Options().Rollup.LinkFieldID
	}

	// 提取所有依赖记录中的该字段值
	var values []interface{}
	for _, depData := range dependencies {
		if depMap, ok := depData.(map[string]interface{}); ok {
			if value, exists := depMap[rollupFieldID]; exists {
				values = append(values, value)
			}
		}
	}

	// 获取聚合表达式
	expression := ""
	if field.Options().Rollup != nil && field.Options().Rollup.Expression != "" {
		expression = field.Options().Rollup.Expression
	} else if field.Options().Formula != nil && field.Options().Formula.Expression != "" {
		expression = field.Options().Formula.Expression
	} else if field.Options().Rollup != nil {
		expression = field.Options().Rollup.AggregationFunction
	}

	if expression == "" {
		return nil, fmt.Errorf("rollup expression is empty")
	}

	// 计算聚合值
	virtualData := map[string]interface{}{
		"values": values,
	}

	return s.evaluator.Evaluate(
		expression,
		make(FieldInstanceMap),
		virtualData,
	)
}

// calculateLookupWithDependencies 计算Lookup字段值
func (s *CrossTableCalculationService) calculateLookupWithDependencies(
	field *fieldEntity.Field,
	dependencies map[string]interface{},
) (interface{}, error) {
	if field.Options == nil {
		return nil, fmt.Errorf("lookup field options is nil")
	}

	// 获取要查找的字段ID
	lookupFieldID := field.Options().Lookup.LinkFieldID
	if lookupFieldID == "" && field.Options().Lookup != nil {
		lookupFieldID = field.Options().Lookup.LookupFieldID
	}

	// Lookup只取第一个依赖记录的值
	for _, depData := range dependencies {
		if depMap, ok := depData.(map[string]interface{}); ok {
			if value, exists := depMap[lookupFieldID]; exists {
				return value, nil
			}
		}
	}

	return nil, nil
}

// calculateFormulaWithDependencies 计算Formula字段值（带依赖）
func (s *CrossTableCalculationService) calculateFormulaWithDependencies(
	field *fieldEntity.Field,
	recordData map[string]interface{},
	dependencies map[string]interface{},
) (interface{}, error) {
	// 合并记录数据和依赖数据
	mergedData := make(map[string]interface{})
	for k, v := range recordData {
		mergedData[k] = v
	}
	for k, v := range dependencies {
		mergedData["_dep_"+k] = v
	}

	return s.evaluator.Evaluate(
		field.Options().Formula.Expression,
		make(FieldInstanceMap),
		mergedData,
	)
}

// 工具函数

func extractFieldIDs(fields []*fieldEntity.Field) []string {
	ids := make([]string, len(fields))
	for i, field := range fields {
		ids[i] = field.ID().String()
	}
	return ids
}

func joinStringsForSQL(strs []string) string {
	if len(strs) == 0 {
		return "''"
	}
	result := "'"
	for i, s := range strs {
		if i > 0 {
			result += "','"
		}
		result += s
	}
	result += "'"
	return result
}

// 辅助函数：从records切片中查找指定ID的record
func findRecordByID(records []map[string]interface{}, id string) map[string]interface{} {
	for _, rec := range records {
		if recID, ok := rec["id"].(string); ok && recID == id {
			return rec
		}
	}
	return nil
}
