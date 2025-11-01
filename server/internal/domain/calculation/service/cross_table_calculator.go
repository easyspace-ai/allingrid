package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/link"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
)

// CrossTableCalculator 跨表计算协调器
type CrossTableCalculator struct {
	linkService *link.LinkService
	logger      *zap.Logger
}

// NewCrossTableCalculator 创建跨表计算协调器
func NewCrossTableCalculator(
	linkService *link.LinkService,
	logger *zap.Logger,
) *CrossTableCalculator {
	return &CrossTableCalculator{
		linkService: linkService,
		logger:      logger,
	}
}

// CalculateCrossTable 执行跨表计算
// 处理Link, Lookup, Rollup等跨表依赖字段
func (c *CrossTableCalculator) CalculateCrossTable(
	ctx context.Context,
	topoOrders []*dependency.TopoItem,
	calcCtx *CalculationContext,
	calculator FieldCalculator,
) (OpsMap, error) {
	opsMap := NewOpsMap()
	recordIDsMap := calcCtx.StartRecordIDs

	c.logger.Debug("Starting cross-table calculation",
		zap.Int("topo_order_count", len(topoOrders)),
		zap.Int("start_fields", len(calcCtx.StartFieldIDs)),
	)

	for _, topoItem := range topoOrders {
		fieldID := topoItem.ID
		field := calcCtx.FieldMap[fieldID]

		if field == nil {
			c.logger.Warn("Field not found in field map", zap.String("field_id", fieldID))
			continue
		}

		// 收集受影响的记录ID
		fromRecordIDs := c.collectFromRecordIDs(topoItem, recordIDsMap)
		toRecordIDs := recordIDsMap[fieldID]

		if len(fromRecordIDs) == 0 && len(toRecordIDs) == 0 {
			c.logger.Debug("No affected records for field", zap.String("field_id", fieldID))
			continue
		}

		fieldType := field.Type().String()

		// 根据字段类型决定是跨表计算还是表内计算
		if c.isCrossTableField(fieldType) {
			// 跨表计算：可能影响其他表的记录
			if err := c.handleCrossTableField(
				ctx, field, fromRecordIDs, toRecordIDs, calcCtx, calculator, opsMap, recordIDsMap,
			); err != nil {
				c.logger.Error("Failed to handle cross-table field",
					zap.String("field_id", fieldID),
					zap.Error(err),
				)
				continue
			}
		} else {
			// 表内计算
			if err := c.handleInTableField(
				ctx, field, toRecordIDs, calcCtx, calculator, opsMap, recordIDsMap,
			); err != nil {
				c.logger.Error("Failed to handle in-table field",
					zap.String("field_id", fieldID),
					zap.Error(err),
				)
				continue
			}
		}
	}

	return opsMap, nil
}

// collectFromRecordIDs 收集依赖字段的受影响记录ID
func (c *CrossTableCalculator) collectFromRecordIDs(
	topoItem *dependency.TopoItem,
	recordIDsMap map[string][]string,
) []string {
	fromRecordIDSet := make(map[string]bool)

	for _, depFieldID := range topoItem.Dependencies {
		if recordIDs, ok := recordIDsMap[depFieldID]; ok {
			for _, recordID := range recordIDs {
				fromRecordIDSet[recordID] = true
			}
		}
	}

	fromRecordIDs := make([]string, 0, len(fromRecordIDSet))
	for recordID := range fromRecordIDSet {
		fromRecordIDs = append(fromRecordIDs, recordID)
	}

	return fromRecordIDs
}

// isCrossTableField 判断是否是跨表字段
func (c *CrossTableCalculator) isCrossTableField(fieldType string) bool {
	return fieldType == valueobject.TypeLink ||
		fieldType == valueobject.TypeLookup ||
		fieldType == valueobject.TypeRollup
}

// handleCrossTableField 处理跨表字段
func (c *CrossTableCalculator) handleCrossTableField(
	ctx context.Context,
	field *entity.Field,
	fromRecordIDs []string,
	toRecordIDs []string,
	calcCtx *CalculationContext,
	calculator FieldCalculator,
	opsMap OpsMap,
	recordIDsMap map[string][]string,
) error {
	fieldID := field.ID().String()

	// 合并受影响的记录ID
	allRecordIDs := c.mergeRecordIDs(fromRecordIDs, toRecordIDs)

	if len(allRecordIDs) == 0 {
		return nil
	}

	// 获取受Link影响的其他表记录
	affectedRecords, err := c.linkService.GetAffectedRecordsByLink(
		ctx,
		field.TableID(),
		fieldID,
		allRecordIDs,
	)
	if err != nil {
		return fmt.Errorf("failed to get affected records: %w", err)
	}

	// 计算本表的记录
	if err := c.calculateFieldForRecords(
		ctx, field, allRecordIDs, calcCtx, calculator, opsMap,
	); err != nil {
		return err
	}

	// 计算受影响的其他表记录
	for linkedTableID, linkedRecordIDs := range affectedRecords {
		// 查找该表中依赖当前字段的字段
		dependentFields := c.findDependentFields(linkedTableID, fieldID, calcCtx)

		for _, depField := range dependentFields {
			if err := c.calculateFieldForRecords(
				ctx, depField, linkedRecordIDs, calcCtx, calculator, opsMap,
			); err != nil {
				c.logger.Error("Failed to calculate dependent field",
					zap.String("field_id", depField.ID().String()),
					zap.Error(err),
				)
				continue
			}
		}

		// 记录受影响的记录，供下一轮使用
		recordIDsMap[fieldID] = append(recordIDsMap[fieldID], linkedRecordIDs...)
	}

	return nil
}

// handleInTableField 处理表内字段
func (c *CrossTableCalculator) handleInTableField(
	ctx context.Context,
	field *entity.Field,
	recordIDs []string,
	calcCtx *CalculationContext,
	calculator FieldCalculator,
	opsMap OpsMap,
	recordIDsMap map[string][]string,
) error {
	if len(recordIDs) == 0 {
		return nil
	}

	if err := c.calculateFieldForRecords(
		ctx, field, recordIDs, calcCtx, calculator, opsMap,
	); err != nil {
		return err
	}

	fieldID := field.ID().String()
	recordIDsMap[fieldID] = recordIDs

	return nil
}

// calculateFieldForRecords 计算字段的多条记录
func (c *CrossTableCalculator) calculateFieldForRecords(
	ctx context.Context,
	field *entity.Field,
	recordIDs []string,
	calcCtx *CalculationContext,
	calculator FieldCalculator,
	opsMap OpsMap,
) error {
	fieldID := field.ID().String()
	tableID := field.TableID()

	for _, recordID := range recordIDs {
		value, err := calculator.CalculateFieldValue(ctx, field, recordID, calcCtx)
		if err != nil {
			c.logger.Error("Failed to calculate field value",
				zap.String("field_id", fieldID),
				zap.String("record_id", recordID),
				zap.Error(err),
			)
			continue
		}

		opsMap.Set(tableID, recordID, fieldID, value)
	}

	return nil
}

// mergeRecordIDs 合并记录ID列表，去重
func (c *CrossTableCalculator) mergeRecordIDs(lists ...[]string) []string {
	recordIDSet := make(map[string]bool)

	for _, list := range lists {
		for _, recordID := range list {
			recordIDSet[recordID] = true
		}
	}

	result := make([]string, 0, len(recordIDSet))
	for recordID := range recordIDSet {
		result = append(result, recordID)
	}

	return result
}

// findDependentFields 查找依赖指定字段的字段
func (c *CrossTableCalculator) findDependentFields(
	tableID string,
	dependsOnFieldID string,
	calcCtx *CalculationContext,
) []*entity.Field {
	dependentFields := []*entity.Field{}

	for _, field := range calcCtx.FieldMap {
		if field.TableID() != tableID {
			continue
		}

		// 检查字段是否依赖指定字段
		if c.fieldDependsOn(field, dependsOnFieldID) {
			dependentFields = append(dependentFields, field)
		}
	}

	return dependentFields
}

// fieldDependsOn 判断字段是否依赖另一个字段
func (c *CrossTableCalculator) fieldDependsOn(field *entity.Field, targetFieldID string) bool {
	options := field.Options()
	if options == nil {
		return false
	}

	fieldType := field.Type().String()

	switch fieldType {
	case valueobject.TypeLookup:
		if options.Lookup != nil {
			return options.Lookup.LinkFieldID == targetFieldID ||
				options.Lookup.LookupFieldID == targetFieldID
		}
	case valueobject.TypeRollup:
		if options.Rollup != nil {
			return options.Rollup.LinkFieldID == targetFieldID ||
				options.Rollup.RollupFieldID == targetFieldID
		}
	case valueobject.TypeFormula:
		// TODO: 解析公式表达式，检查是否引用了目标字段
		// 暂时简化处理
		return false
	}

	return false
}

// FieldCalculator 字段计算器接口
type FieldCalculator interface {
	CalculateFieldValue(ctx context.Context, field *entity.Field, recordID string, calcCtx *CalculationContext) (interface{}, error)
}
