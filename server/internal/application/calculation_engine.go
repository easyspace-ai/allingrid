package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/service"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	recordEntity "github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CalculationEngine 计算引擎
// 参考旧系统: teable-develop/apps/nestjs-backend/src/features/field/field-calculate/
type CalculationEngine struct {
	fieldRepo  repository.FieldRepository
	recordRepo recordRepo.RecordRepository
	depGraph   *service.DependencyGraph
}

// NewCalculationEngine 创建计算引擎
func NewCalculationEngine(
	fieldRepo repository.FieldRepository,
	recordRepo recordRepo.RecordRepository,
) *CalculationEngine {
	return &CalculationEngine{
		fieldRepo:  fieldRepo,
		recordRepo: recordRepo,
		depGraph:   service.NewDependencyGraph(fieldRepo),
	}
}

// CalculateAllFields 计算记录的所有虚拟字段
func (e *CalculationEngine) CalculateAllFields(ctx context.Context, record *recordEntity.Record) error {
	logger.Info("计算所有虚拟字段",
		logger.String("record_id", record.ID().String()),
		logger.String("table_id", record.TableID()))

	// 1. 构建依赖图
	if err := e.depGraph.Build(ctx, record.TableID()); err != nil {
		return fmt.Errorf("构建依赖图失败: %w", err)
	}

	// 2. 拓扑排序
	sortedFieldIDs, err := e.depGraph.TopologicalSort()
	if err != nil {
		return fmt.Errorf("拓扑排序失败: %w", err)
	}

	logger.Info("拓扑排序完成", logger.Int("fields", len(sortedFieldIDs)))

	// 3. 按顺序计算每个字段
	for _, fieldID := range sortedFieldIDs {
		fieldIDVO := valueobject.NewFieldID(fieldID)
		field, _ := e.fieldRepo.FindByID(ctx, fieldIDVO)
		if field == nil {
			continue
		}

		value, err := e.calculateField(ctx, record, field)
		if err != nil {
			logger.Error("字段计算失败",
				logger.String("field_id", fieldID),
				logger.String("field_name", field.Name().String()),
				logger.ErrorField(err))
			continue
		}

		// 更新记录数据
		record.Data().Set(fieldID, value)

		logger.Debug("字段计算成功",
			logger.String("field_id", fieldID),
			logger.String("field_name", field.Name().String()),
			logger.Any("value", value))
	}

	return nil
}

// CalculateAffectedFields 计算受影响的字段
func (e *CalculationEngine) CalculateAffectedFields(
	ctx context.Context,
	record *recordEntity.Record,
	changedFieldIDs []string,
) error {
	logger.Info("计算受影响的字段",
		logger.String("record_id", record.ID().String()),
		logger.Int("changed_fields", len(changedFieldIDs)))

	// 1. 构建依赖图
	if err := e.depGraph.Build(ctx, record.TableID()); err != nil {
		return fmt.Errorf("构建依赖图失败: %w", err)
	}

	// 2. 获取受影响的字段（拓扑排序）
	affectedFieldIDs, err := e.depGraph.GetAffectedFields(changedFieldIDs)
	if err != nil {
		return fmt.Errorf("获取受影响字段失败: %w", err)
	}

	if len(affectedFieldIDs) == 0 {
		logger.Info("没有受影响的计算字段")
		return nil
	}

	logger.Info("受影响的字段",
		logger.Int("count", len(affectedFieldIDs)),
		logger.Any("field_ids", affectedFieldIDs))

	// 3. 按顺序计算每个字段
	for _, fieldID := range affectedFieldIDs {
		fieldIDVO := valueobject.NewFieldID(fieldID)
		field, _ := e.fieldRepo.FindByID(ctx, fieldIDVO)
		if field == nil {
			continue
		}

		value, err := e.calculateField(ctx, record, field)
		if err != nil {
			logger.Error("字段计算失败",
				logger.String("field_id", fieldID),
				logger.ErrorField(err))
			continue
		}

		// 更新记录数据
		record.Data().Set(fieldID, value)

		logger.Debug("字段计算成功",
			logger.String("field_id", fieldID),
			logger.Any("value", value))
	}

	return nil
}

// calculateField 计算单个字段
func (e *CalculationEngine) calculateField(
	ctx context.Context,
	record *recordEntity.Record,
	field *entity.Field,
) (interface{}, error) {
	switch field.Type().String() {
	case "formula":
		return e.calculateFormula(ctx, record, field)
	case "lookup":
		return e.calculateLookup(ctx, record, field)
	case "rollup":
		return e.calculateRollup(ctx, record, field)
	case "count":
		return e.calculateCount(ctx, record, field)
	default:
		return nil, fmt.Errorf("不支持的计算字段类型: %s", field.Type().String())
	}
}

// calculateFormula 计算 Formula 字段
func (e *CalculationEngine) calculateFormula(
	ctx context.Context,
	record *recordEntity.Record,
	field *entity.Field,
) (interface{}, error) {
	// TODO: 实现 Formula 计算引擎
	// 参考旧系统: FormulaField
	logger.Debug("计算 Formula 字段", logger.String("field_id", field.ID().String()))
	return nil, nil
}

// calculateLookup 计算 Lookup 字段
func (e *CalculationEngine) calculateLookup(
	ctx context.Context,
	record *recordEntity.Record,
	field *entity.Field,
) (interface{}, error) {
	// TODO: 实现 Lookup 计算
	// 参考旧系统: LookupField
	logger.Debug("计算 Lookup 字段", logger.String("field_id", field.ID().String()))
	return nil, nil
}

// calculateRollup 计算 Rollup 字段
func (e *CalculationEngine) calculateRollup(
	ctx context.Context,
	record *recordEntity.Record,
	field *entity.Field,
) (interface{}, error) {
	// TODO: 实现 Rollup 计算
	// 参考旧系统: RollupField
	logger.Debug("计算 Rollup 字段", logger.String("field_id", field.ID().String()))
	return nil, nil
}

// calculateCount 计算 Count 字段
func (e *CalculationEngine) calculateCount(
	ctx context.Context,
	record *recordEntity.Record,
	field *entity.Field,
) (interface{}, error) {
	// TODO: 实现 Count 计算
	// 参考旧系统: CountField
	logger.Debug("计算 Count 字段", logger.String("field_id", field.ID().String()))
	return nil, nil
}
