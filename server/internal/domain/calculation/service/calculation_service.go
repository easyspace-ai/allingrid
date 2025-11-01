package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
)

// FieldRepository 字段仓储接口
type FieldRepository interface {
	FindByTableID(ctx context.Context, tableID string) ([]*entity.Field, error)
	FindByID(ctx context.Context, fieldID string) (*entity.Field, error)
	FindByIDs(ctx context.Context, fieldIDs []string) ([]*entity.Field, error)
}

// RecordRepository 记录仓储接口（使用真正的 RecordRepository）
type RecordRepository = recordRepo.RecordRepository

// TableRepository 表仓储接口
type TableRepository interface {
	GetDBTableName(ctx context.Context, tableID string) (string, error)
}

// CalculationService 计算服务
// 负责协调字段依赖计算的整个流程
type CalculationService struct {
	fieldRepo         FieldRepository
	recordRepo        RecordRepository
	tableRepo         TableRepository
	dependencyRepo    *dependency.DependencyGraphRepository
	dependencyBuilder *dependency.DependencyGraphBuilder
	batchService      *BatchService
	logger            *zap.Logger
}

// NewCalculationService 创建计算服务
func NewCalculationService(
	fieldRepo FieldRepository,
	recordRepo RecordRepository,
	tableRepo TableRepository,
	dependencyRepo *dependency.DependencyGraphRepository,
	dependencyBuilder *dependency.DependencyGraphBuilder,
	batchService *BatchService,
	logger *zap.Logger,
) *CalculationService {
	return &CalculationService{
		fieldRepo:         fieldRepo,
		recordRepo:        recordRepo,
		tableRepo:         tableRepo,
		dependencyRepo:    dependencyRepo,
		dependencyBuilder: dependencyBuilder,
		batchService:      batchService,
		logger:            logger,
	}
}

// CalculateAffectedFields 计算受影响的字段
// 这是主入口方法，当字段值变更时调用
// 参考 teable-develop/apps/nestjs-backend/src/features/calculation/service/calculation_service.ts
func (s *CalculationService) CalculateAffectedFields(ctx context.Context, changes []CellChange) (OpsMap, error) {
	if len(changes) == 0 {
		return NewOpsMap(), nil
	}

	s.logger.Info("Starting field calculation",
		zap.Int("changes_count", len(changes)),
	)

	// 1. 准备计算上下文
	calcCtx, err := s.PrepareCalculation(ctx, changes)
	if err != nil {
		s.logger.Error("Failed to prepare calculation",
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to prepare calculation: %w", err)
	}

	// 2. 执行计算
	opsMap, err := s.ExecuteCalculation(ctx, calcCtx)
	if err != nil {
		s.logger.Error("Failed to execute calculation",
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to execute calculation: %w", err)
	}

	s.logger.Info("Field calculation completed",
		zap.Int("affected_tables", len(opsMap)),
	)

	return opsMap, nil
}

// PrepareCalculation 准备计算上下文
// 构建依赖图、拓扑排序、加载字段和记录数据
func (s *CalculationService) PrepareCalculation(ctx context.Context, changes []CellChange) (*CalculationContext, error) {
	if len(changes) == 0 {
		return nil, fmt.Errorf("no changes provided")
	}

	// 假设所有变更在同一个表（后续可扩展支持多表）
	tableID := changes[0].TableID
	calcCtx := NewCalculationContext(tableID)

	// 1. 收集变更的字段ID和记录ID
	changedFieldIDs := make(map[string]bool)
	recordIDsPerField := make(map[string][]string)

	for _, change := range changes {
		changedFieldIDs[change.FieldID] = true
		recordIDsPerField[change.FieldID] = append(recordIDsPerField[change.FieldID], change.RecordID)
	}

	// 转换为数组
	for fieldID := range changedFieldIDs {
		calcCtx.AddStartField(fieldID, recordIDsPerField[fieldID])
	}

	// 2. 获取依赖图
	graph, err := s.dependencyRepo.GetDependencyGraph(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependency graph: %w", err)
	}
	calcCtx.DependencyGraph = graph

	// 3. 获取拓扑排序
	topoOrder, err := dependency.GetTopoOrders(graph)
	if err != nil {
		return nil, fmt.Errorf("failed to get topological order: %w", err)
	}

	// 在拓扑排序前添加起始字段
	topoOrder = dependency.PrependStartFieldIDs(topoOrder, calcCtx.StartFieldIDs)
	calcCtx.TopologicalOrder = topoOrder

	// 4. 加载所有相关字段
	allFieldIDs := dependency.FlatGraph(graph)
	allFieldIDs = append(allFieldIDs, calcCtx.StartFieldIDs...)

	// 去重
	fieldIDSet := make(map[string]bool)
	for _, fieldID := range allFieldIDs {
		fieldIDSet[fieldID] = true
	}
	uniqueFieldIDs := make([]string, 0, len(fieldIDSet))
	for fieldID := range fieldIDSet {
		uniqueFieldIDs = append(uniqueFieldIDs, fieldID)
	}

	fields, err := s.fieldRepo.FindByIDs(ctx, uniqueFieldIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to load fields: %w", err)
	}

	// 构建字段映射
	for _, field := range fields {
		fieldID := field.ID().String()
		calcCtx.FieldMap[fieldID] = field
		calcCtx.FieldID2TableID[fieldID] = field.TableID()
		calcCtx.FieldID2DBFieldName[fieldID] = field.DBFieldName().String()
	}

	// 5. 加载数据库表名
	dbTableName, err := s.tableRepo.GetDBTableName(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get db table name: %w", err)
	}
	calcCtx.TableID2DBTableName[tableID] = dbTableName

	// 6. 加载初始记录数据
	allRecordIDs := make([]string, 0)
	for _, recordIDs := range recordIDsPerField {
		allRecordIDs = append(allRecordIDs, recordIDs...)
	}

	// 去重记录ID
	recordIDSet := make(map[string]bool)
	for _, recordID := range allRecordIDs {
		recordIDSet[recordID] = true
	}
	uniqueRecordIDs := make([]string, 0, len(recordIDSet))
	for recordID := range recordIDSet {
		uniqueRecordIDs = append(uniqueRecordIDs, recordID)
	}

	// 转换 string 到 RecordID
	recordIDs := make([]valueobject.RecordID, len(uniqueRecordIDs))
	for i, id := range uniqueRecordIDs {
		recordIDs[i] = valueobject.NewRecordID(id)
	}

	records, err := s.recordRepo.FindByIDs(ctx, tableID, recordIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to load records: %w", err)
	}

	// 缓存记录数据
	for _, record := range records {
		recordID := record.ID().String()
		// 将 Record 实体转换为 map[string]interface{} 格式
		recordData := make(map[string]interface{})
		recordData["id"] = recordID
		// TODO: 添加其他字段的转换
		calcCtx.SetRecordData(recordID, recordData)
	}

	s.logger.Info("Calculation context prepared",
		zap.String("table_id", tableID),
		zap.Int("start_fields", len(calcCtx.StartFieldIDs)),
		zap.Int("topo_items", len(calcCtx.TopologicalOrder)),
		zap.Int("total_fields", len(calcCtx.FieldMap)),
		zap.Int("cached_records", len(calcCtx.RecordDataCache)),
	)

	return calcCtx, nil
}

// ExecuteCalculation 执行计算
// 按拓扑顺序计算所有受影响的字段
func (s *CalculationService) ExecuteCalculation(ctx context.Context, calcCtx *CalculationContext) (OpsMap, error) {
	opsMap := NewOpsMap()

	// 记录ID映射：fieldID -> []recordID
	// 跟踪每个字段受影响的记录
	recordIDsMap := make(map[string][]string)
	for fieldID, recordIDs := range calcCtx.StartRecordIDs {
		recordIDsMap[fieldID] = recordIDs
	}

	// 按拓扑顺序遍历每个字段
	for _, topoItem := range calcCtx.TopologicalOrder {
		fieldID := topoItem.ID
		field, ok := calcCtx.GetField(fieldID)
		if !ok {
			s.logger.Warn("Field not found in context",
				zap.String("field_id", fieldID),
			)
			continue
		}

		// 收集受影响的记录ID
		// 来自依赖字段或直接指定
		affectedRecordIDs := make(map[string]bool)

		// 从依赖字段收集
		for _, depFieldID := range topoItem.Dependencies {
			if recordIDs, ok := recordIDsMap[depFieldID]; ok {
				for _, recordID := range recordIDs {
					affectedRecordIDs[recordID] = true
				}
			}
		}

		// 直接指定的记录
		if recordIDs, ok := recordIDsMap[fieldID]; ok {
			for _, recordID := range recordIDs {
				affectedRecordIDs[recordID] = true
			}
		}

		if len(affectedRecordIDs) == 0 {
			continue
		}

		// 转换为数组
		recordIDsList := make([]string, 0, len(affectedRecordIDs))
		for recordID := range affectedRecordIDs {
			recordIDsList = append(recordIDsList, recordID)
		}

		s.logger.Debug("Calculating field",
			zap.String("field_id", fieldID),
			zap.String("field_type", field.Type().String()),
			zap.Int("record_count", len(recordIDsList)),
		)

		// 根据字段类型计算值
		if field.IsComputed() {
			// 计算字段值
			for _, recordID := range recordIDsList {
				value, err := s.calculateFieldValue(ctx, field, recordID, calcCtx)
				if err != nil {
					s.logger.Error("Failed to calculate field value",
						zap.String("field_id", fieldID),
						zap.String("record_id", recordID),
						zap.Error(err),
					)
					continue
				}
				opsMap.Set(field.TableID(), recordID, fieldID, value)
			}
		}

		// 记录本字段影响的记录，供下一轮使用
		recordIDsMap[fieldID] = recordIDsList
	}

	return opsMap, nil
}

// ApplyResults 应用计算结果到数据库
// 批量更新记录字段值
func (s *CalculationService) ApplyResults(ctx context.Context, opsMap OpsMap) error {
	if opsMap.IsEmpty() {
		return nil
	}

	return s.batchService.BatchUpdateRecords(ctx, opsMap)
}

// CalculateFieldForRecords 为指定记录计算字段值
// 用于字段创建或公式变更后重新计算所有记录
func (s *CalculationService) CalculateFieldForRecords(ctx context.Context, tableID, fieldID string, recordIDs []string) error {
	// 构造虚拟的变更
	changes := make([]CellChange, len(recordIDs))
	for i, recordID := range recordIDs {
		changes[i] = CellChange{
			TableID:  tableID,
			RecordID: recordID,
			FieldID:  fieldID,
			OldValue: nil,
			NewValue: nil, // 触发重新计算
		}
	}

	// 执行计算
	opsMap, err := s.CalculateAffectedFields(ctx, changes)
	if err != nil {
		return err
	}

	// 应用结果
	return s.ApplyResults(ctx, opsMap)
}

// RecalculateFieldForRecords 重新计算字段的所有记录
// 用于公式变更后的批量重算
func (s *CalculationService) RecalculateFieldForRecords(ctx context.Context, tableID, fieldID string, recordIDs []string) error {
	return s.CalculateFieldForRecords(ctx, tableID, fieldID, recordIDs)
}

// calculateFieldValue 计算单个字段的值
func (s *CalculationService) calculateFieldValue(
	ctx context.Context,
	field *entity.Field,
	recordID string,
	calcCtx *CalculationContext,
) (interface{}, error) {
	// 获取记录数据
	recordData, ok := calcCtx.GetRecordData(recordID)
	if !ok {
		// 记录不在缓存中，从数据库加载
		recordIDVO := valueobject.NewRecordID(recordID)
		record, err := s.recordRepo.FindByTableAndID(ctx, field.TableID(), recordIDVO)
		if err != nil {
			return nil, fmt.Errorf("failed to load record: %w", err)
		}
		// 将 Record 实体转换为 map[string]interface{} 格式
		recordData = make(map[string]interface{})
		recordData["id"] = record.ID().String()
		// TODO: 添加其他字段的转换
		calcCtx.SetRecordData(recordID, recordData)
	}

	// TODO: 实现计算器工厂
	// 暂时返回nil，等待完整实现
	// calculator := s.calculatorFactory.GetCalculator(field)
	// if calculator == nil {
	// 	return nil, fmt.Errorf("no calculator found for field type: %s", field.Type().String())
	// }

	// // 执行计算
	// value, err := calculator.Calculate(ctx, field, recordData, calcCtx.FieldMap)
	// if err != nil {
	// 	return nil, fmt.Errorf("calculation failed: %w", err)
	// }

	// 临时实现：直接返回nil，避免未使用变量警告
	_ = recordData
	var value interface{} = nil

	return value, nil
}

// CalculateFieldValue 实现FieldCalculator接口
func (s *CalculationService) CalculateFieldValue(
	ctx context.Context,
	field *entity.Field,
	recordID string,
	calcCtx *CalculationContext,
) (interface{}, error) {
	return s.calculateFieldValue(ctx, field, recordID, calcCtx)
}
