package application

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/rollup"
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// RollupService Rollup计算服务
// 专门负责Rollup字段的计算
type RollupService struct {
	fieldRepo        repository.FieldRepository
	recordRepo       recordRepo.RecordRepository
	rollupCalculator *rollup.RollupCalculator
	errorService     *ErrorService
}

// NewRollupService 创建Rollup计算服务
func NewRollupService(
	fieldRepo repository.FieldRepository,
	recordRepo recordRepo.RecordRepository,
	rollupCalculator *rollup.RollupCalculator,
	errorService *ErrorService,
) *RollupService {
	return &RollupService{
		fieldRepo:        fieldRepo,
		recordRepo:       recordRepo,
		rollupCalculator: rollupCalculator,
		errorService:     errorService,
	}
}

// Calculate 计算Rollup字段
func (s *RollupService) Calculate(ctx context.Context, record *entity.Record, field *fieldEntity.Field) error {
	// 1. 获取Rollup配置
	options := field.Options()
	if options == nil || options.Rollup == nil {
		return s.errorService.HandleBusinessLogicError(ctx, "RollupService.Calculate", "rollup options not configured")
	}

	linkFieldID := options.Rollup.LinkFieldID
	rollupFieldID := options.Rollup.RollupFieldID
	expression := options.Rollup.Expression

	// 2. 获取Link字段的值（关联记录IDs）
	recordData := record.Data().ToMap()
	linkValue := recordData[linkFieldID]

	if linkValue == nil {
		// 无关联记录，设置为nil
		s.updateRecordField(record, field, nil)
		return nil
	}

	// 3. 查询关联记录的目标字段值
	linkedRecordIDs := s.extractRecordIDs(linkValue)
	values, err := s.fetchFieldValues(ctx, record.TableID(), linkedRecordIDs, rollupFieldID)
	if err != nil {
		return s.errorService.HandleDatabaseError(ctx, "fetchFieldValues", err)
	}

	// 4. 执行汇总计算
	result, err := s.rollupCalculator.Calculate(expression, values)
	if err != nil {
		return s.errorService.HandleBusinessLogicError(ctx, "RollupService.Calculate",
			"rollup calculation failed: "+err.Error())
	}

	// 5. 更新记录数据
	s.updateRecordField(record, field, result)

	logger.Info("rollup calculation completed",
		logger.String("field_id", field.ID().String()),
		logger.String("expression", expression),
		logger.Int("linked_records", len(linkedRecordIDs)),
		logger.Any("result", result))

	return nil
}

// fetchFieldValues 批量查询字段值
func (s *RollupService) fetchFieldValues(ctx context.Context, tableID string, recordIDs []string, fieldID string) ([]interface{}, error) {
	if len(recordIDs) == 0 {
		return []interface{}{}, nil
	}

	// 转换 string 到 RecordID
	recordIDObjects := make([]valueobject.RecordID, len(recordIDs))
	for i, id := range recordIDs {
		recordIDObjects[i] = valueobject.NewRecordID(id)
	}

	// 使用新的批量查询方法
	records, err := s.recordRepo.FindByIDs(ctx, tableID, recordIDObjects)
	if err != nil {
		return nil, err
	}

	// 提取指定字段的值
	values := make([]interface{}, 0, len(records))
	for _, record := range records {
		if value, exists := record.Data().Get(fieldID); exists {
			values = append(values, value)
		}
	}

	return values, nil
}

// extractRecordIDs 从Link字段值中提取Record IDs
func (s *RollupService) extractRecordIDs(linkValue interface{}) []string {
	if linkValue == nil {
		return []string{}
	}

	switch v := linkValue.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if id, ok := item.(string); ok {
				result = append(result, id)
			}
		}
		return result
	default:
		return []string{}
	}
}

// updateRecordField 更新记录字段值
func (s *RollupService) updateRecordField(record *entity.Record, field *fieldEntity.Field, value interface{}) {
	recordData := record.Data().ToMap()
	recordData[field.ID().String()] = value

	// 创建新的记录数据
	newData, err := valueobject.NewRecordData(recordData)
	if err != nil {
		logger.Error("failed to create new record data",
			logger.String("record_id", record.ID().String()),
			logger.String("field_id", field.ID().String()),
			logger.ErrorField(err))
		return
	}

	// 更新记录
	record.Update(newData, "system") // TODO: 从上下文获取实际的更新者
}
