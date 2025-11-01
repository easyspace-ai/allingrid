package application

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/lookup"
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// LookupService Lookup计算服务
// 专门负责Lookup字段的计算
type LookupService struct {
	recordRepo       recordRepo.RecordRepository
	lookupCalculator *lookup.LookupCalculator
	errorService     *ErrorService
}

// NewLookupService 创建Lookup计算服务
func NewLookupService(
	recordRepo recordRepo.RecordRepository,
	lookupCalculator *lookup.LookupCalculator,
	errorService *ErrorService,
) *LookupService {
	return &LookupService{
		recordRepo:       recordRepo,
		lookupCalculator: lookupCalculator,
		errorService:     errorService,
	}
}

// Calculate 计算Lookup字段
func (s *LookupService) Calculate(ctx context.Context, record *entity.Record, field *fieldEntity.Field) error {
	// 1. 获取Lookup配置
	options := field.Options()
	if options == nil || options.Lookup == nil {
		return s.errorService.HandleBusinessLogicError(ctx, "LookupService.Calculate", "lookup options not configured")
	}

	linkFieldID := options.Lookup.LinkFieldID
	lookupFieldID := options.Lookup.LookupFieldID

	// 2. 获取Link字段的值
	recordData := record.Data().ToMap()
	linkValue := recordData[linkFieldID]

	if linkValue == nil {
		// 无关联记录，设置为nil
		s.updateRecordField(record, field, nil)
		return nil
	}

	// 3. 查询关联记录
	linkedRecordIDs := s.extractRecordIDs(linkValue)
	linkedRecordsMap, err := s.fetchRecordsMap(ctx, record.TableID(), linkedRecordIDs)
	if err != nil {
		return s.errorService.HandleDatabaseError(ctx, "fetchRecordsMap", err)
	}

	// 4. 转换为lookup.Calculate需要的格式
	// 当前实现：返回第一条关联记录的数据（简化版）
	// 未来改进：支持返回多条关联记录的lookup结果
	var lookedRecord map[string]interface{}
	for _, record := range linkedRecordsMap {
		lookedRecord = record
		break // 取第一条
	}

	if lookedRecord == nil {
		s.updateRecordField(record, field, nil)
		return nil
	}

	// 5. 执行查找
	result, err := s.lookupCalculator.Calculate(
		linkValue,
		lookedRecord,
		lookupFieldID,
	)

	if err != nil {
		return s.errorService.HandleBusinessLogicError(ctx, "LookupService.Calculate",
			"lookup calculation failed: "+err.Error())
	}

	// 6. 更新记录数据
	s.updateRecordField(record, field, result)

	logger.Info("lookup calculation completed",
		logger.String("field_id", field.ID().String()),
		logger.String("lookup_field_id", lookupFieldID),
		logger.Int("linked_records", len(linkedRecordIDs)),
		logger.Any("result", result))

	return nil
}

// fetchRecordsMap 批量查询Records并转为Map
func (s *LookupService) fetchRecordsMap(ctx context.Context, tableID string, recordIDs []string) (map[string]map[string]interface{}, error) {
	if len(recordIDs) == 0 {
		return map[string]map[string]interface{}{}, nil
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

	// 转换为Map格式
	result := make(map[string]map[string]interface{})
	for _, record := range records {
		result[record.ID().String()] = record.Data().ToMap()
	}

	return result, nil
}

// extractRecordIDs 从Link字段值中提取Record IDs
func (s *LookupService) extractRecordIDs(linkValue interface{}) []string {
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
func (s *LookupService) updateRecordField(record *entity.Record, field *fieldEntity.Field, value interface{}) {
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
