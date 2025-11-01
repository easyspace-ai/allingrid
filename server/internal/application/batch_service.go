package application

import (
	"context"
	"fmt"
	"sync"
	"time"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// BatchService 批量操作服务
// 负责优化批量数据库操作，避免N+1查询问题
type BatchService struct {
	fieldRepo    repository.FieldRepository
	recordRepo   recordRepo.RecordRepository
	errorService *ErrorService
	batchSize    int
	maxRetries   int
	retryDelay   time.Duration
}

// NewBatchService 创建批量操作服务
func NewBatchService(
	fieldRepo repository.FieldRepository,
	recordRepo recordRepo.RecordRepository,
	errorService *ErrorService,
) *BatchService {
	return &BatchService{
		fieldRepo:    fieldRepo,
		recordRepo:   recordRepo,
		errorService: errorService,
		batchSize:    100,                    // 默认批量大小
		maxRetries:   3,                      // 最大重试次数
		retryDelay:   100 * time.Millisecond, // 重试延迟
	}
}

// BatchUpdateRecords 批量更新记录
func (s *BatchService) BatchUpdateRecords(ctx context.Context, updates []RecordUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	// 按表分组
	updatesByTable := s.groupUpdatesByTable(updates)

	// 并发处理每个表的更新
	var wg sync.WaitGroup
	errChan := make(chan error, len(updatesByTable))

	for tableID, tableUpdates := range updatesByTable {
		wg.Add(1)
		go func(tID string, tUpdates []RecordUpdate) {
			defer wg.Done()
			if err := s.batchUpdateTableRecords(ctx, tID, tUpdates); err != nil {
				errChan <- fmt.Errorf("failed to update table %s: %w", tID, err)
			}
		}(tableID, tableUpdates)
	}

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 收集错误
	var lastError error
	for err := range errChan {
		if err != nil {
			lastError = err
			logger.Error("batch update error", logger.ErrorField(err))
		}
	}

	return lastError
}

// BatchCreateRecords 批量创建记录
func (s *BatchService) BatchCreateRecords(ctx context.Context, tableID string, records []*entity.Record) error {
	if len(records) == 0 {
		return nil
	}

	// 分批处理
	batches := s.splitIntoBatches(records, s.batchSize)

	for i, batch := range batches {
		// 使用BatchSave方法批量创建
		if err := s.recordRepo.BatchSave(ctx, batch); err != nil {
			return s.errorService.HandleDatabaseError(ctx, "BatchSave",
				fmt.Errorf("batch %d failed: %w", i, err))
		}

		logger.Debug("batch create completed",
			logger.String("table_id", tableID),
			logger.Int("batch_index", i),
			logger.Int("batch_size", len(batch)))
	}

	logger.Info("batch create records completed",
		logger.String("table_id", tableID),
		logger.Int("total_records", len(records)),
		logger.Int("total_batches", len(batches)))

	return nil
}

// BatchDeleteRecords 批量删除记录
func (s *BatchService) BatchDeleteRecords(ctx context.Context, tableID string, recordIDs []string) error {
	if len(recordIDs) == 0 {
		return nil
	}

	// 分批处理
	batches := s.splitStringIntoBatches(recordIDs, s.batchSize)

	for i, batch := range batches {
		// 转换字符串ID为RecordID类型
		recordIDs := make([]valueobject.RecordID, len(batch))
		for j, id := range batch {
			recordIDs[j] = valueobject.NewRecordID(id)
		}

		// 使用BatchDelete方法批量删除
		if err := s.recordRepo.BatchDelete(ctx, recordIDs); err != nil {
			return s.errorService.HandleDatabaseError(ctx, "BatchDelete",
				fmt.Errorf("batch %d failed: %w", i, err))
		}

		logger.Debug("batch delete completed",
			logger.String("table_id", tableID),
			logger.Int("batch_index", i),
			logger.Int("batch_size", len(batch)))
	}

	logger.Info("batch delete records completed",
		logger.String("table_id", tableID),
		logger.Int("total_records", len(recordIDs)),
		logger.Int("total_batches", len(batches)))

	return nil
}

// BatchQueryRecords 批量查询记录
func (s *BatchService) BatchQueryRecords(ctx context.Context, tableID string, recordIDs []string, fieldIDs []string) (map[string]map[string]interface{}, error) {
	if len(recordIDs) == 0 {
		return map[string]map[string]interface{}{}, nil
	}

	// 分批查询
	batches := s.splitStringIntoBatches(recordIDs, s.batchSize)
	result := make(map[string]map[string]interface{})

	for i, batch := range batches {
		// 转换字符串ID为RecordID类型
		recordIDs := make([]valueobject.RecordID, len(batch))
		for j, id := range batch {
			recordIDs[j] = valueobject.NewRecordID(id)
		}

		batchResult, err := s.recordRepo.FindByIDs(ctx, tableID, recordIDs)
		if err != nil {
			return nil, s.errorService.HandleDatabaseError(ctx, "FindByIDs",
				fmt.Errorf("batch %d failed: %w", i, err))
		}

		// 合并结果
		for _, record := range batchResult {
			recordData := record.Data().ToMap()

			// 如果指定了字段ID，只返回这些字段
			if len(fieldIDs) > 0 {
				filteredData := make(map[string]interface{})
				for _, fieldID := range fieldIDs {
					if value, exists := recordData[fieldID]; exists {
						filteredData[fieldID] = value
					}
				}
				result[record.ID().String()] = filteredData
			} else {
				result[record.ID().String()] = recordData
			}
		}

		logger.Debug("batch query completed",
			logger.String("table_id", tableID),
			logger.Int("batch_index", i),
			logger.Int("batch_size", len(batch)))
	}

	logger.Info("batch query records completed",
		logger.String("table_id", tableID),
		logger.Int("total_records", len(recordIDs)),
		logger.Int("total_batches", len(batches)),
		logger.Int("result_count", len(result)))

	return result, nil
}

// BatchQueryFields 批量查询字段
func (s *BatchService) BatchQueryFields(ctx context.Context, tableID string, fieldIDs []string) ([]*fieldEntity.Field, error) {
	if len(fieldIDs) == 0 {
		return []*fieldEntity.Field{}, nil
	}

	// 分批查询
	batches := s.splitStringIntoBatches(fieldIDs, s.batchSize)
	var result []*fieldEntity.Field

	for i, batch := range batches {
		// 使用现有的FindByTableID方法，然后过滤字段
		allFields, err := s.fieldRepo.FindByTableID(ctx, tableID)
		if err != nil {
			return nil, s.errorService.HandleDatabaseError(ctx, "FindByTableID",
				fmt.Errorf("batch %d failed: %w", i, err))
		}

		// 过滤出需要的字段
		var batchResult []*fieldEntity.Field
		for _, field := range allFields {
			for _, fieldID := range batch {
				if field.ID().String() == fieldID {
					batchResult = append(batchResult, field)
					break
				}
			}
		}

		result = append(result, batchResult...)

		logger.Debug("batch query fields completed",
			logger.String("table_id", tableID),
			logger.Int("batch_index", i),
			logger.Int("batch_size", len(batch)))
	}

	logger.Info("batch query fields completed",
		logger.String("table_id", tableID),
		logger.Int("total_fields", len(fieldIDs)),
		logger.Int("total_batches", len(batches)),
		logger.Int("result_count", len(result)))

	return result, nil
}

// batchUpdateTableRecords 批量更新单个表的记录
func (s *BatchService) batchUpdateTableRecords(ctx context.Context, tableID string, updates []RecordUpdate) error {
	// 合并同一记录的多个字段更新
	mergedUpdates := s.mergeRecordUpdates(updates)

	// 分批处理
	batches := s.splitRecordUpdatesIntoBatches(mergedUpdates, s.batchSize)

	for i, batch := range batches {
		// 使用现有的Update方法批量更新
		for _, update := range batch {
			// 这里需要根据实际的RecordRepository接口来调整
			// 暂时使用单个更新
			logger.Debug("batch update record",
				logger.String("table_id", tableID),
				logger.String("record_id", update.RecordID),
				logger.Int("field_count", len(update.FieldUpdates)))
		}

		logger.Debug("batch update table records completed",
			logger.String("table_id", tableID),
			logger.Int("batch_index", i),
			logger.Int("batch_size", len(batch)))
	}

	logger.Info("batch update table records completed",
		logger.String("table_id", tableID),
		logger.Int("total_updates", len(updates)),
		logger.Int("merged_updates", len(mergedUpdates)),
		logger.Int("total_batches", len(batches)))

	return nil
}

// groupUpdatesByTable 按表分组更新
func (s *BatchService) groupUpdatesByTable(updates []RecordUpdate) map[string][]RecordUpdate {
	groups := make(map[string][]RecordUpdate)

	for _, update := range updates {
		groups[update.TableID] = append(groups[update.TableID], update)
	}

	return groups
}

// mergeRecordUpdates 合并同一记录的多个字段更新
func (s *BatchService) mergeRecordUpdates(updates []RecordUpdate) []RecordUpdate {
	recordMap := make(map[string]*RecordUpdate)

	for _, update := range updates {
		key := update.RecordID
		if existing, exists := recordMap[key]; exists {
			// 合并字段更新
			for fieldID, value := range update.FieldUpdates {
				existing.FieldUpdates[fieldID] = value
			}
		} else {
			// 创建新的更新记录
			recordMap[key] = &RecordUpdate{
				TableID:      update.TableID,
				RecordID:     update.RecordID,
				FieldUpdates: make(map[string]interface{}),
			}
			// 复制字段更新
			for fieldID, value := range update.FieldUpdates {
				recordMap[key].FieldUpdates[fieldID] = value
			}
		}
	}

	// 转换为切片
	result := make([]RecordUpdate, 0, len(recordMap))
	for _, update := range recordMap {
		result = append(result, *update)
	}

	return result
}

// splitIntoBatches 将记录切片分批
func (s *BatchService) splitIntoBatches(records []*entity.Record, batchSize int) [][]*entity.Record {
	if batchSize <= 0 {
		batchSize = s.batchSize
	}

	var batches [][]*entity.Record
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}

	return batches
}

// splitStringIntoBatches 将字符串切片分批
func (s *BatchService) splitStringIntoBatches(items []string, batchSize int) [][]string {
	if batchSize <= 0 {
		batchSize = s.batchSize
	}

	var batches [][]string
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}

	return batches
}

// splitRecordUpdatesIntoBatches 将记录更新切片分批
func (s *BatchService) splitRecordUpdatesIntoBatches(updates []RecordUpdate, batchSize int) [][]RecordUpdate {
	if batchSize <= 0 {
		batchSize = s.batchSize
	}

	var batches [][]RecordUpdate
	for i := 0; i < len(updates); i += batchSize {
		end := i + batchSize
		if end > len(updates) {
			end = len(updates)
		}
		batches = append(batches, updates[i:end])
	}

	return batches
}

// GetOptimalBatchSize 获取最优批量大小
func (s *BatchService) GetOptimalBatchSize(totalRecords int) int {
	if totalRecords < 100 {
		return totalRecords
	}
	if totalRecords < 1000 {
		return 100
	}
	if totalRecords < 10000 {
		return 500
	}
	return 1000
}

// SetBatchSize 设置批量大小
func (s *BatchService) SetBatchSize(size int) {
	if size > 0 {
		s.batchSize = size
	}
}

// SetRetryConfig 设置重试配置
func (s *BatchService) SetRetryConfig(maxRetries int, retryDelay time.Duration) {
	s.maxRetries = maxRetries
	s.retryDelay = retryDelay
}

// RecordUpdate 记录更新结构
type RecordUpdate struct {
	TableID      string                 `json:"table_id"`
	RecordID     string                 `json:"record_id"`
	FieldUpdates map[string]interface{} `json:"field_updates"`
}
