package repository

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	fieldRepo "github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	fieldValueobject "github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	recordEntity "github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	recordValueobject "github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/database"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CachedFieldRepository 带缓存的字段仓储包装器
// ✅ 优化：实现查询缓存，减少数据库查询
type CachedFieldRepository struct {
	repo         fieldRepo.FieldRepository
	cacheService CacheProvider
	ttl          time.Duration
}

// NewCachedFieldRepository 创建带缓存的字段仓储
func NewCachedFieldRepository(
	repo fieldRepo.FieldRepository,
	cacheService CacheProvider,
	ttl time.Duration,
) fieldRepo.FieldRepository {
	if ttl == 0 {
		ttl = 5 * time.Minute // 默认5分钟
	}

	return &CachedFieldRepository{
		repo:         repo,
		cacheService: cacheService,
		ttl:          ttl,
	}
}

// buildCacheKey 构建缓存键
func (r *CachedFieldRepository) buildCacheKey(prefix, id string) string {
	return fmt.Sprintf("field:%s:%s", prefix, id)
}

// FindByID 根据ID查找字段（带缓存）
func (r *CachedFieldRepository) FindByID(ctx context.Context, id fieldValueobject.FieldID) (*fieldEntity.Field, error) {
	cacheKey := r.buildCacheKey("id", id.String())

	// 尝试从缓存获取
	var field *fieldEntity.Field
	if err := r.cacheService.Get(ctx, cacheKey, &field); err == nil {
		logger.Debug("field cache hit",
			logger.String("field_id", id.String()))
		return field, nil
	}

	// 缓存未命中，查询数据库
	field, err := r.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// 写入缓存
	if field != nil {
		if err := r.cacheService.Set(ctx, cacheKey, field, r.ttl); err != nil {
			logger.Warn("failed to cache field",
				logger.String("field_id", id.String()),
				logger.ErrorField(err))
		}
	}

	return field, nil
}

// FindByTableID 查找表的所有字段（带缓存）
func (r *CachedFieldRepository) FindByTableID(ctx context.Context, tableID string) ([]*fieldEntity.Field, error) {
	// ✅ 关键修复：在事务中禁用缓存，直接查询数据库
	// 原因：事务中的查询可能受到隔离级别影响，缓存可能导致数据不一致
	if database.InTransaction(ctx) {
		logger.Info("🔍 CachedFieldRepository.FindByTableID 在事务中，禁用缓存，直接查询数据库",
			logger.String("table_id", tableID))
		return r.repo.FindByTableID(ctx, tableID)
	}

	cacheKey := r.buildCacheKey("table", tableID)

	// ✅ 添加详细日志：缓存查询
	logger.Info("🔍 CachedFieldRepository.FindByTableID 开始查询",
		logger.String("table_id", tableID),
		logger.String("cache_key", cacheKey))

	// 尝试从缓存获取
	var fields []*fieldEntity.Field
	if err := r.cacheService.Get(ctx, cacheKey, &fields); err == nil {
		logger.Info("🔍 CachedFieldRepository.FindByTableID 缓存命中",
			logger.String("table_id", tableID),
			logger.Int("cached_count", len(fields)))
		return fields, nil
	}

	logger.Info("🔍 CachedFieldRepository.FindByTableID 缓存未命中，查询数据库",
		logger.String("table_id", tableID))

	// 缓存未命中，查询数据库
	fields, err := r.repo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, err
	}

	logger.Info("🔍 CachedFieldRepository.FindByTableID 数据库查询完成",
		logger.String("table_id", tableID),
		logger.Int("found_count", len(fields)))

	// 写入缓存
	if err := r.cacheService.Set(ctx, cacheKey, fields, r.ttl); err != nil {
		logger.Warn("failed to cache fields",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
	}

	return fields, nil
}

// Save 保存字段（更新后清除缓存）
func (r *CachedFieldRepository) Save(ctx context.Context, field *fieldEntity.Field) error {
	if err := r.repo.Save(ctx, field); err != nil {
		return err
	}

	// 清除相关缓存
	r.invalidateCache(ctx, field)
	return nil
}

// Delete 删除字段（清除缓存）
func (r *CachedFieldRepository) Delete(ctx context.Context, id fieldValueobject.FieldID) error {
	// 先获取字段信息（用于清除缓存）
	field, _ := r.repo.FindByID(ctx, id)

	if err := r.repo.Delete(ctx, id); err != nil {
		return err
	}

	// 清除缓存
	if field != nil {
		r.invalidateCache(ctx, field)
	}
	return nil
}

// invalidateCache 使字段相关缓存失效
func (r *CachedFieldRepository) invalidateCache(ctx context.Context, field *fieldEntity.Field) {
	keys := []string{
		r.buildCacheKey("id", field.ID().String()),
		r.buildCacheKey("table", field.TableID()),
	}

	if err := r.cacheService.Delete(ctx, keys...); err != nil {
		logger.Warn("failed to invalidate field cache",
			logger.String("field_id", field.ID().String()),
			logger.ErrorField(err))
	}

	// 清除表格字段列表缓存
	pattern := fmt.Sprintf("field:table:%s", field.TableID())
	if err := r.cacheService.InvalidatePattern(ctx, pattern); err != nil {
		logger.Warn("failed to invalidate field pattern cache",
			logger.String("pattern", pattern),
			logger.ErrorField(err))
	}
}

// 实现其他接口方法（直接委托给底层repo）
func (r *CachedFieldRepository) FindByName(ctx context.Context, tableID string, name fieldValueobject.FieldName) (*fieldEntity.Field, error) {
	return r.repo.FindByName(ctx, tableID, name)
}

func (r *CachedFieldRepository) Exists(ctx context.Context, id fieldValueobject.FieldID) (bool, error) {
	return r.repo.Exists(ctx, id)
}

func (r *CachedFieldRepository) ExistsByName(ctx context.Context, tableID string, name fieldValueobject.FieldName, excludeID *fieldValueobject.FieldID) (bool, error) {
	return r.repo.ExistsByName(ctx, tableID, name, excludeID)
}

func (r *CachedFieldRepository) List(ctx context.Context, filter fieldRepo.FieldFilter) ([]*fieldEntity.Field, int64, error) {
	return r.repo.List(ctx, filter)
}

func (r *CachedFieldRepository) BatchSave(ctx context.Context, fields []*fieldEntity.Field) error {
	if err := r.repo.BatchSave(ctx, fields); err != nil {
		return err
	}

	// 清除所有相关表格的缓存
	tableIDs := make(map[string]bool)
	for _, field := range fields {
		tableIDs[field.TableID()] = true
	}

	for tableID := range tableIDs {
		cacheKey := r.buildCacheKey("table", tableID)
		if err := r.cacheService.Delete(ctx, cacheKey); err != nil {
			logger.Warn("failed to invalidate cache after batch save",
				logger.String("table_id", tableID),
				logger.ErrorField(err))
		}
	}

	return nil
}

func (r *CachedFieldRepository) BatchDelete(ctx context.Context, ids []fieldValueobject.FieldID) error {
	return r.repo.BatchDelete(ctx, ids)
}

func (r *CachedFieldRepository) GetVirtualFields(ctx context.Context, tableID string) ([]*fieldEntity.Field, error) {
	return r.repo.GetVirtualFields(ctx, tableID)
}

func (r *CachedFieldRepository) GetComputedFields(ctx context.Context, tableID string) ([]*fieldEntity.Field, error) {
	return r.repo.GetComputedFields(ctx, tableID)
}

func (r *CachedFieldRepository) GetFieldsByType(ctx context.Context, tableID string, fieldType fieldValueobject.FieldType) ([]*fieldEntity.Field, error) {
	return r.repo.GetFieldsByType(ctx, tableID, fieldType)
}

func (r *CachedFieldRepository) UpdateOrder(ctx context.Context, fieldID fieldValueobject.FieldID, order float64) error {
	return r.repo.UpdateOrder(ctx, fieldID, order)
}

func (r *CachedFieldRepository) GetMaxOrder(ctx context.Context, tableID string) (float64, error) {
	return r.repo.GetMaxOrder(ctx, tableID)
}

func (r *CachedFieldRepository) NextID() fieldValueobject.FieldID {
	return r.repo.NextID()
}

// CachedRecordRepository 带缓存的记录仓储包装器
// ✅ 优化：实现查询缓存，减少数据库查询
type CachedRecordRepository struct {
	repo         recordRepo.RecordRepository
	cacheService CacheProvider
	ttl          time.Duration
}

// NewCachedRecordRepository 创建带缓存的记录仓储
func NewCachedRecordRepository(
	repo recordRepo.RecordRepository,
	cacheService CacheProvider,
	ttl time.Duration,
) recordRepo.RecordRepository {
	if ttl == 0 {
		ttl = 2 * time.Minute // 记录缓存时间较短，默认2分钟
	}

	return &CachedRecordRepository{
		repo:         repo,
		cacheService: cacheService,
		ttl:          ttl,
	}
}

// buildCacheKey 构建缓存键
func (r *CachedRecordRepository) buildCacheKey(prefix, tableID, recordID string) string {
	return fmt.Sprintf("record:%s:%s:%s", prefix, tableID, recordID)
}

// FindByTableAndID 根据表格ID和记录ID查找记录（带缓存）
func (r *CachedRecordRepository) FindByTableAndID(ctx context.Context, tableID string, id recordValueobject.RecordID) (*recordEntity.Record, error) {
	cacheKey := r.buildCacheKey("id", tableID, id.String())

	// 尝试从缓存获取
	var record *recordEntity.Record
	if err := r.cacheService.Get(ctx, cacheKey, &record); err == nil {
		logger.Debug("record cache hit",
			logger.String("table_id", tableID),
			logger.String("record_id", id.String()))
		return record, nil
	}

	// 缓存未命中，查询数据库
	record, err := r.repo.FindByTableAndID(ctx, tableID, id)
	if err != nil {
		return nil, err
	}

	// 写入缓存
	if record != nil {
		if err := r.cacheService.Set(ctx, cacheKey, record, r.ttl); err != nil {
			logger.Warn("failed to cache record",
				logger.String("record_id", id.String()),
				logger.ErrorField(err))
		}
	}

	return record, nil
}

// Save 保存记录（更新后清除缓存）
func (r *CachedRecordRepository) Save(ctx context.Context, record *recordEntity.Record) error {
	if err := r.repo.Save(ctx, record); err != nil {
		return err
	}

	// 清除记录缓存
	cacheKey := r.buildCacheKey("id", record.TableID(), record.ID().String())
	if err := r.cacheService.Delete(ctx, cacheKey); err != nil {
		logger.Warn("failed to invalidate record cache",
			logger.String("record_id", record.ID().String()),
			logger.ErrorField(err))
	}

	// 清除表格记录列表缓存
	pattern := fmt.Sprintf("record:list:%s:*", record.TableID())
	if err := r.cacheService.InvalidatePattern(ctx, pattern); err != nil {
		logger.Warn("failed to invalidate record list cache",
			logger.String("pattern", pattern),
			logger.ErrorField(err))
	}

	return nil
}

// DeleteByTableAndID 删除记录（清除缓存）
func (r *CachedRecordRepository) DeleteByTableAndID(ctx context.Context, tableID string, id recordValueobject.RecordID) error {
	if err := r.repo.DeleteByTableAndID(ctx, tableID, id); err != nil {
		return err
	}

	// 清除缓存
	cacheKey := r.buildCacheKey("id", tableID, id.String())
	if err := r.cacheService.Delete(ctx, cacheKey); err != nil {
		logger.Warn("failed to invalidate record cache after delete",
			logger.String("record_id", id.String()),
			logger.ErrorField(err))
	}

	// 清除表格记录列表缓存
	pattern := fmt.Sprintf("record:list:%s:*", tableID)
	if err := r.cacheService.InvalidatePattern(ctx, pattern); err != nil {
		logger.Warn("failed to invalidate record list cache",
			logger.String("pattern", pattern),
			logger.ErrorField(err))
	}

	return nil
}

// List 列出记录（带缓存，但缓存时间较短）
func (r *CachedRecordRepository) List(ctx context.Context, filter recordRepo.RecordFilter) ([]*recordEntity.Record, int64, error) {
	// 记录列表缓存时间很短，因为数据变化频繁
	// 这里使用较短的TTL（30秒）
	shortTTL := 30 * time.Second
	if r.ttl < shortTTL {
		shortTTL = r.ttl
	}

	// 构建缓存键（基于过滤条件）
	cacheKey := fmt.Sprintf("record:list:%s:%d:%d", *filter.TableID, filter.Limit, filter.Offset)

	// 尝试从缓存获取
	var result struct {
		Records []*recordEntity.Record
		Total   int64
	}

	if err := r.cacheService.Get(ctx, cacheKey, &result); err == nil {
		logger.Debug("record list cache hit",
			logger.String("table_id", *filter.TableID))
		return result.Records, result.Total, nil
	}

	// 缓存未命中，查询数据库
	records, total, err := r.repo.List(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	// 写入缓存
	result.Records = records
	result.Total = total
	if err := r.cacheService.Set(ctx, cacheKey, result, shortTTL); err != nil {
		logger.Warn("failed to cache record list",
			logger.String("table_id", *filter.TableID),
			logger.ErrorField(err))
	}

	return records, total, nil
}

// 实现其他接口方法（直接委托给底层repo）
func (r *CachedRecordRepository) FindByID(ctx context.Context, id recordValueobject.RecordID) (*recordEntity.Record, error) {
	return r.repo.FindByID(ctx, id)
}

func (r *CachedRecordRepository) BatchSave(ctx context.Context, records []*recordEntity.Record) error {
	if err := r.repo.BatchSave(ctx, records); err != nil {
		return err
	}

	// 清除所有相关表格的缓存
	tableIDs := make(map[string]bool)
	for _, record := range records {
		tableIDs[record.TableID()] = true
		cacheKey := r.buildCacheKey("id", record.TableID(), record.ID().String())
		if err := r.cacheService.Delete(ctx, cacheKey); err != nil {
			logger.Warn("failed to invalidate cache after batch save",
				logger.String("record_id", record.ID().String()),
				logger.ErrorField(err))
		}
	}

	for tableID := range tableIDs {
		pattern := fmt.Sprintf("record:list:%s:*", tableID)
		if err := r.cacheService.InvalidatePattern(ctx, pattern); err != nil {
			logger.Warn("failed to invalidate record list cache",
				logger.String("pattern", pattern),
				logger.ErrorField(err))
		}
	}

	return nil
}

func (r *CachedRecordRepository) BatchDelete(ctx context.Context, ids []recordValueobject.RecordID) error {
	// 接口定义中没有tableID，但实际实现需要tableID
	// 这里需要先查询记录获取tableID，或者使用其他方式
	// 暂时直接委托给底层repo（假设底层repo会处理）
	if err := r.repo.BatchDelete(ctx, ids); err != nil {
		return err
	}

	// 清除缓存（无法知道tableID，清除所有相关缓存）
	// 注意：这里清除所有记录的缓存，可能会有性能影响
	// 在实际应用中，应该传入tableID或者记录信息
	for _, id := range ids {
		// 尝试从缓存中获取记录信息以获取tableID
		// 如果没有缓存，则跳过（缓存已自动失效）
		pattern := fmt.Sprintf("record:*:*:%s", id.String())
		if err := r.cacheService.InvalidatePattern(ctx, pattern); err != nil {
			logger.Warn("failed to invalidate record cache",
				logger.String("record_id", id.String()),
				logger.ErrorField(err))
		}
	}

	return nil
}

func (r *CachedRecordRepository) Exists(ctx context.Context, id recordValueobject.RecordID) (bool, error) {
	return r.repo.Exists(ctx, id)
}

func (r *CachedRecordRepository) FindByIDs(ctx context.Context, tableID string, ids []recordValueobject.RecordID) ([]*recordEntity.Record, error) {
	return r.repo.FindByIDs(ctx, tableID, ids)
}

func (r *CachedRecordRepository) FindByTableID(ctx context.Context, tableID string) ([]*recordEntity.Record, error) {
	return r.repo.FindByTableID(ctx, tableID)
}

func (r *CachedRecordRepository) Delete(ctx context.Context, id recordValueobject.RecordID) error {
	return r.repo.Delete(ctx, id)
}

func (r *CachedRecordRepository) CountByTableID(ctx context.Context, tableID string) (int64, error) {
	return r.repo.CountByTableID(ctx, tableID)
}

func (r *CachedRecordRepository) FindWithVersion(ctx context.Context, tableID string, id recordValueobject.RecordID, version recordValueobject.RecordVersion) (*recordEntity.Record, error) {
	return r.repo.FindWithVersion(ctx, tableID, id, version)
}

func (r *CachedRecordRepository) NextID() recordValueobject.RecordID {
	return r.repo.NextID()
}

// GetDB 获取数据库连接（用于事务管理）
// 如果底层仓库实现了 GetDB 方法，则返回其数据库连接
func (r *CachedRecordRepository) GetDB() *gorm.DB {
	// 尝试类型断言到 RecordRepositoryDynamic
	if dynamicRepo, ok := r.repo.(*RecordRepositoryDynamic); ok {
		return dynamicRepo.GetDB()
	}
	// 如果底层仓库也是缓存包装器，递归调用
	if cachedRepo, ok := r.repo.(*CachedRecordRepository); ok {
		return cachedRepo.GetDB()
	}
	// 如果都不匹配，返回 nil（这不应该发生）
	return nil
}

