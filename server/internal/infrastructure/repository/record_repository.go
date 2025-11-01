package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository/mapper"
)

// RecordRepositoryImpl 记录仓储实现
type RecordRepositoryImpl struct {
	db *gorm.DB
}

// NewRecordRepository 创建记录仓储
func NewRecordRepository(db *gorm.DB) repository.RecordRepository {
	return &RecordRepositoryImpl{db: db}
}

// Save 保存记录
func (r *RecordRepositoryImpl) Save(ctx context.Context, record *entity.Record) error {
	dbRecord, err := mapper.ToRecordModel(record)
	if err != nil {
		return fmt.Errorf("failed to convert record: %w", err)
	}

	// 检查是否已存在
	var existing models.Record
	err = r.db.WithContext(ctx).Where("id = ?", dbRecord.ID).First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// 创建新记录
		return r.db.WithContext(ctx).Create(dbRecord).Error
	} else if err != nil {
		return fmt.Errorf("failed to check existing record: %w", err)
	}

	// 更新现有记录
	return r.db.WithContext(ctx).Model(&models.Record{}).
		Where("id = ?", dbRecord.ID).
		Updates(dbRecord).Error
}

// FindByID 根据ID查找记录
func (r *RecordRepositoryImpl) FindByID(ctx context.Context, id valueobject.RecordID) (*entity.Record, error) {
	var dbRecord models.Record

	err := r.db.WithContext(ctx).
		Where("id = ?", id.String()).
		Where("deleted_time IS NULL").
		First(&dbRecord).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find record: %w", err)
	}

	return mapper.ToRecordEntity(&dbRecord)
}

// FindByIDs 根据ID列表查找记录（需要提供 tableID）
// ✅ 对齐 Teable 架构：所有记录操作都需要 tableID
func (r *RecordRepositoryImpl) FindByIDs(ctx context.Context, tableID string, ids []valueobject.RecordID) ([]*entity.Record, error) {
	if len(ids) == 0 {
		return []*entity.Record{}, nil
	}

	// 转换 ID 为字符串数组
	idStrs := make([]string, len(ids))
	for i, id := range ids {
		idStrs[i] = id.String()
	}

	var dbRecords []*models.Record
	err := r.db.WithContext(ctx).
		Where("id IN ?", idStrs).
		Where("table_id = ?", tableID).
		Where("deleted_time IS NULL").
		Find(&dbRecords).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find records: %w", err)
	}

	return mapper.ToRecordList(dbRecords)
}

// FindByTableAndID 根据表ID和记录ID查找单条记录
// ✅ 对齐 Teable 架构：所有记录操作都需要 tableID
func (r *RecordRepositoryImpl) FindByTableAndID(ctx context.Context, tableID string, id valueobject.RecordID) (*entity.Record, error) {
	records, err := r.FindByIDs(ctx, tableID, []valueobject.RecordID{id})
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil // 记录不存在
	}
	return records[0], nil
}

// FindByTableID 查找表的所有记录
func (r *RecordRepositoryImpl) FindByTableID(ctx context.Context, tableID string) ([]*entity.Record, error) {
	var dbRecords []*models.Record

	err := r.db.WithContext(ctx).
		Where("table_id = ?", tableID).
		Where("deleted_time IS NULL").
		Find(&dbRecords).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find records: %w", err)
	}

	return mapper.ToRecordList(dbRecords)
}

// Delete 删除记录（软删除）
func (r *RecordRepositoryImpl) Delete(ctx context.Context, id valueobject.RecordID) error {
	return r.db.WithContext(ctx).
		Model(&models.Record{}).
		Where("id = ?", id.String()).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// DeleteByTableAndID 根据表ID和记录ID删除记录（软删除）
// ✅ 对齐 Teable 架构：所有记录操作都需要 tableID
func (r *RecordRepositoryImpl) DeleteByTableAndID(ctx context.Context, tableID string, id valueobject.RecordID) error {
	return r.db.WithContext(ctx).
		Model(&models.Record{}).
		Where("id = ?", id.String()).
		Where("table_id = ?", tableID).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// Exists 检查记录是否存在
func (r *RecordRepositoryImpl) Exists(ctx context.Context, id valueobject.RecordID) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.Record{}).
		Where("id = ?", id.String()).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count > 0, err
}

// List 列出记录
func (r *RecordRepositoryImpl) List(ctx context.Context, filter repository.RecordFilter) ([]*entity.Record, int64, error) {
	query := r.db.WithContext(ctx).Model(&models.Record{}).
		Where("deleted_time IS NULL")

	// 应用过滤条件
	if filter.TableID != nil {
		query = query.Where("table_id = ?", *filter.TableID)
	}
	if filter.CreatedBy != nil {
		query = query.Where("created_by = ?", *filter.CreatedBy)
	}
	if filter.UpdatedBy != nil {
		// 数据库模型中没有 updated_by 字段，暂时跳过
	}

	// 统计总数
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count records: %w", err)
	}

	// 排序
	if filter.OrderBy != "" {
		orderDir := "ASC"
		if filter.OrderDir == "desc" {
			orderDir = "DESC"
		}
		query = query.Order(fmt.Sprintf("%s %s", filter.OrderBy, orderDir))
	} else {
		query = query.Order("created_time DESC")
	}

	// 分页
	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	// 查询
	var dbRecords []*models.Record
	if err := query.Find(&dbRecords).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to list records: %w", err)
	}

	// 转换
	records, err := mapper.ToRecordList(dbRecords)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert records: %w", err)
	}

	return records, total, nil
}

// BatchSave 批量保存记录
func (r *RecordRepositoryImpl) BatchSave(ctx context.Context, records []*entity.Record) error {
	if len(records) == 0 {
		return nil
	}

	// 转换为数据库模型
	dbRecords := make([]*models.Record, 0, len(records))
	for _, record := range records {
		dbRecord, err := mapper.ToRecordModel(record)
		if err != nil {
			return fmt.Errorf("failed to convert record %s: %w", record.ID().String(), err)
		}
		dbRecords = append(dbRecords, dbRecord)
	}

	// 批量插入或更新
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, dbRecord := range dbRecords {
			if err := tx.Save(dbRecord).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// BatchDelete 批量删除记录
func (r *RecordRepositoryImpl) BatchDelete(ctx context.Context, ids []valueobject.RecordID) error {
	if len(ids) == 0 {
		return nil
	}

	// 转换为字符串ID
	idStrs := make([]string, len(ids))
	for i, id := range ids {
		idStrs[i] = id.String()
	}

	return r.db.WithContext(ctx).
		Model(&models.Record{}).
		Where("id IN ?", idStrs).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// CountByTableID 统计表的记录数
func (r *RecordRepositoryImpl) CountByTableID(ctx context.Context, tableID string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.Record{}).
		Where("table_id = ?", tableID).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count, err
}

// FindWithVersion 根据ID和版本查找记录
func (r *RecordRepositoryImpl) FindWithVersion(ctx context.Context, tableID string, id valueobject.RecordID, version valueobject.RecordVersion) (*entity.Record, error) {
	// 目前数据库模型没有版本字段，暂时忽略版本检查
	return r.FindByTableAndID(ctx, tableID, id)
}

// NextID 生成下一个记录ID
func (r *RecordRepositoryImpl) NextID() valueobject.RecordID {
	return valueobject.NewRecordID("")
}
