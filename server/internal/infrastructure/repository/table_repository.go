package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository/mapper"
)

// TableRepositoryImpl 表格仓储实现
type TableRepositoryImpl struct {
	db *gorm.DB
}

// NewTableRepository 创建表格仓储
func NewTableRepository(db *gorm.DB) repository.TableRepository {
	return &TableRepositoryImpl{db: db}
}

// Save 保存表格
func (r *TableRepositoryImpl) Save(ctx context.Context, table *entity.Table) error {
	if table == nil {
		return errors.New("table不能为nil")
	}

	dbTable := mapper.ToTableModel(table)

	var existing models.Table
	err := r.db.WithContext(ctx).Where("id = ?", dbTable.ID).First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		return r.db.WithContext(ctx).Create(dbTable).Error
	} else if err != nil {
		return fmt.Errorf("failed to check existing table: %w", err)
	}

	return r.db.WithContext(ctx).Model(&models.Table{}).
		Where("id = ?", dbTable.ID).
		Updates(dbTable).Error
}

// GetByID 根据ID获取表格实体
func (r *TableRepositoryImpl) GetByID(ctx context.Context, id string) (*entity.Table, error) {
	table, err := r.GetTableByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, nil
	}

	return table, nil
}

// GetTableByID 根据ID获取表格实体
func (r *TableRepositoryImpl) GetTableByID(ctx context.Context, id string) (*entity.Table, error) {
	var dbTable models.Table

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("table_meta").
		Where("id = ?", id).
		Where("deleted_time IS NULL").
		First(&dbTable).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find table: %w", err)
	}

	return mapper.ToTableEntity(&dbTable)
}

// ListByBaseID 获取空间的所有表格
func (r *TableRepositoryImpl) ListByBaseID(ctx context.Context, baseID string) ([]*entity.Table, error) {
	var dbTables []*models.Table

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("table_meta").
		Where("base_id = ?", baseID).
		Where("deleted_time IS NULL").
		Order("created_time DESC").
		Find(&dbTables).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find tables by base: %w", err)
	}

	return mapper.ToTableList(dbTables)
}

// Delete 删除表格
func (r *TableRepositoryImpl) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).
		Model(&models.Table{}).
		Where("id = ?", id).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// Update 更新表格
func (r *TableRepositoryImpl) Update(ctx context.Context, table *entity.Table) error {
	dbTable := mapper.ToTableModel(table)

	return r.db.WithContext(ctx).Model(&models.Table{}).
		Where("id = ?", dbTable.ID).
		Updates(dbTable).Error
}

// Count 统计表格数量
func (r *TableRepositoryImpl) Count(ctx context.Context, baseID string) (int64, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("table_meta").
		Where("base_id = ?", baseID).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count, err
}

// Exists 检查表格是否存在
func (r *TableRepositoryImpl) Exists(ctx context.Context, id string) (bool, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("table_meta").
		Where("id = ?", id).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count > 0, err
}

// ExistsByNameInBase 检查Base下是否存在指定名称的表格
func (r *TableRepositoryImpl) ExistsByNameInBase(ctx context.Context, baseID string, name valueobject.TableName, excludeID *string) (bool, error) {
	var count int64
	query := r.db.WithContext(ctx).
		Table("table_meta").
		Where("base_id = ?", baseID).
		Where("name = ?", name.String()).
		Where("deleted_time IS NULL")

	// 如果指定了排除ID，则排除该ID
	if excludeID != nil {
		query = query.Where("id != ?", *excludeID)
	}

	err := query.Count(&count).Error
	return count > 0, err
}

// GetByBaseID 根据BaseID获取表格列表（别名方法）
func (r *TableRepositoryImpl) GetByBaseID(ctx context.Context, baseID string) ([]*entity.Table, error) {
	return r.ListByBaseID(ctx, baseID)
}
