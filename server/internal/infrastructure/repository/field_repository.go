package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository/mapper"
	"github.com/easyspace-ai/luckdb/server/pkg/database"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// FieldRepositoryImpl 字段仓储实现
type FieldRepositoryImpl struct {
	db *gorm.DB
}

// NewFieldRepository 创建字段仓储
func NewFieldRepository(db *gorm.DB) repository.FieldRepository {
	return &FieldRepositoryImpl{db: db}
}

// Save 保存字段
func (r *FieldRepositoryImpl) Save(ctx context.Context, field *entity.Field) error {
	dbField, err := mapper.ToFieldModel(field)
	if err != nil {
		return fmt.Errorf("failed to convert field: %w", err)
	}

	// 检查是否已存在
	var existing models.Field
	err = r.db.WithContext(ctx).Where("id = ?", dbField.ID).First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// 创建新字段
		return r.db.WithContext(ctx).Create(dbField).Error
	} else if err != nil {
		return fmt.Errorf("failed to check existing field: %w", err)
	}

	// 更新现有字段
	return r.db.WithContext(ctx).Model(&models.Field{}).
		Where("id = ?", dbField.ID).
		Updates(dbField).Error
}

// FindByID 根据ID查找字段
func (r *FieldRepositoryImpl) FindByID(ctx context.Context, id valueobject.FieldID) (*entity.Field, error) {
	var dbField models.Field

	fieldIDStr := id.String()
	logger.Info("🔍 FieldRepositoryImpl.FindByID 开始查询数据库",
		logger.String("field_id", fieldIDStr))

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("field").
		Where("id = ?", fieldIDStr).
		Where("deleted_time IS NULL").
		First(&dbField).Error

	if err == gorm.ErrRecordNotFound {
		logger.Warn("⚠️ FieldRepositoryImpl.FindByID 数据库查询结果为空（记录不存在）",
			logger.String("field_id", fieldIDStr))
		return nil, nil
	}
	if err != nil {
		logger.Error("❌ FieldRepositoryImpl.FindByID 数据库查询失败",
			logger.String("field_id", fieldIDStr),
			logger.ErrorField(err))
		return nil, fmt.Errorf("failed to find field: %w", err)
	}

	logger.Info("✅ FieldRepositoryImpl.FindByID 数据库查询成功",
		logger.String("field_id", fieldIDStr),
		logger.String("field_name", dbField.Name))

	field, err := mapper.ToFieldEntity(&dbField)
	if err != nil {
		logger.Error("❌ FieldRepositoryImpl.FindByID 映射失败",
			logger.String("field_id", fieldIDStr),
			logger.ErrorField(err))
		return nil, fmt.Errorf("failed to map field: %w", err)
	}
	if field == nil {
		logger.Warn("⚠️ FieldRepositoryImpl.FindByID 映射结果为空",
			logger.String("field_id", fieldIDStr))
	}
	
	return field, nil
}

// FindByTableID 查找表的所有字段
func (r *FieldRepositoryImpl) FindByTableID(ctx context.Context, tableID string) ([]*entity.Field, error) {
	var dbFields []*models.Field

	// ✅ 使用事务连接（如果存在）
	db := database.WithTx(ctx, r.db)

	// ✅ 检查是否在事务中
	isInTx := database.InTransaction(ctx)
	var txCtx *database.TxContext
	if isInTx {
		txCtx = database.GetTxContext(ctx)
	}

	logger.Info("🔍 FieldRepository.FindByTableID 开始查询",
		logger.String("table_id", tableID),
		logger.Bool("is_in_tx", isInTx),
		logger.String("tx_id", func() string {
			if txCtx != nil {
				return txCtx.ID
			}
			return "none"
		}()),
		logger.Bool("using_tx_db", db != r.db))

	// ✅ 查询元数据表，依赖默认 public schema
	err := db.WithContext(ctx).
		Table("field").
		Where("table_id = ?", tableID).
		Where("deleted_time IS NULL").
		Order("field_order ASC").
		Find(&dbFields).Error

	if err != nil {
		logger.Error("❌ FieldRepository.FindByTableID 查询失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
		return nil, fmt.Errorf("failed to find fields: %w", err)
	}

	// ✅ 添加详细日志：查询到的字段数量
	logger.Info("FindByTableID 查询结果",
		logger.String("table_id", tableID),
		logger.Int("found_count", len(dbFields)),
		logger.Any("field_ids", func() []string {
			ids := make([]string, len(dbFields))
			for i, f := range dbFields {
				ids[i] = f.ID
			}
			return ids
		}()))

	result, err := mapper.ToFieldList(dbFields)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// FindByName 根据名称查找字段
func (r *FieldRepositoryImpl) FindByName(ctx context.Context, tableID string, name valueobject.FieldName) (*entity.Field, error) {
	var dbField models.Field

	// ✅ 使用事务连接（如果存在）
	db := database.WithTx(ctx, r.db)

	// ✅ 显式指定 schema
	err := db.WithContext(ctx).
		Table("field").
		Where("table_id = ? AND name = ?", tableID, name.String()).
		Where("deleted_time IS NULL").
		First(&dbField).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find field by name: %w", err)
	}

	return mapper.ToFieldEntity(&dbField)
}

// Delete 删除字段（软删除）
func (r *FieldRepositoryImpl) Delete(ctx context.Context, id valueobject.FieldID) error {
	return r.db.WithContext(ctx).
		Model(&models.Field{}).
		Where("id = ?", id.String()).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// Exists 检查字段是否存在
func (r *FieldRepositoryImpl) Exists(ctx context.Context, id valueobject.FieldID) (bool, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("field").
		Where("id = ?", id.String()).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count > 0, err
}

// ExistsByName 检查名称是否已存在
func (r *FieldRepositoryImpl) ExistsByName(ctx context.Context, tableID string, name valueobject.FieldName, excludeID *valueobject.FieldID) (bool, error) {
	// ✅ 显式指定 schema
	query := r.db.WithContext(ctx).
		Table("field").
		Where("table_id = ? AND name = ?", tableID, name.String()).
		Where("deleted_time IS NULL")

	// 排除指定ID（用于更新时检查）
	if excludeID != nil {
		query = query.Where("id != ?", excludeID.String())
	}

	var count int64
	err := query.Count(&count).Error

	return count > 0, err
}

// CountByTableID 统计表的字段数
func (r *FieldRepositoryImpl) CountByTableID(ctx context.Context, tableID string) (int64, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("field").
		Where("table_id = ?", tableID).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count, err
}

// BatchSave 批量保存字段
func (r *FieldRepositoryImpl) BatchSave(ctx context.Context, fields []*entity.Field) error {
	if len(fields) == 0 {
		return nil
	}

	// 转换为数据库模型
	dbFields := make([]*models.Field, 0, len(fields))
	for _, field := range fields {
		dbField, err := mapper.ToFieldModel(field)
		if err != nil {
			return fmt.Errorf("failed to convert field %s: %w", field.ID().String(), err)
		}
		dbFields = append(dbFields, dbField)
	}

	// 批量插入或更新
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, dbField := range dbFields {
			if err := tx.Save(dbField).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// GetComputedFields 获取表的所有计算字段
func (r *FieldRepositoryImpl) GetComputedFields(ctx context.Context, tableID string) ([]*entity.Field, error) {
	var dbFields []*models.Field

	trueVal := true
	err := r.db.WithContext(ctx).
		Where("table_id = ?", tableID).
		Where("is_computed = ?", &trueVal).
		Where("deleted_time IS NULL").
		Order("field_order ASC").
		Find(&dbFields).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find computed fields: %w", err)
	}

	return mapper.ToFieldList(dbFields)
}

// GetFieldsByType 根据类型获取字段
func (r *FieldRepositoryImpl) GetFieldsByType(ctx context.Context, tableID string, fieldType valueobject.FieldType) ([]*entity.Field, error) {
	var dbFields []*models.Field

	err := r.db.WithContext(ctx).
		Where("table_id = ? AND type = ?", tableID, fieldType.String()).
		Where("deleted_time IS NULL").
		Order("field_order ASC").
		Find(&dbFields).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find fields by type: %w", err)
	}

	return mapper.ToFieldList(dbFields)
}

// GetVirtualFields 获取表的所有虚拟字段
func (r *FieldRepositoryImpl) GetVirtualFields(ctx context.Context, tableID string) ([]*entity.Field, error) {
	// 虚拟字段包括：formula, rollup, lookup 等计算字段
	var dbFields []*models.Field

	err := r.db.WithContext(ctx).
		Where("table_id = ?", tableID).
		Where("type IN ?", []string{"formula", "rollup", "lookup"}).
		Where("deleted_time IS NULL").
		Order("field_order ASC").
		Find(&dbFields).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find virtual fields: %w", err)
	}

	return mapper.ToFieldList(dbFields)
}

// List 列出字段（支持过滤和分页）
func (r *FieldRepositoryImpl) List(ctx context.Context, filter repository.FieldFilter) ([]*entity.Field, int64, error) {
	query := r.db.WithContext(ctx).Model(&models.Field{}).
		Where("deleted_time IS NULL")

	// 应用过滤条件
	if filter.TableID != nil {
		query = query.Where("table_id = ?", *filter.TableID)
	}
	if filter.FieldType != nil {
		query = query.Where("type = ?", filter.FieldType.String())
	}
	if filter.Name != nil {
		query = query.Where("name LIKE ?", "%"+*filter.Name+"%")
	}
	if filter.IsComputed != nil {
		query = query.Where("is_computed = ?", *filter.IsComputed)
	}

	// 统计总数
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count fields: %w", err)
	}

	// 排序
	if filter.OrderBy != "" {
		orderDir := "ASC"
		if filter.OrderDir == "desc" {
			orderDir = "DESC"
		}
		query = query.Order(fmt.Sprintf("%s %s", filter.OrderBy, orderDir))
	} else {
		query = query.Order("field_order ASC")
	}

	// 分页
	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	// 查询
	var dbFields []*models.Field
	if err := query.Find(&dbFields).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to list fields: %w", err)
	}

	// 转换
	fields, err := mapper.ToFieldList(dbFields)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert fields: %w", err)
	}

	return fields, total, nil
}

// UpdateOrder 更新字段排序
func (r *FieldRepositoryImpl) UpdateOrder(ctx context.Context, fieldID valueobject.FieldID, order float64) error {
	return r.db.WithContext(ctx).
		Model(&models.Field{}).
		Where("id = ?", fieldID.String()).
		Update("field_order", order).Error
}

// GetMaxOrder 获取表中字段的最大order值（参考原系统实现）
func (r *FieldRepositoryImpl) GetMaxOrder(ctx context.Context, tableID string) (float64, error) {
	var result struct {
		MaxOrder *float64
	}

	err := r.db.WithContext(ctx).
		Model(&models.Field{}).
		Select("MAX(field_order) as max_order").
		Where("table_id = ?", tableID).
		Where("deleted_time IS NULL").
		Scan(&result).Error

	if err != nil {
		return 0, fmt.Errorf("failed to get max order: %w", err)
	}

	// 如果没有字段（第一个字段），返回-1，这样第一个字段order为0
	if result.MaxOrder == nil {
		return -1, nil
	}

	return *result.MaxOrder, nil
}

// BatchDelete 批量删除字段
func (r *FieldRepositoryImpl) BatchDelete(ctx context.Context, ids []valueobject.FieldID) error {
	if len(ids) == 0 {
		return nil
	}

	// 转换为字符串ID
	idStrs := make([]string, len(ids))
	for i, id := range ids {
		idStrs[i] = id.String()
	}

	return r.db.WithContext(ctx).
		Model(&models.Field{}).
		Where("id IN ?", idStrs).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// NextID 生成下一个字段ID
func (r *FieldRepositoryImpl) NextID() valueobject.FieldID {
	return valueobject.NewFieldID("")
}
