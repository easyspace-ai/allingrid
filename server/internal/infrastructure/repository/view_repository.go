package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/view/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"

	"gorm.io/gorm"
)

// ViewRepositoryImpl 视图仓储GORM实现
type ViewRepositoryImpl struct {
	db *gorm.DB
}

// NewViewRepository 创建视图仓储
func NewViewRepository(db *gorm.DB) repository.ViewRepository {
	return &ViewRepositoryImpl{db: db}
}

// 移除自定义的models.View，直接使用models.View以确保与GORM AutoMigrate一致

// Save 保存视图
func (r *ViewRepositoryImpl) Save(ctx context.Context, view *entity.View) error {
	model, err := r.toModel(view)
	if err != nil {
		return fmt.Errorf("failed to convert view to model: %w", err)
	}

	if err := r.db.WithContext(ctx).Create(&model).Error; err != nil {
		return fmt.Errorf("failed to save view: %w", err)
	}

	return nil
}

// Update 更新视图
func (r *ViewRepositoryImpl) Update(ctx context.Context, view *entity.View) error {
	model, err := r.toModel(view)
	if err != nil {
		return fmt.Errorf("failed to convert view to model: %w", err)
	}

	if err := r.db.WithContext(ctx).Save(model).Error; err != nil {
		return fmt.Errorf("failed to update view: %w", err)
	}

	return nil
}

// FindByID 根据ID查找视图
func (r *ViewRepositoryImpl) FindByID(ctx context.Context, id string) (*entity.View, error) {
	var model models.View
	err := r.db.WithContext(ctx).
		Where("id = ? AND deleted_time IS NULL", id).
		First(&model).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to find view: %w", err)
	}

	return r.toEntity(&model)
}

// FindByTableID 根据表格ID查找所有视图
func (r *ViewRepositoryImpl) FindByTableID(ctx context.Context, tableID string) ([]*entity.View, error) {
	var viewModels []models.View
	err := r.db.WithContext(ctx).
		Where("table_id = ? AND deleted_time IS NULL", tableID).
		Order("\"order\"").
		Find(&viewModels).Error

	if err != nil {
		return nil, fmt.Errorf("failed to find views by table ID: %w", err)
	}

	views := make([]*entity.View, 0, len(viewModels))
	for _, model := range viewModels {
		view, err := r.toEntity(&model)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}

	return views, nil
}

// FindByShareID 根据分享ID查找视图
func (r *ViewRepositoryImpl) FindByShareID(ctx context.Context, shareID string) (*entity.View, error) {
	var model models.View
	err := r.db.WithContext(ctx).
		Where("share_id = ? AND enable_share = ? AND deleted_time IS NULL", shareID, true).
		First(&model).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to find view by share ID: %w", err)
	}

	return r.toEntity(&model)
}

// Delete 删除视图（软删除）
func (r *ViewRepositoryImpl) Delete(ctx context.Context, id string) error {
	now := time.Now()
	err := r.db.WithContext(ctx).
		Model(&models.View{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"deleted_time":       now,
			"last_modified_time": now,
		}).Error

	if err != nil {
		return fmt.Errorf("failed to delete view: %w", err)
	}

	return nil
}

// Exists 检查视图是否存在
func (r *ViewRepositoryImpl) Exists(ctx context.Context, id string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.View{}).
		Where("id = ? AND deleted_time IS NULL", id).
		Count(&count).Error

	if err != nil {
		return false, fmt.Errorf("failed to check view existence: %w", err)
	}

	return count > 0, nil
}

// Count 统计表格的视图数量
func (r *ViewRepositoryImpl) Count(ctx context.Context, tableID string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.View{}).
		Where("table_id = ? AND deleted_time IS NULL", tableID).
		Count(&count).Error

	if err != nil {
		return 0, fmt.Errorf("failed to count views: %w", err)
	}

	return count, nil
}

// toModel 实体转模型
func (r *ViewRepositoryImpl) toModel(view *entity.View) (*models.View, error) {
	// Order字段处理：如果为0则设为nil
	var order *float64
	if viewOrder := view.Order(); viewOrder != 0 {
		order = &viewOrder
	}

	// Description字段处理
	var description *string
	if desc := view.Description(); desc != "" {
		description = &desc
	}

	// UpdatedAt字段处理
	updatedAt := view.UpdatedAt()

	model := &models.View{
		ID:               view.ID(),
		Name:             view.Name(),
		Description:      description,
		TableID:          view.TableID(),
		Type:             string(view.ViewType()),
		Order:            order,
		Version:          view.Version(),
		IsLocked:         view.IsLocked(),
		EnableShare:      view.EnableShare(),
		ShareID:          view.ShareID(),
		CreatedBy:        view.CreatedBy(),
		CreatedTime:      view.CreatedAt(),
		LastModifiedTime: &updatedAt,
		DeletedTime:      view.DeletedAt(),
	}

	// JSON字段序列化 - 使用datatypes.JSON，GORM会自动处理nil值为NULL
	if filter := view.Filter(); filter != nil {
		filterJSON, err := json.Marshal(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal filter: %w", err)
		}
		model.Filter = filterJSON
	}

	if sort := view.Sort(); sort != nil {
		sortJSON, err := json.Marshal(sort)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal sort: %w", err)
		}
		model.Sort = sortJSON
	}

	if group := view.Group(); group != nil {
		groupJSON, err := json.Marshal(group)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal group: %w", err)
		}
		model.Group = groupJSON
	}

	if columnMeta := view.ColumnMeta(); columnMeta != nil {
		columnMetaJSON, err := json.Marshal(columnMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal column meta: %w", err)
		}
		model.ColumnMeta = columnMetaJSON
	}

	if options := view.Options(); options != nil && len(options) > 0 {
		optionsJSON, err := json.Marshal(options)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal options: %w", err)
		}
		model.Options = optionsJSON
	}

	if shareMeta := view.ShareMeta(); shareMeta != nil && len(shareMeta) > 0 {
		shareMetaJSON, err := json.Marshal(shareMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal share meta: %w", err)
		}
		model.ShareMeta = shareMetaJSON
	}

	return model, nil
}

// toEntity 模型转实体
func (r *ViewRepositoryImpl) toEntity(model *models.View) (*entity.View, error) {
	// 解析视图类型
	viewType, err := valueobject.NewViewType(model.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to parse view type: %w", err)
	}

	// 解析JSON字段 - 处理datatypes.JSON类型
	var filter *valueobject.Filter
	if len(model.Filter) > 0 {
		var filterData map[string]interface{}
		if err := json.Unmarshal(model.Filter, &filterData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal filter: %w", err)
		}
		filter, _ = valueobject.NewFilter(filterData)
	}

	var sort *valueobject.Sort
	if len(model.Sort) > 0 {
		var sortData []map[string]interface{}
		if err := json.Unmarshal(model.Sort, &sortData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sort: %w", err)
		}
		sort, _ = valueobject.NewSort(sortData)
	}

	var group *valueobject.Group
	if len(model.Group) > 0 {
		var groupData []map[string]interface{}
		if err := json.Unmarshal(model.Group, &groupData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal group: %w", err)
		}
		group, _ = valueobject.NewGroup(groupData)
	}

	var columnMeta *valueobject.ColumnMetaList
	if len(model.ColumnMeta) > 0 {
		// 尝试反序列化为完整的ColumnMetaList结构
		var temp valueobject.ColumnMetaList
		if err := json.Unmarshal(model.ColumnMeta, &temp); err == nil {
			// 成功解析为ColumnMetaList
			columnMeta = &temp
		} else {
			// 如果失败，尝试解析为数组格式（向后兼容）
			var columnMetaData []map[string]interface{}
			if err := json.Unmarshal(model.ColumnMeta, &columnMetaData); err != nil {
				return nil, fmt.Errorf("failed to unmarshal column meta: %w", err)
			}
			columnMeta, _ = valueobject.NewColumnMetaList(columnMetaData)
		}
	} else {
		columnMeta = &valueobject.ColumnMetaList{Columns: []valueobject.ColumnMeta{}}
	}

	var options map[string]interface{}
	if len(model.Options) > 0 {
		if err := json.Unmarshal(model.Options, &options); err != nil {
			return nil, fmt.Errorf("failed to unmarshal options: %w", err)
		}
	}

	var shareMeta map[string]interface{}
	if len(model.ShareMeta) > 0 {
		if err := json.Unmarshal(model.ShareMeta, &shareMeta); err != nil {
			return nil, fmt.Errorf("failed to unmarshal share meta: %w", err)
		}
	}

	// Order字段处理
	var order float64
	if model.Order != nil {
		order = *model.Order
	}

	// 时间字段处理
	var updatedAt time.Time
	if model.LastModifiedTime != nil {
		updatedAt = *model.LastModifiedTime
	} else {
		updatedAt = model.CreatedTime
	}

	// Description字段处理
	var description string
	if model.Description != nil {
		description = *model.Description
	}

	// 重建实体
	view := entity.ReconstructView(
		model.ID,
		model.Name,
		description,
		model.TableID,
		viewType,
		filter,
		sort,
		group,
		columnMeta,
		options,
		order,
		model.Version,
		model.IsLocked,
		model.EnableShare,
		model.ShareID,
		shareMeta,
		model.CreatedBy,
		model.CreatedTime,
		updatedAt,
		model.DeletedTime,
	)

	return view, nil
}
