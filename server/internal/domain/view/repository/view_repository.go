package repository

import (
	"context"
	"github.com/easyspace-ai/luckdb/server/internal/domain/view/entity"
)

// ViewRepository 视图仓储接口
type ViewRepository interface {
	// Save 保存视图（新建）
	Save(ctx context.Context, view *entity.View) error

	// Update 更新视图
	Update(ctx context.Context, view *entity.View) error

	// FindByID 根据ID查找视图
	FindByID(ctx context.Context, id string) (*entity.View, error)

	// FindByTableID 根据表格ID查找所有视图
	FindByTableID(ctx context.Context, tableID string) ([]*entity.View, error)

	// FindByShareID 根据分享ID查找视图
	FindByShareID(ctx context.Context, shareID string) (*entity.View, error)

	// Delete 删除视图（软删除）
	Delete(ctx context.Context, id string) error

	// Exists 检查视图是否存在
	Exists(ctx context.Context, id string) (bool, error)

	// Count 统计表格的视图数量
	Count(ctx context.Context, tableID string) (int64, error)
}
