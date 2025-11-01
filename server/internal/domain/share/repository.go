package share

import "context"

// Repository 分享仓储接口
type Repository interface {
	// CreateShareView 创建分享视图
	CreateShareView(ctx context.Context, shareView *ShareView) error
	// GetShareViewByShareID 通过分享ID获取分享视图
	GetShareViewByShareID(ctx context.Context, shareID string) (*ShareView, error)
	// GetShareViewByViewID 通过视图ID获取分享视图
	GetShareViewByViewID(ctx context.Context, viewID string) (*ShareView, error)
	// UpdateShareView 更新分享视图
	UpdateShareView(ctx context.Context, shareView *ShareView) error
	// DeleteShareView 删除分享视图
	DeleteShareView(ctx context.Context, shareID string) error
	// ListShareViews 列出分享视图
	ListShareViews(ctx context.Context, tableID string) ([]*ShareView, error)
	// GetShareStats 获取分享统计信息
	GetShareStats(ctx context.Context, tableID string) (*ShareStats, error)
}
