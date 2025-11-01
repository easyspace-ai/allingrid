package view

import "context"

// Repository 视图仓储接口
type Repository interface {
	Create(ctx context.Context, view *View) error
	GetByID(ctx context.Context, id string) (*View, error)
	Update(ctx context.Context, view *View) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context, filter ListViewFilter) ([]*View, error)
	Count(ctx context.Context, filter ListViewFilter) (int64, error)
	Exists(ctx context.Context, filter ListViewFilter) (bool, error)

	// 网格视图数据获取
	GetGridViewData(ctx context.Context, req GridViewDataRequest) (*GridViewData, error)

	// 表单视图数据获取
	GetFormViewData(ctx context.Context, req FormViewDataRequest) (*FormViewData, error)

	// 看板视图数据获取
	GetKanbanViewData(ctx context.Context, req KanbanViewDataRequest) (*KanbanViewData, error)

	// 日历视图数据获取
	GetCalendarViewData(ctx context.Context, req CalendarViewDataRequest) (*CalendarViewData, error)

	// 画廊视图数据获取
	GetGalleryViewData(ctx context.Context, req GalleryViewDataRequest) (*GalleryViewData, error)
}
