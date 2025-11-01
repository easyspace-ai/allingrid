package valueobject

import "fmt"

// ViewType 视图类型值对象
type ViewType string

const (
	ViewTypeGrid     ViewType = "grid"     // 表格视图
	ViewTypeKanban   ViewType = "kanban"   // 看板视图
	ViewTypeGallery  ViewType = "gallery"  // 画廊视图
	ViewTypeForm     ViewType = "form"     // 表单视图
	ViewTypeCalendar ViewType = "calendar" // 日历视图
)

// NewViewType 创建视图类型值对象
func NewViewType(value string) (ViewType, error) {
	vt := ViewType(value)
	if !vt.IsValid() {
		return "", fmt.Errorf("invalid view type: %s", value)
	}
	return vt, nil
}

// String 获取字符串值
func (vt ViewType) String() string {
	return string(vt)
}

// IsValid 检查视图类型是否有效
func (vt ViewType) IsValid() bool {
	validTypes := map[ViewType]bool{
		ViewTypeGrid:     true,
		ViewTypeKanban:   true,
		ViewTypeGallery:  true,
		ViewTypeForm:     true,
		ViewTypeCalendar: true,
	}
	return validTypes[vt]
}

// Equals 比较两个视图类型是否相等
func (vt ViewType) Equals(other ViewType) bool {
	return vt == other
}

// IsGrid 是否为表格视图
func (vt ViewType) IsGrid() bool {
	return vt == ViewTypeGrid
}

// IsKanban 是否为看板视图
func (vt ViewType) IsKanban() bool {
	return vt == ViewTypeKanban
}

// IsGallery 是否为画廊视图
func (vt ViewType) IsGallery() bool {
	return vt == ViewTypeGallery
}

// IsForm 是否为表单视图
func (vt ViewType) IsForm() bool {
	return vt == ViewTypeForm
}

// IsCalendar 是否为日历视图
func (vt ViewType) IsCalendar() bool {
	return vt == ViewTypeCalendar
}

// SupportsFilter 是否支持过滤
func (vt ViewType) SupportsFilter() bool {
	// 所有视图类型都支持过滤
	return true
}

// SupportsSort 是否支持排序
func (vt ViewType) SupportsSort() bool {
	// 除了看板视图，其他都支持排序
	return vt != ViewTypeKanban
}

// SupportsGroup 是否支持分组
func (vt ViewType) SupportsGroup() bool {
	// 看板视图支持分组
	return vt == ViewTypeKanban
}
