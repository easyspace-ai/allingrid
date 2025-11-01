package event

import "time"

// BaseDeleted Base删除领域事件
type BaseDeleted struct {
	BaseID    string
	SpaceID   string
	DeletedBy string
	DeletedAt time.Time
}

// EventName 事件名称
func (e *BaseDeleted) EventName() string {
	return "base.deleted"
}

// OccurredAt 事件发生时间
func (e *BaseDeleted) OccurredAt() time.Time {
	return e.DeletedAt
}
