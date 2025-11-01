package event

import "time"

// BaseCreated Base创建领域事件
type BaseCreated struct {
	BaseID    string
	Name      string
	SpaceID   string
	CreatedBy string
	CreatedAt time.Time
}

// EventName 事件名称
func (e *BaseCreated) EventName() string {
	return "base.created"
}

// OccurredAt 事件发生时间
func (e *BaseCreated) OccurredAt() time.Time {
	return e.CreatedAt
}
