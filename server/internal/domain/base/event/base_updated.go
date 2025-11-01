package event

import "time"

// BaseUpdated Base更新领域事件
type BaseUpdated struct {
	BaseID    string
	Changes   map[string]interface{} // 变更的字段
	UpdatedBy string
	UpdatedAt time.Time
}

// EventName 事件名称
func (e *BaseUpdated) EventName() string {
	return "base.updated"
}

// OccurredAt 事件发生时间
func (e *BaseUpdated) OccurredAt() time.Time {
	return e.UpdatedAt
}
