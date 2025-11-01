package event

import "time"

// ViewEvent 视图领域事件接口
type ViewEvent interface {
	EventName() string
	OccurredAt() time.Time
	ViewID() string
	TableID() string
}

// BaseViewEvent 视图事件基类
type BaseViewEvent struct {
	viewID     string
	tableID    string
	occurredAt time.Time
}

func (e *BaseViewEvent) ViewID() string        { return e.viewID }
func (e *BaseViewEvent) TableID() string       { return e.tableID }
func (e *BaseViewEvent) OccurredAt() time.Time { return e.occurredAt }

// ViewCreated 视图创建事件
type ViewCreated struct {
	BaseViewEvent
	ViewName  string
	ViewType  string
	CreatedBy string
}

func NewViewCreated(viewID, tableID, viewName, viewType, createdBy string) *ViewCreated {
	return &ViewCreated{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
		ViewName:  viewName,
		ViewType:  viewType,
		CreatedBy: createdBy,
	}
}

func (e *ViewCreated) EventName() string { return "view.created" }

// ViewUpdated 视图更新事件
type ViewUpdated struct {
	BaseViewEvent
	UpdatedFields []string
}

func NewViewUpdated(viewID, tableID string, updatedFields []string) *ViewUpdated {
	return &ViewUpdated{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
		UpdatedFields: updatedFields,
	}
}

func (e *ViewUpdated) EventName() string { return "view.updated" }

// ViewDeleted 视图删除事件
type ViewDeleted struct {
	BaseViewEvent
}

func NewViewDeleted(viewID, tableID string) *ViewDeleted {
	return &ViewDeleted{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
	}
}

func (e *ViewDeleted) EventName() string { return "view.deleted" }

// ViewShared 视图分享事件
type ViewShared struct {
	BaseViewEvent
	ShareID string
}

func NewViewShared(viewID, tableID, shareID string) *ViewShared {
	return &ViewShared{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
		ShareID: shareID,
	}
}

func (e *ViewShared) EventName() string { return "view.shared" }

// ViewUnshared 视图取消分享事件
type ViewUnshared struct {
	BaseViewEvent
}

func NewViewUnshared(viewID, tableID string) *ViewUnshared {
	return &ViewUnshared{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
	}
}

func (e *ViewUnshared) EventName() string { return "view.unshared" }

// ViewLocked 视图锁定事件
type ViewLocked struct {
	BaseViewEvent
}

func NewViewLocked(viewID, tableID string) *ViewLocked {
	return &ViewLocked{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
	}
}

func (e *ViewLocked) EventName() string { return "view.locked" }

// ViewUnlocked 视图解锁事件
type ViewUnlocked struct {
	BaseViewEvent
}

func NewViewUnlocked(viewID, tableID string) *ViewUnlocked {
	return &ViewUnlocked{
		BaseViewEvent: BaseViewEvent{
			viewID:     viewID,
			tableID:    tableID,
			occurredAt: time.Now(),
		},
	}
}

func (e *ViewUnlocked) EventName() string { return "view.unlocked" }
