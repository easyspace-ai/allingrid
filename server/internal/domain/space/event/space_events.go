package event

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/space/valueobject"
	"github.com/google/uuid"
)

// DomainEvent 领域事件接口
type DomainEvent interface {
	EventID() string
	EventType() string
	OccurredAt() time.Time
	AggregateID() string
}

// BaseDomainEvent 基础领域事件
type BaseDomainEvent struct {
	eventID     string
	eventType   string
	occurredAt  time.Time
	aggregateID string
}

func newBaseDomainEvent(eventType string, aggregateID string) BaseDomainEvent {
	return BaseDomainEvent{
		eventID:     uuid.New().String(),
		eventType:   eventType,
		occurredAt:  time.Now(),
		aggregateID: aggregateID,
	}
}

func (e BaseDomainEvent) EventID() string       { return e.eventID }
func (e BaseDomainEvent) EventType() string     { return e.eventType }
func (e BaseDomainEvent) OccurredAt() time.Time { return e.occurredAt }
func (e BaseDomainEvent) AggregateID() string   { return e.aggregateID }

// SpaceCreated 空间创建事件
type SpaceCreated struct {
	BaseDomainEvent
	spaceID   valueobject.SpaceID
	spaceName valueobject.SpaceName
	ownerID   string
}

// NewSpaceCreated 创建空间创建事件
func NewSpaceCreated(
	spaceID valueobject.SpaceID,
	spaceName valueobject.SpaceName,
	ownerID string,
) *SpaceCreated {
	return &SpaceCreated{
		BaseDomainEvent: newBaseDomainEvent("space.created", spaceID.String()),
		spaceID:         spaceID,
		spaceName:       spaceName,
		ownerID:         ownerID,
	}
}

func (e *SpaceCreated) SpaceID() valueobject.SpaceID     { return e.spaceID }
func (e *SpaceCreated) SpaceName() valueobject.SpaceName { return e.spaceName }
func (e *SpaceCreated) OwnerID() string                  { return e.ownerID }

// MemberAdded 成员添加事件
type MemberAdded struct {
	BaseDomainEvent
	spaceID valueobject.SpaceID
	userID  string
	role    valueobject.CollaboratorRole
	addedBy string
}

// NewMemberAdded 创建成员添加事件
func NewMemberAdded(
	spaceID valueobject.SpaceID,
	userID string,
	role valueobject.CollaboratorRole,
	addedBy string,
) *MemberAdded {
	return &MemberAdded{
		BaseDomainEvent: newBaseDomainEvent("space.member_added", spaceID.String()),
		spaceID:         spaceID,
		userID:          userID,
		role:            role,
		addedBy:         addedBy,
	}
}

func (e *MemberAdded) SpaceID() valueobject.SpaceID       { return e.spaceID }
func (e *MemberAdded) UserID() string                     { return e.userID }
func (e *MemberAdded) Role() valueobject.CollaboratorRole { return e.role }
func (e *MemberAdded) AddedBy() string                    { return e.addedBy }

// MemberRemoved 成员移除事件
type MemberRemoved struct {
	BaseDomainEvent
	spaceID   valueobject.SpaceID
	userID    string
	removedBy string
}

// NewMemberRemoved 创建成员移除事件
func NewMemberRemoved(
	spaceID valueobject.SpaceID,
	userID string,
	removedBy string,
) *MemberRemoved {
	return &MemberRemoved{
		BaseDomainEvent: newBaseDomainEvent("space.member_removed", spaceID.String()),
		spaceID:         spaceID,
		userID:          userID,
		removedBy:       removedBy,
	}
}

func (e *MemberRemoved) SpaceID() valueobject.SpaceID { return e.spaceID }
func (e *MemberRemoved) UserID() string               { return e.userID }
func (e *MemberRemoved) RemovedBy() string            { return e.removedBy }

// MemberRoleChanged 成员角色变更事件
type MemberRoleChanged struct {
	BaseDomainEvent
	spaceID   valueobject.SpaceID
	userID    string
	newRole   valueobject.CollaboratorRole
	changedBy string
}

// NewMemberRoleChanged 创建成员角色变更事件
func NewMemberRoleChanged(
	spaceID valueobject.SpaceID,
	userID string,
	newRole valueobject.CollaboratorRole,
	changedBy string,
) *MemberRoleChanged {
	return &MemberRoleChanged{
		BaseDomainEvent: newBaseDomainEvent("space.member_role_changed", spaceID.String()),
		spaceID:         spaceID,
		userID:          userID,
		newRole:         newRole,
		changedBy:       changedBy,
	}
}

func (e *MemberRoleChanged) SpaceID() valueobject.SpaceID          { return e.spaceID }
func (e *MemberRoleChanged) UserID() string                        { return e.userID }
func (e *MemberRoleChanged) NewRole() valueobject.CollaboratorRole { return e.newRole }
func (e *MemberRoleChanged) ChangedBy() string                     { return e.changedBy }

// OwnershipTransferred 所有权转让事件
type OwnershipTransferred struct {
	BaseDomainEvent
	spaceID    valueobject.SpaceID
	oldOwnerID string
	newOwnerID string
}

// NewOwnershipTransferred 创建所有权转让事件
func NewOwnershipTransferred(
	spaceID valueobject.SpaceID,
	oldOwnerID, newOwnerID string,
) *OwnershipTransferred {
	return &OwnershipTransferred{
		BaseDomainEvent: newBaseDomainEvent("space.ownership_transferred", spaceID.String()),
		spaceID:         spaceID,
		oldOwnerID:      oldOwnerID,
		newOwnerID:      newOwnerID,
	}
}

func (e *OwnershipTransferred) SpaceID() valueobject.SpaceID { return e.spaceID }
func (e *OwnershipTransferred) OldOwnerID() string           { return e.oldOwnerID }
func (e *OwnershipTransferred) NewOwnerID() string           { return e.newOwnerID }
