package event

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"

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

// UserRegistered 用户注册事件
type UserRegistered struct {
	BaseDomainEvent
	userID valueobject.UserID
	email  valueobject.Email
	name   string
}

// NewUserRegistered 创建用户注册事件
func NewUserRegistered(
	userID valueobject.UserID,
	email valueobject.Email,
	name string,
) *UserRegistered {
	return &UserRegistered{
		BaseDomainEvent: newBaseDomainEvent("user.registered", userID.String()),
		userID:          userID,
		email:           email,
		name:            name,
	}
}

func (e *UserRegistered) UserID() valueobject.UserID { return e.userID }
func (e *UserRegistered) Email() valueobject.Email   { return e.email }
func (e *UserRegistered) Name() string               { return e.name }

// UserActivated 用户激活事件
type UserActivated struct {
	BaseDomainEvent
	userID valueobject.UserID
	email  valueobject.Email
}

// NewUserActivated 创建用户激活事件
func NewUserActivated(userID valueobject.UserID, email valueobject.Email) *UserActivated {
	return &UserActivated{
		BaseDomainEvent: newBaseDomainEvent("user.activated", userID.String()),
		userID:          userID,
		email:           email,
	}
}

func (e *UserActivated) UserID() valueobject.UserID { return e.userID }
func (e *UserActivated) Email() valueobject.Email   { return e.email }

// UserDeactivated 用户停用事件
type UserDeactivated struct {
	BaseDomainEvent
	userID valueobject.UserID
	reason string
}

// NewUserDeactivated 创建用户停用事件
func NewUserDeactivated(userID valueobject.UserID, reason string) *UserDeactivated {
	return &UserDeactivated{
		BaseDomainEvent: newBaseDomainEvent("user.deactivated", userID.String()),
		userID:          userID,
		reason:          reason,
	}
}

func (e *UserDeactivated) UserID() valueobject.UserID { return e.userID }
func (e *UserDeactivated) Reason() string             { return e.reason }

// UserDeleted 用户删除事件
type UserDeleted struct {
	BaseDomainEvent
	userID valueobject.UserID
	email  valueobject.Email
}

// NewUserDeleted 创建用户删除事件
func NewUserDeleted(userID valueobject.UserID, email valueobject.Email) *UserDeleted {
	return &UserDeleted{
		BaseDomainEvent: newBaseDomainEvent("user.deleted", userID.String()),
		userID:          userID,
		email:           email,
	}
}

func (e *UserDeleted) UserID() valueobject.UserID { return e.userID }
func (e *UserDeleted) Email() valueobject.Email   { return e.email }

// UserPasswordChanged 用户密码变更事件
type UserPasswordChanged struct {
	BaseDomainEvent
	userID valueobject.UserID
}

// NewUserPasswordChanged 创建密码变更事件
func NewUserPasswordChanged(userID valueobject.UserID) *UserPasswordChanged {
	return &UserPasswordChanged{
		BaseDomainEvent: newBaseDomainEvent("user.password_changed", userID.String()),
		userID:          userID,
	}
}

func (e *UserPasswordChanged) UserID() valueobject.UserID { return e.userID }

// AccountLinked 账户关联事件
type AccountLinked struct {
	BaseDomainEvent
	userID    valueobject.UserID
	accountID string
	provider  string
}

// NewAccountLinked 创建账户关联事件
func NewAccountLinked(userID valueobject.UserID, accountID, provider string) *AccountLinked {
	return &AccountLinked{
		BaseDomainEvent: newBaseDomainEvent("user.account_linked", userID.String()),
		userID:          userID,
		accountID:       accountID,
		provider:        provider,
	}
}

func (e *AccountLinked) UserID() valueobject.UserID { return e.userID }
func (e *AccountLinked) AccountID() string          { return e.accountID }
func (e *AccountLinked) Provider() string           { return e.provider }

// AccountUnlinked 账户解除关联事件
type AccountUnlinked struct {
	BaseDomainEvent
	userID    valueobject.UserID
	accountID string
	provider  string
}

// NewAccountUnlinked 创建账户解除关联事件
func NewAccountUnlinked(userID valueobject.UserID, accountID, provider string) *AccountUnlinked {
	return &AccountUnlinked{
		BaseDomainEvent: newBaseDomainEvent("user.account_unlinked", userID.String()),
		userID:          userID,
		accountID:       accountID,
		provider:        provider,
	}
}

func (e *AccountUnlinked) UserID() valueobject.UserID { return e.userID }
func (e *AccountUnlinked) AccountID() string          { return e.accountID }
func (e *AccountUnlinked) Provider() string           { return e.provider }
