package aggregate

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/space"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/event"
	"github.com/easyspace-ai/luckdb/server/internal/domain/space/valueobject"
)

// SpaceAggregate 空间聚合根
// 管理空间和协作者的关系
type SpaceAggregate struct {
	space         *entity.Space
	collaborators []*entity.Collaborator
	domainEvents  []event.DomainEvent
}

// NewSpaceAggregate 创建空间聚合
func NewSpaceAggregate(space *entity.Space) *SpaceAggregate {
	return &SpaceAggregate{
		space:         space,
		collaborators: make([]*entity.Collaborator, 0),
		domainEvents:  make([]event.DomainEvent, 0),
	}
}

// NewSpaceAggregateWithMembers 创建带成员的空间聚合
func NewSpaceAggregateWithMembers(space *entity.Space, collaborators []*entity.Collaborator) *SpaceAggregate {
	return &SpaceAggregate{
		space:         space,
		collaborators: collaborators,
		domainEvents:  make([]event.DomainEvent, 0),
	}
}

// ==================== 访问器方法 ====================

// Space 获取空间实体
func (agg *SpaceAggregate) Space() *entity.Space {
	return agg.space
}

// Collaborators 获取协作者列表（返回副本）
func (agg *SpaceAggregate) Collaborators() []*entity.Collaborator {
	collaborators := make([]*entity.Collaborator, len(agg.collaborators))
	copy(collaborators, agg.collaborators)
	return collaborators
}

// GetMemberCount 获取成员数量
func (agg *SpaceAggregate) GetMemberCount() int {
	return len(agg.collaborators)
}

// GetMemberByUserID 根据用户ID获取成员
func (agg *SpaceAggregate) GetMemberByUserID(userID string) *entity.Collaborator {
	for _, collab := range agg.collaborators {
		if collab.UserID() == userID {
			return collab
		}
	}
	return nil
}

// IsMember 检查用户是否为成员
func (agg *SpaceAggregate) IsMember(userID string) bool {
	return agg.GetMemberByUserID(userID) != nil
}

// GetOwner 获取拥有者
func (agg *SpaceAggregate) GetOwner() *entity.Collaborator {
	for _, collab := range agg.collaborators {
		if collab.IsOwner() {
			return collab
		}
	}
	return nil
}

// DomainEvents 获取领域事件
func (agg *SpaceAggregate) DomainEvents() []event.DomainEvent {
	return agg.domainEvents
}

// ClearDomainEvents 清空领域事件
func (agg *SpaceAggregate) ClearDomainEvents() {
	agg.domainEvents = make([]event.DomainEvent, 0)
}

// ==================== 业务方法 ====================

// AddMember 添加成员
func (agg *SpaceAggregate) AddMember(
	userID string,
	role valueobject.CollaboratorRole,
	addedBy string,
) error {
	// 检查是否已是成员
	if agg.IsMember(userID) {
		return space.ErrMemberAlreadyExists
	}

	// 检查操作者权限
	operator := agg.GetMemberByUserID(addedBy)
	if operator == nil || !operator.CanManageMembers() {
		return space.ErrInsufficientPermission
	}

	// 创建协作者
	collaborator, err := entity.NewCollaborator(agg.space.ID(), userID, role)
	if err != nil {
		return err
	}

	// 添加到列表
	agg.collaborators = append(agg.collaborators, collaborator)

	// 发布领域事件
	agg.addDomainEvent(event.NewMemberAdded(
		agg.space.ID(),
		userID,
		role,
		addedBy,
	))

	return nil
}

// RemoveMember 移除成员
func (agg *SpaceAggregate) RemoveMember(userID string, removedBy string) error {
	// 检查是否为成员
	if !agg.IsMember(userID) {
		return space.ErrMemberNotFound
	}

	// 不能移除拥有者
	member := agg.GetMemberByUserID(userID)
	if member.IsOwner() {
		return space.ErrCannotRemoveOwner
	}

	// 不能移除最后一个成员
	if agg.GetMemberCount() <= 1 {
		return space.ErrCannotRemoveLastMember
	}

	// 检查操作者权限
	operator := agg.GetMemberByUserID(removedBy)
	if operator == nil || !operator.CanManageMembers() {
		return space.ErrInsufficientPermission
	}

	// 移除成员
	for i, collab := range agg.collaborators {
		if collab.UserID() == userID {
			agg.collaborators = append(agg.collaborators[:i], agg.collaborators[i+1:]...)
			break
		}
	}

	// 发布领域事件
	agg.addDomainEvent(event.NewMemberRemoved(
		agg.space.ID(),
		userID,
		removedBy,
	))

	return nil
}

// ChangeMemberRole 变更成员角色
func (agg *SpaceAggregate) ChangeMemberRole(
	userID string,
	newRole valueobject.CollaboratorRole,
	changedBy string,
) error {
	// 检查是否为成员
	member := agg.GetMemberByUserID(userID)
	if member == nil {
		return space.ErrMemberNotFound
	}

	// 检查操作者权限
	operator := agg.GetMemberByUserID(changedBy)
	if operator == nil || !operator.CanManageMembers() {
		return space.ErrInsufficientPermission
	}

	// 变更角色
	if err := member.ChangeRole(newRole); err != nil {
		return err
	}

	// 发布领域事件
	agg.addDomainEvent(event.NewMemberRoleChanged(
		agg.space.ID(),
		userID,
		newRole,
		changedBy,
	))

	return nil
}

// TransferOwnership 转让所有权
func (agg *SpaceAggregate) TransferOwnership(newOwnerID, currentOwnerID string) error {
	// 检查新拥有者是否为成员
	newOwner := agg.GetMemberByUserID(newOwnerID)
	if newOwner == nil {
		return space.NewDomainError(
			"NEW_OWNER_NOT_MEMBER",
			"new owner must be a member of the space",
			nil,
		)
	}

	// 转让所有权
	if err := agg.space.TransferOwnership(newOwnerID, currentOwnerID); err != nil {
		return err
	}

	// 更新协作者角色
	oldOwner := agg.GetMemberByUserID(currentOwnerID)
	if oldOwner != nil {
		oldOwner.ChangeRole(valueobject.Admin())
	}

	newOwner.ChangeRole(valueobject.Owner())

	// 发布领域事件
	agg.addDomainEvent(event.NewOwnershipTransferred(
		agg.space.ID(),
		currentOwnerID,
		newOwnerID,
	))

	return nil
}

// ==================== 私有辅助方法 ====================

// addDomainEvent 添加领域事件
func (agg *SpaceAggregate) addDomainEvent(evt event.DomainEvent) {
	agg.domainEvents = append(agg.domainEvents, evt)
}
