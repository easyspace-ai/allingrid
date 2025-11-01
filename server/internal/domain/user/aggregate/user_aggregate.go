package aggregate

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/user"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/event"
)

// UserAggregate 用户聚合根
// 管理用户和第三方账户的关系
type UserAggregate struct {
	user         *entity.User
	accounts     []*entity.Account
	domainEvents []event.DomainEvent
}

// NewUserAggregate 创建用户聚合
func NewUserAggregate(user *entity.User) *UserAggregate {
	return &UserAggregate{
		user:         user,
		accounts:     make([]*entity.Account, 0),
		domainEvents: make([]event.DomainEvent, 0),
	}
}

// NewUserAggregateWithAccounts 创建带账户的用户聚合
func NewUserAggregateWithAccounts(user *entity.User, accounts []*entity.Account) *UserAggregate {
	return &UserAggregate{
		user:         user,
		accounts:     accounts,
		domainEvents: make([]event.DomainEvent, 0),
	}
}

// ==================== 访问器方法 ====================

// User 获取用户实体
func (agg *UserAggregate) User() *entity.User {
	return agg.user
}

// Accounts 获取账户列表（返回副本）
func (agg *UserAggregate) Accounts() []*entity.Account {
	accounts := make([]*entity.Account, len(agg.accounts))
	copy(accounts, agg.accounts)
	return accounts
}

// GetAccountByProvider 根据提供商获取账户
func (agg *UserAggregate) GetAccountByProvider(provider string) *entity.Account {
	for _, account := range agg.accounts {
		if account.Provider() == provider {
			return account
		}
	}
	return nil
}

// HasProvider 是否有指定提供商的账户
func (agg *UserAggregate) HasProvider(provider string) bool {
	return agg.GetAccountByProvider(provider) != nil
}

// DomainEvents 获取领域事件
func (agg *UserAggregate) DomainEvents() []event.DomainEvent {
	return agg.domainEvents
}

// ClearDomainEvents 清空领域事件
func (agg *UserAggregate) ClearDomainEvents() {
	agg.domainEvents = make([]event.DomainEvent, 0)
}

// ==================== 业务方法 ====================

// LinkAccount 关联第三方账户
func (agg *UserAggregate) LinkAccount(account *entity.Account) error {
	// 检查是否已经关联该提供商
	if agg.HasProvider(account.Provider()) {
		return user.ErrAccountAlreadyLinked
	}

	// 添加账户
	agg.accounts = append(agg.accounts, account)

	// 发布领域事件
	agg.addDomainEvent(event.NewAccountLinked(
		agg.user.ID(),
		account.ID(),
		account.Provider(),
	))

	return nil
}

// UnlinkAccount 解除账户关联
func (agg *UserAggregate) UnlinkAccount(provider string) error {
	for i, account := range agg.accounts {
		if account.Provider() == provider {
			// 移除账户
			agg.accounts = append(agg.accounts[:i], agg.accounts[i+1:]...)

			// 发布领域事件
			agg.addDomainEvent(event.NewAccountUnlinked(
				agg.user.ID(),
				account.ID(),
				provider,
			))

			return nil
		}
	}

	return user.ErrAccountNotFound
}

// Activate 激活用户
func (agg *UserAggregate) Activate() error {
	if err := agg.user.Activate(); err != nil {
		return err
	}

	// 发布领域事件
	agg.addDomainEvent(event.NewUserActivated(
		agg.user.ID(),
		agg.user.Email(),
	))

	return nil
}

// Deactivate 停用用户
func (agg *UserAggregate) Deactivate(reason string) error {
	if err := agg.user.Deactivate(reason); err != nil {
		return err
	}

	// 发布领域事件
	agg.addDomainEvent(event.NewUserDeactivated(
		agg.user.ID(),
		reason,
	))

	return nil
}

// Delete 删除用户
func (agg *UserAggregate) Delete() error {
	if err := agg.user.SoftDelete(); err != nil {
		return err
	}

	// 发布领域事件
	agg.addDomainEvent(event.NewUserDeleted(
		agg.user.ID(),
		agg.user.Email(),
	))

	return nil
}

// ==================== 私有辅助方法 ====================

// addDomainEvent 添加领域事件
func (agg *UserAggregate) addDomainEvent(evt event.DomainEvent) {
	agg.domainEvents = append(agg.domainEvents, evt)
}
