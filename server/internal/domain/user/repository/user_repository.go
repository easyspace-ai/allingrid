package repository

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user/aggregate"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
)

// UserRepository 用户仓储接口
// 定义在领域层，实现在基础设施层
type UserRepository interface {
	// Save 保存用户
	Save(ctx context.Context, user *entity.User) error

	// FindByID 根据ID查找用户
	FindByID(ctx context.Context, id valueobject.UserID) (*entity.User, error)

	// FindByEmail 根据邮箱查找用户
	FindByEmail(ctx context.Context, email valueobject.Email) (*entity.User, error)

	// FindByPhone 根据手机号查找用户
	FindByPhone(ctx context.Context, phone valueobject.Phone) (*entity.User, error)

	// Delete 删除用户（物理删除）
	Delete(ctx context.Context, id valueobject.UserID) error

	// Exists 检查用户是否存在
	Exists(ctx context.Context, id valueobject.UserID) (bool, error)

	// ExistsByEmail 检查邮箱是否已存在
	ExistsByEmail(ctx context.Context, email valueobject.Email, excludeID *valueobject.UserID) (bool, error)

	// List 列出用户（支持过滤和分页）
	List(ctx context.Context, filter UserFilter) ([]*entity.User, int64, error)

	// SaveAggregate 保存用户聚合（包括账户）
	SaveAggregate(ctx context.Context, agg *aggregate.UserAggregate) error

	// LoadAggregate 加载用户聚合（包括账户）
	LoadAggregate(ctx context.Context, userID valueobject.UserID) (*aggregate.UserAggregate, error)

	// UpdateLastSignTime 更新最后登录时间
	UpdateLastSignTime(ctx context.Context, id valueobject.UserID) error

	// Count 统计用户数量
	Count(ctx context.Context, filter UserFilter) (int64, error)

	// NextID 生成下一个用户ID
	NextID() valueobject.UserID
}

// AccountRepository 账户仓储接口
type AccountRepository interface {
	// Save 保存账户
	Save(ctx context.Context, account *entity.Account) error

	// FindByID 根据ID查找账户
	FindByID(ctx context.Context, id string) (*entity.Account, error)

	// FindByUserID 查找用户的所有账户
	FindByUserID(ctx context.Context, userID valueobject.UserID) ([]*entity.Account, error)

	// FindByProvider 根据提供商和提供商ID查找账户
	FindByProvider(ctx context.Context, provider, providerID string) (*entity.Account, error)

	// Delete 删除账户
	Delete(ctx context.Context, id string) error

	// DeleteByUserID 删除用户的所有账户
	DeleteByUserID(ctx context.Context, userID valueobject.UserID) error
}

// UserFilter 用户过滤器
type UserFilter struct {
	Status    *valueobject.UserStatus
	IsAdmin   *bool
	IsSystem  *bool
	Email     *string
	Name      *string
	IsDeleted *bool
	CreatedBy *string
	OrderBy   string // name, email, created_at
	OrderDir  string // asc, desc
	Limit     int
	Offset    int
}
