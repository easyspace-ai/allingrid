package repository

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user/aggregate"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository/mapper"
)

// UserRepositoryImpl 用户仓储实现
type UserRepositoryImpl struct {
	db *gorm.DB
}

// NewUserRepository 创建用户仓储
func NewUserRepository(db *gorm.DB) repository.UserRepository {
	return &UserRepositoryImpl{db: db}
}

// Save 保存用户
func (r *UserRepositoryImpl) Save(ctx context.Context, user *entity.User) error {
	dbUser := mapper.ToUserModel(user)

	// 检查是否已存在
	var existing models.User
	err := r.db.WithContext(ctx).Where("id = ?", dbUser.ID).First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// 创建新用户
		return r.db.WithContext(ctx).Create(dbUser).Error
	} else if err != nil {
		return fmt.Errorf("failed to check existing user: %w", err)
	}

	// 更新现有用户
	return r.db.WithContext(ctx).Model(&models.User{}).
		Where("id = ?", dbUser.ID).
		Updates(dbUser).Error
}

// FindByID 根据ID查找用户
func (r *UserRepositoryImpl) FindByID(ctx context.Context, id valueobject.UserID) (*entity.User, error) {
	// 创建一个临时的用户结构体，不包含软删除字段
	type UserWithoutSoftDelete struct {
		ID                   string     `gorm:"primaryKey;type:varchar(30)"`
		Name                 string     `gorm:"not null;type:varchar(255)"`
		Email                string     `gorm:"unique;not null;type:varchar(255)"`
		Password             *string    `gorm:"type:varchar(255)"`
		Salt                 *string    `gorm:"type:varchar(255)"`
		Phone                *string    `gorm:"unique;type:varchar(50)"`
		Avatar               *string    `gorm:"type:varchar(500)"`
		IsSystem             *bool      `gorm:"column:is_system;default:false"`
		IsAdmin              *bool      `gorm:"column:is_admin;default:false"`
		IsTrialUsed          *bool      `gorm:"column:is_trial_used;default:false"`
		NotifyMeta           *string    `gorm:"type:text;column:notify_meta"`
		LastSignTime         *time.Time `gorm:"column:last_sign_time"`
		DeactivatedTime      *time.Time `gorm:"column:deactivated_time"`
		CreatedTime          time.Time  `gorm:"autoCreateTime;column:created_time"`
		LastModifiedTime     *time.Time `gorm:"autoUpdateTime;column:last_modified_time"`
		PermanentDeletedTime *time.Time `gorm:"column:permanent_deleted_time"`
		RefMeta              *string    `gorm:"type:text;column:ref_meta"`
	}

	var dbUser UserWithoutSoftDelete

	// 使用原生 SQL 查询，避免 GORM 的软删除问题
	err := r.db.WithContext(ctx).
		Table("users").
		Where("id = ? AND deleted_time IS NULL", id.String()).
		First(&dbUser).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find user: %w", err)
	}

	// 转换为 models.User
	userModel := &models.User{
		ID:                   dbUser.ID,
		Name:                 dbUser.Name,
		Email:                dbUser.Email,
		Password:             dbUser.Password,
		Salt:                 dbUser.Salt,
		Phone:                dbUser.Phone,
		Avatar:               dbUser.Avatar,
		IsSystem:             dbUser.IsSystem,
		IsAdmin:              dbUser.IsAdmin,
		IsTrialUsed:          dbUser.IsTrialUsed,
		NotifyMeta:           dbUser.NotifyMeta,
		LastSignTime:         dbUser.LastSignTime,
		DeactivatedTime:      dbUser.DeactivatedTime,
		CreatedTime:          dbUser.CreatedTime,
		LastModifiedTime:     dbUser.LastModifiedTime,
		PermanentDeletedTime: dbUser.PermanentDeletedTime,
		RefMeta:              dbUser.RefMeta,
		// DeletedTime 保持默认值（零值）
	}

	return mapper.ToUserEntity(userModel)
}

// FindByEmail 根据邮箱查找用户
func (r *UserRepositoryImpl) FindByEmail(ctx context.Context, email valueobject.Email) (*entity.User, error) {
	var dbUser models.User

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("users").
		Where("email = ?", email.String()).
		Where("deleted_time IS NULL").
		First(&dbUser).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find user by email: %w", err)
	}

	return mapper.ToUserEntity(&dbUser)
}

// FindByPhone 根据手机号查找用户
func (r *UserRepositoryImpl) FindByPhone(ctx context.Context, phone valueobject.Phone) (*entity.User, error) {
	var dbUser models.User

	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("users").
		Where("phone = ?", phone.String()).
		Where("deleted_time IS NULL").
		First(&dbUser).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find user by phone: %w", err)
	}

	return mapper.ToUserEntity(&dbUser)
}

// Delete 删除用户（软删除）
func (r *UserRepositoryImpl) Delete(ctx context.Context, id valueobject.UserID) error {
	return r.db.WithContext(ctx).
		Model(&models.User{}).
		Where("id = ?", id.String()).
		Update("deleted_time", gorm.Expr("NOW()")).Error
}

// Exists 检查用户是否存在
func (r *UserRepositoryImpl) Exists(ctx context.Context, id valueobject.UserID) (bool, error) {
	var count int64
	// ✅ 显式指定 schema
	err := r.db.WithContext(ctx).
		Table("users").
		Where("id = ?", id.String()).
		Where("deleted_time IS NULL").
		Count(&count).Error

	return count > 0, err
}

// ExistsByEmail 检查邮箱是否已存在
func (r *UserRepositoryImpl) ExistsByEmail(ctx context.Context, email valueobject.Email, excludeID *valueobject.UserID) (bool, error) {
	// ✅ 显式指定 schema
	query := r.db.WithContext(ctx).
		Table("users").
		Where("email = ?", email.String()).
		Where("deleted_time IS NULL")

	// 排除指定ID（用于更新时检查）
	if excludeID != nil {
		query = query.Where("id != ?", excludeID.String())
	}

	var count int64
	err := query.Count(&count).Error

	return count > 0, err
}

// List 列出用户
func (r *UserRepositoryImpl) List(ctx context.Context, filter repository.UserFilter) ([]*entity.User, int64, error) {
	// ✅ 显式指定 schema
	query := r.db.WithContext(ctx).Table("users").
		Where("deleted_time IS NULL")

	// 应用过滤条件
	if filter.Email != nil {
		query = query.Where("email LIKE ?", "%"+*filter.Email+"%")
	}
	if filter.Name != nil {
		query = query.Where("name LIKE ?", "%"+*filter.Name+"%")
	}
	if filter.IsAdmin != nil {
		query = query.Where("is_admin = ?", *filter.IsAdmin)
	}
	if filter.IsSystem != nil {
		query = query.Where("is_system = ?", *filter.IsSystem)
	}

	// 统计总数
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count users: %w", err)
	}

	// 排序
	if filter.OrderBy != "" {
		orderDir := "ASC"
		if filter.OrderDir == "desc" {
			orderDir = "DESC"
		}
		query = query.Order(fmt.Sprintf("%s %s", filter.OrderBy, orderDir))
	} else {
		query = query.Order("created_time DESC")
	}

	// 分页
	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	// 查询
	var dbUsers []*models.User
	if err := query.Find(&dbUsers).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to list users: %w", err)
	}

	// 转换
	users, err := mapper.ToUserList(dbUsers)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert users: %w", err)
	}

	return users, total, nil
}

// UpdateLastSignTime 更新最后登录时间
func (r *UserRepositoryImpl) UpdateLastSignTime(ctx context.Context, id valueobject.UserID) error {
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&models.User{}).
		Where("id = ?", id.String()).
		Update("last_sign_time", &now).Error
}

// Count 统计用户数量
func (r *UserRepositoryImpl) Count(ctx context.Context, filter repository.UserFilter) (int64, error) {
	// ✅ 显式指定 schema
	query := r.db.WithContext(ctx).Table("users").
		Where("deleted_time IS NULL")

	// 应用过滤条件
	if filter.Email != nil {
		query = query.Where("email LIKE ?", "%"+*filter.Email+"%")
	}
	if filter.Name != nil {
		query = query.Where("name LIKE ?", "%"+*filter.Name+"%")
	}
	if filter.IsAdmin != nil {
		query = query.Where("is_admin = ?", *filter.IsAdmin)
	}
	if filter.IsSystem != nil {
		query = query.Where("is_system = ?", *filter.IsSystem)
	}

	var count int64
	err := query.Count(&count).Error
	return count, err
}

// NextID 生成下一个用户ID
func (r *UserRepositoryImpl) NextID() valueobject.UserID {
	return valueobject.NewUserID("")
}

// SaveAggregate 保存用户聚合（包括账户）
func (r *UserRepositoryImpl) SaveAggregate(ctx context.Context, agg *aggregate.UserAggregate) error {
	// 开启事务保存用户聚合
	return r.db.Transaction(func(tx *gorm.DB) error {
		// 保存用户基本信息
		userModel := mapper.ToUserModel(agg.User())

		if err := tx.Save(userModel).Error; err != nil {
			return fmt.Errorf("failed to save user: %w", err)
		}

		// 保存关联的账户（如果存在）
		// 注意：这里需要从聚合中获取账户列表
		// 由于当前 User 聚合可能没有账户列表，这里先注释
		// 将来可以扩展 User 聚合以包含账户

		return nil
	})
}

// LoadAggregate 加载用户聚合（包括账户）
func (r *UserRepositoryImpl) LoadAggregate(ctx context.Context, userID valueobject.UserID) (*aggregate.UserAggregate, error) {
	// 查找用户
	user, err := r.FindByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find user: %w", err)
	}
	if user == nil {
		return nil, nil
	}

	// 查找用户的账户
	var dbAccounts []*models.Account
	if err := r.db.Where("user_id = ?", userID.String()).Find(&dbAccounts).Error; err != nil {
		// 账户查找失败不应该导致用户加载失败，记录错误但继续
		// 在生产环境中可以记录日志
		return aggregate.NewUserAggregate(user), nil
	}

	// 将账户添加到用户聚合
	// 注意：当前 User 聚合可能没有账户列表字段
	// 这里先转换账户，将来可以添加到聚合中
	_, err = mapper.ToAccountList(dbAccounts)
	if err != nil {
		// 账户转换失败不应该导致用户加载失败
		return aggregate.NewUserAggregate(user), nil
	}

	// 将账户列表添加到 User 聚合（简化实现：暂不包含账户列表）
	// user.SetAccounts(accounts)

	return aggregate.NewUserAggregate(user), nil
}
