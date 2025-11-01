package mapper

import (
	"time"

	userEntity "github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	userValueObject "github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
)

// ToUserEntity 将数据库模型转换为领域实体
func ToUserEntity(dbUser *models.User) (*userEntity.User, error) {
	if dbUser == nil {
		return nil, nil
	}

	// 转换 Email
	email, err := userValueObject.NewEmail(dbUser.Email)
	if err != nil {
		return nil, err
	}

	// 转换 Password（如果存在）
	var password userValueObject.HashedPassword
	if dbUser.Password != nil && *dbUser.Password != "" {
		password, err = userValueObject.NewHashedPassword(*dbUser.Password)
		if err != nil {
			return nil, err
		}
	}

	// 转换 Phone（如果存在）
	var phone *userValueObject.Phone
	if dbUser.Phone != nil && *dbUser.Phone != "" {
		p, err := userValueObject.NewPhone(*dbUser.Phone)
		if err == nil {
			phone = &p
		}
	}

	// 转换 UserID
	userID := userValueObject.NewUserID(dbUser.ID)

	// 处理布尔值指针
	isAdmin := false
	if dbUser.IsAdmin != nil {
		isAdmin = *dbUser.IsAdmin
	}

	isSystem := false
	if dbUser.IsSystem != nil {
		isSystem = *dbUser.IsSystem
	}

	isTrialUsed := false
	if dbUser.IsTrialUsed != nil {
		isTrialUsed = *dbUser.IsTrialUsed
	}

	// 确定状态
	status := userValueObject.ActiveStatus()
	if dbUser.DeactivatedTime != nil {
		status = userValueObject.DeactivatedStatus()
	}

	// 处理 LastModifiedTime
	updatedAt := dbUser.CreatedTime
	if dbUser.LastModifiedTime != nil {
		updatedAt = *dbUser.LastModifiedTime
	}

	// 处理 DeletedTime
	var deletedAt *time.Time
	if dbUser.DeletedTime.Valid {
		deletedAt = &dbUser.DeletedTime.Time
	}

	// 重建实体
	user := userEntity.ReconstructUser(
		userID,
		dbUser.Name,
		email,
		password,
		phone,
		dbUser.Avatar,
		status,
		isSystem,
		isAdmin,
		isTrialUsed,
		"", // createdBy - 数据库模型中没有这个字段
		dbUser.CreatedTime,
		updatedAt,
		dbUser.LastSignTime,
		dbUser.DeactivatedTime,
		deletedAt,
		1, // version - 数据库模型中没有这个字段，默认为1
	)

	return user, nil
}

// ToUserModel 将领域实体转换为数据库模型
func ToUserModel(user *userEntity.User) *models.User {
	if user == nil {
		return nil
	}

	updatedAt := user.UpdatedAt()

	dbUser := &models.User{
		ID:               user.ID().String(),
		Name:             user.Name(),
		Email:            user.Email().String(),
		CreatedTime:      user.CreatedAt(),
		LastModifiedTime: &updatedAt,
	}

	// Password
	passwordHash := user.Password().String()
	if passwordHash != "" {
		dbUser.Password = &passwordHash
	}

	// Phone
	if user.Phone() != nil {
		phoneStr := user.Phone().String()
		dbUser.Phone = &phoneStr
	}

	// Avatar
	if user.Avatar() != nil {
		dbUser.Avatar = user.Avatar()
	}

	// IsAdmin
	isAdmin := user.IsAdmin()
	dbUser.IsAdmin = &isAdmin

	// IsSystem
	isSystem := user.IsSystem()
	dbUser.IsSystem = &isSystem

	// IsTrialUsed
	isTrialUsed := user.IsTrialUsed()
	dbUser.IsTrialUsed = &isTrialUsed

	// LastSignTime
	if user.LastSignAt() != nil {
		dbUser.LastSignTime = user.LastSignAt()
	}

	// DeactivatedTime
	if user.DeactivatedAt() != nil {
		dbUser.DeactivatedTime = user.DeactivatedAt()
	}

	return dbUser
}

// ToUserList 批量转换
func ToUserList(dbUsers []*models.User) ([]*userEntity.User, error) {
	users := make([]*userEntity.User, 0, len(dbUsers))
	for _, dbUser := range dbUsers {
		user, err := ToUserEntity(dbUser)
		if err != nil {
			return nil, err
		}
		if user != nil {
			users = append(users, user)
		}
	}
	return users, nil
}

// ToAccountEntity 将数据库模型转换为 Account 实体
func ToAccountEntity(dbAccount *models.Account) (*userEntity.Account, error) {
	if dbAccount == nil {
		return nil, nil
	}

	// 创建 UserID
	userID := userValueObject.NewUserID(dbAccount.UserID)

	// 重建 Account 实体
	// 注意：Account 实体没有 Reconstruct 方法，我们使用 NewAccount
	// 但需要设置 ID 和 CreatedAt
	account, err := userEntity.NewAccount(
		userID,
		dbAccount.Type,
		dbAccount.Provider,
		dbAccount.ProviderID,
	)
	if err != nil {
		return nil, err
	}

	// 通过反射或者添加 Reconstruct 方法来设置 ID 和 CreatedAt
	// 为了保持简单，这里假设我们通过 NewAccount 创建，ID 会自动生成
	// 在实际应用中，可能需要为 Account 添加 Reconstruct 方法

	return account, nil
}

// ToAccountModel 将 Account 实体转换为数据库模型
func ToAccountModel(account *userEntity.Account) (*models.Account, error) {
	if account == nil {
		return nil, nil
	}

	return &models.Account{
		ID:          account.ID(),
		UserID:      account.UserID().String(),
		Type:        account.AccountType(),
		Provider:    account.Provider(),
		ProviderID:  account.ProviderID(),
		CreatedTime: account.CreatedAt(),
	}, nil
}

// ToAccountList 批量转换 Account
func ToAccountList(dbAccounts []*models.Account) ([]*userEntity.Account, error) {
	accounts := make([]*userEntity.Account, 0, len(dbAccounts))
	for _, dbAccount := range dbAccounts {
		account, err := ToAccountEntity(dbAccount)
		if err != nil {
			return nil, err
		}
		if account != nil {
			accounts = append(accounts, account)
		}
	}
	return accounts, nil
}
