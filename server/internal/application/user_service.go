package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// UserService 用户应用服务
type UserService struct {
	userRepo    repository.UserRepository
	hookService *HookService
}

// NewUserService 创建用户服务
func NewUserService(userRepo repository.UserRepository) *UserService {
	return &UserService{
		userRepo: userRepo,
	}
}

// SetHookService 设置钩子服务（用于延迟注入）
func (s *UserService) SetHookService(hookService *HookService) {
	s.hookService = hookService
}

// CreateUser 创建用户
func (s *UserService) CreateUser(ctx context.Context, req dto.CreateUserRequest) (*dto.UserResponse, error) {
	// 1. 验证邮箱格式
	email, err := valueobject.NewEmail(req.Email)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("邮箱格式无效: %v", err))
	}

	// 2. 检查邮箱是否已存在
	exists, err := s.userRepo.ExistsByEmail(ctx, email, nil)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查邮箱失败: %v", err))
	}
	if exists {
		return nil, pkgerrors.ErrConflict.WithDetails("邮箱已被注册")
	}

	// 3. 创建密码值对象
	password, err := valueobject.NewPassword(req.Password)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("密码格式无效: %v", err))
	}

	// 4. 创建用户实体
	user, err := entity.NewUser(email, req.Name, password, "system")
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("创建用户实体失败: %v", err))
	}

	// 5. 设置可选属性
	if req.Avatar != "" {
		user.UpdateAvatar(req.Avatar)
	}

	// 6. 保存用户
	if err := s.userRepo.Save(ctx, user); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存用户失败: %v", err))
	}

	logger.Info("用户创建成功",
		logger.String("user_id", user.ID().String()),
		logger.String("email", email.String()),
	)

	// 7. 触发用户创建钩子
	if s.hookService != nil {
		s.hookService.TriggerUserCreateHook(ctx, user.ID().String(), email.String(), user.Name())
	}

	// 8. 返回响应
	return dto.FromUserEntity(user), nil
}

// GetUser 获取用户信息
func (s *UserService) GetUser(ctx context.Context, userID string) (*dto.UserResponse, error) {
	// 1. 创建 UserID 值对象
	id := valueobject.NewUserID(userID)

	// 2. 查找用户
	user, err := s.userRepo.FindByID(ctx, id)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找用户失败: %v", err))
	}
	if user == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("用户不存在")
	}

	// 3. 返回响应
	return dto.FromUserEntity(user), nil
}

// UpdateUser 更新用户信息
func (s *UserService) UpdateUser(ctx context.Context, userID string, req dto.UpdateUserRequest) (*dto.UserResponse, error) {
	// 1. 查找用户
	id := valueobject.NewUserID(userID)
	user, err := s.userRepo.FindByID(ctx, id)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找用户失败: %v", err))
	}
	if user == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("用户不存在")
	}

	// 2. 更新名称
	if req.Name != nil && *req.Name != "" {
		if err := user.UpdateName(*req.Name); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("更新名称失败: %v", err))
		}
	}

	// 3. 更新邮箱
	if req.Email != nil && *req.Email != "" {
		email, err := valueobject.NewEmail(*req.Email)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("邮箱格式无效: %v", err))
		}

		// 检查邮箱是否已被使用
		exists, err := s.userRepo.ExistsByEmail(ctx, email, &id)
		if err != nil {
			return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查邮箱失败: %v", err))
		}
		if exists {
			return nil, pkgerrors.ErrConflict.WithDetails("邮箱已被使用")
		}

		user.UpdateEmail(email)
	}

	// 4. 更新头像
	if req.Avatar != nil && *req.Avatar != "" {
		user.UpdateAvatar(*req.Avatar)
	}

	// 6. 保存更新
	if err := s.userRepo.Save(ctx, user); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存用户失败: %v", err))
	}

	logger.Info("用户更新成功",
		logger.String("user_id", userID),
	)

	// 5. 触发用户更新钩子
	if s.hookService != nil {
		updates := make(map[string]interface{})
		if req.Name != nil {
			updates["name"] = *req.Name
		}
		if req.Email != nil {
			updates["email"] = *req.Email
		}
		if req.Avatar != nil {
			updates["avatar"] = *req.Avatar
		}
		s.hookService.TriggerUserUpdateHook(ctx, userID, updates)
	}

	return dto.FromUserEntity(user), nil
}

// DeleteUser 删除用户
func (s *UserService) DeleteUser(ctx context.Context, userID string) error {
	id := valueobject.NewUserID(userID)

	// 1. 检查用户是否存在
	exists, err := s.userRepo.Exists(ctx, id)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查用户失败: %v", err))
	}
	if !exists {
		return pkgerrors.ErrNotFound.WithDetails("用户不存在")
	}

	// 2. 删除用户
	if err := s.userRepo.Delete(ctx, id); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("删除用户失败: %v", err))
	}

	logger.Info("用户删除成功",
		logger.String("user_id", userID),
	)

	return nil
}

// ListUsers 列出用户
func (s *UserService) ListUsers(ctx context.Context, req dto.UserListFilter) (*dto.UserListResponse, error) {
	// 构建过滤器
	filter := repository.UserFilter{
		Limit:  req.Limit,
		Offset: req.Offset,
	}

	if req.Email != nil {
		filter.Email = req.Email
	}
	if req.Name != nil {
		filter.Name = req.Name
	}

	// 查询用户列表
	users, total, err := s.userRepo.List(ctx, filter)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查询用户列表失败: %v", err))
	}

	// 转换为 DTO
	userList := make([]*dto.UserResponse, 0, len(users))
	for _, user := range users {
		userList = append(userList, dto.FromUserEntity(user))
	}

	return &dto.UserListResponse{
		Users: userList,
		Pagination: &dto.PaginationResponse{
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
	}, nil
}

// GetUserByEmail 根据邮箱获取用户
func (s *UserService) GetUserByEmail(ctx context.Context, emailStr string) (*dto.UserResponse, error) {
	email, err := valueobject.NewEmail(emailStr)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails("邮箱格式无效")
	}

	user, err := s.userRepo.FindByEmail(ctx, email)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找用户失败: %v", err))
	}
	if user == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("用户不存在")
	}

	return dto.FromUserEntity(user), nil
}

// ChangePassword 修改密码
func (s *UserService) ChangePassword(ctx context.Context, userID string, oldPassword, newPassword string) error {
	// 1. 查找用户
	id := valueobject.NewUserID(userID)
	user, err := s.userRepo.FindByID(ctx, id)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找用户失败: %v", err))
	}
	if user == nil {
		return pkgerrors.ErrNotFound.WithDetails("用户不存在")
	}

	// 2. 验证旧密码
	oldPwd, err := valueobject.NewPassword(oldPassword)
	if err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails("旧密码格式无效")
	}

	if !user.Password().Verify(oldPwd) {
		return pkgerrors.ErrUnauthorized.WithDetails("旧密码不正确")
	}

	// 3. 创建新密码
	newPwd, err := valueobject.NewPassword(newPassword)
	if err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("新密码格式无效: %v", err))
	}

	// 4. 更新密码
	if err := user.UpdatePassword(oldPwd, newPwd); err != nil {
		return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("修改密码失败: %v", err))
	}

	// 5. 保存
	if err := s.userRepo.Save(ctx, user); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存用户失败: %v", err))
	}

	logger.Info("密码修改成功",
		logger.String("user_id", userID),
	)

	return nil
}
