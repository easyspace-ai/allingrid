package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// AuthService 认证服务
type AuthService struct {
	userRepo     repository.UserRepository
	tokenService *TokenService
}

// NewAuthService 创建认证服务
func NewAuthService(userRepo repository.UserRepository, tokenService *TokenService) *AuthService {
	return &AuthService{
		userRepo:     userRepo,
		tokenService: tokenService,
	}
}

// Login 用户登录
func (s *AuthService) Login(ctx context.Context, req dto.LoginRequest) (*dto.LoginResponse, error) {
	// 1. 查找用户
	email, err := valueobject.NewEmail(req.Email)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails("邮箱格式无效")
	}

	user, err := s.userRepo.FindByEmail(ctx, email)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找用户失败: %v", err))
	}
	if user == nil {
		return nil, pkgerrors.ErrUnauthorized.WithDetails("邮箱或密码错误")
	}

	// 2. 验证密码
	password, err := valueobject.NewPassword(req.Password)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails("密码格式无效")
	}

	if !user.Password().Verify(password) {
		return nil, pkgerrors.ErrUnauthorized.WithDetails("邮箱或密码错误")
	}

	// 3. 检查用户状态
	if !user.IsActive() {
		return nil, pkgerrors.ErrForbidden.WithDetails("账户已被停用")
	}

	// 4. 更新最后登录时间
	if err := s.userRepo.UpdateLastSignTime(ctx, user.ID()); err != nil {
		logger.Error("更新最后登录时间失败", logger.ErrorField(err))
	}

	// 5. 生成Token
	accessToken, refreshToken, err := s.tokenService.GenerateTokens(user.ID().String(), user.Email().String(), user.IsAdmin())
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("生成Token失败: %v", err))
	}

	logger.Info("用户登录成功",
		logger.String("user_id", user.ID().String()),
		logger.String("email", email.String()),
	)

	return &dto.LoginResponse{
		User:         dto.FromUserEntity(user),
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// Register 用户注册
func (s *AuthService) Register(ctx context.Context, req dto.RegisterRequest) (*dto.LoginResponse, error) {
	// 1. 验证邮箱
	email, err := valueobject.NewEmail(req.Email)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails("邮箱格式无效")
	}

	// 2. 检查邮箱是否已存在
	exists, err := s.userRepo.ExistsByEmail(ctx, email, nil)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查邮箱失败: %v", err))
	}
	if exists {
		return nil, pkgerrors.ErrConflict.WithDetails("邮箱已被注册")
	}

	// 3. 创建用户（使用 UserService 的逻辑）
	createUserReq := dto.CreateUserRequest{
		Email:    req.Email,
		Name:     req.Name,
		Password: req.Password,
	}

	userService := NewUserService(s.userRepo)
	userResp, err := userService.CreateUser(ctx, createUserReq)
	if err != nil {
		return nil, err
	}

	// 4. 生成Token
	accessToken, refreshToken, err := s.tokenService.GenerateTokens(userResp.ID, userResp.Email, false)
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("生成Token失败: %v", err))
	}

	logger.Info("用户注册成功",
		logger.String("user_id", userResp.ID),
		logger.String("email", email.String()),
	)

	return &dto.LoginResponse{
		User:         userResp,
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// RefreshToken 刷新令牌
func (s *AuthService) RefreshToken(ctx context.Context, refreshToken string) (*dto.TokenResponse, error) {
	// 1. 验证并解析刷新Token
	claims, err := s.tokenService.ValidateRefreshToken(refreshToken)
	if err != nil {
		return nil, pkgerrors.ErrUnauthorized.WithDetails("刷新Token无效")
	}

	// 2. 查找用户
	userID := valueobject.NewUserID(claims.UserID)
	user, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找用户失败: %v", err))
	}
	if user == nil {
		return nil, pkgerrors.ErrUnauthorized.WithDetails("用户不存在")
	}

	// 3. 检查用户状态
	if !user.IsActive() {
		return nil, pkgerrors.ErrForbidden.WithDetails("账户已被停用")
	}

	// 4. 生成新的Token
	accessToken, newRefreshToken, err := s.tokenService.GenerateTokens(user.ID().String(), user.Email().String(), user.IsAdmin())
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("生成Token失败: %v", err))
	}

	return &dto.TokenResponse{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
	}, nil
}

// Logout 用户登出
func (s *AuthService) Logout(ctx context.Context, userID string) error {
	// 实现Token黑名单或缓存失效逻辑（参考 teable-develop 使用 Redis）
	logger.Info("用户登出",
		logger.String("user_id", userID),
	)

	return nil
}

// ValidateToken 验证Token
func (s *AuthService) ValidateToken(ctx context.Context, token string) (*dto.TokenClaims, error) {
	claims, err := s.tokenService.ValidateAccessToken(token)
	if err != nil {
		return nil, pkgerrors.ErrUnauthorized.WithDetails("Token无效")
	}

	return &dto.TokenClaims{
		UserID:  claims.UserID,
		Email:   claims.Email,
		IsAdmin: claims.IsAdmin,
	}, nil
}
