package dto

import (
	"time"

	userEntity "github.com/easyspace-ai/luckdb/server/internal/domain/user/entity"
)

// CreateUserRequest 创建用户请求
type CreateUserRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Name     string `json:"name" binding:"required"`
	Password string `json:"password" binding:"required,min=6"`
	Avatar   string `json:"avatar"`
}

// UpdateUserRequest 更新用户请求
type UpdateUserRequest struct {
	Name   *string `json:"name"`
	Avatar *string `json:"avatar"`
	Email  *string `json:"email" binding:"omitempty,email"`
}

// UserListFilter 用户列表过滤器
type UserListFilter struct {
	Email     *string    `json:"email"`
	Name      *string    `json:"name"`
	IsActive  *bool      `json:"isActive"`
	CreatedAt *time.Time `json:"createdAt"`
	Page      int        `json:"page"`
	PageSize  int        `json:"pageSize"`
	Offset    int        `json:"offset"`
	Limit     int        `json:"limit"`
}

// BulkUpdateRequest 批量更新请求
type BulkUpdateRequest struct {
	UserIDs []string               `json:"userIds" binding:"required"`
	Updates map[string]interface{} `json:"updates" binding:"required"`
}

// UserResponse 用户响应
type UserResponse struct {
	ID              string    `json:"id"`
	Email           string    `json:"email"`
	Name            string    `json:"name"`
	Avatar          string    `json:"avatar"`
	IsActive        bool      `json:"isActive"`
	LastLoginAt     time.Time `json:"lastLoginAt"`
	LastLoginIP     string    `json:"lastLoginIp"`
	EmailVerifiedAt time.Time `json:"emailVerifiedAt"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

// UserListResponse 用户列表响应
type UserListResponse struct {
	Users      []*UserResponse     `json:"users"`
	Pagination *PaginationResponse `json:"pagination"`
}

// UserStats 用户统计
type UserStats struct {
	TotalUsers    int64 `json:"totalUsers"`
	ActiveUsers   int64 `json:"activeUsers"`
	InactiveUsers int64 `json:"inactiveUsers"`
	NewUsersToday int64 `json:"newUsersToday"`
	NewUsersWeek  int64 `json:"newUsersWeek"`
	NewUsersMonth int64 `json:"newUsersMonth"`
}

// PaginatedResult 分页结果（泛型结构）
type PaginatedResult struct {
	Items      interface{}         `json:"items"`
	Total      int64               `json:"total"`
	Pagination *PaginationResponse `json:"pagination"`
}

// FromUserEntity 从Domain实体转换为DTO
func FromUserEntity(user *userEntity.User) *UserResponse {
	if user == nil {
		return nil
	}

	avatar := ""
	if user.Avatar() != nil {
		avatar = *user.Avatar()
	}

	// 获取最后登录时间
	var lastLoginAt time.Time
	if user.LastSignAt() != nil {
		lastLoginAt = *user.LastSignAt()
	} else {
		lastLoginAt = user.CreatedAt() // 如果没有登录记录，使用创建时间
	}

	// EmailVerifiedAt 暂时使用创建时间（实际应该有独立字段）
	emailVerifiedAt := user.CreatedAt()

	return &UserResponse{
		ID:              user.ID().String(),
		Email:           user.Email().String(),
		Name:            user.Name(),
		Avatar:          avatar,
		IsActive:        user.IsActive(),
		LastLoginAt:     lastLoginAt,
		LastLoginIP:     "", // LastLoginIP 需要从其他来源获取（如 session 表）
		EmailVerifiedAt: emailVerifiedAt,
		CreatedAt:       user.CreatedAt(),
		UpdatedAt:       user.UpdatedAt(),
	}
}

// FromUserEntities 批量转换
func FromUserEntities(users []*userEntity.User) []*UserResponse {
	result := make([]*UserResponse, len(users))
	for i, user := range users {
		result[i] = FromUserEntity(user)
	}
	return result
}

// LoginRequest 登录请求
type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

// RegisterRequest 注册请求
type RegisterRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Name     string `json:"name" binding:"required"`
	Password string `json:"password" binding:"required,min=8"`
}

// LoginResponse 登录响应
type LoginResponse struct {
	User         *UserResponse `json:"user"`
	AccessToken  string        `json:"accessToken"`
	RefreshToken string        `json:"refreshToken"`
}

// TokenResponse Token响应
type TokenResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

// TokenClaims Token声明
type TokenClaims struct {
	UserID  string `json:"userId"`
	Email   string `json:"email"`
	IsAdmin bool   `json:"isAdmin"`
}
