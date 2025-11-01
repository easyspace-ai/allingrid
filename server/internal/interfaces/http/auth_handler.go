package http

import (
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// AuthHandler 认证处理器
type AuthHandler struct {
	authService *application.AuthService
}

// NewAuthHandler 创建认证处理器
func NewAuthHandler(authService *application.AuthService) *AuthHandler {
	return &AuthHandler{
		authService: authService,
	}
}

// Register 用户注册
// @Summary 用户注册
// @Tags Auth
// @Accept json
// @Produce json
// @Param request body dto.RegisterRequest true "注册请求"
// @Success 200 {object} dto.LoginResponse
// @Router /auth/register [post]
func (h *AuthHandler) Register(c *gin.Context) {
	var req dto.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	resp, err := h.authService.Register(c.Request.Context(), req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "注册成功")
}

// Login 用户登录
// @Summary 用户登录
// @Tags Auth
// @Accept json
// @Produce json
// @Param request body dto.LoginRequest true "登录请求"
// @Success 200 {object} dto.LoginResponse
// @Router /auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	var req dto.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	resp, err := h.authService.Login(c.Request.Context(), req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "登录成功")
}

// Logout 用户登出
// @Summary 用户登出
// @Tags Auth
// @Produce json
// @Security BearerAuth
// @Success 200 {object} gin.H
// @Router /auth/logout [post]
func (h *AuthHandler) Logout(c *gin.Context) {
	// 优先从认证中间件获取用户ID
	userID := c.GetString("user_id")

	// 如果没有通过中间件，尝试从 Authorization header 解析 token
	if userID == "" {
		authHeader := c.GetHeader("Authorization")
		if authHeader != "" {
			// 提取 Bearer token
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) == 2 && parts[0] == "Bearer" {
				token := parts[1]
				// 验证 token 并获取用户ID
				claims, err := h.authService.ValidateToken(c.Request.Context(), token)
				if err == nil && claims != nil {
					userID = claims.UserID
				}
			}
		}
	}

	// 如果还是没有用户ID，说明未登录
	if userID == "" {
		// 即使未登录，也返回成功（客户端可能只是想清除本地状态）
		response.Success(c, nil, "登出成功")
		return
	}

	// 执行登出操作
	if err := h.authService.Logout(c.Request.Context(), userID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "登出成功")
}

// RefreshToken 刷新令牌
// @Summary 刷新令牌
// @Tags Auth
// @Accept json
// @Produce json
// @Param request body RefreshTokenRequest true "刷新令牌请求"
// @Success 200 {object} dto.TokenResponse
// @Router /auth/refresh [post]
func (h *AuthHandler) RefreshToken(c *gin.Context) {
	var req struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	resp, err := h.authService.RefreshToken(c.Request.Context(), req.RefreshToken)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "令牌刷新成功")
}

// GetCurrentUser 获取当前登录用户信息
// @Summary 获取当前用户信息
// @Tags Auth
// @Produce json
// @Security BearerAuth
// @Success 200 {object} dto.TokenClaims
// @Router /auth/me [get]
func (h *AuthHandler) GetCurrentUser(c *gin.Context) {
	// 从 Authorization header 获取 token
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("缺少认证信息"))
		return
	}

	// 提取 Bearer token
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("认证格式错误"))
		return
	}

	token := parts[1]

	// 验证 token
	claims, err := h.authService.ValidateToken(c.Request.Context(), token)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, claims, "获取用户信息成功")
}
