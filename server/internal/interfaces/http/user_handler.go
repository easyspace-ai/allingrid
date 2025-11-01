package http

import (
	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// UserHandler 用户处理器
type UserHandler struct {
	userService *application.UserService
}

// NewUserHandler 创建用户处理器
func NewUserHandler(userService *application.UserService) *UserHandler {
	return &UserHandler{
		userService: userService,
	}
}

// CreateUser 创建用户
// @Summary 创建用户
// @Tags User
// @Accept json
// @Produce json
// @Param request body dto.CreateUserRequest true "创建用户请求"
// @Success 200 {object} dto.UserResponse
// @Router /users [post]
func (h *UserHandler) CreateUser(c *gin.Context) {
	var req dto.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	user, err := h.userService.CreateUser(c.Request.Context(), req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, user, "创建用户成功")
}

// GetUser 获取用户信息
// @Summary 获取用户信息
// @Tags User
// @Produce json
// @Param id path string true "用户ID"
// @Success 200 {object} dto.UserResponse
// @Router /users/{id} [get]
func (h *UserHandler) GetUser(c *gin.Context) {
	userID := c.Param("id")

	user, err := h.userService.GetUser(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, user, "获取用户信息成功")
}

// UpdateUser 更新用户
// @Summary 更新用户
// @Tags User
// @Accept json
// @Produce json
// @Param id path string true "用户ID"
// @Param request body dto.UpdateUserRequest true "更新用户请求"
// @Success 200 {object} dto.UserResponse
// @Router /users/{id} [put]
func (h *UserHandler) UpdateUser(c *gin.Context) {
	userID := c.Param("id")

	var req dto.UpdateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	user, err := h.userService.UpdateUser(c.Request.Context(), userID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, user, "更新用户成功")
}

// DeleteUser 删除用户
// @Summary 删除用户
// @Tags User
// @Produce json
// @Param id path string true "用户ID"
// @Success 200 {object} gin.H
// @Router /users/{id} [delete]
func (h *UserHandler) DeleteUser(c *gin.Context) {
	userID := c.Param("id")

	if err := h.userService.DeleteUser(c.Request.Context(), userID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除用户成功")
}

// ListUsers 用户列表
// @Summary 用户列表
// @Tags User
// @Produce json
// @Param email query string false "邮箱过滤"
// @Param name query string false "名称过滤"
// @Param page query int false "页码"
// @Param page_size query int false "每页数量"
// @Success 200 {object} dto.UserListResponse
// @Router /users [get]
func (h *UserHandler) ListUsers(c *gin.Context) {
	var req dto.UserListFilter

	// 解析查询参数
	if err := c.ShouldBindQuery(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 设置默认分页
	if req.Limit == 0 {
		req.Limit = 20
	}

	users, err := h.userService.ListUsers(c.Request.Context(), req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, users, "获取用户列表成功")
}

// ChangePassword 修改密码
// @Summary 修改密码
// @Tags User
// @Accept json
// @Produce json
// @Param id path string true "用户ID"
// @Param request body ChangePasswordRequest true "修改密码请求"
// @Success 200 {object} gin.H
// @Router /users/{id}/password [put]
func (h *UserHandler) ChangePassword(c *gin.Context) {
	userID := c.Param("id")

	var req struct {
		OldPassword string `json:"old_password" binding:"required"`
		NewPassword string `json:"new_password" binding:"required,min=8"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.userService.ChangePassword(c.Request.Context(), userID, req.OldPassword, req.NewPassword); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "密码修改成功")
}
