package http

import (
	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/authctx"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// BaseHandler Base HTTP处理器（严格遵守API标准）
type BaseHandler struct {
	service      *application.BaseService
	tableService *application.TableService
}

// NewBaseHandler 创建Base处理器
func NewBaseHandler(service *application.BaseService, tableService *application.TableService) *BaseHandler {
	return &BaseHandler{
		service:      service,
		tableService: tableService,
	}
}

// CreateBase 创建Base
// POST /api/v1/spaces/:spaceId/bases
// ✅ 严格使用 response.Success
// ✅ 严格使用 errors.ErrXxx
func (h *BaseHandler) CreateBase(c *gin.Context) {
	spaceID := c.Param("spaceId")

	// 1. 参数绑定
	var req dto.CreateBaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 设置SpaceID
	req.SpaceID = spaceID

	// 2. 获取用户ID（从request context中获取）
	userID, exists := authctx.UserFrom(c.Request.Context())
	if !exists {
		response.Error(c, errors.ErrUnauthorized)
		return
	}

	// 3. 调用Service
	base, err := h.service.CreateBase(c.Request.Context(), req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 4. 返回成功响应（✅ 严格使用response.Success）
	response.Success(c, base, "创建Base成功")
}

// GetBase 获取Base详情
// GET /api/v1/bases/:baseId
// ✅ 严格使用 response.Success
func (h *BaseHandler) GetBase(c *gin.Context) {
	baseID := c.Param("baseId")

	// 调用Service
	base, err := h.service.GetBase(c.Request.Context(), baseID)
	if err != nil {
		response.Error(c, err)
		return
	}

	// ✅ 严格使用response.Success
	response.Success(c, base, "获取Base成功")
}

// ListBases 获取Base列表
// GET /api/v1/spaces/:spaceId/bases
// ✅ Base列表不分页，返回数组
func (h *BaseHandler) ListBases(c *gin.Context) {
	spaceID := c.Param("spaceId")

	// 调用Service
	bases, err := h.service.ListBases(c.Request.Context(), spaceID)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 返回数组
	response.Success(c, bases, "获取Base列表成功")
}

// UpdateBase 更新Base
// PATCH /api/v1/bases/:baseId
// ✅ 严格使用 response.Success
func (h *BaseHandler) UpdateBase(c *gin.Context) {
	baseID := c.Param("baseId")

	// 1. 参数绑定
	var req dto.UpdateBaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 2. 调用Service
	base, err := h.service.UpdateBase(c.Request.Context(), baseID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 3. ✅ 严格使用response.Success
	response.Success(c, base, "更新Base成功")
}

// DeleteBase 删除Base
// DELETE /api/v1/bases/:baseId
// ✅ 严格使用 response.Success，data为nil
func (h *BaseHandler) DeleteBase(c *gin.Context) {
	baseID := c.Param("baseId")

	// 调用Service
	if err := h.service.DeleteBase(c.Request.Context(), baseID); err != nil {
		response.Error(c, err)
		return
	}

	// ✅ 严格使用response.Success，删除成功data为nil
	response.Success(c, nil, "删除Base成功")
}

// GetBaseTables 获取Base下的所有Table
// GET /api/v1/bases/:baseId/tables
// ✅ 严格使用 response.Success
func (h *BaseHandler) GetBaseTables(c *gin.Context) {
	baseID := c.Param("baseId")

	// 调用TableService获取该Base下的所有Table
	tables, err := h.tableService.ListTables(c.Request.Context(), baseID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, tables, "获取Table列表成功")
}

// DuplicateBase 复制Base
// POST /api/v1/bases/:baseId/duplicate
// ✅ 严格使用 response.Success
//
// TODO: 需要实现Base复制功能
// 实现步骤：
//  1. 在BaseService中添加DuplicateBase方法
//  2. 复制Base的基本信息（名称、描述等）
//  3. 复制Base下的所有Table（调用TableService）
//  4. 复制每个Table下的Fields（调用FieldService）
//  5. 可选：复制Records数据
func (h *BaseHandler) DuplicateBase(c *gin.Context) {
	baseID := c.Param("baseId")

	// 1. 参数绑定
	var req dto.DuplicateBaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 2. 获取用户ID
	_, exists := authctx.UserFrom(c.Request.Context())
	if !exists {
		response.Error(c, errors.ErrUnauthorized)
		return
	}

	// 3. 调用服务
	result, err := h.service.DuplicateBase(c.Request.Context(), baseID, &req)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 4. 返回成功响应
	response.Success(c, result, "Base复制成功")
}

// GetBaseCollaborators 获取Base协作者列表
// GET /api/v1/bases/:baseId/collaborators
// ✅ 严格使用 response.Success
//
// TODO: 需要实现协作者管理功能
// 实现步骤：
//  1. 创建CollaboratorService
//  2. 创建BaseCollaborator实体和Repository
//  3. 实现GetCollaborators方法返回协作者列表
//  4. 返回字段：userId, userName, userEmail, role, joinedAt
func (h *BaseHandler) GetBaseCollaborators(c *gin.Context) {
	baseID := c.Param("baseId")

	// TODO: 调用CollaboratorService.GetCollaborators(ctx, baseID)
	_ = baseID

	// 暂时返回空列表，等待CollaboratorService实现
	collaborators := []interface{}{}
	response.Success(c, collaborators, "获取协作者列表成功")
}

// AddBaseCollaborator 添加Base协作者
// POST /api/v1/bases/:baseId/collaborators
// ✅ 严格使用 response.Success
//
// TODO: 需要实现协作者管理功能
// 实现步骤：
//  1. 验证用户是否有权限添加协作者（需要是owner或admin）
//  2. 验证目标用户是否存在
//  3. 检查用户是否已经是协作者
//  4. 调用CollaboratorService.AddCollaborator
//  5. 可选：发送邮件通知
func (h *BaseHandler) AddBaseCollaborator(c *gin.Context) {
	baseID := c.Param("baseId")

	// 1. 参数绑定
	var req struct {
		UserID string `json:"userId" binding:"required"`
		Role   string `json:"role" binding:"required,oneof=viewer editor admin"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// TODO: 调用CollaboratorService.AddCollaborator(ctx, baseID, req.UserID, req.Role)
	_ = baseID
	_ = req
	response.Error(c, errors.ErrNotImplemented.WithDetails("添加协作者功能正在开发中"))
}

// GetBasePermission 获取当前用户对Base的权限
// GET /api/v1/bases/:baseId/permission
// ✅ 严格使用 response.Success
func (h *BaseHandler) GetBasePermission(c *gin.Context) {
	baseID := c.Param("baseId")

	userID, exists := authctx.UserFrom(c.Request.Context())
	if !exists {
		response.Error(c, errors.ErrUnauthorized)
		return
	}

	// TODO: 查询用户对该Base的权限
	// 暂时返回owner权限
	permission := map[string]interface{}{
		"baseId":    baseID,
		"userId":    userID,
		"role":      "owner",
		"canRead":   true,
		"canWrite":  true,
		"canDelete": true,
		"canManage": true,
	}

	response.Success(c, permission, "获取权限成功")
}

