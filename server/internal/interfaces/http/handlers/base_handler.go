package handlers

import (
	"fmt"
	"mime/multipart"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/internal/interfaces/middleware"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// BaseHandler 基础处理器
type BaseHandler struct {
	logger *zap.Logger
}

// NewBaseHandler 创建基础处理器
func NewBaseHandler(logger *zap.Logger) *BaseHandler {
	if logger == nil {
		logger = zap.L()
	}
	return &BaseHandler{
		logger: logger.Named("handler"),
	}
}

// HandleSuccess 处理成功响应
func (h *BaseHandler) HandleSuccess(c *gin.Context, data interface{}, message ...string) {
	msg := "Success"
	if len(message) > 0 {
		msg = message[0]
	}
	response.SuccessWithMessage(c, data, msg)
}

// HandleCreated 处理创建成功响应
func (h *BaseHandler) HandleCreated(c *gin.Context, data interface{}, message ...string) {
	msg := "Created successfully"
	if len(message) > 0 {
		msg = message[0]
	}
	response.Success(c, data, msg)
}

// HandleError 处理错误响应
func (h *BaseHandler) HandleError(c *gin.Context, err error) {
	// 记录错误
	h.logger.Error("Request failed",
		zap.Error(err),
		zap.String("path", c.Request.URL.Path),
		zap.String("method", c.Request.Method),
	)

	// 检查是否是应用错误
	if appErr, ok := errors.IsAppError(err); ok {
		response.Error(c, appErr)
		return
	}

	// 处理特定错误类型
	switch err {
	case gorm.ErrRecordNotFound:
		response.Error(c, errors.ErrNotFound)
	default:
		response.Error(c, errors.ErrInternalServer)
	}
}

// HandleNotFound 处理资源未找到
func (h *BaseHandler) HandleNotFound(c *gin.Context, resource string) {
	response.Error(c, errors.ErrNotFound.WithDetails(resource+" not found"))
}

// HandleBadRequest 处理错误请求
func (h *BaseHandler) HandleBadRequest(c *gin.Context, message string) {
	response.Error(c, errors.ErrBadRequest.WithDetails(message))
}

// HandleUnauthorized 处理未授权
func (h *BaseHandler) HandleUnauthorized(c *gin.Context, message ...string) {
	msg := "Unauthorized"
	if len(message) > 0 {
		msg = message[0]
	}
	response.Error(c, errors.ErrUnauthorized.WithDetails(msg))
}

// HandleForbidden 处理禁止访问
func (h *BaseHandler) HandleForbidden(c *gin.Context, message ...string) {
	msg := "Forbidden"
	if len(message) > 0 {
		msg = message[0]
	}
	response.Error(c, errors.ErrForbidden.WithDetails(msg))
}

// GetCurrentUser 获取当前用户
func (h *BaseHandler) GetCurrentUser(c *gin.Context) (*models.User, error) {
	return middleware.GetCurrentUser(c)
}

// GetCurrentUserID 获取当前用户ID
func (h *BaseHandler) GetCurrentUserID(c *gin.Context) (string, error) {
	return middleware.GetCurrentUserID(c)
}

// RequireAuth 确保用户已认证
func (h *BaseHandler) RequireAuth(c *gin.Context) (*models.User, error) {
	user, err := h.GetCurrentUser(c)
	if err != nil {
		h.HandleUnauthorized(c)
		return nil, err
	}
	return user, nil
}

// GetPagination 获取分页参数
func (h *BaseHandler) GetPagination(c *gin.Context) (offset, limit int) {
	offsetStr := c.DefaultQuery("offset", "0")
	limitStr := c.DefaultQuery("limit", "20")

	offset, _ = strconv.Atoi(offsetStr)
	limit, _ = strconv.Atoi(limitStr)

	// 限制最大值
	if limit > 100 {
		limit = 100
	}
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	return offset, limit
}

// GetSortParams 获取排序参数
func (h *BaseHandler) GetSortParams(c *gin.Context) (sortBy string, sortOrder string) {
	sortBy = c.DefaultQuery("sort_by", "created_time")
	sortOrder = c.DefaultQuery("sort_order", "desc")

	// 验证排序顺序
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	return sortBy, sortOrder
}

// GetQueryParam 获取查询参数
func (h *BaseHandler) GetQueryParam(c *gin.Context, key string, defaultValue ...string) string {
	value := c.Query(key)
	if value == "" && len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return value
}

// GetPathParam 获取路径参数
func (h *BaseHandler) GetPathParam(c *gin.Context, key string) string {
	return c.Param(key)
}

// ValidateRequest 验证请求
func (h *BaseHandler) ValidateRequest(c *gin.Context, req interface{}) error {
	if err := c.ShouldBindJSON(req); err != nil {
		h.HandleBadRequest(c, "Invalid request body: "+err.Error())
		return err
	}

	// 使用验证器验证
	if v, ok := req.(Validator); ok {
		if err := v.Validate(); err != nil {
			h.HandleBadRequest(c, err.Error())
			return err
		}
	}

	return nil
}

// ValidateQueryParams 验证查询参数
func (h *BaseHandler) ValidateQueryParams(c *gin.Context, req interface{}) error {
	if err := c.ShouldBindQuery(req); err != nil {
		h.HandleBadRequest(c, "Invalid query parameters: "+err.Error())
		return err
	}

	// 使用验证器验证
	if v, ok := req.(Validator); ok {
		if err := v.Validate(); err != nil {
			h.HandleBadRequest(c, err.Error())
			return err
		}
	}

	return nil
}

// Validator 验证器接口
type Validator interface {
	Validate() error
}

// PaginatedResponse 分页响应
type PaginatedResponse struct {
	Data    interface{} `json:"data"`
	Total   int64       `json:"total"`
	Offset  int         `json:"offset"`
	Limit   int         `json:"limit"`
	HasMore bool        `json:"has_more"`
}

// HandlePaginatedSuccess 处理分页成功响应
func (h *BaseHandler) HandlePaginatedSuccess(c *gin.Context, data interface{}, total int64, offset, limit int) {
	hasMore := int64(offset+limit) < total

	resp := PaginatedResponse{
		Data:    data,
		Total:   total,
		Offset:  offset,
		Limit:   limit,
		HasMore: hasMore,
	}

	h.HandleSuccess(c, resp)
}

// WithTransaction 在事务中执行
func (h *BaseHandler) WithTransaction(c *gin.Context, fn func(*gorm.DB) error) error {
	// 这里需要访问数据库连接，可以通过依赖注入获取
	// 简化示例，实际使用时需要从容器获取
	return nil
}

// CheckPermission 检查权限
func (h *BaseHandler) CheckPermission(c *gin.Context, resource, action string) error {
	user, err := h.GetCurrentUser(c)
	if err != nil {
		return err
	}

	// 这里需要调用权限服务检查权限
	// 简化示例
	_ = user
	_ = resource
	_ = action

	return nil
}

// LogActivity 记录活动日志
func (h *BaseHandler) LogActivity(c *gin.Context, action, resource string, metadata map[string]interface{}) {
	userID, _ := h.GetCurrentUserID(c)

	h.logger.Info("User activity",
		zap.String("user_id", userID),
		zap.String("action", action),
		zap.String("resource", resource),
		zap.Any("metadata", metadata),
		zap.String("ip", c.ClientIP()),
		zap.String("user_agent", c.Request.UserAgent()),
	)
}

// HandleFileUpload 处理文件上传
func (h *BaseHandler) HandleFileUpload(c *gin.Context, fieldName string, maxSize int64) (*multipart.FileHeader, error) {
	file, err := c.FormFile(fieldName)
	if err != nil {
		h.HandleBadRequest(c, "No file uploaded")
		return nil, err
	}

	// 检查文件大小
	if file.Size > maxSize {
		h.HandleBadRequest(c, fmt.Sprintf("File size exceeds limit of %d bytes", maxSize))
		return nil, fmt.Errorf("file too large")
	}

	return file, nil
}

// HandleBatchOperation 处理批量操作
func (h *BaseHandler) HandleBatchOperation(c *gin.Context, fn func(ids []string) error) {
	var req struct {
		IDs []string `json:"ids" binding:"required,min=1"`
	}

	if err := h.ValidateRequest(c, &req); err != nil {
		return
	}

	if err := fn(req.IDs); err != nil {
		h.HandleError(c, err)
		return
	}

	h.HandleSuccess(c, map[string]interface{}{
		"affected": len(req.IDs),
	})
}

// CacheKey 生成缓存键
func (h *BaseHandler) CacheKey(prefix string, parts ...string) string {
	key := prefix
	for _, part := range parts {
		key += ":" + part
	}
	return key
}

// SetCache 设置缓存
func (h *BaseHandler) SetCache(c *gin.Context, key string, value interface{}, ttl time.Duration) error {
	// 这里需要访问缓存服务，可以通过依赖注入获取
	// 简化示例
	return nil
}

// GetCache 获取缓存
func (h *BaseHandler) GetCache(c *gin.Context, key string, dest interface{}) error {
	// 这里需要访问缓存服务，可以通过依赖注入获取
	// 简化示例
	return fmt.Errorf("cache miss")
}

// InvalidateCache 使缓存失效
func (h *BaseHandler) InvalidateCache(c *gin.Context, keys ...string) error {
	// 这里需要访问缓存服务，可以通过依赖注入获取
	// 简化示例
	return nil
}
