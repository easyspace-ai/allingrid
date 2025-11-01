package http

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// LicenseHandler 许可证处理器
type LicenseHandler struct {
	service *application.LicenseService
}

// NewLicenseHandler 创建许可证处理器
func NewLicenseHandler(service *application.LicenseService) *LicenseHandler {
	return &LicenseHandler{
		service: service,
	}
}

// CreateLicense 创建许可证
func (h *LicenseHandler) CreateLicense(c *gin.Context) {
	var license models.License
	if err := c.ShouldBindJSON(&license); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 从上下文中获取用户ID
	userID, exists := c.Get("user_id")
	if !exists {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	license.CreatedBy = userID.(string)

	// 调用服务创建许可证
	if err := h.service.Create(c.Request.Context(), &license); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, license, "创建成功")
}

// GetLicense 获取许可证
func (h *LicenseHandler) GetLicense(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("License ID is required"))
		return
	}

	// 调用服务获取许可证
	license, err := h.service.GetByID(c.Request.Context(), id)
	if err != nil {
		response.Error(c, errors.ErrNotFound)
		return
	}

	response.Success(c, license, "操作成功")
}

// ListLicenses 列出许可证
func (h *LicenseHandler) ListLicenses(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	// 调用服务获取列表
	licenses, total, err := h.service.List(c.Request.Context(), page, limit)
	if err != nil {
		response.Error(c, err)
		return
	}

	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      int(total),
		TotalPages: (int(total) + limit - 1) / limit,
	}
	response.PaginatedSuccess(c, licenses, pagination, "获取许可证列表成功")
}

// UpdateLicense 更新许可证
func (h *LicenseHandler) UpdateLicense(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	var license models.License
	if err := c.ShouldBindJSON(&license); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	license.ID = id

	// 调用服务更新许可证
	if err := h.service.Update(c.Request.Context(), id, &license); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, license, "操作成功")
}

// DeleteLicense 删除许可证
func (h *LicenseHandler) DeleteLicense(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务删除许可证
	if err := h.service.Delete(c.Request.Context(), id); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除成功")
}

// CreateLicenseCustomer 创建许可证客户
func (h *LicenseHandler) CreateLicenseCustomer(c *gin.Context) {
	licenseID := c.Param("license_id")
	if licenseID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	var customer models.LicenseCustomer
	if err := c.ShouldBindJSON(&customer); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	customer.LicenseID = licenseID

	// 简化实现：返回创建成功（实际应该有 CustomerService）
	response.Success(c, customer, "License customer created (simplified implementation)")
}

// ListLicenseCustomers 列出许可证客户
func (h *LicenseHandler) ListLicenseCustomers(c *gin.Context) {
	licenseID := c.Param("license_id")
	if licenseID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 简化实现：返回空列表（实际应该有 CustomerService）
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      0,
		TotalPages: 0,
	}
	response.PaginatedSuccess(c, []models.LicenseCustomer{}, pagination, "Simplified implementation")
}

// GetLicenseStats 获取许可证统计信息
func (h *LicenseHandler) GetLicenseStats(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务获取使用情况
	usage, err := h.service.GetUsage(c.Request.Context(), id)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, usage, "操作成功")
}

// CreateEnterpriseLicense 创建企业许可证
func (h *LicenseHandler) CreateEnterpriseLicense(c *gin.Context) {
	var license models.EnterpriseLicense
	if err := c.ShouldBindJSON(&license); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 从上下文中获取用户ID
	userID, exists := c.Get("user_id")
	if !exists {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	license.CreatedBy = userID.(string)

	// 简化实现：返回创建成功（EnterpriseLicense 可能需要专门的服务）
	response.Success(c, license, "Enterprise license created (simplified implementation)")
}
