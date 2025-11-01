package application

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"

	"gorm.io/gorm"
)

// LicenseService 许可证服务
type LicenseService struct {
	db *gorm.DB
}

// NewLicenseService 创建许可证服务
func NewLicenseService(db *gorm.DB) *LicenseService {
	return &LicenseService{db: db}
}

// Create 创建许可证
func (s *LicenseService) Create(ctx context.Context, license *models.License) error {
	license.ID = utils.GenerateIDWithPrefix("lic")
	now := time.Now()
	license.CreatedTime = now
	license.LastModifiedTime = &now

	if license.Status == "" {
		license.Status = "active"
	}

	return s.db.WithContext(ctx).Create(license).Error
}

// GetByID 根据ID获取许可证
func (s *LicenseService) GetByID(ctx context.Context, id string) (*models.License, error) {
	var license models.License
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&license).Error
	if err != nil {
		return nil, err
	}
	return &license, nil
}

// List 列出许可证
func (s *LicenseService) List(ctx context.Context, page, limit int) ([]*models.License, int64, error) {
	var licenses []*models.License
	var total int64

	if err := s.db.WithContext(ctx).Model(&models.License{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	err := s.db.WithContext(ctx).
		Order("created_time DESC").
		Offset(offset).
		Limit(limit).
		Find(&licenses).Error

	return licenses, total, err
}

// Update 更新许可证
func (s *LicenseService) Update(ctx context.Context, id string, license *models.License) error {
	now := time.Now()
	license.LastModifiedTime = &now
	return s.db.WithContext(ctx).
		Model(&models.License{}).
		Where("id = ?", id).
		Updates(license).Error
}

// Delete 删除许可证
func (s *LicenseService) Delete(ctx context.Context, id string) error {
	return s.db.WithContext(ctx).Delete(&models.License{}, "id = ?", id).Error
}

// Validate 验证许可证
func (s *LicenseService) Validate(ctx context.Context, licenseKey string) (bool, error) {
	var license models.License
	err := s.db.WithContext(ctx).
		Where("license_key = ? AND status = ?", licenseKey, "active").
		First(&license).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}

	// 简化处理：许可证验证逻辑
	// 实际应该检查更多条件（如用户数限制、功能限制等）

	return true, nil
}

// Activate 激活许可证
func (s *LicenseService) Activate(ctx context.Context, licenseKey, userID string) error {
	return s.db.WithContext(ctx).
		Model(&models.License{}).
		Where("license_key = ?", licenseKey).
		Updates(map[string]interface{}{
			"status":       "active",
			"activated_by": userID,
			"activated_at": time.Now(),
		}).Error
}

// Deactivate 停用许可证
func (s *LicenseService) Deactivate(ctx context.Context, licenseKey string) error {
	return s.db.WithContext(ctx).
		Model(&models.License{}).
		Where("license_key = ?", licenseKey).
		Update("status", "inactive").Error
}

// GetUsage 获取许可证使用情况
func (s *LicenseService) GetUsage(ctx context.Context, licenseID string) (map[string]interface{}, error) {
	var license models.License
	err := s.db.WithContext(ctx).Where("id = ?", licenseID).First(&license).Error
	if err != nil {
		return nil, err
	}

	// 简化的使用情况统计
	return map[string]interface{}{
		"license_id":  licenseID,
		"max_users":   license.MaxUsers,
		"max_spaces":  license.MaxSpaces,
		"max_storage": license.MaxStorage,
		"status":      license.Status,
		"is_active":   license.IsActive,
	}, nil
}
