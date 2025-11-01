package application

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"

	"gorm.io/gorm"
)

// OrganizationService 组织服务
type OrganizationService struct {
	db *gorm.DB
}

// NewOrganizationService 创建组织服务
func NewOrganizationService(db *gorm.DB) *OrganizationService {
	return &OrganizationService{db: db}
}

// Create 创建组织
func (s *OrganizationService) Create(ctx context.Context, org *models.Organization) error {
	org.ID = utils.GenerateIDWithPrefix("org")
	now := time.Now()
	org.CreatedTime = now
	org.LastModifiedTime = &now

	if org.Status == "" {
		org.Status = "active"
	}

	return s.db.WithContext(ctx).Create(org).Error
}

// GetByID 根据ID获取组织
func (s *OrganizationService) GetByID(ctx context.Context, id string) (*models.Organization, error) {
	var org models.Organization
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&org).Error
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// List 列出组织
func (s *OrganizationService) List(ctx context.Context, page, limit int) ([]*models.Organization, int64, error) {
	var orgs []*models.Organization
	var total int64

	if err := s.db.WithContext(ctx).Model(&models.Organization{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	err := s.db.WithContext(ctx).
		Order("created_time DESC").
		Offset(offset).
		Limit(limit).
		Find(&orgs).Error

	return orgs, total, err
}

// Update 更新组织
func (s *OrganizationService) Update(ctx context.Context, id string, org *models.Organization) error {
	now := time.Now()
	org.LastModifiedTime = &now
	return s.db.WithContext(ctx).
		Model(&models.Organization{}).
		Where("id = ?", id).
		Updates(org).Error
}

// Delete 删除组织
func (s *OrganizationService) Delete(ctx context.Context, id string) error {
	return s.db.WithContext(ctx).Delete(&models.Organization{}, "id = ?", id).Error
}

// GetStats 获取组织统计信息
func (s *OrganizationService) GetStats(ctx context.Context, orgID string) (map[string]interface{}, error) {
	var userCount int64
	var spaceCount int64

	// 统计用户数
	s.db.WithContext(ctx).Model(&models.OrganizationUser{}).
		Where("organization_id = ?", orgID).
		Count(&userCount)

	// 统计空间数（假设有这个关联）
	s.db.WithContext(ctx).Model(&models.Space{}).
		Where("organization_id = ?", orgID).
		Count(&spaceCount)

	return map[string]interface{}{
		"users":   userCount,
		"spaces":  spaceCount,
		"storage": "计算中", // 实现实际的存储统计（需要聚合附件大小）
	}, nil
}

// AddUser 添加组织用户
func (s *OrganizationService) AddUser(ctx context.Context, orgUser *models.OrganizationUser) error {
	orgUser.ID = utils.GenerateIDWithPrefix("ogu")
	orgUser.JoinedTime = time.Now()

	if orgUser.Role == "" {
		orgUser.Role = "member"
	}

	return s.db.WithContext(ctx).Create(orgUser).Error
}

// ListUsers 列出组织用户
func (s *OrganizationService) ListUsers(ctx context.Context, orgID string, page, limit int) ([]*models.OrganizationUser, int64, error) {
	var users []*models.OrganizationUser
	var total int64

	query := s.db.WithContext(ctx).Model(&models.OrganizationUser{}).
		Where("organization_id = ?", orgID)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	err := query.Order("joined_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&users).Error

	return users, total, err
}

// RemoveUser 移除组织用户
func (s *OrganizationService) RemoveUser(ctx context.Context, orgID, userID string) error {
	return s.db.WithContext(ctx).
		Where("organization_id = ? AND user_id = ?", orgID, userID).
		Delete(&models.OrganizationUser{}).Error
}
