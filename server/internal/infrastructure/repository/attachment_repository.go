package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/attachment"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// AttachmentRepositoryImpl 附件仓储实现
type AttachmentRepositoryImpl struct {
	db        *gorm.DB
	tokenRepo attachment.UploadTokenRepository
}

// NewAttachmentRepository 创建附件仓储
func NewAttachmentRepository(db *gorm.DB, tokenRepo attachment.UploadTokenRepository) attachment.Repository {
	return &AttachmentRepositoryImpl{
		db:        db,
		tokenRepo: tokenRepo,
	}
}

// CreateAttachment 创建附件
func (r *AttachmentRepositoryImpl) CreateAttachment(ctx context.Context, item *attachment.AttachmentItem) error {
	// 从上传令牌中获取 tableID, fieldID, recordID, createdBy
	var tableID, fieldID, recordID, createdBy string
	if r.tokenRepo != nil && item.Token != "" {
		token, err := r.tokenRepo.GetUploadToken(ctx, item.Token)
		if err == nil && token != nil {
			tableID = token.TableID
			fieldID = token.FieldID
			recordID = token.RecordID
			createdBy = token.UserID
		}
	}

	dbAttachment := &models.Attachment{
		ID:             item.ID,
		Name:           item.Name,
		Path:           item.Path,
		Token:          item.Token,
		Size:           item.Size,
		MimeType:       item.MimeType,
		PresignedURL:   item.PresignedURL,
		Width:          item.Width,
		Height:         item.Height,
		SmallThumbnail: item.SmallThumbnail,
		LargeThumbnail: item.LargeThumbnail,
		TableID:        tableID,
		FieldID:        fieldID,
		RecordID:       recordID,
		CreatedBy:      createdBy,
		CreatedTime:    item.CreatedTime,
		UpdatedTime:    item.UpdatedTime,
	}

	return r.db.WithContext(ctx).Create(dbAttachment).Error
}

// GetAttachmentByID 通过ID获取附件
func (r *AttachmentRepositoryImpl) GetAttachmentByID(ctx context.Context, id string) (*attachment.AttachmentItem, error) {
	var dbAttachment models.Attachment
	err := r.db.WithContext(ctx).
		Where("id = ? AND deleted_time IS NULL", id).
		First(&dbAttachment).Error

	if err == gorm.ErrRecordNotFound {
		return nil, errors.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get attachment: %w", err)
	}

	return r.toDomainEntity(&dbAttachment), nil
}

// GetAttachmentByToken 通过令牌获取附件
func (r *AttachmentRepositoryImpl) GetAttachmentByToken(ctx context.Context, token string) (*attachment.AttachmentItem, error) {
	var dbAttachment models.Attachment
	err := r.db.WithContext(ctx).
		Where("token = ? AND deleted_time IS NULL", token).
		First(&dbAttachment).Error

	if err == gorm.ErrRecordNotFound {
		return nil, errors.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get attachment: %w", err)
	}

	return r.toDomainEntity(&dbAttachment), nil
}

// GetAttachmentByPath 通过路径获取附件
func (r *AttachmentRepositoryImpl) GetAttachmentByPath(ctx context.Context, path string) (*attachment.AttachmentItem, error) {
	var dbAttachment models.Attachment
	err := r.db.WithContext(ctx).
		Where("path = ? AND deleted_time IS NULL", path).
		First(&dbAttachment).Error

	if err == gorm.ErrRecordNotFound {
		return nil, errors.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get attachment: %w", err)
	}

	return r.toDomainEntity(&dbAttachment), nil
}

// UpdateAttachment 更新附件
func (r *AttachmentRepositoryImpl) UpdateAttachment(ctx context.Context, item *attachment.AttachmentItem) error {
	dbAttachment := &models.Attachment{
		Name:           item.Name,
		Path:           item.Path,
		Token:          item.Token,
		Size:           item.Size,
		MimeType:       item.MimeType,
		PresignedURL:   item.PresignedURL,
		Width:          item.Width,
		Height:         item.Height,
		SmallThumbnail: item.SmallThumbnail,
		LargeThumbnail: item.LargeThumbnail,
		UpdatedTime:    item.UpdatedTime,
	}

	return r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Where("id = ?", item.ID).
		Updates(dbAttachment).Error
}

// DeleteAttachment 删除附件
func (r *AttachmentRepositoryImpl) DeleteAttachment(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Where("id = ?", id).
		Delete(&models.Attachment{}).Error
}

// ListAttachments 列出附件
func (r *AttachmentRepositoryImpl) ListAttachments(ctx context.Context, tableID, fieldID, recordID string) ([]*attachment.AttachmentItem, error) {
	var dbAttachments []models.Attachment
	query := r.db.WithContext(ctx).
		Where("deleted_time IS NULL")

	if tableID != "" {
		query = query.Where("table_id = ?", tableID)
	}
	if fieldID != "" {
		query = query.Where("field_id = ?", fieldID)
	}
	if recordID != "" {
		query = query.Where("record_id = ?", recordID)
	}

	err := query.Find(&dbAttachments).Error
	if err != nil {
		return nil, fmt.Errorf("failed to list attachments: %w", err)
	}

	result := make([]*attachment.AttachmentItem, len(dbAttachments))
	for i, dbAttachment := range dbAttachments {
		result[i] = r.toDomainEntity(&dbAttachment)
	}

	return result, nil
}

// GetAttachmentStats 获取附件统计信息
func (r *AttachmentRepositoryImpl) GetAttachmentStats(ctx context.Context, tableID string) (*attachment.AttachmentStats, error) {
	var stats attachment.AttachmentStats

	// 查询总数
	var totalCount int64
	err := r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Where("table_id = ? AND deleted_time IS NULL", tableID).
		Count(&totalCount).Error
	if err != nil {
		return nil, fmt.Errorf("failed to count attachments: %w", err)
	}
	stats.TotalFiles = totalCount

	// 查询总大小
	var totalSize struct {
		Sum int64
	}
	err = r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Select("COALESCE(SUM(size), 0) as sum").
		Where("table_id = ? AND deleted_time IS NULL", tableID).
		Scan(&totalSize).Error
	if err != nil {
		return nil, fmt.Errorf("failed to sum attachment sizes: %w", err)
	}
	stats.TotalSize = totalSize.Sum

	// 查询各类型文件数量
	var imageCount, videoCount, audioCount, documentCount int64

	// 图片
	r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Where("table_id = ? AND deleted_time IS NULL AND mime_type LIKE 'image/%'", tableID).
		Count(&imageCount)
	stats.ImageFiles = imageCount

	// 视频
	r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Where("table_id = ? AND deleted_time IS NULL AND mime_type LIKE 'video/%'", tableID).
		Count(&videoCount)
	stats.VideoFiles = videoCount

	// 音频
	r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Where("table_id = ? AND deleted_time IS NULL AND mime_type LIKE 'audio/%'", tableID).
		Count(&audioCount)
	stats.AudioFiles = audioCount

	// 文档
	documentTypes := []string{
		"application/pdf",
		"application/msword",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.ms-excel",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/vnd.ms-powerpoint",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"text/plain",
		"text/csv",
	}
	for _, docType := range documentTypes {
		var count int64
		r.db.WithContext(ctx).
			Model(&models.Attachment{}).
			Where("table_id = ? AND deleted_time IS NULL AND mime_type = ?", tableID, docType).
			Count(&count)
		documentCount += count
	}
	stats.DocumentFiles = documentCount

	// 其他文件
	stats.OtherFiles = totalCount - imageCount - videoCount - audioCount - documentCount

	// 查询最后上传时间
	var lastUpload struct {
		CreatedTime time.Time
	}
	r.db.WithContext(ctx).
		Model(&models.Attachment{}).
		Select("MAX(created_time) as created_time").
		Where("table_id = ? AND deleted_time IS NULL", tableID).
		Scan(&lastUpload)
	stats.LastUploaded = lastUpload.CreatedTime

	return &stats, nil
}

// toDomainEntity 转换为领域实体
func (r *AttachmentRepositoryImpl) toDomainEntity(dbAttachment *models.Attachment) *attachment.AttachmentItem {
	return &attachment.AttachmentItem{
		ID:             dbAttachment.ID,
		Name:           dbAttachment.Name,
		Path:           dbAttachment.Path,
		Token:          dbAttachment.Token,
		Size:           dbAttachment.Size,
		MimeType:       dbAttachment.MimeType,
		PresignedURL:   dbAttachment.PresignedURL,
		Width:          dbAttachment.Width,
		Height:         dbAttachment.Height,
		SmallThumbnail: dbAttachment.SmallThumbnail,
		LargeThumbnail: dbAttachment.LargeThumbnail,
		CreatedTime:    dbAttachment.CreatedTime,
		UpdatedTime:    dbAttachment.UpdatedTime,
	}
}

// UploadTokenRepositoryImpl 上传令牌仓储实现
type UploadTokenRepositoryImpl struct {
	db *gorm.DB
}

// NewUploadTokenRepository 创建上传令牌仓储
func NewUploadTokenRepository(db *gorm.DB) attachment.UploadTokenRepository {
	return &UploadTokenRepositoryImpl{db: db}
}

// CreateUploadToken 创建上传令牌
func (r *UploadTokenRepositoryImpl) CreateUploadToken(ctx context.Context, token *attachment.UploadToken) error {
	allowedTypesJSON, err := json.Marshal(token.AllowedTypes)
	if err != nil {
		return fmt.Errorf("failed to marshal allowed types: %w", err)
	}

	allowedTypesStr := string(allowedTypesJSON)

	dbToken := &models.UploadToken{
		Token:        token.Token,
		UserID:       token.UserID,
		TableID:      token.TableID,
		FieldID:      token.FieldID,
		RecordID:     token.RecordID,
		ExpiresAt:    token.ExpiresAt,
		MaxSize:      token.MaxSize,
		AllowedTypes: &allowedTypesStr,
		CreatedTime:  token.CreatedTime,
	}

	return r.db.WithContext(ctx).Create(dbToken).Error
}

// GetUploadToken 获取上传令牌
func (r *UploadTokenRepositoryImpl) GetUploadToken(ctx context.Context, token string) (*attachment.UploadToken, error) {
	var dbToken models.UploadToken
	err := r.db.WithContext(ctx).
		Where("token = ?", token).
		First(&dbToken).Error

	if err == gorm.ErrRecordNotFound {
		return nil, errors.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get upload token: %w", err)
	}

	var allowedTypes []string
	if dbToken.AllowedTypes != nil {
		if err := json.Unmarshal([]byte(*dbToken.AllowedTypes), &allowedTypes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal allowed types: %w", err)
		}
	}

	return &attachment.UploadToken{
		Token:        dbToken.Token,
		UserID:       dbToken.UserID,
		TableID:      dbToken.TableID,
		FieldID:      dbToken.FieldID,
		RecordID:     dbToken.RecordID,
		ExpiresAt:    dbToken.ExpiresAt,
		MaxSize:      dbToken.MaxSize,
		AllowedTypes: allowedTypes,
		CreatedTime:  dbToken.CreatedTime,
	}, nil
}

// DeleteUploadToken 删除上传令牌
func (r *UploadTokenRepositoryImpl) DeleteUploadToken(ctx context.Context, token string) error {
	return r.db.WithContext(ctx).
		Where("token = ?", token).
		Delete(&models.UploadToken{}).Error
}

// CleanupExpiredTokens 清理过期令牌
func (r *UploadTokenRepositoryImpl) CleanupExpiredTokens(ctx context.Context) error {
	return r.db.WithContext(ctx).
		Where("expires_at < ?", time.Now()).
		Delete(&models.UploadToken{}).Error
}

