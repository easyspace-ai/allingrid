package attachment

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// Service 附件服务接口
type Service interface {
	// GenerateSignature 生成上传签名
	GenerateSignature(ctx context.Context, userID string, req *SignatureRequest) (*SignatureResponse, error)
	// UploadFile 上传文件
	UploadFile(ctx context.Context, token string, reader io.Reader, filename string, size int64) error
	// NotifyUpload 通知上传完成
	NotifyUpload(ctx context.Context, token, filename string) (*NotifyResponse, error)
	// ReadFile 读取文件
	ReadFile(ctx context.Context, path, token string) (*ReadResponse, error)
	// DeleteFile 删除文件
	DeleteFile(ctx context.Context, id string) error
	// GetAttachment 获取附件信息
	GetAttachment(ctx context.Context, id string) (*AttachmentItem, error)
	// ListAttachments 列出附件
	ListAttachments(ctx context.Context, tableID, fieldID, recordID string) ([]*AttachmentItem, error)
	// GetAttachmentStats 获取附件统计
	GetAttachmentStats(ctx context.Context, tableID string) (*AttachmentStats, error)
	// CleanupExpiredTokens 清理过期令牌
	CleanupExpiredTokens(ctx context.Context) error
}

// service 附件服务实现
type service struct {
	repo               Repository
	tokenRepo          UploadTokenRepository
	storage            Storage
	thumbnailGenerator ThumbnailGenerator
	validator          FileValidator
	config             *AttachmentStorageConfig
	thumbnailConfig    *ThumbnailConfig
	logger             *zap.Logger
}

// NewService 创建附件服务
func NewService(
	repo Repository,
	tokenRepo UploadTokenRepository,
	storage Storage,
	thumbnailGenerator ThumbnailGenerator,
	validator FileValidator,
	config *AttachmentStorageConfig,
	thumbnailConfig *ThumbnailConfig,
	logger *zap.Logger,
) Service {
	return &service{
		repo:               repo,
		tokenRepo:          tokenRepo,
		storage:            storage,
		thumbnailGenerator: thumbnailGenerator,
		validator:          validator,
		config:             config,
		thumbnailConfig:    thumbnailConfig,
		logger:             logger,
	}
}

// GenerateSignature 生成上传签名
func (s *service) GenerateSignature(ctx context.Context, userID string, req *SignatureRequest) (*SignatureResponse, error) {
	// 设置默认值
	maxSize := req.MaxSize
	if maxSize == 0 {
		maxSize = s.config.MaxFileSize
	}

	allowedTypes := req.AllowedTypes
	if len(allowedTypes) == 0 {
		allowedTypes = s.config.AllowedTypes
	}

	// 创建上传令牌
	uploadToken := NewUploadToken(userID, req.TableID, req.FieldID, req.RecordID, maxSize, allowedTypes)

	// 保存令牌
	if err := s.tokenRepo.CreateUploadToken(ctx, uploadToken); err != nil {
		s.logger.Error("Failed to create upload token",
			logger.String("user_id", userID),
			logger.String("table_id", req.TableID),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to generate signature")
	}

	// 生成上传URL
	uploadURL := fmt.Sprintf("/api/attachments/upload/%s", uploadToken.Token)

	response := &SignatureResponse{
		Token:        uploadToken.Token,
		UploadURL:    uploadURL,
		ExpiresAt:    uploadToken.ExpiresAt.Unix(),
		MaxSize:      maxSize,
		AllowedTypes: allowedTypes,
	}

	s.logger.Info("Upload signature generated",
		logger.String("token", uploadToken.Token),
		logger.String("user_id", userID),
		logger.String("table_id", req.TableID),
	)
	return response, nil
}

// UploadFile 上传文件
func (s *service) UploadFile(ctx context.Context, token string, reader io.Reader, filename string, size int64) error {
	// 获取上传令牌
	uploadToken, err := s.tokenRepo.GetUploadToken(ctx, token)
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrBadRequest.WithDetails("Invalid upload token")
		}
		s.logger.Error("Failed to get upload token",
			logger.String("token", token),
			logger.ErrorField(err),
		)
		return errors.ErrInternalServer.WithDetails("Failed to validate upload token")
	}

	// 检查令牌是否过期
	if uploadToken.IsExpired() {
		return errors.ErrBadRequest.WithDetails("Upload token has expired")
	}

	// 获取文件MIME类型
	mimeType := s.validator.GetMimeType(filename)

	// 验证文件
	if err := s.validator.ValidateFile(ctx, filename, size, mimeType, uploadToken.AllowedTypes, uploadToken.MaxSize); err != nil {
		return err
	}

	// 生成文件路径
	filePath := s.generateFilePath(uploadToken, filename)

	s.logger.Info("UploadFile: uploading file",
		logger.String("token", token),
		logger.String("file_path", filePath),
		logger.String("filename", filename),
		logger.Int64("size", size),
		logger.String("token_created_time", uploadToken.CreatedTime.Format("2006/01/02")),
	)

	// 上传文件到存储
	if err := s.storage.Upload(ctx, filePath, reader, size, mimeType); err != nil {
		s.logger.Error("Failed to upload file to storage",
			logger.String("token", token),
			logger.String("file_path", filePath),
			logger.ErrorField(err),
		)
		return errors.ErrInternalServer.WithDetails("Failed to upload file")
	}

	s.logger.Info("File uploaded successfully",
		logger.String("token", token),
		logger.String("file_path", filePath),
		logger.String("filename", filename),
		logger.Int64("size", size),
	)
	return nil
}

// NotifyUpload 通知上传完成
func (s *service) NotifyUpload(ctx context.Context, token, filename string) (*NotifyResponse, error) {
	// 获取上传令牌
	uploadToken, err := s.tokenRepo.GetUploadToken(ctx, token)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrBadRequest.WithDetails("Invalid upload token")
		}
		s.logger.Error("Failed to get upload token",
			logger.String("token", token),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to validate upload token")
	}

	// 检查令牌是否过期
	if uploadToken.IsExpired() {
		return nil, errors.ErrBadRequest.WithDetails("Upload token has expired")
	}

	// 生成文件路径
	filePath := s.generateFilePath(uploadToken, filename)

	s.logger.Info("NotifyUpload: checking file existence",
		logger.String("token", token),
		logger.String("file_path", filePath),
		logger.String("filename", filename),
		logger.String("token_created_time", uploadToken.CreatedTime.Format("2006/01/02")),
	)

	// 检查文件是否存在
	exists, err := s.storage.Exists(ctx, filePath)
	if err != nil {
		s.logger.Error("Failed to check file existence",
			logger.String("token", token),
			logger.String("file_path", filePath),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to check file")
	}
	if !exists {
		return nil, errors.ErrNotFound.WithDetails("File not found")
	}

	// 获取文件大小和元数据
	fileSize, err := s.storage.GetSize(ctx, filePath)
	if err != nil {
		s.logger.Error("Failed to get file size",
			logger.String("token", token),
			logger.String("file_path", filePath),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to get file size")
	}

	metadata, err := s.storage.GetMetadata(ctx, filePath)
	if err != nil {
		s.logger.Error("Failed to get file metadata",
			logger.String("token", token),
			logger.String("file_path", filePath),
			logger.ErrorField(err),
		)
		// 继续处理，使用默认值
		metadata = make(map[string]string)
	}

	// 获取MIME类型
	mimeType := metadata["content-type"]
	if mimeType == "" {
		mimeType = s.validator.GetMimeType(filename)
	}

	// 创建附件项
	attachment := NewAttachmentItem(filename, filePath, token, mimeType, fileSize)

	// 如果是图片，生成缩略图
	if s.thumbnailGenerator != nil && s.thumbnailGenerator.IsSupported(mimeType) {
		thumbnails, err := s.generateThumbnails(ctx, filePath, attachment.ID)
		if err != nil {
			s.logger.Warn("Failed to generate thumbnails",
				logger.String("file_path", filePath),
				logger.ErrorField(err),
			)
		} else {
			if smallThumb, ok := thumbnails["small"]; ok {
				attachment.SmallThumbnail = &smallThumb
			}
			if largeThumb, ok := thumbnails["large"]; ok {
				attachment.LargeThumbnail = &largeThumb
			}
		}
	}

	// 生成预签名URL
	if presignedURL, err := s.storage.GetURL(ctx, filePath, 24*time.Hour); err == nil {
		attachment.SetPresignedURL(presignedURL)
	}

	// 保存附件信息
	if err := s.repo.CreateAttachment(ctx, attachment); err != nil {
		s.logger.Error("Failed to create attachment record",
			logger.String("token", token),
			logger.String("attachment_id", attachment.ID),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to save attachment")
	}

	// 删除上传令牌
	if err := s.tokenRepo.DeleteUploadToken(ctx, token); err != nil {
		s.logger.Warn("Failed to delete upload token",
			logger.String("token", token),
			logger.ErrorField(err),
		)
	}

	response := &NotifyResponse{
		Attachment: attachment,
		Success:    true,
		Message:    "File uploaded successfully",
	}

	s.logger.Info("Upload notification processed",
		logger.String("token", token),
		logger.String("attachment_id", attachment.ID),
		logger.String("filename", filename),
	)
	return response, nil
}

// ReadFile 读取文件
func (s *service) ReadFile(ctx context.Context, path, token string) (*ReadResponse, error) {
	// 获取附件信息
	attachment, err := s.repo.GetAttachmentByPath(ctx, path)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrNotFound.WithDetails("File not found")
		}
		s.logger.Error("Failed to get attachment by path",
			logger.String("path", path),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to get file")
	}

	// 检查文件是否存在
	exists, err := s.storage.Exists(ctx, path)
	if err != nil {
		s.logger.Error("Failed to check file existence",
			logger.String("path", path),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to check file")
	}
	if !exists {
		return nil, errors.ErrNotFound.WithDetails("File not found")
	}

	// 读取文件
	reader, err := s.storage.Download(ctx, path)
	if err != nil {
		s.logger.Error("Failed to download file",
			logger.String("path", path),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to read file")
	}
	defer reader.Close()

	// 读取文件内容
	data, err := io.ReadAll(reader)
	if err != nil {
		s.logger.Error("Failed to read file content",
			logger.String("path", path),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to read file content")
	}

	// 设置响应头
	headers := map[string]string{
		"Content-Type":        attachment.MimeType,
		"Content-Length":      fmt.Sprintf("%d", len(data)),
		"Cache-Control":       "public, max-age=31536000",
		"Content-Disposition": fmt.Sprintf("inline; filename=\"%s\"", attachment.Name),
	}

	response := &ReadResponse{
		Data:     data,
		Headers:  headers,
		MimeType: attachment.MimeType,
		Size:     int64(len(data)),
	}

	return response, nil
}

// DeleteFile 删除文件
func (s *service) DeleteFile(ctx context.Context, id string) error {
	// 获取附件信息
	attachment, err := s.repo.GetAttachmentByID(ctx, id)
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrNotFound.WithDetails("Attachment not found")
		}
		s.logger.Error("Failed to get attachment by ID",
			logger.String("id", id),
			logger.ErrorField(err),
		)
		return errors.ErrInternalServer.WithDetails("Failed to get attachment")
	}

	// 删除存储中的文件
	if err := s.storage.Delete(ctx, attachment.Path); err != nil {
		s.logger.Error("Failed to delete file from storage",
			logger.String("id", id),
			logger.String("path", attachment.Path),
			logger.ErrorField(err),
		)
		// 继续删除数据库记录
	}

	// 删除缩略图
	if attachment.SmallThumbnail != nil {
		s.storage.Delete(ctx, *attachment.SmallThumbnail)
	}
	if attachment.LargeThumbnail != nil {
		s.storage.Delete(ctx, *attachment.LargeThumbnail)
	}

	// 删除数据库记录
	if err := s.repo.DeleteAttachment(ctx, id); err != nil {
		s.logger.Error("Failed to delete attachment record",
			logger.String("id", id),
			logger.ErrorField(err),
		)
		return errors.ErrInternalServer.WithDetails("Failed to delete attachment")
	}

	s.logger.Info("File deleted successfully",
		logger.String("id", id),
		logger.String("path", attachment.Path),
	)
	return nil
}

// GetAttachment 获取附件信息
func (s *service) GetAttachment(ctx context.Context, id string) (*AttachmentItem, error) {
	attachment, err := s.repo.GetAttachmentByID(ctx, id)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrNotFound.WithDetails("Attachment not found")
		}
		s.logger.Error("Failed to get attachment by ID",
			logger.String("id", id),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to get attachment")
	}
	return attachment, nil
}

// ListAttachments 列出附件
func (s *service) ListAttachments(ctx context.Context, tableID, fieldID, recordID string) ([]*AttachmentItem, error) {
	attachments, err := s.repo.ListAttachments(ctx, tableID, fieldID, recordID)
	if err != nil {
		s.logger.Error("Failed to list attachments",
			logger.String("table_id", tableID),
			logger.String("field_id", fieldID),
			logger.String("record_id", recordID),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to list attachments")
	}
	return attachments, nil
}

// GetAttachmentStats 获取附件统计
func (s *service) GetAttachmentStats(ctx context.Context, tableID string) (*AttachmentStats, error) {
	stats, err := s.repo.GetAttachmentStats(ctx, tableID)
	if err != nil {
		s.logger.Error("Failed to get attachment stats",
			logger.String("table_id", tableID),
			logger.ErrorField(err),
		)
		return nil, errors.ErrInternalServer.WithDetails("Failed to get attachment stats")
	}
	return stats, nil
}

// CleanupExpiredTokens 清理过期令牌
func (s *service) CleanupExpiredTokens(ctx context.Context) error {
	if err := s.tokenRepo.CleanupExpiredTokens(ctx); err != nil {
		s.logger.Error("Failed to cleanup expired tokens", logger.ErrorField(err))
		return err
	}
	return nil
}

// generateFilePath 生成文件路径
func (s *service) generateFilePath(token *UploadToken, filename string) string {
	// 使用 token 的创建时间来生成日期路径，确保同一 token 的所有调用都生成相同的路径
	// 这样可以避免 UploadFile 和 NotifyUpload 使用不同时间导致路径不一致的问题
	datePath := token.CreatedTime.Format("2006/01/02")

	// 生成唯一文件名
	// 如果 filename 为空，使用 token 的前8位作为文件名
	if filename == "" {
		filename = fmt.Sprintf("file_%s", token.Token[:8])
	}
	
	ext := filepath.Ext(filename)
	name := strings.TrimSuffix(filename, ext)
	uniqueName := fmt.Sprintf("%s_%s%s", name, token.Token[:8], ext)

	return fmt.Sprintf("attachments/%s/%s/%s/%s", token.TableID, token.FieldID, datePath, uniqueName)
}

// generateThumbnails 生成缩略图
func (s *service) generateThumbnails(ctx context.Context, sourcePath, attachmentID string) (map[string]string, error) {
	if s.thumbnailConfig == nil || !s.thumbnailConfig.Enabled {
		return nil, fmt.Errorf("thumbnail generation is disabled")
	}

	thumbnails := make(map[string]string)

	// 生成小缩略图
	smallPath := fmt.Sprintf("thumbnails/small/%s.jpg", attachmentID)
	if err := s.thumbnailGenerator.GenerateThumbnail(ctx, sourcePath, smallPath, s.thumbnailConfig.SmallWidth, s.thumbnailConfig.SmallHeight, s.thumbnailConfig.Quality); err != nil {
		return nil, fmt.Errorf("failed to generate small thumbnail: %w", err)
	}
	thumbnails["small"] = smallPath

	// 生成大缩略图
	largePath := fmt.Sprintf("thumbnails/large/%s.jpg", attachmentID)
	if err := s.thumbnailGenerator.GenerateThumbnail(ctx, sourcePath, largePath, s.thumbnailConfig.LargeWidth, s.thumbnailConfig.LargeHeight, s.thumbnailConfig.Quality); err != nil {
		return nil, fmt.Errorf("failed to generate large thumbnail: %w", err)
	}
	thumbnails["large"] = largePath

	return thumbnails, nil
}
