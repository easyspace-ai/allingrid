package attachment

import (
	"context"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strings"
	"time"
)

// AttachmentFieldHandler 附件字段处理器
type AttachmentFieldHandler struct {
	storageProvider  StorageProvider
	thumbnailService ThumbnailService
	validator        *AttachmentValidator
	config           *AttachmentFieldConfig
}

// AttachmentFieldConfig 附件字段配置
type AttachmentFieldConfig struct {
	MaxFileSize       int64    `json:"max_file_size"`      // 最大文件大小（字节）
	MaxFileCount      int      `json:"max_file_count"`     // 最大文件数量
	AllowedTypes      []string `json:"allowed_types"`      // 允许的文件类型
	AllowedExtensions []string `json:"allowed_extensions"` // 允许的文件扩展名
	EnableThumbnail   bool     `json:"enable_thumbnail"`   // 是否启用缩略图
	EnablePreview     bool     `json:"enable_preview"`     // 是否启用预览
	EnableVersioning  bool     `json:"enable_versioning"`  // 是否启用版本控制
	StoragePath       string   `json:"storage_path"`       // 存储路径模板
	CDNEnabled        bool     `json:"cdn_enabled"`        // 是否启用CDN
	CDNBaseURL        string   `json:"cdn_base_url"`       // CDN基础URL
}

// NewAttachmentFieldHandler 创建附件字段处理器
func NewAttachmentFieldHandler(
	storageProvider StorageProvider,
	thumbnailService ThumbnailService,
	config *AttachmentFieldConfig,
) *AttachmentFieldHandler {
	return &AttachmentFieldHandler{
		storageProvider:  storageProvider,
		thumbnailService: thumbnailService,
		validator:        NewAttachmentValidator(config),
		config:           config,
	}
}

// ProcessUpload 处理文件上传
func (h *AttachmentFieldHandler) ProcessUpload(ctx context.Context, request *ProcessUploadRequest) (*AttachmentItem, error) {
	// 验证上传请求
	if err := h.validator.ValidateUpload(request); err != nil {
		return nil, fmt.Errorf("上传验证失败: %w", err)
	}

	// 生成存储路径
	storagePath, err := h.generateStoragePath(request)
	if err != nil {
		return nil, fmt.Errorf("生成存储路径失败: %w", err)
	}

	// 上传文件到存储
	uploadRequest := UploadRequest{
		Path:        storagePath,
		Reader:      request.FileReader,
		Size:        request.FileSize,
		ContentType: request.ContentType,
		Metadata: map[string]string{
			"table_id":      request.TableID,
			"field_id":      request.FieldID,
			"record_id":     request.RecordID,
			"uploaded_by":   request.UploadedBy,
			"original_name": request.FileName,
		},
		Options: UploadOptions{
			Overwrite:   false,
			CreateDir:   true,
			Permissions: "0644",
		},
	}

	uploadResult, err := h.storageProvider.Upload(ctx, uploadRequest)
	if err != nil {
		return nil, fmt.Errorf("文件上传失败: %w", err)
	}

	// 创建附件项
	attachment := &AttachmentItem{
		ID:          generateAttachmentID(),
		Name:        request.FileName,
		Path:        uploadResult.Path,
		Token:       generateAttachmentToken(),
		Size:        uploadResult.Size,
		MimeType:    uploadResult.ContentType,
		CreatedTime: uploadResult.UploadedAt,
		UpdatedTime: uploadResult.UploadedAt,
	}

	// 处理图片附件
	if attachment.IsImage() {
		if err := h.processImageAttachment(ctx, attachment); err != nil {
			// 记录错误但不影响主流程
			// logger.Warn("处理图片附件失败", logger.ErrorField(err))
		}
	}

	// 生成访问URL
	if err := h.generateAccessURL(ctx, attachment); err != nil {
		// 记录错误但不影响主流程
		// logger.Warn("生成访问URL失败", logger.ErrorField(err))
	}

	return attachment, nil
}

// ProcessDelete 处理文件删除
func (h *AttachmentFieldHandler) ProcessDelete(ctx context.Context, attachment *AttachmentItem) error {
	// 删除主文件
	if err := h.storageProvider.Delete(ctx, attachment.Path); err != nil {
		return fmt.Errorf("删除主文件失败: %w", err)
	}

	// 删除缩略图
	if attachment.SmallThumbnail != nil {
		h.storageProvider.Delete(ctx, *attachment.SmallThumbnail)
	}
	if attachment.LargeThumbnail != nil {
		h.storageProvider.Delete(ctx, *attachment.LargeThumbnail)
	}

	return nil
}

// ProcessBatchUpload 处理批量上传
func (h *AttachmentFieldHandler) ProcessBatchUpload(ctx context.Context, requests []*ProcessUploadRequest) ([]*AttachmentItem, error) {
	var attachments []*AttachmentItem
	var errors []error

	for _, request := range requests {
		attachment, err := h.ProcessUpload(ctx, request)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		attachments = append(attachments, attachment)
	}

	if len(errors) > 0 {
		return attachments, fmt.Errorf("批量上传部分失败: %d个错误", len(errors))
	}

	return attachments, nil
}

// ProcessBatchDelete 处理批量删除
func (h *AttachmentFieldHandler) ProcessBatchDelete(ctx context.Context, attachments []*AttachmentItem) error {
	var errors []error

	for _, attachment := range attachments {
		if err := h.ProcessDelete(ctx, attachment); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("批量删除部分失败: %d个错误", len(errors))
	}

	return nil
}

// GetAttachmentURL 获取附件访问URL
func (h *AttachmentFieldHandler) GetAttachmentURL(ctx context.Context, attachment *AttachmentItem, options URLOptions) (string, error) {
	if h.config.CDNEnabled && h.config.CDNBaseURL != "" {
		return fmt.Sprintf("%s/%s", h.config.CDNBaseURL, attachment.Path), nil
	}

	return h.storageProvider.GetURL(ctx, attachment.Path, options)
}

// GetThumbnailURL 获取缩略图URL
func (h *AttachmentFieldHandler) GetThumbnailURL(ctx context.Context, attachment *AttachmentItem, size ThumbnailSize) (string, error) {
	if !attachment.IsImage() {
		return "", fmt.Errorf("非图片文件不支持缩略图")
	}

	var thumbnailPath string
	switch size {
	case ThumbnailSizeSmall:
		if attachment.SmallThumbnail == nil {
			return "", fmt.Errorf("小缩略图不存在")
		}
		thumbnailPath = *attachment.SmallThumbnail
	case ThumbnailSizeLarge:
		if attachment.LargeThumbnail == nil {
			return "", fmt.Errorf("大缩略图不存在")
		}
		thumbnailPath = *attachment.LargeThumbnail
	default:
		return "", fmt.Errorf("不支持的缩略图大小")
	}

	if h.config.CDNEnabled && h.config.CDNBaseURL != "" {
		return fmt.Sprintf("%s/%s", h.config.CDNBaseURL, thumbnailPath), nil
	}

	return h.storageProvider.GetURL(ctx, thumbnailPath, URLOptions{})
}

// ValidateAttachmentField 验证附件字段值
func (h *AttachmentFieldHandler) ValidateAttachmentField(attachments []*AttachmentItem) error {
	// 验证文件数量
	if h.config.MaxFileCount > 0 && len(attachments) > h.config.MaxFileCount {
		return fmt.Errorf("文件数量超过限制: %d > %d", len(attachments), h.config.MaxFileCount)
	}

	// 验证每个附件
	for _, attachment := range attachments {
		if err := h.validator.ValidateAttachment(attachment); err != nil {
			return fmt.Errorf("附件验证失败 %s: %w", attachment.Name, err)
		}
	}

	return nil
}

// CleanupOrphanedAttachments 清理孤立的附件
func (h *AttachmentFieldHandler) CleanupOrphanedAttachments(ctx context.Context, tableID, fieldID string, validAttachmentIDs []string) error {
	// 这里应该查询数据库找到孤立的附件
	// 然后删除它们
	// 简化实现，实际需要依赖数据层
	return nil
}

// generateStoragePath 生成存储路径
func (h *AttachmentFieldHandler) generateStoragePath(request *ProcessUploadRequest) (string, error) {
	// 使用模板生成路径
	template := h.config.StoragePath
	if template == "" {
		template = "attachments/{table_id}/{field_id}/{year}/{month}/{day}/{uuid}{ext}"
	}

	now := time.Now()
	uuid := generateUUID()
	ext := filepath.Ext(request.FileName)

	replacements := map[string]string{
		"{table_id}":  request.TableID,
		"{field_id}":  request.FieldID,
		"{record_id}": request.RecordID,
		"{year}":      fmt.Sprintf("%04d", now.Year()),
		"{month}":     fmt.Sprintf("%02d", now.Month()),
		"{day}":       fmt.Sprintf("%02d", now.Day()),
		"{uuid}":      uuid,
		"{ext}":       ext,
		"{timestamp}": fmt.Sprintf("%d", now.Unix()),
	}

	path := template
	for placeholder, value := range replacements {
		path = strings.ReplaceAll(path, placeholder, value)
	}

	return path, nil
}

// processImageAttachment 处理图片附件
func (h *AttachmentFieldHandler) processImageAttachment(ctx context.Context, attachment *AttachmentItem) error {
	if !h.config.EnableThumbnail {
		return nil
	}

	// 获取图片尺寸
	dimensions, err := h.thumbnailService.GetImageDimensions(ctx, attachment.Path)
	if err != nil {
		return fmt.Errorf("获取图片尺寸失败: %w", err)
	}

	attachment.SetDimensions(dimensions.Width, dimensions.Height)

	// 生成缩略图
	thumbnails, err := h.thumbnailService.GenerateThumbnails(ctx, attachment.Path, ThumbnailOptions{
		Sizes:   []ThumbnailSize{ThumbnailSizeSmall, ThumbnailSizeLarge},
		Quality: 85,
		Format:  "jpeg",
	})
	if err != nil {
		return fmt.Errorf("生成缩略图失败: %w", err)
	}

	// 设置缩略图路径
	if smallPath, exists := thumbnails[ThumbnailSizeSmall]; exists {
		attachment.SmallThumbnail = &smallPath
	}
	if largePath, exists := thumbnails[ThumbnailSizeLarge]; exists {
		attachment.LargeThumbnail = &largePath
	}

	return nil
}

// generateAccessURL 生成访问URL
func (h *AttachmentFieldHandler) generateAccessURL(ctx context.Context, attachment *AttachmentItem) error {
	url, err := h.GetAttachmentURL(ctx, attachment, URLOptions{
		Expires: 24 * time.Hour,
		Method:  "GET",
	})
	if err != nil {
		return err
	}

	attachment.SetPresignedURL(url)
	return nil
}

// ProcessUploadRequest 处理上传请求
type ProcessUploadRequest struct {
	TableID     string            `json:"table_id"`
	FieldID     string            `json:"field_id"`
	RecordID    string            `json:"record_id"`
	FileName    string            `json:"file_name"`
	FileSize    int64             `json:"file_size"`
	ContentType string            `json:"content_type"`
	FileReader  io.Reader         `json:"-"`
	UploadedBy  string            `json:"uploaded_by"`
	Metadata    map[string]string `json:"metadata"`
}

// ThumbnailService 缩略图服务接口
type ThumbnailService interface {
	GetImageDimensions(ctx context.Context, path string) (*ImageDimensions, error)
	GenerateThumbnails(ctx context.Context, path string, options ThumbnailOptions) (map[ThumbnailSize]string, error)
	DeleteThumbnails(ctx context.Context, paths []string) error
}

// ImageDimensions 图片尺寸
type ImageDimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// ThumbnailSize 缩略图大小
type ThumbnailSize string

const (
	ThumbnailSizeSmall ThumbnailSize = "small"
	ThumbnailSizeLarge ThumbnailSize = "large"
)

// ThumbnailOptions 缩略图选项
type ThumbnailOptions struct {
	Sizes   []ThumbnailSize `json:"sizes"`
	Quality int             `json:"quality"`
	Format  string          `json:"format"`
}

// AttachmentValidator 附件验证器
type AttachmentValidator struct {
	config *AttachmentFieldConfig
}

// NewAttachmentValidator 创建附件验证器
func NewAttachmentValidator(config *AttachmentFieldConfig) *AttachmentValidator {
	return &AttachmentValidator{
		config: config,
	}
}

// ValidateUpload 验证上传请求
func (v *AttachmentValidator) ValidateUpload(request *ProcessUploadRequest) error {
	// 验证文件大小
	if v.config.MaxFileSize > 0 && request.FileSize > v.config.MaxFileSize {
		return fmt.Errorf("文件大小超过限制: %d > %d", request.FileSize, v.config.MaxFileSize)
	}

	// 验证文件类型
	if len(v.config.AllowedTypes) > 0 {
		allowed := false
		for _, allowedType := range v.config.AllowedTypes {
			if request.ContentType == allowedType {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("不允许的文件类型: %s", request.ContentType)
		}
	}

	// 验证文件扩展名
	if len(v.config.AllowedExtensions) > 0 {
		ext := strings.ToLower(filepath.Ext(request.FileName))
		allowed := false
		for _, allowedExt := range v.config.AllowedExtensions {
			if ext == strings.ToLower(allowedExt) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("不允许的文件扩展名: %s", ext)
		}
	}

	// 验证文件名
	if request.FileName == "" {
		return fmt.Errorf("文件名不能为空")
	}

	// 验证内容类型与文件扩展名的一致性
	if err := v.validateContentTypeConsistency(request.FileName, request.ContentType); err != nil {
		return err
	}

	return nil
}

// ValidateAttachment 验证附件
func (v *AttachmentValidator) ValidateAttachment(attachment *AttachmentItem) error {
	// 验证文件大小
	if v.config.MaxFileSize > 0 && attachment.Size > v.config.MaxFileSize {
		return fmt.Errorf("文件大小超过限制: %d > %d", attachment.Size, v.config.MaxFileSize)
	}

	// 验证文件类型
	if len(v.config.AllowedTypes) > 0 {
		allowed := false
		for _, allowedType := range v.config.AllowedTypes {
			if attachment.MimeType == allowedType {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("不允许的文件类型: %s", attachment.MimeType)
		}
	}

	return nil
}

// validateContentTypeConsistency 验证内容类型与文件扩展名的一致性
func (v *AttachmentValidator) validateContentTypeConsistency(fileName, contentType string) error {
	ext := filepath.Ext(fileName)
	if ext == "" {
		return nil // 没有扩展名，跳过验证
	}

	expectedType := mime.TypeByExtension(ext)
	if expectedType == "" {
		return nil // 无法确定期望的类型，跳过验证
	}

	// 简化验证：只检查主类型
	expectedMain := strings.Split(expectedType, "/")[0]
	actualMain := strings.Split(contentType, "/")[0]

	if expectedMain != actualMain {
		return fmt.Errorf("文件类型不一致: 扩展名 %s 期望 %s，实际 %s", ext, expectedType, contentType)
	}

	return nil
}

// 辅助函数
func generateAttachmentID() string {
	return generateUUID()
}

func generateAttachmentToken() string {
	return generateUUID()
}

func generateUUID() string {
	// 简化实现，实际应该使用更好的UUID生成器
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
