package attachment

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// AttachmentItem 附件项
type AttachmentItem struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Path           string    `json:"path"`
	Token          string    `json:"token"`
	Size           int64     `json:"size"`
	MimeType       string    `json:"mimetype"`
	PresignedURL   *string   `json:"presigned_url,omitempty"`
	Width          *int      `json:"width,omitempty"`
	Height         *int      `json:"height,omitempty"`
	SmallThumbnail *string   `json:"sm_thumbnail_url,omitempty"`
	LargeThumbnail *string   `json:"lg_thumbnail_url,omitempty"`
	CreatedTime    time.Time `json:"created_time"`
	UpdatedTime    time.Time `json:"updated_time"`
}

// NewAttachmentItem 创建新的附件项
func NewAttachmentItem(name, path, token, mimeType string, size int64) *AttachmentItem {
	return &AttachmentItem{
		ID:          utils.GenerateNanoID(10),
		Name:        name,
		Path:        path,
		Token:       token,
		Size:        size,
		MimeType:    mimeType,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// SetDimensions 设置图片尺寸
func (a *AttachmentItem) SetDimensions(width, height int) {
	a.Width = &width
	a.Height = &height
	a.UpdatedTime = time.Now()
}

// SetThumbnails 设置缩略图
func (a *AttachmentItem) SetThumbnails(smallThumbnail, largeThumbnail string) {
	a.SmallThumbnail = &smallThumbnail
	a.LargeThumbnail = &largeThumbnail
	a.UpdatedTime = time.Now()
}

// SetPresignedURL 设置预签名URL
func (a *AttachmentItem) SetPresignedURL(url string) {
	a.PresignedURL = &url
	a.UpdatedTime = time.Now()
}

// IsImage 检查是否为图片
func (a *AttachmentItem) IsImage() bool {
	return len(a.MimeType) >= 5 && a.MimeType[:5] == "image"
}

// IsVideo 检查是否为视频
func (a *AttachmentItem) IsVideo() bool {
	return len(a.MimeType) >= 5 && a.MimeType[:5] == "video"
}

// IsAudio 检查是否为音频
func (a *AttachmentItem) IsAudio() bool {
	return len(a.MimeType) >= 5 && a.MimeType[:5] == "audio"
}

// IsDocument 检查是否为文档
func (a *AttachmentItem) IsDocument() bool {
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
		if a.MimeType == docType {
			return true
		}
	}
	return false
}

// UploadToken 上传令牌
type UploadToken struct {
	Token        string    `json:"token"`
	UserID       string    `json:"user_id"`
	TableID      string    `json:"table_id"`
	FieldID      string    `json:"field_id"`
	RecordID     string    `json:"record_id"`
	ExpiresAt    time.Time `json:"expires_at"`
	MaxSize      int64     `json:"max_size"`
	AllowedTypes []string  `json:"allowed_types"`
	CreatedTime  time.Time `json:"created_time"`
}

// NewUploadToken 创建新的上传令牌
func NewUploadToken(userID, tableID, fieldID, recordID string, maxSize int64, allowedTypes []string) *UploadToken {
	return &UploadToken{
		Token:        utils.GenerateNanoID(20),
		UserID:       userID,
		TableID:      tableID,
		FieldID:      fieldID,
		RecordID:     recordID,
		ExpiresAt:    time.Now().Add(24 * time.Hour), // 24小时过期
		MaxSize:      maxSize,
		AllowedTypes: allowedTypes,
		CreatedTime:  time.Now(),
	}
}

// IsExpired 检查令牌是否过期
func (t *UploadToken) IsExpired() bool {
	return time.Now().After(t.ExpiresAt)
}

// IsValidType 检查文件类型是否允许
func (t *UploadToken) IsValidType(mimeType string) bool {
	if len(t.AllowedTypes) == 0 {
		return true // 如果没有限制，则允许所有类型
	}

	for _, allowedType := range t.AllowedTypes {
		if mimeType == allowedType {
			return true
		}
	}
	return false
}

// IsValidSize 检查文件大小是否允许
func (t *UploadToken) IsValidSize(size int64) bool {
	return size <= t.MaxSize
}

// SignatureRequest 签名请求
type SignatureRequest struct {
	TableID      string   `json:"table_id" binding:"required"`
	FieldID      string   `json:"field_id" binding:"required"`
	RecordID     string   `json:"record_id" binding:"required"`
	MaxSize      int64    `json:"max_size,omitempty"`
	AllowedTypes []string `json:"allowed_types,omitempty"`
}

// SignatureResponse 签名响应
type SignatureResponse struct {
	Token        string   `json:"token"`
	UploadURL    string   `json:"upload_url"`
	ExpiresAt    int64    `json:"expires_at"`
	MaxSize      int64    `json:"max_size"`
	AllowedTypes []string `json:"allowed_types"`
}

// HTTPUploadRequest HTTP上传请求
type HTTPUploadRequest struct {
	Token    string `form:"token" binding:"required"`
	Filename string `form:"filename,omitempty"`
}

// UploadResponse 上传响应
type UploadResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// NotifyRequest 通知请求
type NotifyRequest struct {
	Token    string `form:"token" binding:"required"`
	Filename string `form:"filename,omitempty"`
}

// NotifyResponse 通知响应
type NotifyResponse struct {
	Attachment *AttachmentItem `json:"attachment"`
	Success    bool            `json:"success"`
	Message    string          `json:"message,omitempty"`
}

// ReadRequest 读取请求
type ReadRequest struct {
	Path                       string `form:"path" binding:"required"`
	Token                      string `form:"token,omitempty"`
	ResponseContentDisposition string `form:"response-content-disposition,omitempty"`
}

// ReadResponse 读取响应
type ReadResponse struct {
	Data     []byte            `json:"data"`
	Headers  map[string]string `json:"headers"`
	MimeType string            `json:"mime_type"`
	Size     int64             `json:"size"`
}

// AttachmentStats 附件统计信息
type AttachmentStats struct {
	TotalFiles    int64     `json:"total_files"`
	TotalSize     int64     `json:"total_size"`
	ImageFiles    int64     `json:"image_files"`
	VideoFiles    int64     `json:"video_files"`
	AudioFiles    int64     `json:"audio_files"`
	DocumentFiles int64     `json:"document_files"`
	OtherFiles    int64     `json:"other_files"`
	LastUploaded  time.Time `json:"last_uploaded"`
}

// AttachmentStorageConfig 附件存储配置
type AttachmentStorageConfig struct {
	Type         string   `json:"type"` // local, s3, oss, etc.
	LocalPath    string   `json:"local_path,omitempty"`
	BucketName   string   `json:"bucket_name,omitempty"`
	AccessKey    string   `json:"access_key,omitempty"`
	SecretKey    string   `json:"secret_key,omitempty"`
	Region       string   `json:"region,omitempty"`
	Endpoint     string   `json:"endpoint,omitempty"`
	CDNBaseURL   string   `json:"cdn_base_url,omitempty"`
	MaxFileSize  int64    `json:"max_file_size"`
	AllowedTypes []string `json:"allowed_types"`
}

// ThumbnailConfig 缩略图配置
type ThumbnailConfig struct {
	Enabled     bool   `json:"enabled"`
	SmallWidth  int    `json:"small_width"`
	SmallHeight int    `json:"small_height"`
	LargeWidth  int    `json:"large_width"`
	LargeHeight int    `json:"large_height"`
	Quality     int    `json:"quality"`
	Format      string `json:"format"`
}
