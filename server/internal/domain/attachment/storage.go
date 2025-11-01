package attachment

import (
	"context"
	"io"
	"time"
)

// Storage 存储接口
type Storage interface {
	// Upload 上传文件
	Upload(ctx context.Context, path string, reader io.Reader, size int64, contentType string) error
	// Download 下载文件
	Download(ctx context.Context, path string) (io.ReadCloser, error)
	// Delete 删除文件
	Delete(ctx context.Context, path string) error
	// Exists 检查文件是否存在
	Exists(ctx context.Context, path string) (bool, error)
	// GetURL 获取文件访问URL
	GetURL(ctx context.Context, path string, expires time.Duration) (string, error)
	// GetSize 获取文件大小
	GetSize(ctx context.Context, path string) (int64, error)
	// GetMetadata 获取文件元数据
	GetMetadata(ctx context.Context, path string) (map[string]string, error)
}

// ThumbnailGenerator 缩略图生成器接口
type ThumbnailGenerator interface {
	// GenerateThumbnail 生成缩略图
	GenerateThumbnail(ctx context.Context, sourcePath, targetPath string, width, height int, quality int) error
	// GenerateThumbnails 生成多种尺寸的缩略图
	GenerateThumbnails(ctx context.Context, sourcePath string, config *ThumbnailConfig) (map[string]string, error)
	// IsSupported 检查是否支持该文件类型
	IsSupported(mimeType string) bool
}

// FileValidator 文件验证器接口
type FileValidator interface {
	// ValidateFile 验证文件
	ValidateFile(ctx context.Context, filename string, size int64, contentType string, allowedTypes []string, maxSize int64) error
	// GetMimeType 获取文件MIME类型
	GetMimeType(filename string) string
	// IsImage 检查是否为图片
	IsImage(mimeType string) bool
	// IsVideo 检查是否为视频
	IsVideo(mimeType string) bool
	// IsAudio 检查是否为音频
	IsAudio(mimeType string) bool
	// IsDocument 检查是否为文档
	IsDocument(mimeType string) bool
}
