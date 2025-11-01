package storage

import (
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"

	"github.com/nfnt/resize"

	"github.com/easyspace-ai/luckdb/server/internal/domain/attachment"
)

// ThumbnailServiceImpl 缩略图服务实现
type ThumbnailServiceImpl struct {
	storageProvider attachment.StorageProvider
	config          *ThumbnailConfig
}

// ThumbnailConfig 缩略图配置
type ThumbnailConfig struct {
	SmallWidth  uint   `json:"small_width"`
	SmallHeight uint   `json:"small_height"`
	LargeWidth  uint   `json:"large_width"`
	LargeHeight uint   `json:"large_height"`
	Quality     int    `json:"quality"`
	Format      string `json:"format"`
	StoragePath string `json:"storage_path"`
}

// NewThumbnailService 创建缩略图服务
func NewThumbnailService(storageProvider attachment.StorageProvider, config *ThumbnailConfig) attachment.ThumbnailService {
	if config == nil {
		config = &ThumbnailConfig{
			SmallWidth:  150,
			SmallHeight: 150,
			LargeWidth:  400,
			LargeHeight: 400,
			Quality:     85,
			Format:      "jpeg",
			StoragePath: "thumbnails",
		}
	}

	return &ThumbnailServiceImpl{
		storageProvider: storageProvider,
		config:          config,
	}
}

// GetImageDimensions 获取图片尺寸
func (s *ThumbnailServiceImpl) GetImageDimensions(ctx context.Context, path string) (*attachment.ImageDimensions, error) {
	// 下载图片
	downloadResult, err := s.storageProvider.Download(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("下载图片失败: %w", err)
	}
	defer downloadResult.Reader.Close()

	// 解码图片获取尺寸
	config, _, err := image.DecodeConfig(downloadResult.Reader)
	if err != nil {
		return nil, fmt.Errorf("解码图片失败: %w", err)
	}

	return &attachment.ImageDimensions{
		Width:  config.Width,
		Height: config.Height,
	}, nil
}

// GenerateThumbnails 生成缩略图
func (s *ThumbnailServiceImpl) GenerateThumbnails(ctx context.Context, path string, options attachment.ThumbnailOptions) (map[attachment.ThumbnailSize]string, error) {
	// 下载原图
	downloadResult, err := s.storageProvider.Download(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("下载原图失败: %w", err)
	}
	defer downloadResult.Reader.Close()

	// 解码图片
	img, format, err := image.Decode(downloadResult.Reader)
	if err != nil {
		return nil, fmt.Errorf("解码图片失败: %w", err)
	}

	thumbnails := make(map[attachment.ThumbnailSize]string)

	// 为每个尺寸生成缩略图
	for _, size := range options.Sizes {
		thumbnailPath, err := s.generateThumbnail(ctx, img, path, size, format, options)
		if err != nil {
			return nil, fmt.Errorf("生成 %s 缩略图失败: %w", size, err)
		}
		thumbnails[size] = thumbnailPath
	}

	return thumbnails, nil
}

// DeleteThumbnails 删除缩略图
func (s *ThumbnailServiceImpl) DeleteThumbnails(ctx context.Context, paths []string) error {
	var errors []error

	for _, path := range paths {
		if err := s.storageProvider.Delete(ctx, path); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("删除缩略图部分失败: %d个错误", len(errors))
	}

	return nil
}

// generateThumbnail 生成单个缩略图
func (s *ThumbnailServiceImpl) generateThumbnail(
	ctx context.Context,
	img image.Image,
	originalPath string,
	size attachment.ThumbnailSize,
	originalFormat string,
	options attachment.ThumbnailOptions,
) (string, error) {
	// 获取目标尺寸
	width, height := s.getThumbnailSize(size)

	// 调整图片大小
	thumbnail := resize.Thumbnail(width, height, img, resize.Lanczos3)

	// 生成缩略图路径
	thumbnailPath := s.generateThumbnailPath(originalPath, size)

	// 创建临时文件
	tempFile, err := os.CreateTemp("", "thumbnail_*."+s.getOutputFormat(options.Format))
	if err != nil {
		return "", fmt.Errorf("创建临时文件失败: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// 编码并保存缩略图
	if err := s.encodeThumbnail(tempFile, thumbnail, options); err != nil {
		return "", fmt.Errorf("编码缩略图失败: %w", err)
	}

	// 获取文件大小
	stat, err := tempFile.Stat()
	if err != nil {
		return "", fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 重置文件指针
	tempFile.Seek(0, 0)

	// 上传缩略图
	uploadRequest := attachment.UploadRequest{
		Path:        thumbnailPath,
		Reader:      tempFile,
		Size:        stat.Size(),
		ContentType: s.getContentType(options.Format),
		Metadata: map[string]string{
			"type":          "thumbnail",
			"size":          string(size),
			"original_path": originalPath,
		},
		Options: attachment.UploadOptions{
			Overwrite: true,
			CreateDir: true,
		},
	}

	_, err = s.storageProvider.Upload(ctx, uploadRequest)
	if err != nil {
		return "", fmt.Errorf("上传缩略图失败: %w", err)
	}

	return thumbnailPath, nil
}

// getThumbnailSize 获取缩略图尺寸
func (s *ThumbnailServiceImpl) getThumbnailSize(size attachment.ThumbnailSize) (uint, uint) {
	switch size {
	case attachment.ThumbnailSizeSmall:
		return s.config.SmallWidth, s.config.SmallHeight
	case attachment.ThumbnailSizeLarge:
		return s.config.LargeWidth, s.config.LargeHeight
	default:
		return s.config.SmallWidth, s.config.SmallHeight
	}
}

// generateThumbnailPath 生成缩略图路径
func (s *ThumbnailServiceImpl) generateThumbnailPath(originalPath string, size attachment.ThumbnailSize) string {
	dir := filepath.Dir(originalPath)
	filename := filepath.Base(originalPath)
	ext := filepath.Ext(filename)
	nameWithoutExt := strings.TrimSuffix(filename, ext)

	outputFormat := s.getOutputFormat(s.config.Format)
	thumbnailFilename := fmt.Sprintf("%s_%s.%s", nameWithoutExt, size, outputFormat)

	return filepath.Join(s.config.StoragePath, dir, thumbnailFilename)
}

// encodeThumbnail 编码缩略图
func (s *ThumbnailServiceImpl) encodeThumbnail(file *os.File, img image.Image, options attachment.ThumbnailOptions) error {
	format := s.getOutputFormat(options.Format)
	quality := options.Quality
	if quality <= 0 {
		quality = s.config.Quality
	}

	switch format {
	case "jpeg", "jpg":
		return jpeg.Encode(file, img, &jpeg.Options{Quality: quality})
	case "png":
		return png.Encode(file, img)
	default:
		return jpeg.Encode(file, img, &jpeg.Options{Quality: quality})
	}
}

// getOutputFormat 获取输出格式
func (s *ThumbnailServiceImpl) getOutputFormat(format string) string {
	if format == "" {
		return s.config.Format
	}
	return format
}

// getContentType 获取内容类型
func (s *ThumbnailServiceImpl) getContentType(format string) string {
	outputFormat := s.getOutputFormat(format)
	switch outputFormat {
	case "jpeg", "jpg":
		return "image/jpeg"
	case "png":
		return "image/png"
	default:
		return "image/jpeg"
	}
}

// ImageProcessor 图片处理器
type ImageProcessor struct {
	config *ImageProcessorConfig
}

// ImageProcessorConfig 图片处理器配置
type ImageProcessorConfig struct {
	MaxWidth         uint    `json:"max_width"`
	MaxHeight        uint    `json:"max_height"`
	Quality          int     `json:"quality"`
	Format           string  `json:"format"`
	EnableResize     bool    `json:"enable_resize"`
	EnableCrop       bool    `json:"enable_crop"`
	EnableRotate     bool    `json:"enable_rotate"`
	EnableWatermark  bool    `json:"enable_watermark"`
	WatermarkPath    string  `json:"watermark_path"`
	WatermarkOpacity float64 `json:"watermark_opacity"`
}

// NewImageProcessor 创建图片处理器
func NewImageProcessor(config *ImageProcessorConfig) *ImageProcessor {
	if config == nil {
		config = &ImageProcessorConfig{
			MaxWidth:     2048,
			MaxHeight:    2048,
			Quality:      85,
			Format:       "jpeg",
			EnableResize: true,
		}
	}

	return &ImageProcessor{
		config: config,
	}
}

// ProcessImage 处理图片
func (p *ImageProcessor) ProcessImage(ctx context.Context, img image.Image, options ImageProcessOptions) (image.Image, error) {
	result := img

	// 调整大小
	if p.config.EnableResize && (options.Width > 0 || options.Height > 0) {
		width := options.Width
		height := options.Height

		if width == 0 {
			width = p.config.MaxWidth
		}
		if height == 0 {
			height = p.config.MaxHeight
		}

		result = resize.Resize(width, height, result, resize.Lanczos3)
	}

	// 裁剪
	if p.config.EnableCrop && options.CropRect != nil {
		result = p.cropImage(result, *options.CropRect)
	}

	// 旋转
	if p.config.EnableRotate && options.Rotation != 0 {
		result = p.rotateImage(result, options.Rotation)
	}

	// 添加水印
	if p.config.EnableWatermark && p.config.WatermarkPath != "" {
		watermarked, err := p.addWatermark(result, p.config.WatermarkPath)
		if err == nil {
			result = watermarked
		}
	}

	return result, nil
}

// ImageProcessOptions 图片处理选项
type ImageProcessOptions struct {
	Width    uint             `json:"width"`
	Height   uint             `json:"height"`
	Quality  int              `json:"quality"`
	Format   string           `json:"format"`
	CropRect *image.Rectangle `json:"crop_rect"`
	Rotation int              `json:"rotation"` // 旋转角度（度）
}

// cropImage 裁剪图片
func (p *ImageProcessor) cropImage(img image.Image, rect image.Rectangle) image.Image {
	// 简化实现，实际应该使用更好的裁剪算法
	bounds := img.Bounds()
	if rect.Max.X > bounds.Max.X {
		rect.Max.X = bounds.Max.X
	}
	if rect.Max.Y > bounds.Max.Y {
		rect.Max.Y = bounds.Max.Y
	}

	// 这里需要实现实际的裁剪逻辑
	// 暂时返回原图
	return img
}

// rotateImage 旋转图片
func (p *ImageProcessor) rotateImage(img image.Image, rotation int) image.Image {
	// 简化实现，实际应该使用图片旋转库
	// 暂时返回原图
	return img
}

// addWatermark 添加水印
func (p *ImageProcessor) addWatermark(img image.Image, watermarkPath string) (image.Image, error) {
	// 简化实现，实际应该加载水印图片并合成
	// 暂时返回原图
	return img, nil
}

// FileTypeDetector 文件类型检测器
type FileTypeDetector struct{}

// NewFileTypeDetector 创建文件类型检测器
func NewFileTypeDetector() *FileTypeDetector {
	return &FileTypeDetector{}
}

// DetectFileType 检测文件类型
func (d *FileTypeDetector) DetectFileType(data []byte) (string, error) {
	// 检测图片类型
	if len(data) >= 8 {
		// PNG
		if data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47 {
			return "image/png", nil
		}

		// JPEG
		if data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
			return "image/jpeg", nil
		}

		// GIF
		if string(data[0:6]) == "GIF87a" || string(data[0:6]) == "GIF89a" {
			return "image/gif", nil
		}

		// WebP
		if string(data[0:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
			return "image/webp", nil
		}
	}

	// PDF
	if len(data) >= 4 && string(data[0:4]) == "%PDF" {
		return "application/pdf", nil
	}

	// ZIP
	if len(data) >= 4 && data[0] == 0x50 && data[1] == 0x4B && data[2] == 0x03 && data[3] == 0x04 {
		return "application/zip", nil
	}

	// 默认返回二进制类型
	return "application/octet-stream", nil
}

// IsImageType 检查是否为图片类型
func (d *FileTypeDetector) IsImageType(contentType string) bool {
	return strings.HasPrefix(contentType, "image/")
}

// IsVideoType 检查是否为视频类型
func (d *FileTypeDetector) IsVideoType(contentType string) bool {
	return strings.HasPrefix(contentType, "video/")
}

// IsAudioType 检查是否为音频类型
func (d *FileTypeDetector) IsAudioType(contentType string) bool {
	return strings.HasPrefix(contentType, "audio/")
}

// IsDocumentType 检查是否为文档类型
func (d *FileTypeDetector) IsDocumentType(contentType string) bool {
	documentTypes := []string{
		"application/pdf",
		"application/msword",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.ms-excel",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"text/plain",
		"text/csv",
	}

	for _, docType := range documentTypes {
		if contentType == docType {
			return true
		}
	}

	return false
}
