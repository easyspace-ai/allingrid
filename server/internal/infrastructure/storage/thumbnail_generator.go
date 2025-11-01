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
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/attachment"
)

// ThumbnailGenerator 缩略图生成器实现
type ThumbnailGenerator struct {
	logger *zap.Logger
}

// NewThumbnailGenerator 创建缩略图生成器
func NewThumbnailGenerator(logger *zap.Logger) *ThumbnailGenerator {
	return &ThumbnailGenerator{
		logger: logger,
	}
}

// GenerateThumbnail 生成单个缩略图
func (g *ThumbnailGenerator) GenerateThumbnail(ctx context.Context, sourcePath, targetPath string, width, height int, quality int) error {
	// 打开源文件
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// 解码图片
	img, format, err := image.Decode(sourceFile)
	if err != nil {
		return fmt.Errorf("failed to decode image: %w", err)
	}

	// 调整大小
	thumbnail := resize.Thumbnail(uint(width), uint(height), img, resize.Lanczos3)

	// 创建目标目录
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// 创建目标文件
	targetFile, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	defer targetFile.Close()

	// 编码并保存
	switch strings.ToLower(format) {
	case "jpeg", "jpg":
		err = jpeg.Encode(targetFile, thumbnail, &jpeg.Options{Quality: quality})
	case "png":
		err = png.Encode(targetFile, thumbnail)
	default:
		// 默认使用 JPEG 格式
		err = jpeg.Encode(targetFile, thumbnail, &jpeg.Options{Quality: quality})
	}

	if err != nil {
		return fmt.Errorf("failed to encode thumbnail: %w", err)
	}

	g.logger.Debug("Generated thumbnail",
		zap.String("source", sourcePath),
		zap.String("target", targetPath),
		zap.Int("width", width),
		zap.Int("height", height),
		zap.String("format", format),
	)

	return nil
}

// GenerateThumbnails 生成多个尺寸的缩略图
func (g *ThumbnailGenerator) GenerateThumbnails(ctx context.Context, sourcePath string, config *attachment.ThumbnailConfig) (map[string]string, error) {
	if config == nil {
		return nil, fmt.Errorf("thumbnail config is nil")
	}

	thumbnails := make(map[string]string)

	// 生成小尺寸缩略图
	if config.SmallWidth > 0 && config.SmallHeight > 0 {
		smallPath := g.generateThumbnailPath(sourcePath, "small", config.Format)
		err := g.GenerateThumbnail(ctx, sourcePath, smallPath, config.SmallWidth, config.SmallHeight, config.Quality)
		if err != nil {
			g.logger.Error("Failed to generate small thumbnail", zap.Error(err))
		} else {
			thumbnails["small"] = smallPath
		}
	}

	// 生成大尺寸缩略图
	if config.LargeWidth > 0 && config.LargeHeight > 0 {
		largePath := g.generateThumbnailPath(sourcePath, "large", config.Format)
		err := g.GenerateThumbnail(ctx, sourcePath, largePath, config.LargeWidth, config.LargeHeight, config.Quality)
		if err != nil {
			g.logger.Error("Failed to generate large thumbnail", zap.Error(err))
		} else {
			thumbnails["large"] = largePath
		}
	}

	g.logger.Info("Generated thumbnails",
		zap.String("source", sourcePath),
		zap.Int("count", len(thumbnails)),
	)

	return thumbnails, nil
}

// IsSupported 检查是否支持该MIME类型
func (g *ThumbnailGenerator) IsSupported(mimeType string) bool {
	supportedTypes := []string{
		"image/jpeg",
		"image/jpg",
		"image/png",
		"image/gif",
		"image/webp",
	}

	for _, supported := range supportedTypes {
		if mimeType == supported {
			return true
		}
	}

	return false
}

// generateThumbnailPath 生成缩略图路径
func (g *ThumbnailGenerator) generateThumbnailPath(sourcePath, size, format string) string {
	dir := filepath.Dir(sourcePath)
	filename := filepath.Base(sourcePath)
	ext := filepath.Ext(filename)
	nameWithoutExt := strings.TrimSuffix(filename, ext)

	// 使用指定格式或保持原格式
	outputExt := format
	if outputExt == "" {
		outputExt = strings.TrimPrefix(ext, ".")
	}

	thumbnailFilename := fmt.Sprintf("%s_%s.%s", nameWithoutExt, size, outputExt)
	return filepath.Join(dir, "thumbnails", thumbnailFilename)
}

// GetImageDimensions 获取图片尺寸
func (g *ThumbnailGenerator) GetImageDimensions(sourcePath string) (width int, height int, err error) {
	file, err := os.Open(sourcePath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	config, _, err := image.DecodeConfig(file)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode image config: %w", err)
	}

	return config.Width, config.Height, nil
}

// OptimizeImage 优化图片
func (g *ThumbnailGenerator) OptimizeImage(ctx context.Context, sourcePath, targetPath string, maxWidth, maxHeight, quality int) error {
	// 打开源文件
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// 解码图片
	img, format, err := image.Decode(sourceFile)
	if err != nil {
		return fmt.Errorf("failed to decode image: %w", err)
	}

	// 获取原始尺寸
	bounds := img.Bounds()
	originalWidth := bounds.Dx()
	originalHeight := bounds.Dy()

	// 如果尺寸超过最大限制，则调整大小
	var optimized image.Image = img
	if originalWidth > maxWidth || originalHeight > maxHeight {
		optimized = resize.Thumbnail(uint(maxWidth), uint(maxHeight), img, resize.Lanczos3)
	}

	// 创建目标目录
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// 创建目标文件
	targetFile, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	defer targetFile.Close()

	// 编码并保存（使用优化的质量参数）
	switch strings.ToLower(format) {
	case "jpeg", "jpg":
		err = jpeg.Encode(targetFile, optimized, &jpeg.Options{Quality: quality})
	case "png":
		// PNG 使用默认压缩
		err = png.Encode(targetFile, optimized)
	default:
		// 默认转换为 JPEG
		err = jpeg.Encode(targetFile, optimized, &jpeg.Options{Quality: quality})
	}

	if err != nil {
		return fmt.Errorf("failed to encode optimized image: %w", err)
	}

	g.logger.Debug("Optimized image",
		zap.String("source", sourcePath),
		zap.String("target", targetPath),
		zap.Int("original_width", originalWidth),
		zap.Int("original_height", originalHeight),
		zap.Int("quality", quality),
	)

	return nil
}
