package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// LocalStorage 本地存储实现
type LocalStorage struct {
	basePath string
	logger   *zap.Logger
}

// NewLocalStorage 创建本地存储
func NewLocalStorage(basePath string, logger *zap.Logger) *LocalStorage {
	return &LocalStorage{
		basePath: basePath,
		logger:   logger,
	}
}

// Upload 上传文件
func (s *LocalStorage) Upload(ctx context.Context, path string, reader io.Reader, size int64, contentType string) error {
	fullPath := filepath.Join(s.basePath, path)

	// 创建目录
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		s.logger.Error("Failed to create directory",
			logger.String("dir", dir),
			logger.ErrorField(err),
		)
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建文件
	file, err := os.Create(fullPath)
	if err != nil {
		s.logger.Error("Failed to create file",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	s.logger.Info("LocalStorage.Upload: creating file",
		logger.String("base_path", s.basePath),
		logger.String("relative_path", path),
		logger.String("full_path", fullPath),
		logger.Int64("size", size),
	)

	// 复制文件内容
	written, err := io.Copy(file, reader)
	if err != nil {
		s.logger.Error("Failed to write file content",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return fmt.Errorf("failed to write file content: %w", err)
	}

	if written != size {
		s.logger.Warn("File size mismatch",
			logger.String("path", fullPath),
			logger.Int64("expected", size),
			logger.Int64("written", written),
		)
	}

	s.logger.Info("File uploaded successfully",
		logger.String("path", fullPath),
		logger.Int64("size", written),
	)
	return nil
}

// Download 下载文件
func (s *LocalStorage) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(s.basePath, path)

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		s.logger.Error("Failed to open file",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return file, nil
}

// Delete 删除文件
func (s *LocalStorage) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(s.basePath, path)

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，认为删除成功
		}
		s.logger.Error("Failed to delete file",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return fmt.Errorf("failed to delete file: %w", err)
	}

	s.logger.Info("File deleted successfully",
		logger.String("path", fullPath),
	)
	return nil
}

// Exists 检查文件是否存在
func (s *LocalStorage) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := filepath.Join(s.basePath, path)

	s.logger.Info("LocalStorage.Exists: checking file",
		logger.String("base_path", s.basePath),
		logger.String("relative_path", path),
		logger.String("full_path", fullPath),
	)

	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.Warn("LocalStorage.Exists: file not found",
				logger.String("full_path", fullPath),
			)
			return false, nil
		}
		s.logger.Error("Failed to check file existence",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return false, fmt.Errorf("failed to check file existence: %w", err)
	}

	s.logger.Info("LocalStorage.Exists: file found",
		logger.String("full_path", fullPath),
	)
	return true, nil
}

// GetURL 获取文件访问URL
func (s *LocalStorage) GetURL(ctx context.Context, path string, expires time.Duration) (string, error) {
	// 本地存储直接返回文件路径
	// 在实际应用中，这里应该返回一个可以通过HTTP访问的URL
	return fmt.Sprintf("/api/attachments/read/%s", path), nil
}

// GetSize 获取文件大小
func (s *LocalStorage) GetSize(ctx context.Context, path string) (int64, error) {
	fullPath := filepath.Join(s.basePath, path)

	stat, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("file not found: %s", path)
		}
		s.logger.Error("Failed to get file size",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return 0, fmt.Errorf("failed to get file size: %w", err)
	}

	return stat.Size(), nil
}

// GetMetadata 获取文件元数据
func (s *LocalStorage) GetMetadata(ctx context.Context, path string) (map[string]string, error) {
	fullPath := filepath.Join(s.basePath, path)

	stat, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		s.logger.Error("Failed to get file metadata",
			logger.String("path", fullPath),
			logger.ErrorField(err),
		)
		return nil, fmt.Errorf("failed to get file metadata: %w", err)
	}

	metadata := map[string]string{
		"size":     fmt.Sprintf("%d", stat.Size()),
		"modified": stat.ModTime().Format(time.RFC3339),
		"mode":     stat.Mode().String(),
	}

	return metadata, nil
}
