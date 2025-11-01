package storage

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/attachment"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// EnhancedLocalStorage 增强的本地存储实现
type EnhancedLocalStorage struct {
	basePath    string
	maxFileSize int64
	permissions os.FileMode
	features    []string
}

// NewEnhancedLocalStorage 创建增强的本地存储
func NewEnhancedLocalStorage(config attachment.LocalStorageConfig) *EnhancedLocalStorage {
	permissions := os.FileMode(0644)
	if config.Permissions != "" {
		// 解析权限字符串
		// 简化实现，实际应该解析八进制权限
	}

	return &EnhancedLocalStorage{
		basePath:    config.BasePath,
		maxFileSize: config.MaxSize,
		permissions: permissions,
		features: []string{
			"upload", "download", "delete", "exists", "metadata",
			"copy", "move", "list", "versioning",
		},
	}
}

// Upload 上传文件
func (s *EnhancedLocalStorage) Upload(ctx context.Context, request attachment.UploadRequest) (*attachment.UploadResult, error) {
	// 验证文件大小
	if s.maxFileSize > 0 && request.Size > s.maxFileSize {
		return nil, fmt.Errorf("文件大小超过限制: %d > %d", request.Size, s.maxFileSize)
	}

	fullPath := filepath.Join(s.basePath, request.Path)

	// 检查文件是否已存在
	if !request.Options.Overwrite {
		if _, err := os.Stat(fullPath); err == nil {
			return nil, fmt.Errorf("文件已存在: %s", request.Path)
		}
	}

	// 创建目录
	if request.Options.CreateDir {
		dir := filepath.Dir(fullPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("创建目录失败: %w", err)
		}
	}

	// 创建临时文件
	tempPath := fullPath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("创建临时文件失败: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempPath) // 清理临时文件
	}()

	// 计算文件哈希
	hash := md5.New()
	multiWriter := io.MultiWriter(tempFile, hash)

	// 复制文件内容
	written, err := io.Copy(multiWriter, request.Reader)
	if err != nil {
		return nil, fmt.Errorf("写入文件失败: %w", err)
	}

	// 验证文件大小
	if written != request.Size {
		return nil, fmt.Errorf("文件大小不匹配: 期望 %d，实际 %d", request.Size, written)
	}

	// 设置文件权限
	if err := tempFile.Chmod(s.permissions); err != nil {
		logger.Warn("设置文件权限失败", logger.ErrorField(err))
	}

	// 关闭临时文件
	tempFile.Close()

	// 原子性移动文件
	if err := os.Rename(tempPath, fullPath); err != nil {
		return nil, fmt.Errorf("移动文件失败: %w", err)
	}

	// 生成ETag
	etag := fmt.Sprintf("%x", hash.Sum(nil))

	// 生成访问URL
	url := fmt.Sprintf("/api/attachments/read/%s", request.Path)

	result := &attachment.UploadResult{
		Path:        request.Path,
		Size:        written,
		ContentType: request.ContentType,
		ETag:        etag,
		URL:         url,
		Metadata:    request.Metadata,
		UploadedAt:  time.Now(),
	}

	// 保存元数据
	if err := s.saveMetadata(fullPath, result); err != nil {
		logger.Warn("保存元数据失败", logger.ErrorField(err))
	}

	return result, nil
}

// Download 下载文件
func (s *EnhancedLocalStorage) Download(ctx context.Context, path string) (*attachment.DownloadResult, error) {
	fullPath := filepath.Join(s.basePath, path)

	// 检查文件是否存在
	stat, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("文件不存在: %s", path)
		}
		return nil, fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 打开文件
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}

	// 加载元数据
	metadata, err := s.loadMetadata(fullPath)
	if err != nil {
		logger.Warn("加载元数据失败", logger.ErrorField(err))
		metadata = make(map[string]string)
	}

	// 获取内容类型
	contentType := metadata["content_type"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	result := &attachment.DownloadResult{
		Reader:       file,
		Size:         stat.Size(),
		ContentType:  contentType,
		ETag:         metadata["etag"],
		LastModified: stat.ModTime(),
		Metadata:     metadata,
	}

	return result, nil
}

// Delete 删除文件
func (s *EnhancedLocalStorage) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(s.basePath, path)

	// 删除文件
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，认为删除成功
		}
		return fmt.Errorf("删除文件失败: %w", err)
	}

	// 删除元数据文件
	metadataPath := fullPath + ".meta"
	os.Remove(metadataPath) // 忽略错误

	return nil
}

// Exists 检查文件是否存在
func (s *EnhancedLocalStorage) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := filepath.Join(s.basePath, path)

	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("检查文件存在性失败: %w", err)
	}

	return true, nil
}

// GetURL 获取文件访问URL
func (s *EnhancedLocalStorage) GetURL(ctx context.Context, path string, options attachment.URLOptions) (string, error) {
	// 检查文件是否存在
	exists, err := s.Exists(ctx, path)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("文件不存在: %s", path)
	}

	// 生成基础URL
	baseURL := fmt.Sprintf("/api/attachments/read/%s", path)

	// 添加查询参数
	if len(options.QueryParams) > 0 {
		params := make([]string, 0, len(options.QueryParams))
		for key, value := range options.QueryParams {
			params = append(params, fmt.Sprintf("%s=%s", key, value))
		}
		baseURL += "?" + strings.Join(params, "&")
	}

	return baseURL, nil
}

// GetMetadata 获取文件元数据
func (s *EnhancedLocalStorage) GetMetadata(ctx context.Context, path string) (*attachment.FileMetadata, error) {
	fullPath := filepath.Join(s.basePath, path)

	// 获取文件信息
	stat, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("文件不存在: %s", path)
		}
		return nil, fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 加载元数据
	metadata, err := s.loadMetadata(fullPath)
	if err != nil {
		logger.Warn("加载元数据失败", logger.ErrorField(err))
		metadata = make(map[string]string)
	}

	// 获取内容类型
	contentType := metadata["content_type"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	result := &attachment.FileMetadata{
		Path:         path,
		Size:         stat.Size(),
		ContentType:  contentType,
		ETag:         metadata["etag"],
		LastModified: stat.ModTime(),
		CreatedAt:    stat.ModTime(), // 本地存储无法区分创建时间和修改时间
		Metadata:     metadata,
		Permissions:  stat.Mode().String(),
		IsDirectory:  stat.IsDir(),
	}

	return result, nil
}

// Copy 复制文件
func (s *EnhancedLocalStorage) Copy(ctx context.Context, sourcePath, destPath string) error {
	sourceFullPath := filepath.Join(s.basePath, sourcePath)
	destFullPath := filepath.Join(s.basePath, destPath)

	// 打开源文件
	sourceFile, err := os.Open(sourceFullPath)
	if err != nil {
		return fmt.Errorf("打开源文件失败: %w", err)
	}
	defer sourceFile.Close()

	// 创建目标目录
	destDir := filepath.Dir(destFullPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("创建目标目录失败: %w", err)
	}

	// 创建目标文件
	destFile, err := os.Create(destFullPath)
	if err != nil {
		return fmt.Errorf("创建目标文件失败: %w", err)
	}
	defer destFile.Close()

	// 复制文件内容
	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return fmt.Errorf("复制文件内容失败: %w", err)
	}

	// 复制元数据
	sourceMetadataPath := sourceFullPath + ".meta"
	destMetadataPath := destFullPath + ".meta"
	if _, err := os.Stat(sourceMetadataPath); err == nil {
		s.copyFile(sourceMetadataPath, destMetadataPath)
	}

	return nil
}

// Move 移动文件
func (s *EnhancedLocalStorage) Move(ctx context.Context, sourcePath, destPath string) error {
	sourceFullPath := filepath.Join(s.basePath, sourcePath)
	destFullPath := filepath.Join(s.basePath, destPath)

	// 创建目标目录
	destDir := filepath.Dir(destFullPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("创建目标目录失败: %w", err)
	}

	// 移动文件
	if err := os.Rename(sourceFullPath, destFullPath); err != nil {
		return fmt.Errorf("移动文件失败: %w", err)
	}

	// 移动元数据文件
	sourceMetadataPath := sourceFullPath + ".meta"
	destMetadataPath := destFullPath + ".meta"
	if _, err := os.Stat(sourceMetadataPath); err == nil {
		os.Rename(sourceMetadataPath, destMetadataPath)
	}

	return nil
}

// List 列出文件
func (s *EnhancedLocalStorage) List(ctx context.Context, prefix string, options attachment.ListOptions) (*attachment.ListResult, error) {
	fullPrefix := filepath.Join(s.basePath, prefix)

	var files []*attachment.FileMetadata
	var directories []string

	// 遍历目录
	err := filepath.Walk(fullPrefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 计算相对路径
		relPath, err := filepath.Rel(s.basePath, path)
		if err != nil {
			return err
		}

		// 跳过元数据文件
		if strings.HasSuffix(path, ".meta") {
			return nil
		}

		if info.IsDir() {
			if relPath != prefix { // 不包含根目录
				directories = append(directories, relPath)
			}
			if !options.Recursive {
				return filepath.SkipDir
			}
		} else {
			// 加载元数据
			metadata, err := s.loadMetadata(path)
			if err != nil {
				metadata = make(map[string]string)
			}

			contentType := metadata["content_type"]
			if contentType == "" {
				contentType = "application/octet-stream"
			}

			fileMetadata := &attachment.FileMetadata{
				Path:         relPath,
				Size:         info.Size(),
				ContentType:  contentType,
				ETag:         metadata["etag"],
				LastModified: info.ModTime(),
				CreatedAt:    info.ModTime(),
				Metadata:     metadata,
				Permissions:  info.Mode().String(),
				IsDirectory:  false,
			}

			files = append(files, fileMetadata)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("遍历目录失败: %w", err)
	}

	// 排序
	s.sortFiles(files, options.SortBy, options.SortOrder)

	// 分页
	total := int64(len(files))
	start := options.Offset
	end := start + options.Limit

	if start > len(files) {
		start = len(files)
	}
	if end > len(files) {
		end = len(files)
	}

	if options.Limit > 0 {
		files = files[start:end]
	}

	result := &attachment.ListResult{
		Files:       files,
		Directories: directories,
		Total:       total,
		HasMore:     end < int(total),
	}

	return result, nil
}

// GetStorageInfo 获取存储信息
func (s *EnhancedLocalStorage) GetStorageInfo(ctx context.Context) (*attachment.StorageInfo, error) {
	// 计算存储使用情况
	var totalSize int64
	var fileCount int64

	err := filepath.Walk(s.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && !strings.HasSuffix(path, ".meta") {
			totalSize += info.Size()
			fileCount++
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("计算存储使用情况失败: %w", err)
	}

	info := &attachment.StorageInfo{
		Provider:  "local",
		Region:    "local",
		Bucket:    s.basePath,
		TotalSize: totalSize,
		UsedSize:  totalSize,
		FileCount: fileCount,
		Features:  s.features,
		Limits: map[string]int64{
			"max_file_size": s.maxFileSize,
		},
		Metadata: map[string]string{
			"base_path":   s.basePath,
			"permissions": s.permissions.String(),
		},
	}

	return info, nil
}

// saveMetadata 保存元数据
func (s *EnhancedLocalStorage) saveMetadata(filePath string, result *attachment.UploadResult) error {
	metadataPath := filePath + ".meta"

	metadata := map[string]string{
		"content_type": result.ContentType,
		"etag":         result.ETag,
		"uploaded_at":  result.UploadedAt.Format(time.RFC3339),
		"size":         fmt.Sprintf("%d", result.Size),
	}

	// 合并用户元数据
	for key, value := range result.Metadata {
		metadata[key] = value
	}

	// 简化实现：使用JSON格式保存元数据
	// 实际应用中可以使用更高效的格式
	file, err := os.Create(metadataPath)
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range metadata {
		fmt.Fprintf(file, "%s=%s\n", key, value)
	}

	return nil
}

// loadMetadata 加载元数据
func (s *EnhancedLocalStorage) loadMetadata(filePath string) (map[string]string, error) {
	metadataPath := filePath + ".meta"

	file, err := os.Open(metadataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	metadata := make(map[string]string)

	// 简化实现：解析键值对格式
	// 实际应用中应该使用更健壮的解析方法
	buffer := make([]byte, 4096)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	content := string(buffer[:n])
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			metadata[parts[0]] = parts[1]
		}
	}

	return metadata, nil
}

// copyFile 复制文件
func (s *EnhancedLocalStorage) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// sortFiles 排序文件
func (s *EnhancedLocalStorage) sortFiles(files []*attachment.FileMetadata, sortBy, sortOrder string) {
	if sortBy == "" {
		sortBy = "name"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

	sort.Slice(files, func(i, j int) bool {
		var less bool

		switch sortBy {
		case "name":
			less = files[i].Path < files[j].Path
		case "size":
			less = files[i].Size < files[j].Size
		case "modified":
			less = files[i].LastModified.Before(files[j].LastModified)
		default:
			less = files[i].Path < files[j].Path
		}

		if sortOrder == "desc" {
			less = !less
		}

		return less
	})
}
