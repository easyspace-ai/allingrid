package storage

import (
	"context"
	"fmt"
	"mime"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// FileValidator 文件验证器实现
type FileValidator struct {
	logger *zap.Logger
}

// NewFileValidator 创建文件验证器
func NewFileValidator(logger *zap.Logger) *FileValidator {
	return &FileValidator{
		logger: logger,
	}
}

// ValidateFile 验证文件
func (v *FileValidator) ValidateFile(ctx context.Context, filename string, size int64, contentType string, allowedTypes []string, maxSize int64) error {
	// 验证文件大小
	if size <= 0 {
		return errors.ErrBadRequest.WithDetails("Invalid file size")
	}

	if size > maxSize {
		return errors.ErrBadRequest.WithDetails(fmt.Sprintf("File size %d exceeds maximum allowed size %d", size, maxSize))
	}

	// 验证文件类型
	if len(allowedTypes) > 0 {
		if !v.isAllowedType(contentType, allowedTypes) {
			return errors.ErrBadRequest.WithDetails(fmt.Sprintf("File type %s is not allowed", contentType))
		}
	}

	// 验证文件名
	if filename == "" {
		return errors.ErrBadRequest.WithDetails("Filename is required")
	}

	// 检查文件名是否包含路径遍历字符（../ 或 ..\）
	// 注意：不要使用 ContainsAny，因为它会匹配单个字符（如 .），导致正常的文件名被拒绝
	if strings.Contains(filename, "../") || strings.Contains(filename, "..\\") {
		return errors.ErrBadRequest.WithDetails("Filename contains invalid characters")
	}

	// 检查文件名是否包含反斜杠（Windows路径分隔符）
	if strings.Contains(filename, "\\") {
		return errors.ErrBadRequest.WithDetails("Filename contains invalid characters")
	}

	// 检查文件扩展名
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" {
		return errors.ErrBadRequest.WithDetails("File must have an extension")
	}

	// 检查是否为危险文件类型
	if v.isDangerousFileType(ext) {
		return errors.ErrBadRequest.WithDetails(fmt.Sprintf("File type %s is not allowed for security reasons", ext))
	}

	return nil
}

// GetMimeType 获取文件MIME类型
func (v *FileValidator) GetMimeType(filename string) string {
	mimeType := mime.TypeByExtension(filepath.Ext(filename))
	if mimeType == "" {
		// 如果无法通过扩展名确定MIME类型，使用默认值
		return "application/octet-stream"
	}
	return mimeType
}

// IsImage 检查是否为图片
func (v *FileValidator) IsImage(mimeType string) bool {
	return strings.HasPrefix(mimeType, "image/")
}

// IsVideo 检查是否为视频
func (v *FileValidator) IsVideo(mimeType string) bool {
	return strings.HasPrefix(mimeType, "video/")
}

// IsAudio 检查是否为音频
func (v *FileValidator) IsAudio(mimeType string) bool {
	return strings.HasPrefix(mimeType, "audio/")
}

// IsDocument 检查是否为文档
func (v *FileValidator) IsDocument(mimeType string) bool {
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
		"application/rtf",
		"application/json",
		"application/xml",
		"text/xml",
		"text/html",
		"text/css",
		"text/javascript",
		"application/javascript",
	}

	for _, docType := range documentTypes {
		if mimeType == docType {
			return true
		}
	}
	return false
}

// isAllowedType 检查文件类型是否被允许
func (v *FileValidator) isAllowedType(contentType string, allowedTypes []string) bool {
	// 提取 MIME 类型的主部分（去掉参数，如 charset=utf-8）
	baseType := strings.TrimSpace(strings.Split(contentType, ";")[0])
	
	for _, allowedType := range allowedTypes {
		// 精确匹配
		if contentType == allowedType || baseType == allowedType {
			return true
		}
		// 支持通配符匹配，如 "image/*"
		if strings.HasSuffix(allowedType, "/*") {
			prefix := strings.TrimSuffix(allowedType, "/*")
			if strings.HasPrefix(contentType, prefix+"/") || strings.HasPrefix(baseType, prefix+"/") {
				return true
			}
		}
	}
	return false
}

// isDangerousFileType 检查是否为危险文件类型
func (v *FileValidator) isDangerousFileType(ext string) bool {
	dangerousTypes := []string{
		".exe", ".bat", ".cmd", ".com", ".pif", ".scr", ".vbs", ".js", ".jar",
		".app", ".deb", ".pkg", ".dmg", ".iso", ".bin", ".sh", ".ps1", ".php",
		".asp", ".jsp", ".py", ".rb", ".pl", ".cgi", ".htaccess", ".htpasswd",
	}

	for _, dangerousType := range dangerousTypes {
		if ext == dangerousType {
			return true
		}
	}
	return false
}
