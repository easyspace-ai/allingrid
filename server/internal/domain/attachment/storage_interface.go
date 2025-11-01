package attachment

import (
	"context"
	"fmt"
	"io"
	"time"
)

// StorageProvider 存储提供者接口
type StorageProvider interface {
	// Upload 上传文件
	Upload(ctx context.Context, request UploadRequest) (*UploadResult, error)

	// Download 下载文件
	Download(ctx context.Context, path string) (*DownloadResult, error)

	// Delete 删除文件
	Delete(ctx context.Context, path string) error

	// Exists 检查文件是否存在
	Exists(ctx context.Context, path string) (bool, error)

	// GetURL 获取文件访问URL
	GetURL(ctx context.Context, path string, options URLOptions) (string, error)

	// GetMetadata 获取文件元数据
	GetMetadata(ctx context.Context, path string) (*FileMetadata, error)

	// Copy 复制文件
	Copy(ctx context.Context, sourcePath, destPath string) error

	// Move 移动文件
	Move(ctx context.Context, sourcePath, destPath string) error

	// List 列出文件
	List(ctx context.Context, prefix string, options ListOptions) (*ListResult, error)

	// GetStorageInfo 获取存储信息
	GetStorageInfo(ctx context.Context) (*StorageInfo, error)
}

// UploadRequest 上传请求
type UploadRequest struct {
	Path        string            `json:"path"`
	Reader      io.Reader         `json:"-"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata"`
	Options     UploadOptions     `json:"options"`
}

// UploadOptions 上传选项
type UploadOptions struct {
	Overwrite     bool              `json:"overwrite"`
	CreateDir     bool              `json:"create_dir"`
	Permissions   string            `json:"permissions"`
	Encryption    bool              `json:"encryption"`
	Compression   bool              `json:"compression"`
	CustomHeaders map[string]string `json:"custom_headers"`
}

// UploadResult 上传结果
type UploadResult struct {
	Path        string            `json:"path"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	ETag        string            `json:"etag"`
	VersionID   string            `json:"version_id"`
	URL         string            `json:"url"`
	Metadata    map[string]string `json:"metadata"`
	UploadedAt  time.Time         `json:"uploaded_at"`
}

// DownloadResult 下载结果
type DownloadResult struct {
	Reader       io.ReadCloser     `json:"-"`
	Size         int64             `json:"size"`
	ContentType  string            `json:"content_type"`
	ETag         string            `json:"etag"`
	LastModified time.Time         `json:"last_modified"`
	Metadata     map[string]string `json:"metadata"`
}

// URLOptions URL选项
type URLOptions struct {
	Expires     time.Duration     `json:"expires"`
	Method      string            `json:"method"` // GET, PUT, POST, DELETE
	Headers     map[string]string `json:"headers"`
	QueryParams map[string]string `json:"query_params"`
	Secure      bool              `json:"secure"`
}

// FileMetadata 文件元数据
type FileMetadata struct {
	Path         string            `json:"path"`
	Size         int64             `json:"size"`
	ContentType  string            `json:"content_type"`
	ETag         string            `json:"etag"`
	LastModified time.Time         `json:"last_modified"`
	CreatedAt    time.Time         `json:"created_at"`
	Metadata     map[string]string `json:"metadata"`
	Permissions  string            `json:"permissions"`
	IsDirectory  bool              `json:"is_directory"`
	VersionID    string            `json:"version_id"`
}

// ListOptions 列表选项
type ListOptions struct {
	Limit       int    `json:"limit"`
	Offset      int    `json:"offset"`
	Recursive   bool   `json:"recursive"`
	IncludeSize bool   `json:"include_size"`
	SortBy      string `json:"sort_by"`    // name, size, modified
	SortOrder   string `json:"sort_order"` // asc, desc
}

// ListResult 列表结果
type ListResult struct {
	Files       []*FileMetadata `json:"files"`
	Directories []string        `json:"directories"`
	Total       int64           `json:"total"`
	HasMore     bool            `json:"has_more"`
	NextToken   string          `json:"next_token"`
}

// StorageInfo 存储信息
type StorageInfo struct {
	Provider  string            `json:"provider"`
	Region    string            `json:"region"`
	Bucket    string            `json:"bucket"`
	TotalSize int64             `json:"total_size"`
	UsedSize  int64             `json:"used_size"`
	FileCount int64             `json:"file_count"`
	Features  []string          `json:"features"`
	Limits    map[string]int64  `json:"limits"`
	Metadata  map[string]string `json:"metadata"`
}

// StorageType 存储类型
type StorageType string

const (
	StorageTypeLocal StorageType = "local"
	StorageTypeS3    StorageType = "s3"
	StorageTypeOSS   StorageType = "oss"
	StorageTypeCOS   StorageType = "cos"
	StorageTypeGCS   StorageType = "gcs"
	StorageTypeMinio StorageType = "minio"
)

// StorageConfig 存储配置
type StorageConfig struct {
	Type     StorageType       `json:"type"`
	Config   map[string]string `json:"config"`
	Features []string          `json:"features"`
	Limits   map[string]int64  `json:"limits"`
}

// LocalStorageConfig 本地存储配置
type LocalStorageConfig struct {
	BasePath    string `json:"base_path"`
	Permissions string `json:"permissions"`
	MaxSize     int64  `json:"max_size"`
}

// S3StorageConfig S3存储配置
type S3StorageConfig struct {
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Endpoint        string `json:"endpoint"`
	UseSSL          bool   `json:"use_ssl"`
	PathStyle       bool   `json:"path_style"`
}

// StorageFactory 存储工厂接口
type StorageFactory interface {
	CreateStorage(config StorageConfig) (StorageProvider, error)
	GetSupportedTypes() []StorageType
	ValidateConfig(storageType StorageType, config map[string]string) error
}

// StorageManager 存储管理器
type StorageManager struct {
	providers map[string]StorageProvider
	factory   StorageFactory
	config    map[string]StorageConfig
}

// NewStorageManager 创建存储管理器
func NewStorageManager(factory StorageFactory) *StorageManager {
	return &StorageManager{
		providers: make(map[string]StorageProvider),
		factory:   factory,
		config:    make(map[string]StorageConfig),
	}
}

// RegisterProvider 注册存储提供者
func (m *StorageManager) RegisterProvider(name string, provider StorageProvider) {
	m.providers[name] = provider
}

// GetProvider 获取存储提供者
func (m *StorageManager) GetProvider(name string) (StorageProvider, error) {
	provider, exists := m.providers[name]
	if !exists {
		return nil, fmt.Errorf("存储提供者 %s 不存在", name)
	}
	return provider, nil
}

// CreateProvider 创建存储提供者
func (m *StorageManager) CreateProvider(name string, config StorageConfig) error {
	provider, err := m.factory.CreateStorage(config)
	if err != nil {
		return err
	}

	m.providers[name] = provider
	m.config[name] = config
	return nil
}

// GetDefaultProvider 获取默认存储提供者
func (m *StorageManager) GetDefaultProvider() (StorageProvider, error) {
	return m.GetProvider("default")
}

// MultiStorageProvider 多存储提供者
type MultiStorageProvider struct {
	primary   StorageProvider
	secondary []StorageProvider
	strategy  ReplicationStrategy
}

// ReplicationStrategy 复制策略
type ReplicationStrategy string

const (
	ReplicationStrategySync  ReplicationStrategy = "sync"  // 同步复制
	ReplicationStrategyAsync ReplicationStrategy = "async" // 异步复制
	ReplicationStrategyNone  ReplicationStrategy = "none"  // 不复制
)

// NewMultiStorageProvider 创建多存储提供者
func NewMultiStorageProvider(primary StorageProvider, secondary []StorageProvider, strategy ReplicationStrategy) *MultiStorageProvider {
	return &MultiStorageProvider{
		primary:   primary,
		secondary: secondary,
		strategy:  strategy,
	}
}

// Upload 上传文件到多个存储
func (m *MultiStorageProvider) Upload(ctx context.Context, request UploadRequest) (*UploadResult, error) {
	// 上传到主存储
	result, err := m.primary.Upload(ctx, request)
	if err != nil {
		return nil, err
	}

	// 根据策略复制到辅助存储
	switch m.strategy {
	case ReplicationStrategySync:
		for _, provider := range m.secondary {
			if _, err := provider.Upload(ctx, request); err != nil {
				// 记录错误但不影响主要上传结果
				// 这里应该记录日志
			}
		}
	case ReplicationStrategyAsync:
		// 异步复制到辅助存储
		go func() {
			for _, provider := range m.secondary {
				provider.Upload(context.Background(), request)
			}
		}()
	}

	return result, nil
}

// Download 从存储下载文件
func (m *MultiStorageProvider) Download(ctx context.Context, path string) (*DownloadResult, error) {
	// 优先从主存储下载
	result, err := m.primary.Download(ctx, path)
	if err == nil {
		return result, nil
	}

	// 如果主存储失败，尝试从辅助存储下载
	for _, provider := range m.secondary {
		if result, err := provider.Download(ctx, path); err == nil {
			return result, nil
		}
	}

	return nil, err
}

// Delete 删除文件
func (m *MultiStorageProvider) Delete(ctx context.Context, path string) error {
	// 从所有存储中删除
	var lastErr error

	if err := m.primary.Delete(ctx, path); err != nil {
		lastErr = err
	}

	for _, provider := range m.secondary {
		if err := provider.Delete(ctx, path); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Exists 检查文件是否存在
func (m *MultiStorageProvider) Exists(ctx context.Context, path string) (bool, error) {
	return m.primary.Exists(ctx, path)
}

// GetURL 获取文件访问URL
func (m *MultiStorageProvider) GetURL(ctx context.Context, path string, options URLOptions) (string, error) {
	return m.primary.GetURL(ctx, path, options)
}

// GetMetadata 获取文件元数据
func (m *MultiStorageProvider) GetMetadata(ctx context.Context, path string) (*FileMetadata, error) {
	return m.primary.GetMetadata(ctx, path)
}

// Copy 复制文件
func (m *MultiStorageProvider) Copy(ctx context.Context, sourcePath, destPath string) error {
	return m.primary.Copy(ctx, sourcePath, destPath)
}

// Move 移动文件
func (m *MultiStorageProvider) Move(ctx context.Context, sourcePath, destPath string) error {
	return m.primary.Move(ctx, sourcePath, destPath)
}

// List 列出文件
func (m *MultiStorageProvider) List(ctx context.Context, prefix string, options ListOptions) (*ListResult, error) {
	return m.primary.List(ctx, prefix, options)
}

// GetStorageInfo 获取存储信息
func (m *MultiStorageProvider) GetStorageInfo(ctx context.Context) (*StorageInfo, error) {
	return m.primary.GetStorageInfo(ctx)
}
