package storage

import (
	"fmt"
	"strconv"

	"github.com/easyspace-ai/luckdb/server/internal/domain/attachment"
)

// DefaultStorageFactory 默认存储工厂
type DefaultStorageFactory struct{}

// NewDefaultStorageFactory 创建默认存储工厂
func NewDefaultStorageFactory() attachment.StorageFactory {
	return &DefaultStorageFactory{}
}

// CreateStorage 创建存储提供者
func (f *DefaultStorageFactory) CreateStorage(config attachment.StorageConfig) (attachment.StorageProvider, error) {
	switch config.Type {
	case attachment.StorageTypeLocal:
		return f.createLocalStorage(config.Config)
	case attachment.StorageTypeS3:
		return f.createS3Storage(config.Config)
	case attachment.StorageTypeMinio:
		return f.createMinioStorage(config.Config)
	default:
		return nil, fmt.Errorf("不支持的存储类型: %s", config.Type)
	}
}

// GetSupportedTypes 获取支持的存储类型
func (f *DefaultStorageFactory) GetSupportedTypes() []attachment.StorageType {
	return []attachment.StorageType{
		attachment.StorageTypeLocal,
		attachment.StorageTypeS3,
		attachment.StorageTypeMinio,
	}
}

// ValidateConfig 验证配置
func (f *DefaultStorageFactory) ValidateConfig(storageType attachment.StorageType, config map[string]string) error {
	switch storageType {
	case attachment.StorageTypeLocal:
		return f.validateLocalConfig(config)
	case attachment.StorageTypeS3:
		return f.validateS3Config(config)
	case attachment.StorageTypeMinio:
		return f.validateMinioConfig(config)
	default:
		return fmt.Errorf("不支持的存储类型: %s", storageType)
	}
}

// createLocalStorage 创建本地存储
func (f *DefaultStorageFactory) createLocalStorage(config map[string]string) (attachment.StorageProvider, error) {
	basePath := config["base_path"]
	if basePath == "" {
		return nil, fmt.Errorf("本地存储需要指定 base_path")
	}

	permissions := config["permissions"]
	if permissions == "" {
		permissions = "0644"
	}

	maxSizeStr := config["max_size"]
	var maxSize int64 = 0
	if maxSizeStr != "" {
		var err error
		maxSize, err = strconv.ParseInt(maxSizeStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("无效的 max_size: %s", maxSizeStr)
		}
	}

	localConfig := attachment.LocalStorageConfig{
		BasePath:    basePath,
		Permissions: permissions,
		MaxSize:     maxSize,
	}

	return NewEnhancedLocalStorage(localConfig), nil
}

// createS3Storage 创建S3存储
func (f *DefaultStorageFactory) createS3Storage(config map[string]string) (attachment.StorageProvider, error) {
	// 这里应该创建S3存储实现
	// 暂时返回错误，表示未实现
	return nil, fmt.Errorf("S3存储暂未实现")
}

// createMinioStorage 创建Minio存储
func (f *DefaultStorageFactory) createMinioStorage(config map[string]string) (attachment.StorageProvider, error) {
	// 这里应该创建Minio存储实现
	// 暂时返回错误，表示未实现
	return nil, fmt.Errorf("Minio存储暂未实现")
}

// validateLocalConfig 验证本地存储配置
func (f *DefaultStorageFactory) validateLocalConfig(config map[string]string) error {
	if config["base_path"] == "" {
		return fmt.Errorf("本地存储需要指定 base_path")
	}

	if maxSizeStr := config["max_size"]; maxSizeStr != "" {
		if _, err := strconv.ParseInt(maxSizeStr, 10, 64); err != nil {
			return fmt.Errorf("无效的 max_size: %s", maxSizeStr)
		}
	}

	return nil
}

// validateS3Config 验证S3存储配置
func (f *DefaultStorageFactory) validateS3Config(config map[string]string) error {
	required := []string{"region", "bucket", "access_key_id", "secret_access_key"}
	for _, key := range required {
		if config[key] == "" {
			return fmt.Errorf("S3存储需要指定 %s", key)
		}
	}
	return nil
}

// validateMinioConfig 验证Minio存储配置
func (f *DefaultStorageFactory) validateMinioConfig(config map[string]string) error {
	required := []string{"endpoint", "bucket", "access_key_id", "secret_access_key"}
	for _, key := range required {
		if config[key] == "" {
			return fmt.Errorf("Minio存储需要指定 %s", key)
		}
	}
	return nil
}

// StorageRegistry 存储注册表
type StorageRegistry struct {
	factories map[attachment.StorageType]attachment.StorageFactory
}

// NewStorageRegistry 创建存储注册表
func NewStorageRegistry() *StorageRegistry {
	registry := &StorageRegistry{
		factories: make(map[attachment.StorageType]attachment.StorageFactory),
	}

	// 注册默认工厂
	defaultFactory := NewDefaultStorageFactory()
	for _, storageType := range defaultFactory.GetSupportedTypes() {
		registry.factories[storageType] = defaultFactory
	}

	return registry
}

// RegisterFactory 注册存储工厂
func (r *StorageRegistry) RegisterFactory(storageType attachment.StorageType, factory attachment.StorageFactory) {
	r.factories[storageType] = factory
}

// GetFactory 获取存储工厂
func (r *StorageRegistry) GetFactory(storageType attachment.StorageType) (attachment.StorageFactory, error) {
	factory, exists := r.factories[storageType]
	if !exists {
		return nil, fmt.Errorf("未找到存储类型 %s 的工厂", storageType)
	}
	return factory, nil
}

// CreateStorage 创建存储提供者
func (r *StorageRegistry) CreateStorage(config attachment.StorageConfig) (attachment.StorageProvider, error) {
	factory, err := r.GetFactory(config.Type)
	if err != nil {
		return nil, err
	}

	return factory.CreateStorage(config)
}

// GetSupportedTypes 获取所有支持的存储类型
func (r *StorageRegistry) GetSupportedTypes() []attachment.StorageType {
	var types []attachment.StorageType
	for storageType := range r.factories {
		types = append(types, storageType)
	}
	return types
}

// ValidateConfig 验证配置
func (r *StorageRegistry) ValidateConfig(storageType attachment.StorageType, config map[string]string) error {
	factory, err := r.GetFactory(storageType)
	if err != nil {
		return err
	}

	return factory.ValidateConfig(storageType, config)
}

// 全局存储注册表
var globalStorageRegistry *StorageRegistry

// GetGlobalStorageRegistry 获取全局存储注册表
func GetGlobalStorageRegistry() *StorageRegistry {
	if globalStorageRegistry == nil {
		globalStorageRegistry = NewStorageRegistry()
	}
	return globalStorageRegistry
}

// StorageConfigBuilder 存储配置构建器
type StorageConfigBuilder struct {
	config attachment.StorageConfig
}

// NewStorageConfigBuilder 创建存储配置构建器
func NewStorageConfigBuilder(storageType attachment.StorageType) *StorageConfigBuilder {
	return &StorageConfigBuilder{
		config: attachment.StorageConfig{
			Type:     storageType,
			Config:   make(map[string]string),
			Features: []string{},
			Limits:   make(map[string]int64),
		},
	}
}

// SetConfig 设置配置项
func (b *StorageConfigBuilder) SetConfig(key, value string) *StorageConfigBuilder {
	b.config.Config[key] = value
	return b
}

// SetConfigs 批量设置配置项
func (b *StorageConfigBuilder) SetConfigs(configs map[string]string) *StorageConfigBuilder {
	for key, value := range configs {
		b.config.Config[key] = value
	}
	return b
}

// AddFeature 添加功能
func (b *StorageConfigBuilder) AddFeature(feature string) *StorageConfigBuilder {
	b.config.Features = append(b.config.Features, feature)
	return b
}

// SetLimit 设置限制
func (b *StorageConfigBuilder) SetLimit(key string, value int64) *StorageConfigBuilder {
	b.config.Limits[key] = value
	return b
}

// Build 构建配置
func (b *StorageConfigBuilder) Build() attachment.StorageConfig {
	return b.config
}

// Validate 验证配置
func (b *StorageConfigBuilder) Validate() error {
	registry := GetGlobalStorageRegistry()
	return registry.ValidateConfig(b.config.Type, b.config.Config)
}

// CreateStorage 创建存储提供者
func (b *StorageConfigBuilder) CreateStorage() (attachment.StorageProvider, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}

	registry := GetGlobalStorageRegistry()
	return registry.CreateStorage(b.config)
}

// 便捷函数

// NewLocalStorageConfig 创建本地存储配置
func NewLocalStorageConfig(basePath string) *StorageConfigBuilder {
	return NewStorageConfigBuilder(attachment.StorageTypeLocal).
		SetConfig("base_path", basePath)
}

// NewS3StorageConfig 创建S3存储配置
func NewS3StorageConfig(region, bucket, accessKeyID, secretAccessKey string) *StorageConfigBuilder {
	return NewStorageConfigBuilder(attachment.StorageTypeS3).
		SetConfig("region", region).
		SetConfig("bucket", bucket).
		SetConfig("access_key_id", accessKeyID).
		SetConfig("secret_access_key", secretAccessKey)
}

// NewMinioStorageConfig 创建Minio存储配置
func NewMinioStorageConfig(endpoint, bucket, accessKeyID, secretAccessKey string) *StorageConfigBuilder {
	return NewStorageConfigBuilder(attachment.StorageTypeMinio).
		SetConfig("endpoint", endpoint).
		SetConfig("bucket", bucket).
		SetConfig("access_key_id", accessKeyID).
		SetConfig("secret_access_key", secretAccessKey)
}
