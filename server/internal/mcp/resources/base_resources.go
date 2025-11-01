package resources

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// ResourceService 资源服务接口
type ResourceService interface {
	// GetResources 获取所有可用资源
	GetResources(ctx context.Context) ([]protocol.MCPResource, error)

	// ReadResource 读取指定资源
	ReadResource(ctx context.Context, uri string) (*protocol.MCPResourceContent, error)
}

// BaseResourceService 基础资源服务实现
type BaseResourceService struct {
	resources map[string]Resource
}

// Resource 资源接口
type Resource interface {
	// GetInfo 获取资源信息
	GetInfo() protocol.MCPResource

	// Read 读取资源内容
	Read(ctx context.Context, uri string) (*protocol.MCPResourceContent, error)

	// ValidateURI 验证URI
	ValidateURI(uri string) error
}

// NewBaseResourceService 创建基础资源服务
func NewBaseResourceService() *BaseResourceService {
	service := &BaseResourceService{
		resources: make(map[string]Resource),
	}

	// 注册默认资源
	service.registerDefaultResources()

	return service
}

// registerDefaultResources 注册默认资源
func (s *BaseResourceService) registerDefaultResources() {
	// 表结构资源
	s.RegisterResource(NewTableSchemaResource())

	// 数据资源
	s.RegisterResource(NewTableDataResource())

	// 元数据资源
	s.RegisterResource(NewTableMetadataResource())
}

// RegisterResource 注册资源
func (s *BaseResourceService) RegisterResource(resource Resource) {
	info := resource.GetInfo()
	s.resources[info.URI] = resource
}

// GetResources 获取所有可用资源
func (s *BaseResourceService) GetResources(ctx context.Context) ([]protocol.MCPResource, error) {
	resources := make([]protocol.MCPResource, 0, len(s.resources))

	for _, resource := range s.resources {
		resources = append(resources, resource.GetInfo())
	}

	return resources, nil
}

// ReadResource 读取指定资源
func (s *BaseResourceService) ReadResource(ctx context.Context, uri string) (*protocol.MCPResourceContent, error) {
	// 查找匹配的资源
	for _, resource := range s.resources {
		if err := resource.ValidateURI(uri); err == nil {
			return resource.Read(ctx, uri)
		}
	}

	return nil, fmt.Errorf("resource not found: %s", uri)
}

// parseTableURI 解析表URI
// 格式: table://{space_id}/{table_id}/schema
// 格式: data://{space_id}/{table_id}/records
func parseTableURI(uri string) (spaceID, tableID, resourceType string, err error) {
	// 简单的URI解析，实际项目中可能需要更复杂的解析逻辑
	if len(uri) < 10 {
		return "", "", "", fmt.Errorf("invalid URI format")
	}

	// 移除协议前缀
	var prefix string
	if uri[:8] == "table://" {
		prefix = "table://"
		resourceType = "schema"
	} else if uri[:7] == "data://" {
		prefix = "data://"
		resourceType = "records"
	} else if uri[:12] == "metadata://" {
		prefix = "metadata://"
		resourceType = "metadata"
	} else {
		return "", "", "", fmt.Errorf("unsupported URI scheme")
	}

	// 解析路径
	path := uri[len(prefix):]
	parts := make([]string, 0)
	current := ""

	for _, char := range path {
		if char == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}

	if current != "" {
		parts = append(parts, current)
	}

	if len(parts) < 2 {
		return "", "", "", fmt.Errorf("URI must contain space_id and table_id")
	}

	spaceID = parts[0]
	tableID = parts[1]

	// 如果有第三个部分，使用它作为资源类型
	if len(parts) > 2 {
		resourceType = parts[2]
	}

	return spaceID, tableID, resourceType, nil
}
