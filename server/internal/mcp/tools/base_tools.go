package tools

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// ToolService 工具服务接口
type ToolService interface {
	// GetTools 获取所有可用工具
	GetTools(ctx context.Context) ([]protocol.MCPTool, error)

	// CallTool 调用指定工具
	CallTool(ctx context.Context, name string, arguments map[string]interface{}) (*protocol.MCPToolResult, error)
}

// BaseToolService 基础工具服务实现
type BaseToolService struct {
	tools map[string]Tool
}

// Tool 工具接口
type Tool interface {
	// GetInfo 获取工具信息
	GetInfo() protocol.MCPTool

	// Execute 执行工具
	Execute(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPToolResult, error)

	// ValidateArguments 验证参数
	ValidateArguments(arguments map[string]interface{}) error
}

// NewBaseToolServiceWithDependencies 创建带依赖注入的工具服务
func NewBaseToolServiceWithDependencies(
	tableService *application.TableService,
	fieldService *application.FieldService,
	recordService *application.RecordService,
) *BaseToolService {
	service := &BaseToolService{
		tools: make(map[string]Tool),
	}

	// 注册带依赖的工具
	service.registerToolsWithDependencies(tableService, fieldService, recordService)

	return service
}

// registerToolsWithDependencies 注册带依赖的工具
func (s *BaseToolService) registerToolsWithDependencies(
	tableService *application.TableService,
	fieldService *application.FieldService,
	recordService *application.RecordService,
) {
	// 结构管理工具
	s.RegisterTool(NewGetTableSchemaTool(tableService, fieldService))
	s.RegisterTool(NewListTablesTool(tableService))

	// 数据操作工具
	s.RegisterTool(NewCreateRecordTool(recordService))
	// TODO: 以下工具需要实现服务注入
	s.RegisterTool(NewUpdateRecordTool())
	s.RegisterTool(NewDeleteRecordTool())

	// 数据查询工具
	// TODO: 以下工具需要实现服务注入
	s.RegisterTool(NewQueryRecordsTool())
	s.RegisterTool(NewSearchRecordsTool())
}

// NewBaseToolService 创建基础工具服务（向后兼容）
func NewBaseToolService() *BaseToolService {
	service := &BaseToolService{
		tools: make(map[string]Tool),
	}

	// 注册默认工具（占位符实现）
	service.registerDefaultTools()

	return service
}

// registerDefaultTools 注册默认工具（占位符实现）
// 注意：这些工具没有服务依赖，只提供占位符实现
func (s *BaseToolService) registerDefaultTools() {
	// 数据查询工具
	s.RegisterTool(NewQueryRecordsTool())
	s.RegisterTool(NewSearchRecordsTool())

	// 数据操作工具（占位符实现，没有服务依赖）
	s.RegisterTool(NewUpdateRecordTool())
	s.RegisterTool(NewDeleteRecordTool())

	// 结构管理工具（占位符实现，没有服务依赖）
	// 注意：CreateRecordTool、GetTableSchemaTool、ListTablesTool 需要服务依赖，不能在这里注册
}

// RegisterTool 注册工具
func (s *BaseToolService) RegisterTool(tool Tool) {
	info := tool.GetInfo()
	s.tools[info.Name] = tool
}

// GetTools 获取所有可用工具
func (s *BaseToolService) GetTools(ctx context.Context) ([]protocol.MCPTool, error) {
	tools := make([]protocol.MCPTool, 0, len(s.tools))

	for _, tool := range s.tools {
		tools = append(tools, tool.GetInfo())
	}

	return tools, nil
}

// CallTool 调用指定工具
func (s *BaseToolService) CallTool(ctx context.Context, name string, arguments map[string]interface{}) (*protocol.MCPToolResult, error) {
	tool, exists := s.tools[name]
	if !exists {
		return nil, fmt.Errorf("tool '%s' not found", name)
	}

	// 验证参数
	if err := tool.ValidateArguments(arguments); err != nil {
		return nil, fmt.Errorf("invalid arguments for tool '%s': %w", name, err)
	}

	// 执行工具
	return tool.Execute(ctx, arguments)
}

// validateRequiredString 验证必需的字符串参数
func validateRequiredString(arguments map[string]interface{}, key string) (string, error) {
	value, exists := arguments[key]
	if !exists {
		return "", fmt.Errorf("required argument '%s' is missing", key)
	}

	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("argument '%s' must be a string", key)
	}

	if str == "" {
		return "", fmt.Errorf("argument '%s' cannot be empty", key)
	}

	return str, nil
}

// validateOptionalString 验证可选的字符串参数
func validateOptionalString(arguments map[string]interface{}, key string) (string, error) {
	value, exists := arguments[key]
	if !exists {
		return "", nil
	}

	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("argument '%s' must be a string", key)
	}

	return str, nil
}

// validateOptionalInt 验证可选的整数参数
func validateOptionalInt(arguments map[string]interface{}, key string) (int, error) {
	value, exists := arguments[key]
	if !exists {
		return 0, nil
	}

	switch v := value.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("argument '%s' must be an integer", key)
	}
}

// validateOptionalBool 验证可选的布尔参数
func validateOptionalBool(arguments map[string]interface{}, key string) (bool, error) {
	value, exists := arguments[key]
	if !exists {
		return false, nil
	}

	boolVal, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("argument '%s' must be a boolean", key)
	}

	return boolVal, nil
}
