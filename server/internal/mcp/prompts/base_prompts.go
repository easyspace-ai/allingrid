package prompts

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// PromptService 提示服务接口
type PromptService interface {
	// GetPrompts 获取所有可用提示
	GetPrompts(ctx context.Context) ([]protocol.MCPPrompt, error)

	// GetPrompt 获取指定提示
	GetPrompt(ctx context.Context, name string, arguments map[string]interface{}) (*protocol.MCPPromptResult, error)
}

// BasePromptService 基础提示服务实现
type BasePromptService struct {
	prompts map[string]Prompt
}

// Prompt 提示接口
type Prompt interface {
	// GetInfo 获取提示信息
	GetInfo() protocol.MCPPrompt

	// Generate 生成提示内容
	Generate(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPPromptResult, error)

	// ValidateArguments 验证参数
	ValidateArguments(arguments map[string]interface{}) error
}

// NewBasePromptService 创建基础提示服务
func NewBasePromptService() *BasePromptService {
	service := &BasePromptService{
		prompts: make(map[string]Prompt),
	}

	// 注册默认提示
	service.registerDefaultPrompts()

	return service
}

// registerDefaultPrompts 注册默认提示
func (s *BasePromptService) registerDefaultPrompts() {
	// 数据分析提示
	s.RegisterPrompt(NewAnalyzeDataPrompt())

	// 数据查询提示
	s.RegisterPrompt(NewQueryDataPrompt())

	// 表结构分析提示
	s.RegisterPrompt(NewAnalyzeSchemaPrompt())
}

// RegisterPrompt 注册提示
func (s *BasePromptService) RegisterPrompt(prompt Prompt) {
	info := prompt.GetInfo()
	s.prompts[info.Name] = prompt
}

// GetPrompts 获取所有可用提示
func (s *BasePromptService) GetPrompts(ctx context.Context) ([]protocol.MCPPrompt, error) {
	prompts := make([]protocol.MCPPrompt, 0, len(s.prompts))

	for _, prompt := range s.prompts {
		prompts = append(prompts, prompt.GetInfo())
	}

	return prompts, nil
}

// GetPrompt 获取指定提示
func (s *BasePromptService) GetPrompt(ctx context.Context, name string, arguments map[string]interface{}) (*protocol.MCPPromptResult, error) {
	prompt, exists := s.prompts[name]
	if !exists {
		return nil, fmt.Errorf("prompt '%s' not found", name)
	}

	// 验证参数
	if err := prompt.ValidateArguments(arguments); err != nil {
		return nil, fmt.Errorf("invalid arguments for prompt '%s': %w", name, err)
	}

	// 生成提示内容
	return prompt.Generate(ctx, arguments)
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
