package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/prompts"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/resources"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/tools"
)

// StdioMCPService stdio 模式的 MCP 服务
type StdioMCPService struct {
	config          *config.MCPConfig
	logger          *log.Logger
	toolService     tools.ToolService
	resourceService resources.ResourceService
	promptService   prompts.PromptService
}

// NewStdioMCPService 创建新的 stdio MCP 服务
func NewStdioMCPService(cfg *config.MCPConfig, logger *log.Logger) *StdioMCPService {
	if logger == nil {
		logger = log.New(os.Stderr, "[MCP-Stdio] ", log.LstdFlags|log.Lshortfile)
	}

	// 初始化服务
	toolService := tools.NewBaseToolService()
	resourceService := resources.NewBaseResourceService()
	promptService := prompts.NewBasePromptService()

	return &StdioMCPService{
		config:          cfg,
		logger:          logger,
		toolService:     toolService,
		resourceService: resourceService,
		promptService:   promptService,
	}
}

// Start 启动 stdio MCP 服务
func (s *StdioMCPService) Start() error {
	if !s.config.Enabled {
		s.logger.Println("MCP service is disabled")
		return nil
	}

	s.logger.Println("Starting MCP stdio service...")

	// 处理标准输入输出
	return s.handleStdio()
}

// Stop 停止 stdio MCP 服务
func (s *StdioMCPService) Stop(ctx context.Context) error {
	s.logger.Println("Stopping MCP stdio service...")
	return nil
}

// handleStdio 处理标准输入输出
func (s *StdioMCPService) handleStdio() error {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// 解析 JSON-RPC 请求
		var req protocol.MCPRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			s.sendError(nil, -32700, "Invalid JSON", nil)
			continue
		}

		// 处理请求
		resp, err := s.handleRequest(context.Background(), &req)
		if err != nil {
			s.sendError(&req.ID, -32603, err.Error(), nil)
			continue
		}

		// 发送响应
		if err := s.sendResponse(resp); err != nil {
			s.logger.Printf("Failed to send response: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from stdin: %w", err)
	}

	return nil
}

// handleRequest 处理 MCP 请求
func (s *StdioMCPService) handleRequest(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)
	case "tools/list":
		return s.handleToolsList(ctx, req)
	case "tools/call":
		return s.handleToolsCall(ctx, req)
	case "resources/list":
		return s.handleResourcesList(ctx, req)
	case "resources/read":
		return s.handleResourcesRead(ctx, req)
	case "prompts/list":
		return s.handlePromptsList(ctx, req)
	case "prompts/get":
		return s.handlePromptsGet(ctx, req)
	default:
		return nil, fmt.Errorf("unknown method: %s", req.Method)
	}
}

// handleInitialize 处理初始化请求
func (s *StdioMCPService) handleInitialize(req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]interface{}{
				"tools": map[string]interface{}{
					"listChanged": true,
				},
				"resources": map[string]interface{}{
					"listChanged": true,
				},
				"prompts": map[string]interface{}{
					"listChanged": true,
				},
				"logging": map[string]interface{}{},
			},
			"serverInfo": map[string]interface{}{
				"name":    "LuckDB MCP Server",
				"version": "1.0.0",
			},
		},
	}
	return response, nil
}

// handleToolsList 处理工具列表请求
func (s *StdioMCPService) handleToolsList(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	tools, err := s.toolService.GetTools(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tools: %w", err)
	}

	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: &protocol.ToolsListResponse{
			Tools: tools,
		},
	}
	return response, nil
}

// handleToolsCall 处理工具调用请求
func (s *StdioMCPService) handleToolsCall(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	// 解析参数
	var params struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}

	paramsBytes, err := json.Marshal(req.Params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	if err := json.Unmarshal(paramsBytes, &params); err != nil {
		return nil, fmt.Errorf("invalid tool call parameters: %w", err)
	}

	// 调用工具
	result, err := s.toolService.CallTool(ctx, params.Name, params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("failed to call tool %s: %w", params.Name, err)
	}

	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
	return response, nil
}

// handleResourcesList 处理资源列表请求
func (s *StdioMCPService) handleResourcesList(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	resources, err := s.resourceService.GetResources(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list resources: %w", err)
	}

	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: &protocol.ResourcesListResponse{
			Resources: resources,
		},
	}
	return response, nil
}

// handleResourcesRead 处理资源读取请求
func (s *StdioMCPService) handleResourcesRead(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	// 解析参数
	var params struct {
		URI string `json:"uri"`
	}

	paramsBytes, err := json.Marshal(req.Params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	if err := json.Unmarshal(paramsBytes, &params); err != nil {
		return nil, fmt.Errorf("invalid resource read parameters: %w", err)
	}

	// 读取资源
	content, err := s.resourceService.ReadResource(ctx, params.URI)
	if err != nil {
		return nil, fmt.Errorf("failed to read resource: %w", err)
	}

	// 转换内容类型
	contents := []protocol.ResourceContent{
		{
			URI:      content.URI,
			MimeType: content.MimeType,
			Text:     content.Text,
			Blob:     string(content.Blob),
		},
	}

	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: &protocol.ResourceReadResponse{
			Contents: contents,
		},
	}
	return response, nil
}

// handlePromptsList 处理提示列表请求
func (s *StdioMCPService) handlePromptsList(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	prompts, err := s.promptService.GetPrompts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list prompts: %w", err)
	}

	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: &protocol.PromptsListResponse{
			Prompts: prompts,
		},
	}
	return response, nil
}

// handlePromptsGet 处理提示获取请求
func (s *StdioMCPService) handlePromptsGet(ctx context.Context, req *protocol.MCPRequest) (*protocol.MCPResponse, error) {
	// 解析参数
	var params struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}

	paramsBytes, err := json.Marshal(req.Params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	if err := json.Unmarshal(paramsBytes, &params); err != nil {
		return nil, fmt.Errorf("invalid prompt get parameters: %w", err)
	}

	// 获取提示
	result, err := s.promptService.GetPrompt(ctx, params.Name, params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("failed to get prompt: %w", err)
	}

	// 转换消息类型
	messages := make([]protocol.PromptMessage, len(result.Messages))
	for i, m := range result.Messages {
		messages[i] = protocol.PromptMessage{
			Role: m.Role,
			Content: protocol.PromptMessageContent{
				Type:     m.Content.Type,
				Text:     m.Content.Text,
				Data:     m.Content.Data,
				MimeType: m.Content.MimeType,
			},
			Metadata: m.Metadata,
		}
	}

	response := &protocol.MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: &protocol.PromptGetResponse{
			Description: result.Description,
			Messages:    messages,
		},
	}
	return response, nil
}

// sendResponse 发送响应
func (s *StdioMCPService) sendResponse(resp *protocol.MCPResponse) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	_, err = fmt.Fprintln(os.Stdout, string(data))
	return err
}

// sendError 发送错误响应
func (s *StdioMCPService) sendError(id *interface{}, code int, message string, data interface{}) {
	errorResp := protocol.NewMCPErrorResponse(id, code, message, data)
	if err := s.sendResponse(errorResp); err != nil {
		s.logger.Printf("Failed to send error response: %v", err)
	}
}
