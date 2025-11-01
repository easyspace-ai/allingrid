package protocol

import (
	"encoding/json"
	"time"
)

// MCPVersion MCP 协议版本
const MCPVersion = "2024-11-05"

// MessageType MCP 消息类型
type MessageType string

const (
	// 初始化消息
	MessageTypeInitialize  MessageType = "initialize"
	MessageTypeInitialized MessageType = "initialized"

	// 工具相关消息
	MessageTypeToolsList MessageType = "tools/list"
	MessageTypeToolsCall MessageType = "tools/call"

	// 资源相关消息
	MessageTypeResourcesList MessageType = "resources/list"
	MessageTypeResourcesRead MessageType = "resources/read"

	// 提示相关消息
	MessageTypePromptsList MessageType = "prompts/list"
	MessageTypePromptsGet  MessageType = "prompts/get"

	// 错误消息
	MessageTypeError MessageType = "error"

	// 通知消息
	MessageTypeNotification MessageType = "notification"
)

// MCPRequest MCP 请求消息
type MCPRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// MCPResponse MCP 响应消息
type MCPResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      interface{}      `json:"id,omitempty"`
	Result  interface{}      `json:"result,omitempty"`
	Error   *MCPErrorMessage `json:"error,omitempty"`
}

// MCPErrorMessage MCP 错误信息
type MCPErrorMessage struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// InitializeRequest 初始化请求
type InitializeRequest struct {
	ProtocolVersion string                 `json:"protocolVersion"`
	Capabilities    ClientCapabilities     `json:"capabilities"`
	ClientInfo      ClientInfo             `json:"clientInfo"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// InitializeResponse 初始化响应
type InitializeResponse struct {
	ProtocolVersion string                 `json:"protocolVersion"`
	Capabilities    ServerCapabilities     `json:"capabilities"`
	ServerInfo      ServerInfo             `json:"serverInfo"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// ClientCapabilities 客户端能力
type ClientCapabilities struct {
	Roots    *RootsCapability    `json:"roots,omitempty"`
	Sampling *SamplingCapability `json:"sampling,omitempty"`
}

// ServerCapabilities 服务端能力
type ServerCapabilities struct {
	Tools     *ToolsCapability     `json:"tools,omitempty"`
	Resources *ResourcesCapability `json:"resources,omitempty"`
	Prompts   *PromptsCapability   `json:"prompts,omitempty"`
	Logging   *LoggingCapability   `json:"logging,omitempty"`
}

// RootsCapability 根目录能力
type RootsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// SamplingCapability 采样能力
type SamplingCapability struct{}

// ToolsCapability 工具能力
type ToolsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// ResourcesCapability 资源能力
type ResourcesCapability struct {
	Subscribe   bool `json:"subscribe,omitempty"`
	ListChanged bool `json:"listChanged,omitempty"`
}

// PromptsCapability 提示能力
type PromptsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// LoggingCapability 日志能力
type LoggingCapability struct{}

// ClientInfo 客户端信息
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ServerInfo 服务端信息
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Tool 工具定义
type Tool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

// ToolCallRequest 工具调用请求
type ToolCallRequest struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// ToolCallResponse 工具调用响应
type ToolCallResponse struct {
	Content []ToolCallContent `json:"content"`
	IsError bool              `json:"isError,omitempty"`
}

// ToolCallContent 工具调用内容
type ToolCallContent struct {
	Type     string                 `json:"type"`
	Text     string                 `json:"text,omitempty"`
	Data     interface{}            `json:"data,omitempty"`
	MimeType string                 `json:"mimeType,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Resource 资源定义
type Resource struct {
	URI         string                 `json:"uri"`
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	MimeType    string                 `json:"mimeType,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ResourceReadRequest 资源读取请求
type ResourceReadRequest struct {
	URI string `json:"uri"`
}

// ResourceReadResponse 资源读取响应
type ResourceReadResponse struct {
	Contents []ResourceContent `json:"contents"`
}

// ResourceContent 资源内容
type ResourceContent struct {
	URI      string                 `json:"uri"`
	MimeType string                 `json:"mimeType,omitempty"`
	Text     string                 `json:"text,omitempty"`
	Blob     string                 `json:"blob,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Prompt 提示定义
type Prompt struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Arguments   []PromptArgument       `json:"arguments,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// PromptArgument 提示参数
type PromptArgument struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// PromptGetRequest 提示获取请求
type PromptGetRequest struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// PromptGetResponse 提示获取响应
type PromptGetResponse struct {
	Description string                 `json:"description,omitempty"`
	Messages    []PromptMessage        `json:"messages"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// PromptMessage 提示消息
type PromptMessage struct {
	Role     string                 `json:"role"`
	Content  PromptMessageContent   `json:"content"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// PromptMessageContent 提示消息内容
type PromptMessageContent struct {
	Type     string                 `json:"type"`
	Text     string                 `json:"text,omitempty"`
	Data     interface{}            `json:"data,omitempty"`
	MimeType string                 `json:"mimeType,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Notification 通知消息
type Notification struct {
	Method string      `json:"method"`
	Params interface{} `json:"params,omitempty"`
}

// LoggingMessage 日志消息
type LoggingMessage struct {
	Level    string                 `json:"level"`
	Data     interface{}            `json:"data"`
	Logger   string                 `json:"logger,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// MCPTool MCP 工具定义
type MCPTool struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	InputSchema MCPToolInputSchema `json:"inputSchema"`
}

// MCPToolInputSchema 工具输入模式
type MCPToolInputSchema struct {
	Type                 string                     `json:"type"`
	Properties           map[string]MCPToolProperty `json:"properties,omitempty"`
	Required             []string                   `json:"required,omitempty"`
	AdditionalProperties *MCPToolProperty           `json:"additionalProperties,omitempty"`
}

// MCPToolProperty 工具属性定义
type MCPToolProperty struct {
	Type        string           `json:"type,omitempty"`
	Description string           `json:"description,omitempty"`
	Minimum     *float64         `json:"minimum,omitempty"`
	Maximum     *float64         `json:"maximum,omitempty"`
	Enum        []string         `json:"enum,omitempty"`
	Items       *MCPToolProperty `json:"items,omitempty"`
}

// MCPToolResult 工具执行结果
type MCPToolResult struct {
	Content  []MCPToolResultContent `json:"content"`
	IsError  bool                   `json:"isError"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// MCPToolResultContent 工具结果内容
type MCPToolResultContent struct {
	Type     string      `json:"type"`
	Text     string      `json:"text,omitempty"`
	Data     interface{} `json:"data,omitempty"`
	MimeType string      `json:"mimeType,omitempty"`
}

// MCPResource MCP 资源定义
type MCPResource struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

// MCPResourceContent MCP 资源内容
type MCPResourceContent struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType"`
	Text     string `json:"text,omitempty"`
	Blob     []byte `json:"blob,omitempty"`
}

// MCPPrompt MCP 提示定义
type MCPPrompt struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Arguments   []MCPPromptArgument `json:"arguments,omitempty"`
}

// MCPPromptArgument 提示参数
type MCPPromptArgument struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
}

// MCPPromptResult 提示结果
type MCPPromptResult struct {
	Description string                 `json:"description,omitempty"`
	Messages    []MCPPromptMessage     `json:"messages"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// MCPPromptMessage 提示消息
type MCPPromptMessage struct {
	Role     string                 `json:"role"`
	Content  MCPPromptContent       `json:"content"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// MCPPromptContent 提示内容
type MCPPromptContent struct {
	Type     string      `json:"type"`
	Text     string      `json:"text,omitempty"`
	Data     interface{} `json:"data,omitempty"`
	MimeType string      `json:"mimeType,omitempty"`
}

// MCPContext MCP 请求上下文
type MCPContext struct {
	RequestID string                 `json:"request_id"`
	UserID    string                 `json:"user_id,omitempty"`
	APIKeyID  string                 `json:"api_key_id,omitempty"`
	AuthType  string                 `json:"auth_type"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// NewMCPRequest 创建新的 MCP 请求
func NewMCPRequest(method string, params interface{}) *MCPRequest {
	return &MCPRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}
}

// NewMCPResponse 创建新的 MCP 响应
func NewMCPResponse(id interface{}, result interface{}) *MCPResponse {
	return &MCPResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
}

// NewMCPErrorResponse 创建新的 MCP 错误响应
func NewMCPErrorResponse(id interface{}, code int, message string, data interface{}) *MCPResponse {
	return &MCPResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &MCPErrorMessage{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
}

// ToJSON 将消息转换为 JSON
func (req *MCPRequest) ToJSON() ([]byte, error) {
	return json.Marshal(req)
}

// ToJSON 将响应转换为 JSON
func (resp *MCPResponse) ToJSON() ([]byte, error) {
	return json.Marshal(resp)
}

// FromJSON 从 JSON 解析请求
func (req *MCPRequest) FromJSON(data []byte) error {
	return json.Unmarshal(data, req)
}

// FromJSON 从 JSON 解析响应
func (resp *MCPResponse) FromJSON(data []byte) error {
	return json.Unmarshal(data, resp)
}

// ==================== 响应类型定义 ====================

// ToolsListResponse 工具列表响应
type ToolsListResponse struct {
	Tools []MCPTool `json:"tools"`
}

// ResourcesListResponse 资源列表响应
type ResourcesListResponse struct {
	Resources []MCPResource `json:"resources"`
}

// PromptsListResponse 提示列表响应
type PromptsListResponse struct {
	Prompts []MCPPrompt `json:"prompts"`
}
