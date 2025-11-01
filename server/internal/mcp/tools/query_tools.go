package tools

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// QueryRecordsTool 查询记录工具
type QueryRecordsTool struct {
	// 这里将来会注入记录仓储
	// recordRepo recordRepo.RecordRepository
}

// NewQueryRecordsTool 创建查询记录工具
func NewQueryRecordsTool() *QueryRecordsTool {
	return &QueryRecordsTool{}
}

// GetInfo 获取工具信息
func (t *QueryRecordsTool) GetInfo() protocol.MCPTool {
	return protocol.MCPTool{
		Name:        "query_records",
		Description: "查询指定表的记录数据",
		InputSchema: protocol.MCPToolInputSchema{
			Type: "object",
			Properties: map[string]protocol.MCPToolProperty{
				"space_id": {
					Type:        "string",
					Description: "空间ID",
				},
				"table_id": {
					Type:        "string",
					Description: "表ID",
				},
				"limit": {
					Type:        "integer",
					Description: "返回记录数量限制（默认100，最大1000）",
					Minimum:     func() *float64 { v := 1.0; return &v }(),
					Maximum:     func() *float64 { v := 1000.0; return &v }(),
				},
				"offset": {
					Type:        "integer",
					Description: "偏移量（默认0）",
					Minimum:     func() *float64 { v := 0.0; return &v }(),
				},
				"order_by": {
					Type:        "string",
					Description: "排序字段（可选）",
				},
				"order_direction": {
					Type:        "string",
					Description: "排序方向：asc 或 desc（默认desc）",
					Enum:        []string{"asc", "desc"},
				},
			},
			Required: []string{"space_id", "table_id"},
		},
	}
}

// ValidateArguments 验证参数
func (t *QueryRecordsTool) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "space_id"); err != nil {
		return err
	}
	if _, err := validateRequiredString(arguments, "table_id"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalInt(arguments, "limit"); err != nil {
		return err
	}
	if _, err := validateOptionalInt(arguments, "offset"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "order_by"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "order_direction"); err != nil {
		return err
	}

	// 验证排序方向
	if orderDir, exists := arguments["order_direction"]; exists {
		if orderDir != "asc" && orderDir != "desc" {
			return fmt.Errorf("order_direction must be 'asc' or 'desc'")
		}
	}

	return nil
}

// Execute 执行工具
func (t *QueryRecordsTool) Execute(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPToolResult, error) {
	spaceID, _ := validateRequiredString(arguments, "space_id")
	tableID, _ := validateRequiredString(arguments, "table_id")
	limit, _ := validateOptionalInt(arguments, "limit")
	offset, _ := validateOptionalInt(arguments, "offset")
	orderBy, _ := validateOptionalString(arguments, "order_by")
	orderDirection, _ := validateOptionalString(arguments, "order_direction")

	// 设置默认值
	if limit == 0 {
		limit = 100
	}
	if orderDirection == "" {
		orderDirection = "desc"
	}

	// TODO: 实现实际的记录查询逻辑
	// 这里需要集成 LuckDB 的记录仓储

	// 模拟查询结果
	result := map[string]interface{}{
		"space_id":        spaceID,
		"table_id":        tableID,
		"records":         []interface{}{},
		"total_count":     0,
		"returned_count":  0,
		"limit":           limit,
		"offset":          offset,
		"order_by":        orderBy,
		"order_direction": orderDirection,
		"message":         "查询功能待实现，需要集成 LuckDB 记录仓储",
	}

	return &protocol.MCPToolResult{
		Content: []protocol.MCPToolResultContent{
			{
				Type: "text",
				Text: fmt.Sprintf("查询表 %s 的记录（空间: %s）\n参数: limit=%d, offset=%d, order_by=%s, order_direction=%s\n\n注意：此功能需要集成 LuckDB 记录仓储才能正常工作",
					tableID, spaceID, limit, offset, orderBy, orderDirection),
			},
		},
		IsError:  false,
		Metadata: result,
	}, nil
}

// SearchRecordsTool 搜索记录工具
type SearchRecordsTool struct {
	// 这里将来会注入记录仓储
	// recordRepo recordRepo.RecordRepository
}

// NewSearchRecordsTool 创建搜索记录工具
func NewSearchRecordsTool() *SearchRecordsTool {
	return &SearchRecordsTool{}
}

// GetInfo 获取工具信息
func (t *SearchRecordsTool) GetInfo() protocol.MCPTool {
	return protocol.MCPTool{
		Name:        "search_records",
		Description: "在指定表中搜索记录",
		InputSchema: protocol.MCPToolInputSchema{
			Type: "object",
			Properties: map[string]protocol.MCPToolProperty{
				"space_id": {
					Type:        "string",
					Description: "空间ID",
				},
				"table_id": {
					Type:        "string",
					Description: "表ID",
				},
				"query": {
					Type:        "string",
					Description: "搜索查询字符串",
				},
				"fields": {
					Type:        "array",
					Description: "要搜索的字段列表（可选，默认搜索所有字段）",
					Items: &protocol.MCPToolProperty{
						Type: "string",
					},
				},
				"limit": {
					Type:        "integer",
					Description: "返回记录数量限制（默认50，最大500）",
					Minimum:     func() *float64 { v := 1.0; return &v }(),
					Maximum:     func() *float64 { v := 500.0; return &v }(),
				},
				"offset": {
					Type:        "integer",
					Description: "偏移量（默认0）",
					Minimum:     func() *float64 { v := 0.0; return &v }(),
				},
				"case_sensitive": {
					Type:        "boolean",
					Description: "是否区分大小写（默认false）",
				},
			},
			Required: []string{"space_id", "table_id", "query"},
		},
	}
}

// ValidateArguments 验证参数
func (t *SearchRecordsTool) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "space_id"); err != nil {
		return err
	}
	if _, err := validateRequiredString(arguments, "table_id"); err != nil {
		return err
	}
	if _, err := validateRequiredString(arguments, "query"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalInt(arguments, "limit"); err != nil {
		return err
	}
	if _, err := validateOptionalInt(arguments, "offset"); err != nil {
		return err
	}
	if _, err := validateOptionalBool(arguments, "case_sensitive"); err != nil {
		return err
	}

	// 验证字段列表
	if fields, exists := arguments["fields"]; exists {
		if fieldsList, ok := fields.([]interface{}); ok {
			for i, field := range fieldsList {
				if _, ok := field.(string); !ok {
					return fmt.Errorf("fields[%d] must be a string", i)
				}
			}
		} else {
			return fmt.Errorf("fields must be an array of strings")
		}
	}

	return nil
}

// Execute 执行工具
func (t *SearchRecordsTool) Execute(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPToolResult, error) {
	spaceID, _ := validateRequiredString(arguments, "space_id")
	tableID, _ := validateRequiredString(arguments, "table_id")
	query, _ := validateRequiredString(arguments, "query")
	limit, _ := validateOptionalInt(arguments, "limit")
	offset, _ := validateOptionalInt(arguments, "offset")
	caseSensitive, _ := validateOptionalBool(arguments, "case_sensitive")

	// 设置默认值
	if limit == 0 {
		limit = 50
	}

	// 获取字段列表
	var fields []string
	if fieldsArg, exists := arguments["fields"]; exists {
		if fieldsList, ok := fieldsArg.([]interface{}); ok {
			fields = make([]string, len(fieldsList))
			for i, field := range fieldsList {
				fields[i] = field.(string)
			}
		}
	}

	// TODO: 实现实际的记录搜索逻辑
	// 这里需要集成 LuckDB 的记录仓储和搜索功能

	// 模拟搜索结果
	result := map[string]interface{}{
		"space_id":       spaceID,
		"table_id":       tableID,
		"query":          query,
		"fields":         fields,
		"records":        []interface{}{},
		"total_count":    0,
		"returned_count": 0,
		"limit":          limit,
		"offset":         offset,
		"case_sensitive": caseSensitive,
		"message":        "搜索功能待实现，需要集成 LuckDB 记录仓储和搜索功能",
	}

	return &protocol.MCPToolResult{
		Content: []protocol.MCPToolResultContent{
			{
				Type: "text",
				Text: fmt.Sprintf("在表 %s 中搜索 '%s'（空间: %s）\n参数: limit=%d, offset=%d, case_sensitive=%t, fields=%v\n\n注意：此功能需要集成 LuckDB 记录仓储和搜索功能才能正常工作",
					tableID, query, spaceID, limit, offset, caseSensitive, fields),
			},
		},
		IsError:  false,
		Metadata: result,
	}, nil
}
