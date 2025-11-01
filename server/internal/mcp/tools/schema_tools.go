package tools

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// GetTableSchemaTool 获取表结构工具
type GetTableSchemaTool struct {
	tableService *application.TableService
	fieldService *application.FieldService
}

// NewGetTableSchemaTool 创建获取表结构工具
func NewGetTableSchemaTool(tableService *application.TableService, fieldService *application.FieldService) *GetTableSchemaTool {
	return &GetTableSchemaTool{
		tableService: tableService,
		fieldService: fieldService,
	}
}

// GetInfo 获取工具信息
func (t *GetTableSchemaTool) GetInfo() protocol.MCPTool {
	return protocol.MCPTool{
		Name:        "get_table_schema",
		Description: "获取指定表的结构信息",
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
				"include_fields": {
					Type:        "boolean",
					Description: "是否包含字段信息（默认true）",
				},
				"include_metadata": {
					Type:        "boolean",
					Description: "是否包含元数据信息（默认true）",
				},
			},
			Required: []string{"space_id", "table_id"},
		},
	}
}

// ValidateArguments 验证参数
func (t *GetTableSchemaTool) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "space_id"); err != nil {
		return err
	}
	if _, err := validateRequiredString(arguments, "table_id"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalBool(arguments, "include_fields"); err != nil {
		return err
	}
	if _, err := validateOptionalBool(arguments, "include_metadata"); err != nil {
		return err
	}

	return nil
}

// Execute 执行工具
func (t *GetTableSchemaTool) Execute(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPToolResult, error) {
	spaceID, _ := validateRequiredString(arguments, "space_id")
	tableID, _ := validateRequiredString(arguments, "table_id")
	includeFields, _ := validateOptionalBool(arguments, "include_fields")
	includeMetadata, _ := validateOptionalBool(arguments, "include_metadata")

	// 设置默认值
	if includeFields == false && arguments["include_fields"] == nil {
		includeFields = true
	}
	if includeMetadata == false && arguments["include_metadata"] == nil {
		includeMetadata = true
	}

	// 获取表信息
	table, err := t.tableService.GetTable(ctx, tableID)
	if err != nil {
		return &protocol.MCPToolResult{
			Content: []protocol.MCPToolResultContent{
				{
					Type: "text",
					Text: fmt.Sprintf("获取表信息失败: %v", err),
				},
			},
			IsError: true,
		}, nil
	}

	result := map[string]interface{}{
		"space_id": spaceID,
		"table_id": tableID,
		"table": map[string]interface{}{
			"id":          table.ID,
			"name":        table.Name,
			"description": table.Description,
			"base_id":     table.BaseID,
			"field_count": table.FieldCount,
			"record_count": table.RecordCount,
			"created_at":  table.CreatedAt,
			"updated_at":  table.UpdatedAt,
		},
	}

	// 如果需要包含字段信息
	if includeFields {
		fields, err := t.fieldService.ListFields(ctx, tableID)
		if err != nil {
			result["fields_error"] = fmt.Sprintf("获取字段信息失败: %v", err)
		} else {
			fieldList := make([]map[string]interface{}, len(fields))
			for i, field := range fields {
				fieldList[i] = map[string]interface{}{
					"id":          field.ID,
					"name":        field.Name,
					"type":        field.Type,
					"description": field.Description,
					"options":     field.Options,
					"created_at":  field.CreatedAt,
					"updated_at":  field.UpdatedAt,
				}
			}
			result["fields"] = fieldList
		}
	}

	// 如果需要包含元数据信息
	if includeMetadata {
		fieldCount := 0
		if includeFields {
			if fields, ok := result["fields"].([]map[string]interface{}); ok {
				fieldCount = len(fields)
			}
		}
		result["metadata"] = map[string]interface{}{
			"field_count":  fieldCount,
			"record_count": 0, // TODO: 从记录服务获取
			"permissions":  []string{"read", "write"}, // TODO: 从权限服务获取
		}
	}

	return &protocol.MCPToolResult{
		Content: []protocol.MCPToolResultContent{
			{
				Type: "text",
				Text: fmt.Sprintf("成功获取表 %s 的结构信息（空间: %s）", table.Name, spaceID),
			},
		},
		IsError:  false,
		Metadata: result,
	}, nil
}

// ListTablesTool 列出表工具
type ListTablesTool struct {
	tableService *application.TableService
}

// NewListTablesTool 创建列出表工具
func NewListTablesTool(tableService *application.TableService) *ListTablesTool {
	return &ListTablesTool{
		tableService: tableService,
	}
}

// GetInfo 获取工具信息
func (t *ListTablesTool) GetInfo() protocol.MCPTool {
	return protocol.MCPTool{
		Name:        "list_tables",
		Description: "列出指定空间中的所有表",
		InputSchema: protocol.MCPToolInputSchema{
			Type: "object",
			Properties: map[string]protocol.MCPToolProperty{
				"space_id": {
					Type:        "string",
					Description: "空间ID",
				},
				"limit": {
					Type:        "integer",
					Description: "返回表数量限制（默认100，最大1000）",
					Minimum:     func() *float64 { v := 1.0; return &v }(),
					Maximum:     func() *float64 { v := 1000.0; return &v }(),
				},
				"offset": {
					Type:        "integer",
					Description: "偏移量（默认0）",
					Minimum:     func() *float64 { v := 0.0; return &v }(),
				},
				"include_metadata": {
					Type:        "boolean",
					Description: "是否包含元数据信息（默认false）",
				},
				"order_by": {
					Type:        "string",
					Description: "排序字段：name, created_at, updated_at（默认name）",
					Enum:        []string{"name", "created_at", "updated_at"},
				},
				"order_direction": {
					Type:        "string",
					Description: "排序方向：asc 或 desc（默认asc）",
					Enum:        []string{"asc", "desc"},
				},
			},
			Required: []string{"space_id"},
		},
	}
}

// ValidateArguments 验证参数
func (t *ListTablesTool) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "space_id"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalInt(arguments, "limit"); err != nil {
		return err
	}
	if _, err := validateOptionalInt(arguments, "offset"); err != nil {
		return err
	}
	if _, err := validateOptionalBool(arguments, "include_metadata"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "order_by"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "order_direction"); err != nil {
		return err
	}

	// 验证排序字段
	if orderBy, exists := arguments["order_by"]; exists {
		validOrderBy := []string{"name", "created_at", "updated_at"}
		isValid := false
		for _, valid := range validOrderBy {
			if orderBy == valid {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("order_by must be one of: %v", validOrderBy)
		}
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
func (t *ListTablesTool) Execute(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPToolResult, error) {
	spaceID, _ := validateRequiredString(arguments, "space_id")
	limit, _ := validateOptionalInt(arguments, "limit")
	offset, _ := validateOptionalInt(arguments, "offset")
	includeMetadata, _ := validateOptionalBool(arguments, "include_metadata")
	orderBy, _ := validateOptionalString(arguments, "order_by")
	orderDirection, _ := validateOptionalString(arguments, "order_direction")

	// 设置默认值
	if limit == 0 {
		limit = 100
	}
	if orderBy == "" {
		orderBy = "name"
	}
	if orderDirection == "" {
		orderDirection = "asc"
	}

	// TODO: 需要将 spaceID 转换为 baseID，或者修改 TableService.ListTables 接受 spaceID
	// 当前实现假设 spaceID 就是 baseID（这可能不正确）
	baseID := spaceID

	// 获取空间中的所有表
	tables, err := t.tableService.ListTables(ctx, baseID)
	if err != nil {
		return &protocol.MCPToolResult{
			Content: []protocol.MCPToolResultContent{
				{
					Type: "text",
					Text: fmt.Sprintf("获取表列表失败: %v", err),
				},
			},
			IsError: true,
		}, nil
	}

	// 转换为结果格式
	tableList := make([]map[string]interface{}, len(tables))
	for i, table := range tables {
		tableList[i] = map[string]interface{}{
			"id":          table.ID,
			"name":        table.Name,
			"description": table.Description,
			"base_id":     table.BaseID,
			"field_count": table.FieldCount,
			"record_count": table.RecordCount,
			"created_at":  table.CreatedAt,
			"updated_at":  table.UpdatedAt,
		}

		if includeMetadata {
			tableList[i]["metadata"] = map[string]interface{}{
				"field_count":  table.FieldCount,
				"record_count": table.RecordCount,
				"permissions":  []string{"read", "write"}, // TODO: 从权限服务获取
			}
		}
	}

	result := map[string]interface{}{
		"space_id":         spaceID,
		"tables":           tableList,
		"total_count":      len(tableList),
		"returned_count":   len(tableList),
		"limit":            limit,
		"offset":           offset,
		"order_by":         orderBy,
		"order_direction":  orderDirection,
		"include_metadata": includeMetadata,
	}

	return &protocol.MCPToolResult{
		Content: []protocol.MCPToolResultContent{
			{
				Type: "text",
				Text: fmt.Sprintf("成功获取空间 %s 中的表列表\n参数: limit=%d, offset=%d, order_by=%s, order_direction=%s, include_metadata=%t\n找到 %d 个表",
					spaceID, limit, offset, orderBy, orderDirection, includeMetadata, len(tableList)),
			},
		},
		IsError:  false,
		Metadata: result,
	}, nil
}
