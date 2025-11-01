package prompts

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// AnalyzeDataPrompt 数据分析提示
type AnalyzeDataPrompt struct{}

// NewAnalyzeDataPrompt 创建数据分析提示
func NewAnalyzeDataPrompt() *AnalyzeDataPrompt {
	return &AnalyzeDataPrompt{}
}

// GetInfo 获取提示信息
func (p *AnalyzeDataPrompt) GetInfo() protocol.MCPPrompt {
	return protocol.MCPPrompt{
		Name:        "analyze_data",
		Description: "数据分析提示模板",
		Arguments: []protocol.MCPPromptArgument{
			{
				Name:        "data_description",
				Description: "数据描述",
				Required:    true,
			},
			{
				Name:        "analysis_type",
				Description: "分析类型：statistical, trend, pattern, summary",
				Required:    false,
			},
			{
				Name:        "focus_areas",
				Description: "关注领域（可选）",
				Required:    false,
			},
		},
	}
}

// ValidateArguments 验证参数
func (p *AnalyzeDataPrompt) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "data_description"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalString(arguments, "analysis_type"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "focus_areas"); err != nil {
		return err
	}

	// 验证分析类型
	if analysisType, exists := arguments["analysis_type"]; exists {
		validTypes := []string{"statistical", "trend", "pattern", "summary"}
		isValid := false
		for _, valid := range validTypes {
			if analysisType == valid {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("analysis_type must be one of: %v", validTypes)
		}
	}

	return nil
}

// Generate 生成提示内容
func (p *AnalyzeDataPrompt) Generate(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPPromptResult, error) {
	dataDescription, _ := validateRequiredString(arguments, "data_description")
	analysisType, _ := validateOptionalString(arguments, "analysis_type")
	focusAreas, _ := validateOptionalString(arguments, "focus_areas")

	// 设置默认分析类型
	if analysisType == "" {
		analysisType = "summary"
	}

	// 生成提示内容
	prompt := fmt.Sprintf(`请分析以下数据：

数据描述：%s

分析要求：
- 分析类型：%s
- 关注领域：%s

请提供：
1. 数据概览和关键指标
2. 数据质量评估
3. 主要发现和洞察
4. 潜在问题和建议
5. 可视化建议

请以结构化的方式呈现分析结果，并提供具体的数值和趋势。`, dataDescription, analysisType, focusAreas)

	return &protocol.MCPPromptResult{
		Description: "数据分析提示",
		Messages: []protocol.MCPPromptMessage{
			{
				Role: "user",
				Content: protocol.MCPPromptContent{
					Type: "text",
					Text: prompt,
				},
			},
		},
	}, nil
}

// QueryDataPrompt 数据查询提示
type QueryDataPrompt struct{}

// NewQueryDataPrompt 创建数据查询提示
func NewQueryDataPrompt() *QueryDataPrompt {
	return &QueryDataPrompt{}
}

// GetInfo 获取提示信息
func (p *QueryDataPrompt) GetInfo() protocol.MCPPrompt {
	return protocol.MCPPrompt{
		Name:        "query_data",
		Description: "数据查询提示模板",
		Arguments: []protocol.MCPPromptArgument{
			{
				Name:        "query_intent",
				Description: "查询意图描述",
				Required:    true,
			},
			{
				Name:        "data_source",
				Description: "数据源信息",
				Required:    false,
			},
			{
				Name:        "output_format",
				Description: "输出格式：table, json, csv, summary",
				Required:    false,
			},
		},
	}
}

// ValidateArguments 验证参数
func (p *QueryDataPrompt) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "query_intent"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalString(arguments, "data_source"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "output_format"); err != nil {
		return err
	}

	// 验证输出格式
	if outputFormat, exists := arguments["output_format"]; exists {
		validFormats := []string{"table", "json", "csv", "summary"}
		isValid := false
		for _, valid := range validFormats {
			if outputFormat == valid {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("output_format must be one of: %v", validFormats)
		}
	}

	return nil
}

// Generate 生成提示内容
func (p *QueryDataPrompt) Generate(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPPromptResult, error) {
	queryIntent, _ := validateRequiredString(arguments, "query_intent")
	dataSource, _ := validateOptionalString(arguments, "data_source")
	outputFormat, _ := validateOptionalString(arguments, "output_format")

	// 设置默认输出格式
	if outputFormat == "" {
		outputFormat = "table"
	}

	// 生成提示内容
	prompt := fmt.Sprintf(`请帮助我构建数据查询：

查询意图：%s
数据源：%s
输出格式：%s

请提供：
1. 查询策略和步骤
2. 需要的数据字段
3. 过滤条件和排序要求
4. 预期的查询结果
5. 查询优化建议

请以清晰的步骤说明如何执行这个查询，并提供示例查询语句。`, queryIntent, dataSource, outputFormat)

	return &protocol.MCPPromptResult{
		Description: "数据查询提示",
		Messages: []protocol.MCPPromptMessage{
			{
				Role: "user",
				Content: protocol.MCPPromptContent{
					Type: "text",
					Text: prompt,
				},
			},
		},
	}, nil
}

// AnalyzeSchemaPrompt 表结构分析提示
type AnalyzeSchemaPrompt struct{}

// NewAnalyzeSchemaPrompt 创建表结构分析提示
func NewAnalyzeSchemaPrompt() *AnalyzeSchemaPrompt {
	return &AnalyzeSchemaPrompt{}
}

// GetInfo 获取提示信息
func (p *AnalyzeSchemaPrompt) GetInfo() protocol.MCPPrompt {
	return protocol.MCPPrompt{
		Name:        "analyze_schema",
		Description: "表结构分析提示模板",
		Arguments: []protocol.MCPPromptArgument{
			{
				Name:        "schema_description",
				Description: "表结构描述",
				Required:    true,
			},
			{
				Name:        "analysis_goal",
				Description: "分析目标：optimization, validation, documentation, migration",
				Required:    false,
			},
			{
				Name:        "constraints",
				Description: "约束条件（可选）",
				Required:    false,
			},
		},
	}
}

// ValidateArguments 验证参数
func (p *AnalyzeSchemaPrompt) ValidateArguments(arguments map[string]interface{}) error {
	// 验证必需参数
	if _, err := validateRequiredString(arguments, "schema_description"); err != nil {
		return err
	}

	// 验证可选参数
	if _, err := validateOptionalString(arguments, "analysis_goal"); err != nil {
		return err
	}
	if _, err := validateOptionalString(arguments, "constraints"); err != nil {
		return err
	}

	// 验证分析目标
	if analysisGoal, exists := arguments["analysis_goal"]; exists {
		validGoals := []string{"optimization", "validation", "documentation", "migration"}
		isValid := false
		for _, valid := range validGoals {
			if analysisGoal == valid {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("analysis_goal must be one of: %v", validGoals)
		}
	}

	return nil
}

// Generate 生成提示内容
func (p *AnalyzeSchemaPrompt) Generate(ctx context.Context, arguments map[string]interface{}) (*protocol.MCPPromptResult, error) {
	schemaDescription, _ := validateRequiredString(arguments, "schema_description")
	analysisGoal, _ := validateOptionalString(arguments, "analysis_goal")
	constraints, _ := validateOptionalString(arguments, "constraints")

	// 设置默认分析目标
	if analysisGoal == "" {
		analysisGoal = "validation"
	}

	// 生成提示内容
	prompt := fmt.Sprintf(`请分析以下表结构：

表结构描述：%s

分析目标：%s
约束条件：%s

请提供：
1. 表结构概览和字段分析
2. 数据类型和约束评估
3. 索引和性能优化建议
4. 数据完整性检查
5. 改进建议和最佳实践

请以专业的角度分析表结构，并提供具体的优化建议。`, schemaDescription, analysisGoal, constraints)

	return &protocol.MCPPromptResult{
		Description: "表结构分析提示",
		Messages: []protocol.MCPPromptMessage{
			{
				Role: "user",
				Content: protocol.MCPPromptContent{
					Type: "text",
					Text: prompt,
				},
			},
		},
	}, nil
}
