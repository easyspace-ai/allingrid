package application

import (
	"context"

	formulaPkg "github.com/easyspace-ai/luckdb/server/internal/domain/calculation/formula"
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// FormulaService 公式计算服务
// 专门负责公式字段的计算
type FormulaService struct {
	errorService *ErrorService
}

// NewFormulaService 创建公式计算服务
func NewFormulaService(errorService *ErrorService) *FormulaService {
	return &FormulaService{
		errorService: errorService,
	}
}

// Calculate 计算公式字段
func (s *FormulaService) Calculate(ctx context.Context, record *entity.Record, field *fieldEntity.Field) error {
	// 1. 获取公式配置
	options := field.Options()
	if options == nil || options.Formula == nil {
		return s.errorService.HandleBusinessLogicError(ctx, "FormulaService.Calculate", "formula options not configured")
	}

	expression := options.Formula.Expression
	if expression == "" {
		return s.errorService.HandleBusinessLogicError(ctx, "FormulaService.Calculate", "formula expression is empty")
	}

	// 2. 准备计算上下文
	context := s.buildCalculationContext(record, field)

	// 3. 执行公式计算
	timezone := "UTC" // 默认时区，后续可以从用户配置获取

	logger.Info("calculating formula field",
		logger.String("field_id", field.ID().String()),
		logger.String("expression", expression))

	result, err := formulaPkg.Evaluate(
		expression,
		context,
		context, // record context (使用相同的上下文数据)
		timezone,
	)

	if err != nil {
		return s.errorService.HandleBusinessLogicError(ctx, "FormulaService.Calculate",
			"formula evaluation failed: "+err.Error())
	}

	// 4. 更新记录数据
	recordData := record.Data().ToMap()
	recordData[field.ID().String()] = result.Value

	// 创建新的记录数据
	newData, err := valueobject.NewRecordData(recordData)
	if err != nil {
		return s.errorService.HandleBusinessLogicError(ctx, "FormulaService.Calculate",
			"failed to create new record data: "+err.Error())
	}

	// 更新记录
	record.Update(newData, "system") // TODO: 从上下文获取实际的更新者

	logger.Info("formula calculation completed",
		logger.String("field_id", field.ID().String()),
		logger.Any("result", result.Value))

	return nil
}

// buildCalculationContext 构建计算上下文
func (s *FormulaService) buildCalculationContext(record *entity.Record, field *fieldEntity.Field) map[string]interface{} {
	context := make(map[string]interface{})

	// 添加记录数据
	recordData := record.Data().ToMap()
	for key, value := range recordData {
		context[key] = value
	}

	// 添加字段信息
	context["_field_id"] = field.ID().String()
	context["_field_name"] = field.Name().String()
	context["_field_type"] = field.Type().String()
	context["_table_id"] = record.TableID()
	context["_record_id"] = record.ID().String()

	return context
}
