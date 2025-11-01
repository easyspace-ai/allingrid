package calculators

import (
	"context"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
)

// Calculator 字段计算器接口
type Calculator interface {
	// Calculate 计算字段值
	Calculate(ctx context.Context, field *entity.Field, record map[string]interface{}, fieldMap map[string]*entity.Field) (interface{}, error)
	// CanCalculate 判断是否可以计算该字段
	CanCalculate(field *entity.Field) bool
}

// BaseCalculator 基础计算器
type BaseCalculator struct{}

// GetFieldValue 从记录中获取字段值
func (bc *BaseCalculator) GetFieldValue(record map[string]interface{}, fieldID string) interface{} {
	if record == nil {
		return nil
	}
	return record[fieldID]
}

// GetFieldValues 从记录中获取多个字段值
func (bc *BaseCalculator) GetFieldValues(record map[string]interface{}, fieldIDs []string) map[string]interface{} {
	values := make(map[string]interface{})
	for _, fieldID := range fieldIDs {
		values[fieldID] = bc.GetFieldValue(record, fieldID)
	}
	return values
}
