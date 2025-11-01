package application

import (
	"context"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CountService Count计算服务
// 专门负责Count字段的计算
type CountService struct {
	errorService *ErrorService
}

// NewCountService 创建Count计算服务
func NewCountService(errorService *ErrorService) *CountService {
	return &CountService{
		errorService: errorService,
	}
}

// Calculate 计算Count字段
func (s *CountService) Calculate(ctx context.Context, record *entity.Record, field *fieldEntity.Field) error {
	// 1. 获取Count配置
	options := field.Options()
	if options == nil || options.Link == nil {
		return s.errorService.HandleBusinessLogicError(ctx, "CountService.Calculate", "count options not configured")
	}

	// 使用Link字段配置（当前实现）
	// 未来改进：定义专门的CountOptions配置
	linkFieldID := options.Link.LinkedTableID

	// 2. 获取Link字段的值
	recordData := record.Data().ToMap()
	linkValue := recordData[linkFieldID]

	if linkValue == nil {
		// 无关联记录，设置为0
		s.updateRecordField(record, field, 0)
		return nil
	}

	// 3. 计算关联记录数量
	linkedRecordIDs := s.extractRecordIDs(linkValue)
	count := len(linkedRecordIDs)

	// 4. 更新记录数据
	s.updateRecordField(record, field, count)

	logger.Info("count calculation completed",
		logger.String("field_id", field.ID().String()),
		logger.String("link_field_id", linkFieldID),
		logger.Int("count", count))

	return nil
}

// extractRecordIDs 从Link字段值中提取Record IDs
func (s *CountService) extractRecordIDs(linkValue interface{}) []string {
	if linkValue == nil {
		return []string{}
	}

	switch v := linkValue.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if id, ok := item.(string); ok {
				result = append(result, id)
			}
		}
		return result
	default:
		return []string{}
	}
}

// updateRecordField 更新记录字段值
func (s *CountService) updateRecordField(record *entity.Record, field *fieldEntity.Field, value interface{}) {
	recordData := record.Data().ToMap()
	recordData[field.ID().String()] = value

	// 创建新的记录数据
	newData, err := valueobject.NewRecordData(recordData)
	if err != nil {
		logger.Error("failed to create new record data",
			logger.String("record_id", record.ID().String()),
			logger.String("field_id", field.ID().String()),
			logger.ErrorField(err))
		return
	}

	// 更新记录
	record.Update(newData, "system") // TODO: 从上下文获取实际的更新者
}
