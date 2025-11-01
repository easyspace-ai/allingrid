package application

import (
	"context"
	"fmt"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/validation"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// TypecastService 类型转换服务
// 参考旧系统: TypeCastAndValidate (teable-develop/apps/nestjs-backend/src/features/record/typecast.validate.ts)
type TypecastService struct {
	fieldRepo repository.FieldRepository
	factory   *validation.ValidatorFactory
}

// NewTypecastService 创建类型转换服务
func NewTypecastService(fieldRepo repository.FieldRepository) *TypecastService {
	return &TypecastService{
		fieldRepo: fieldRepo,
		factory:   validation.NewValidatorFactory(),
	}
}

// ValidateAndTypecastRecord 验证并转换记录数据
//
// typecast:
//   - true: 尝试自动转换类型（宽松模式）
//   - false: 严格验证，不匹配则报错
//
// 参考旧系统: validateFieldsAndTypecast
func (s *TypecastService) ValidateAndTypecastRecord(
	ctx context.Context,
	tableID string,
	data map[string]interface{},
	typecast bool,
) (map[string]interface{}, error) {
	logger.Info("开始验证和类型转换",
		logger.String("table_id", tableID),
		logger.Bool("typecast", typecast),
		logger.Int("field_count", len(data)))

	// 1. 获取所有涉及的字段
	fields, err := s.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("获取字段列表失败: %w", err)
	}

	// 2. 构建字段映射（同时支持 field_id 和 field_name）
	fieldMapByID := make(map[string]*entity.Field)
	fieldMapByName := make(map[string]*entity.Field)
	for _, field := range fields {
		fieldMapByID[field.ID().String()] = field
		fieldMapByName[field.Name().String()] = field
	}

	// 3. 验证和转换每个字段的值
	result := make(map[string]interface{})

	for fieldKey, value := range data {
		// 先尝试通过 ID 查找，再尝试通过名称查找
		field, exists := fieldMapByID[fieldKey]
		if !exists {
			field, exists = fieldMapByName[fieldKey]
			if !exists {
				// ✅ 字段不存在的处理
				if typecast {
					// 宽松模式：跳过不存在的字段
					logger.Warn("字段不存在，跳过",
						logger.String("field_key", fieldKey))
					continue
				} else {
					// 严格模式：返回错误
					return nil, errors.ErrFieldNotFound.WithDetails(map[string]interface{}{
						"field_key": fieldKey,
						"table_id":  tableID,
					})
				}
			}
		}

		// 统一使用字段ID作为结果的键
		fieldID := field.ID().String()

		// 跳过计算字段（只读）
		if field.IsComputed() {
			logger.Debug("跳过计算字段",
				logger.String("field_id", fieldID),
				logger.String("field_name", field.Name().String()))
			continue
		}

		// 验证值
		validationResult := s.factory.ValidateField(ctx, value, field)

		if !validationResult.Success {
			if typecast {
				// 宽松模式：尝试修复
				logger.Info("验证失败，尝试类型转换",
					logger.String("field_id", fieldID),
					logger.String("field_name", field.Name().String()),
					logger.ErrorField(validationResult.Error))

				repairedValue := s.factory.RepairValue(ctx, value, field)
				if repairedValue != nil {
					result[fieldID] = repairedValue
					logger.Info("类型转换成功",
						logger.String("field_id", fieldID),
						logger.Any("original_value", value),
						logger.Any("repaired_value", repairedValue))
				} else {
					logger.Warn("类型转换失败，跳过此字段",
						logger.String("field_id", fieldID))
				}
			} else {
				// 严格模式：返回具体的 AppError
				return nil, s.convertValidationError(validationResult.Error, field, value)
			}
		} else {
			result[fieldID] = validationResult.Value
		}
	}

	logger.Info("验证和类型转换完成",
		logger.Int("input_fields", len(data)),
		logger.Int("output_fields", len(result)))

	return result, nil
}

// ValidateFieldValue 验证单个字段值
func (s *TypecastService) ValidateFieldValue(
	ctx context.Context,
	value interface{},
	field *entity.Field,
) *validation.ValidationResult {
	return s.factory.ValidateField(ctx, value, field)
}

// RepairFieldValue 修复单个字段值
func (s *TypecastService) RepairFieldValue(
	ctx context.Context,
	value interface{},
	field *entity.Field,
) interface{} {
	return s.factory.RepairValue(ctx, value, field)
}

// convertValidationError 将验证错误转换为具体的 AppError
func (s *TypecastService) convertValidationError(
	validationErr error,
	field *entity.Field,
	value interface{},
) error {
	if validationErr == nil {
		return nil
	}

	fieldName := field.Name().String()
	fieldType := field.Type().String()
	errMsg := validationErr.Error()

	// 根据字段类型和错误消息返回具体的 AppError
	switch {
	// 邮箱相关
	case fieldType == "email" && strings.Contains(errMsg, "邮箱"):
		return errors.ErrInvalidEmail.WithDetails(map[string]interface{}{
			"field": fieldName,
			"value": value,
		})

	// URL相关
	case fieldType == "url" && strings.Contains(errMsg, "URL"):
		return errors.ErrInvalidURL.WithDetails(map[string]interface{}{
			"field": fieldName,
			"value": value,
		})

	// 电话相关
	case fieldType == "phone" && strings.Contains(errMsg, "电话"):
		return errors.ErrInvalidPhone.WithDetails(map[string]interface{}{
			"field": fieldName,
			"value": value,
		})

	// 数字超出范围
	case (fieldType == "rating" || fieldType == "number") && strings.Contains(errMsg, "范围"):
		return errors.ErrFieldOutOfRange.WithDetails(map[string]interface{}{
			"field": fieldName,
			"value": value,
			"error": errMsg,
		})

	// 类型不匹配
	case strings.Contains(errMsg, "必须是") || strings.Contains(errMsg, "类型"):
		return errors.ErrFieldTypeMismatch.WithDetails(map[string]interface{}{
			"field":         fieldName,
			"expected":      fieldType,
			"value":         value,
			"originalError": errMsg,
		})

	// 格式不匹配
	case strings.Contains(errMsg, "格式") || strings.Contains(errMsg, "无效"):
		return errors.ErrInvalidPattern.WithDetails(map[string]interface{}{
			"field": fieldName,
			"type":  fieldType,
			"value": value,
			"error": errMsg,
		})

	// 默认：返回通用字段值无效错误
	default:
		return errors.ErrInvalidFieldValue.WithDetails(map[string]interface{}{
			"field": fieldName,
			"value": value,
			"error": errMsg,
		})
	}
}
