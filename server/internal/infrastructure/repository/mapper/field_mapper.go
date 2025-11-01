package mapper

import (
	"encoding/json"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
)

// ToFieldEntity 将数据库模型转换为领域实体
func ToFieldEntity(dbField *models.Field) (*entity.Field, error) {
	if dbField == nil {
		return nil, nil
	}

	// 创建 FieldID
	fieldID := valueobject.NewFieldID(dbField.ID)

	// 创建 FieldName
	fieldName, err := valueobject.NewFieldName(dbField.Name)
	if err != nil {
		return nil, err
	}

	// 创建 FieldType
	fieldType, err := valueobject.NewFieldType(dbField.Type)
	if err != nil {
		return nil, err
	}

	// 创建 DBFieldName
	dbFieldName, err := valueobject.NewDBFieldNameFromString(dbField.DBFieldName)
	if err != nil {
		return nil, err
	}

	// 解析 Options 字段
	options := valueobject.NewFieldOptions()
	if dbField.Options != nil && *dbField.Options != "" {
		// 解析 JSON 到 FieldOptions
		if err := json.Unmarshal([]byte(*dbField.Options), options); err != nil {
			// 如果解析失败，记录错误但继续（使用空 Options）
			// 这样即使 Options 格式有问题也不会导致整个字段加载失败
			// 生产环境可以考虑记录日志
			options = valueobject.NewFieldOptions()
		}
	}

	// 处理版本
	version := 1
	if dbField.Version != nil {
		version = *dbField.Version
	}

	// 处理 LastModifiedTime
	updatedAt := dbField.CreatedTime
	if dbField.LastModifiedTime != nil {
		updatedAt = *dbField.LastModifiedTime
	}

	// 重建实体
	field := entity.ReconstructField(
		fieldID,
		dbField.TableID,
		fieldName,
		fieldType,
		dbFieldName,
		dbField.DBFieldType,
		options,
		dbField.FieldOrder,
		version,
		dbField.CreatedBy,
		dbField.CreatedTime,
		updatedAt,
	)

	// 设置额外属性
	if dbField.Description != nil {
		field.UpdateDescription(*dbField.Description)
	}

	// 设置约束
	field.SetRequired(dbField.IsRequired)
	field.SetUnique(dbField.IsUnique)

	return field, nil
}

// ToFieldModel 将领域实体转换为数据库模型（参考原版 dbCreateField 逻辑）
func ToFieldModel(field *entity.Field) (*models.Field, error) {
	if field == nil {
		return nil, nil
	}

	// 序列化 Options（参考原版会 JSON.stringify options）
	var optionsStr *string
	if field.Options() != nil {
		optionsJSON, err := json.Marshal(field.Options())
		if err == nil {
			optionsJSONStr := string(optionsJSON)
			optionsStr = &optionsJSONStr
		}
	}

	version := field.Version()
	updatedAt := field.UpdatedAt()

	// 处理布尔值（参考原版 dbCreateField 的字段设置）
	isComputed := field.IsComputed()
	isRequired := field.IsRequired()
	isUnique := field.IsUnique()
	isPrimary := field.IsPrimary()

	// 初始化布尔指针字段为 false（参考原版所有布尔字段都需要设置）
	falseVal := false
	notNull := &falseVal
	isLookup := &falseVal
	isMultipleCellValue := &falseVal
	hasError := &falseVal
	isPending := &falseVal

	// 设置Order字段
	orderValue := field.Order()

	dbField := &models.Field{
		ID:                  field.ID().String(),
		TableID:             field.TableID(),
		Name:                field.Name().String(),
		Type:                field.Type().String(),
		CellValueType:       field.Type().String(),
		DBFieldType:         field.DBFieldType(),
		DBFieldName:         field.DBFieldName().String(),
		IsComputed:          &isComputed,
		IsRequired:          isRequired,
		IsUnique:            isUnique,
		IsPrimary:           &isPrimary,
		NotNull:             notNull,
		IsLookup:            isLookup,
		IsMultipleCellValue: isMultipleCellValue,
		HasError:            hasError,
		IsPending:           isPending,
		FieldOrder:          field.Order(),
		Order:               &orderValue,
		Options:             optionsStr,
		Version:             &version,
		CreatedBy:           field.CreatedBy(),
		CreatedTime:         field.CreatedAt(),
		LastModifiedTime:    &updatedAt,
	}

	// Description
	if field.Description() != nil && *field.Description() != "" {
		dbField.Description = field.Description()
	}

	return dbField, nil
}

// ToFieldList 批量转换
func ToFieldList(dbFields []*models.Field) ([]*entity.Field, error) {
	fields := make([]*entity.Field, 0, len(dbFields))
	for _, dbField := range dbFields {
		field, err := ToFieldEntity(dbField)
		if err != nil {
			return nil, err
		}
		if field != nil {
			fields = append(fields, field)
		}
	}
	return fields, nil
}
