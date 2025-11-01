package service

import (
	"context"
	"fmt"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/schema/entity"
)

// SchemaValidator Schema验证服务
type SchemaValidator struct{}

// NewSchemaValidator 创建Schema验证器
func NewSchemaValidator() *SchemaValidator {
	return &SchemaValidator{}
}

// ValidateChange 验证Schema变更
func (v *SchemaValidator) ValidateChange(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	switch change.ChangeType() {
	case entity.ChangeTypeAddField:
		return v.validateAddField(ctx, change, currentFields)

	case entity.ChangeTypeRemoveField:
		return v.validateRemoveField(ctx, change, currentFields)

	case entity.ChangeTypeModifyField:
		return v.validateModifyField(ctx, change, currentFields)

	case entity.ChangeTypeRenameField:
		return v.validateRenameField(ctx, change, currentFields)

	case entity.ChangeTypeChangeType:
		return v.validateChangeType(ctx, change, currentFields)

	default:
		return fmt.Errorf("unknown change type: %s", change.ChangeType())
	}
}

// ValidateChanges 验证多个Schema变更
func (v *SchemaValidator) ValidateChanges(
	ctx context.Context,
	changes []*entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	for i, change := range changes {
		if err := v.ValidateChange(ctx, change, currentFields); err != nil {
			return fmt.Errorf("change %d validation failed: %w", i, err)
		}
	}

	// 验证变更之间的依赖关系
	if err := v.validateChangeDependencies(ctx, changes, currentFields); err != nil {
		return fmt.Errorf("change dependencies validation failed: %w", err)
	}

	return nil
}

// AnalyzeImpact 分析Schema变更的影响
func (v *SchemaValidator) AnalyzeImpact(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) (*ImpactAnalysis, error) {
	impact := &ImpactAnalysis{
		AffectedFields:  []string{},
		AffectedRecords: 0,
		BreakingChanges: []string{},
		Warnings:        []string{},
		Migrations:      []string{},
	}

	// 根据变更类型分析影响
	switch change.ChangeType() {
	case entity.ChangeTypeRemoveField:
		impact.BreakingChanges = append(impact.BreakingChanges,
			"removing field will delete all data in this field")
		impact.Migrations = append(impact.Migrations,
			fmt.Sprintf("DROP COLUMN %s", change.FieldID()))

	case entity.ChangeTypeChangeType:
		impact.Warnings = append(impact.Warnings,
			"changing field type may require data migration")

		// 检查数据兼容性
		if err := v.checkDataCompatibility(ctx, change, currentFields); err != nil {
			impact.BreakingChanges = append(impact.BreakingChanges,
				fmt.Sprintf("data compatibility issue: %s", err.Error()))
		}
	}

	return impact, nil
}

// ==================== 私有验证方法 ====================

func (v *SchemaValidator) validateAddField(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 检查字段名是否已存在
	if change.NewValue() != nil {
		if newName, ok := change.NewValue().(string); ok {
			for _, field := range currentFields {
				if field.Name().String() == newName {
					return fmt.Errorf("field name '%s' already exists", newName)
				}
			}
		}
	}

	// 检查字段类型是否有效
	// 这里可以添加更多业务规则检查

	return nil
}

func (v *SchemaValidator) validateRemoveField(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 不能删除主键字段
	for _, field := range currentFields {
		if field.ID().String() == change.FieldID() && field.IsPrimary() {
			return fmt.Errorf("cannot remove primary key field")
		}
	}

	return nil
}

func (v *SchemaValidator) validateModifyField(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 查找要修改的字段
	var targetField *fieldEntity.Field
	for _, field := range currentFields {
		if field.ID().String() == change.FieldID() {
			targetField = field
			break
		}
	}

	if targetField == nil {
		return fmt.Errorf("field %s not found", change.FieldID())
	}

	// 检查修改是否会破坏字段约束
	// 例如：不能将有数据的字段设置为 required
	// 这里可以添加更多业务规则

	return nil
}

func (v *SchemaValidator) validateRenameField(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 检查新名称是否已存在
	if change.NewValue() != nil {
		if newName, ok := change.NewValue().(string); ok {
			for _, field := range currentFields {
				// 跳过当前字段本身
				if field.ID().String() == change.FieldID() {
					continue
				}
				// 检查名称冲突
				if field.Name().String() == newName {
					return fmt.Errorf("field name '%s' already exists", newName)
				}
			}
		}
	}

	return nil
}

func (v *SchemaValidator) validateChangeType(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 查找要修改的字段
	var targetField *fieldEntity.Field
	for _, field := range currentFields {
		if field.ID().String() == change.FieldID() {
			targetField = field
			break
		}
	}

	if targetField == nil {
		return fmt.Errorf("field %s not found", change.FieldID())
	}

	// 检查类型转换是否兼容
	if change.NewValue() != nil {
		if newType, ok := change.NewValue().(string); ok {
			// 使用字段类型的兼容性检查
			if !targetField.Type().IsCompatibleWith(targetField.Type()) {
				// 这里应该创建新的 FieldType 并检查兼容性
				// 简化处理：检查一些基本的不兼容情况
				if targetField.Type().IsVirtual() {
					return fmt.Errorf("cannot change type of virtual field")
				}

				// 某些类型转换需要特殊处理
				if (targetField.Type().String() == "link" && newType != "link") ||
					(newType == "link" && targetField.Type().String() != "link") {
					return fmt.Errorf("link field cannot be converted to/from other types")
				}
			}
		}
	}

	return nil
}

// validateChangeDependencies 验证变更之间的依赖关系
func (v *SchemaValidator) validateChangeDependencies(
	ctx context.Context,
	changes []*entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 检查是否有循环依赖
	// 例如：添加字段A依赖字段B，而字段B又依赖字段A

	// 检查删除操作的依赖
	// 例如：不能删除被其他字段引用的字段

	removedFields := make(map[string]bool)
	for _, change := range changes {
		if change.ChangeType() == entity.ChangeTypeRemoveField {
			removedFields[change.FieldID()] = true
		}
	}

	// 检查是否有其他字段依赖于被删除的字段
	for _, field := range currentFields {
		if field.Type().IsVirtual() {
			// 虚拟字段可能依赖其他字段
			// 这里需要检查依赖关系
			// 简化处理：如果有虚拟字段，给出警告
			if len(removedFields) > 0 {
				// 实际应该检查具体的依赖关系
				continue
			}
		}
	}

	return nil
}

// checkDataCompatibility 检查数据兼容性
func (v *SchemaValidator) checkDataCompatibility(
	ctx context.Context,
	change *entity.SchemaChange,
	currentFields []*fieldEntity.Field,
) error {
	// 查找目标字段
	var targetField *fieldEntity.Field
	for _, field := range currentFields {
		if field.ID().String() == change.FieldID() {
			targetField = field
			break
		}
	}

	if targetField == nil {
		return fmt.Errorf("field not found")
	}

	// 检查数据兼容性
	// 例如：text -> number 需要检查现有数据是否都能转换为数字
	// 这里是简化实现，实际应该查询数据库进行检查

	return nil
}

// ImpactAnalysis 影响分析结果
type ImpactAnalysis struct {
	AffectedFields  []string
	AffectedRecords int64
	BreakingChanges []string
	Warnings        []string
	Migrations      []string
}
