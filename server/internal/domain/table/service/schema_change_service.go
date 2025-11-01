package service

import (
	"context"
	"fmt"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	tableAggregate "github.com/easyspace-ai/luckdb/server/internal/domain/table/aggregate"
	tableEntity "github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
)

// SchemaChangeType 表示schema变更类型
type SchemaChangeType string

const (
	SchemaChangeAddField     SchemaChangeType = "add_field"
	SchemaChangeUpdateField  SchemaChangeType = "update_field"
	SchemaChangeDeleteField  SchemaChangeType = "delete_field"
	SchemaChangeReorderField SchemaChangeType = "reorder_field"
)

// SchemaChange 表示一个schema变更
type SchemaChange struct {
	Type        SchemaChangeType   `json:"type"`
	FieldID     string             `json:"field_id,omitempty"`
	OldField    *fieldEntity.Field `json:"old_field,omitempty"`
	NewField    *fieldEntity.Field `json:"new_field,omitempty"`
	Description string             `json:"description"`
}

// SchemaChangeRequest 表示schema变更请求
type SchemaChangeRequest struct {
	TableID string         `json:"table_id"`
	Changes []SchemaChange `json:"changes"`
	UserID  string         `json:"user_id"`
}

// SchemaChangeResult 表示schema变更结果
type SchemaChangeResult struct {
	Success    bool           `json:"success"`
	Changes    []SchemaChange `json:"changes"`
	Errors     []string       `json:"errors,omitempty"`
	Warnings   []string       `json:"warnings,omitempty"`
	NewVersion int64          `json:"new_version"`
}

// SchemaService 提供安全的schema变更服务
type SchemaService interface {
	// ValidateSchemaChange 验证schema变更的安全性
	ValidateSchemaChange(ctx context.Context, tableAgg *tableAggregate.TableAggregate, changes []SchemaChange) error

	// ApplySchemaChanges 应用schema变更
	ApplySchemaChanges(ctx context.Context, req SchemaChangeRequest) (*SchemaChangeResult, error)

	// PreviewSchemaChanges 预览schema变更的影响
	PreviewSchemaChanges(ctx context.Context, tableAgg *tableAggregate.TableAggregate, changes []SchemaChange) (*SchemaChangeResult, error)

	// CanSafelyChangeFieldType 检查是否可以安全地更改字段类型
	CanSafelyChangeFieldType(ctx context.Context, field *fieldEntity.Field, newType string) (bool, []string, error)

	// GetSchemaHistory 获取表格的schema变更历史
	GetSchemaHistory(ctx context.Context, tableID string) ([]SchemaChange, error)
}

// TableRepository 表格仓储接口（本地定义，避免循环依赖）
// 注意：仓储层面只保存 Table 实体，不保存整个聚合
type TableRepository interface {
	Save(ctx context.Context, table *tableEntity.Table) error
	GetByID(ctx context.Context, id string) (*tableEntity.Table, error)
}

// SchemaServiceImpl schema服务实现
type SchemaServiceImpl struct {
	tableRepo TableRepository
}

// NewSchemaService 创建schema服务
func NewSchemaService(tableRepo TableRepository) SchemaService {
	return &SchemaServiceImpl{
		tableRepo: tableRepo,
	}
}

// ValidateSchemaChange 验证schema变更的安全性
func (s *SchemaServiceImpl) ValidateSchemaChange(ctx context.Context, tableAgg *tableAggregate.TableAggregate, changes []SchemaChange) error {
	for _, change := range changes {
		switch change.Type {
		case SchemaChangeAddField:
			if err := s.validateAddField(tableAgg, change.NewField); err != nil {
				return fmt.Errorf("添加字段验证失败: %v", err)
			}
		case SchemaChangeUpdateField:
			if err := s.validateUpdateField(tableAgg, change.OldField, change.NewField); err != nil {
				return fmt.Errorf("更新字段验证失败: %v", err)
			}
		case SchemaChangeDeleteField:
			if err := s.validateDeleteField(tableAgg, change.OldField); err != nil {
				return fmt.Errorf("删除字段验证失败: %v", err)
			}
		}
	}
	return nil
}

// ApplySchemaChanges 应用schema变更
func (s *SchemaServiceImpl) ApplySchemaChanges(ctx context.Context, req SchemaChangeRequest) (*SchemaChangeResult, error) {
	// 获取表格
	table, err := s.tableRepo.GetByID(ctx, req.TableID)
	if err != nil {
		return nil, fmt.Errorf("获取表格失败: %v", err)
	}
	if table == nil {
		return nil, fmt.Errorf("表格不存在")
	}

	// 创建聚合（从仓储获取完整的聚合）
	// 简化实现：直接重建，完整实现应该用仓储的LoadAggregate
	tableAgg := tableAggregate.NewTableAggregate(table)

	// 验证变更
	if err := s.ValidateSchemaChange(ctx, tableAgg, req.Changes); err != nil {
		return &SchemaChangeResult{
			Success: false,
			Errors:  []string{err.Error()},
		}, nil
	}

	// 应用变更
	var appliedChanges []SchemaChange
	var warnings []string

	for _, change := range req.Changes {
		switch change.Type {
		case SchemaChangeAddField:
			if err := tableAgg.AddField(change.NewField); err != nil {
				return &SchemaChangeResult{
					Success: false,
					Errors:  []string{fmt.Sprintf("添加字段失败: %v", err)},
				}, nil
			}
			appliedChanges = append(appliedChanges, change)

		case SchemaChangeUpdateField:
			field := tableAgg.GetFieldByID(change.FieldID)
			if field == nil {
				return &SchemaChangeResult{
					Success: false,
					Errors:  []string{"字段不存在"},
				}, nil
			}

			// 应用字段更新
			if change.NewField.Name().String() != field.Name().String() {
				if err := field.Rename(change.NewField.Name()); err != nil {
					warnings = append(warnings, fmt.Sprintf("重命名失败: %v", err))
				}
			}
			if change.NewField.Type().String() != field.Type().String() {
				// 更改字段类型
				if err := field.ChangeType(change.NewField.Type()); err != nil {
					warnings = append(warnings, fmt.Sprintf("类型变更失败: %v", err))
				} else {
					warnings = append(warnings, "字段类型变更可能影响现有数据")
				}
			}
			if change.NewField.Options() != nil {
				if err := field.UpdateOptions(change.NewField.Options()); err != nil {
					warnings = append(warnings, fmt.Sprintf("选项更新失败: %v", err))
				}
			}
			if change.NewField.IsRequired() != field.IsRequired() {
				if err := field.SetRequired(change.NewField.IsRequired()); err != nil {
					warnings = append(warnings, fmt.Sprintf("必填设置失败: %v", err))
				}
			}
			if change.NewField.IsUnique() != field.IsUnique() {
				if err := field.SetUnique(change.NewField.IsUnique()); err != nil {
					warnings = append(warnings, fmt.Sprintf("唯一性设置失败: %v", err))
				}
			}
			appliedChanges = append(appliedChanges, change)

		case SchemaChangeDeleteField:
			if err := tableAgg.RemoveField(change.FieldID); err != nil {
				return &SchemaChangeResult{
					Success: false,
					Errors:  []string{fmt.Sprintf("删除字段失败: %v", err)},
				}, nil
			}
			appliedChanges = append(appliedChanges, change)
			warnings = append(warnings, "删除字段将永久丢失该字段的所有数据")
		}
	}

	// 保存表格（保存聚合中的 table 实体）
	if err := s.tableRepo.Save(ctx, tableAgg.Table()); err != nil {
		return &SchemaChangeResult{
			Success: false,
			Errors:  []string{fmt.Sprintf("保存表格失败: %v", err)},
		}, nil
	}

	return &SchemaChangeResult{
		Success:    true,
		Changes:    appliedChanges,
		Warnings:   warnings,
		NewVersion: int64(tableAgg.Table().Version()),
	}, nil
}

// PreviewSchemaChanges 预览schema变更的影响
func (s *SchemaServiceImpl) PreviewSchemaChanges(ctx context.Context, tableAgg *tableAggregate.TableAggregate, changes []SchemaChange) (*SchemaChangeResult, error) {
	// 创建表格副本进行预览（深拷贝聚合，简化实现）
	// 现在简化实现：只做验证，不做实际预览
	// tableCopy := *tableAgg.Table()

	var previewChanges []SchemaChange
	var warnings []string
	var errors []string

	for _, change := range changes {
		switch change.Type {
		case SchemaChangeAddField:
			if err := s.validateAddField(tableAgg, change.NewField); err != nil {
				errors = append(errors, fmt.Sprintf("添加字段 '%s': %v", change.NewField.Name().String(), err))
			} else {
				previewChanges = append(previewChanges, change)
			}

		case SchemaChangeUpdateField:
			if err := s.validateUpdateField(tableAgg, change.OldField, change.NewField); err != nil {
				errors = append(errors, fmt.Sprintf("更新字段 '%s': %v", change.NewField.Name().String(), err))
			} else {
				previewChanges = append(previewChanges, change)
				if change.NewField.Type().String() != change.OldField.Type().String() {
					warnings = append(warnings, fmt.Sprintf("字段 '%s' 类型变更可能影响现有数据", change.NewField.Name().String()))
				}
			}

		case SchemaChangeDeleteField:
			if err := s.validateDeleteField(tableAgg, change.OldField); err != nil {
				errors = append(errors, fmt.Sprintf("删除字段 '%s': %v", change.OldField.Name().String(), err))
			} else {
				previewChanges = append(previewChanges, change)
				warnings = append(warnings, fmt.Sprintf("删除字段 '%s' 将永久丢失该字段的所有数据", change.OldField.Name().String()))
			}
		}
	}

	return &SchemaChangeResult{
		Success:  len(errors) == 0,
		Changes:  previewChanges,
		Errors:   errors,
		Warnings: warnings,
	}, nil
}

// CanSafelyChangeFieldType 检查是否可以安全地更改字段类型
func (s *SchemaServiceImpl) CanSafelyChangeFieldType(ctx context.Context, field *fieldEntity.Field, newType string) (bool, []string, error) {
	var warnings []string

	// 检查类型兼容性
	// 简化的类型兼容性检查
	canChange := true // 暂时允许所有类型转换，实际应该有更详细的检查
	if !canChange {
		return false, []string{fmt.Sprintf("字段类型从 %s 到 %s 不兼容", field.Type().String(), newType)}, nil
	}

	// 检查数据兼容性警告
	if field.Type().String() != string(newType) {
		warnings = append(warnings, "类型转换可能导致数据丢失或格式错误")
	}

	// 检查约束兼容性
	if field.IsUnique() {
		warnings = append(warnings, "请注意新类型是否支持唯一性约束")
	}

	return true, warnings, nil
}

// GetSchemaHistory 获取表格的schema变更历史
func (s *SchemaServiceImpl) GetSchemaHistory(ctx context.Context, tableID string) ([]SchemaChange, error) {
	// 实现schema变更历史记录（通过领域事件记录）
	// 这需要在数据库中存储schema变更日志
	return []SchemaChange{}, nil
}

// validateAddField 验证添加字段
func (s *SchemaServiceImpl) validateAddField(tableAgg *tableAggregate.TableAggregate, field *fieldEntity.Field) error {
	if field == nil {
		return fmt.Errorf("字段不能为空")
	}

	if field.Name().String() == "" {
		return fmt.Errorf("字段名称不能为空")
	}

	if tableAgg.GetFieldByName(field.Name().String()) != nil {
		return fmt.Errorf("字段名称 '%s' 已存在", field.Name().String())
	}
	if field.IsPrimary() && tableAgg.GetPrimaryField() != nil {
		return fmt.Errorf("表格已存在主键字段")
	}

	// 验证字段类型是否需要选项配置
	requiresOptions := field.Type().String() == "select" || field.Type().String() == "multiSelect"
	if requiresOptions && (field.Options == nil || 0 == 0) {
		return fmt.Errorf("字段类型 %s 需要配置选项", field.Type().String())
	}

	return nil
}

// validateUpdateField 验证更新字段
func (s *SchemaServiceImpl) validateUpdateField(tableAgg *tableAggregate.TableAggregate, oldField *fieldEntity.Field, newField *fieldEntity.Field) error {
	if oldField == nil || newField == nil {
		return fmt.Errorf("字段不能为空")
	}

	if newField.Name().String() == "" {
		return fmt.Errorf("字段名称不能为空")
	}

	// 检查名称冲突（除了自己）
	if newField.Name().String() != oldField.Name().String() && tableAgg.GetFieldByName(newField.Name().String()) != nil {
		return fmt.Errorf("字段名称 '%s' 已存在", newField.Name().String())
	}

	// 检查类型变更
	if newField.Type().String() != oldField.Type().String() {
		// 简化的类型兼容性检查
		canChange := true // 暂时允许所有类型转换
		if !canChange {
			return fmt.Errorf("字段类型从 %s 到 %s 不兼容", oldField.Type().String(), newField.Type().String())
		}
	}

	// 验证新字段类型的选项配置
	// Select 和 MultiSelect 类型需要选项配置
	requiresOptions := newField.Type().String() == "select" || newField.Type().String() == "multiSelect"
	if requiresOptions && (newField.Options == nil || 0 == 0) {
		return fmt.Errorf("字段类型 %s 需要配置选项", newField.Type().String())
	}

	return nil
}

// validateDeleteField 验证删除字段
func (s *SchemaServiceImpl) validateDeleteField(tableAgg *tableAggregate.TableAggregate, field *fieldEntity.Field) error {
	if field == nil {
		return fmt.Errorf("字段不能为空")
	}

	// 检查字段是否可以删除
	if field.IsPrimary() {
		return fmt.Errorf("不能删除主键字段")
	}

	return nil
}
