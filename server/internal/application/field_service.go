package application

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/factory"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	tableRepo "github.com/easyspace-ai/luckdb/server/internal/domain/table/repository"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// FieldService 字段应用服务（集成依赖图管理+实时推送）✨
// 集成完全动态表架构：字段作为列
type FieldService struct {
	fieldRepo    repository.FieldRepository
	fieldFactory *factory.FieldFactory
	depGraphRepo *dependency.DependencyGraphRepository // ✨ 依赖图仓储
	broadcaster  FieldBroadcaster                      // ✨ WebSocket广播器
	tableRepo    tableRepo.TableRepository             // ✅ 表格仓储（获取Base ID）
	dbProvider   database.DBProvider                   // ✅ 数据库提供者（列管理）
}

// FieldBroadcaster 字段变更广播器接口
type FieldBroadcaster interface {
	BroadcastFieldCreate(tableID string, field *entity.Field)
	BroadcastFieldUpdate(tableID string, field *entity.Field)
	BroadcastFieldDelete(tableID, fieldID string)
}

// NewFieldService 创建字段服务（集成依赖图管理+实时推送）✨
func NewFieldService(
	fieldRepo repository.FieldRepository,
	depGraphRepo *dependency.DependencyGraphRepository,
	broadcaster FieldBroadcaster,
	tableRepo tableRepo.TableRepository,
	dbProvider database.DBProvider,
) *FieldService {
	return &FieldService{
		fieldRepo:    fieldRepo,
		fieldFactory: factory.NewFieldFactory(),
		depGraphRepo: depGraphRepo,
		broadcaster:  broadcaster,
		tableRepo:    tableRepo,
		dbProvider:   dbProvider,
	}
}

// SetBroadcaster 设置广播器（用于延迟注入）
func (s *FieldService) SetBroadcaster(broadcaster FieldBroadcaster) {
	s.broadcaster = broadcaster
}

// CreateField 创建字段（参考原版实现逻辑）
func (s *FieldService) CreateField(ctx context.Context, req dto.CreateFieldRequest, userID string) (*dto.FieldResponse, error) {
	// 1. 验证字段名称
	fieldName, err := valueobject.NewFieldName(req.Name)
	if err != nil {
		return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("字段名称无效: %v", err))
	}

	// 2. 检查字段名称是否重复
	exists, err := s.fieldRepo.ExistsByName(ctx, req.TableID, fieldName, nil)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查字段名称失败: %v", err))
	}
	if exists {
		return nil, pkgerrors.ErrConflict.WithMessage(fmt.Sprintf("字段名 '%s' 已存在", req.Name))
	}

	// 3. 根据类型使用工厂创建字段（保留原始类型名称）
	var field *entity.Field
	switch req.Type {
	case "number":
		// 从 Options 中提取 precision, minValue, maxValue
		var precision, minValue, maxValue *int
		if req.Options != nil {
			if p, ok := req.Options["precision"].(float64); ok {
				precisionInt := int(p)
				precision = &precisionInt
			}
			if min, ok := req.Options["minValue"].(float64); ok {
				minInt := int(min)
				minValue = &minInt
			}
			if max, ok := req.Options["maxValue"].(float64); ok {
				maxInt := int(max)
				maxValue = &maxInt
			}
		}
		field, err = s.fieldFactory.CreateNumberField(req.TableID, req.Name, userID, precision)
		// ✅ 设置 min/max 值
		if err == nil && (minValue != nil || maxValue != nil) {
			options := field.Options()
			if options == nil {
				options = valueobject.NewFieldOptions()
			}
			if options.Number == nil {
				options.Number = &valueobject.NumberOptions{}
			}
			options.Number.MinValue = minValue
			options.Number.MaxValue = maxValue
			field.UpdateOptions(options)
		}

	case "singleSelect":
		// 解析 choices
		choices := s.extractChoicesFromOptions(req.Options)
		field, err = s.fieldFactory.CreateSelectField(req.TableID, req.Name, userID, choices, false)

	case "multipleSelect", "multipleSelects":
		// 解析 choices
		choices := s.extractChoicesFromOptions(req.Options)
		field, err = s.fieldFactory.CreateSelectField(req.TableID, req.Name, userID, choices, true)

	case "date":
		field, err = s.fieldFactory.CreateDateField(req.TableID, req.Name, userID, false)

	case "datetime":
		field, err = s.fieldFactory.CreateDateField(req.TableID, req.Name, userID, true)

	case "formula":
		// 从 Options 中提取 expression
		expression := s.extractExpressionFromOptions(req.Options)
		field, err = s.fieldFactory.CreateFormulaField(req.TableID, req.Name, userID, expression)

	case "rollup":
		// Rollup 字段需要 linkFieldId, rollupFieldId, aggregationFunc
		linkFieldID, rollupFieldID, aggFunc := s.extractRollupOptionsFromOptions(req.Options)
		field, err = s.fieldFactory.CreateRollupField(req.TableID, req.Name, userID, linkFieldID, rollupFieldID, aggFunc)

	case "lookup":
		// Lookup 字段需要 linkFieldId, lookupFieldId
		linkFieldID, lookupFieldID := s.extractLookupOptionsFromOptions(req.Options)
		field, err = s.fieldFactory.CreateLookupField(req.TableID, req.Name, userID, linkFieldID, lookupFieldID)

	default:
		// ✅ 使用通用方法创建字段，保留原始类型名称（如 singleLineText, longText, email 等）
		field, err = s.fieldFactory.CreateFieldWithType(req.TableID, req.Name, req.Type, userID)
	}

	if err != nil {
		logger.Error("创建字段实例失败",
			logger.String("table_id", req.TableID),
			logger.String("name", req.Name),
			logger.String("type", req.Type),
			logger.ErrorField(err),
		)
		// 检查是否为字段类型无效错误
		errMsg := err.Error()
		if strings.Contains(errMsg, "invalid field type") || strings.Contains(errMsg, "不支持的字段类型") {
			return nil, pkgerrors.ErrInvalidFieldType.WithDetails(map[string]interface{}{
				"type":  req.Type,
				"error": errMsg,
			})
		}
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("创建字段失败: %v", err))
	}

	// 4. 设置可选属性
	if req.Required {
		field.SetRequired(true)
	}
	if req.Unique {
		field.SetUnique(true)
	}

    // 5. ✨ 应用通用字段配置（defaultValue, showAs, formatting 等）
    // 顶层 defaultValue 兼容：注入到 options 中
    if req.DefaultValue != nil {
        if req.Options == nil {
            req.Options = make(map[string]interface{})
        }
        req.Options["defaultValue"] = req.DefaultValue
    }
    // 参考 Teable 的优秀设计，补充我们之前缺失的配置
    s.applyCommonFieldOptions(field, req.Options)

	// 6. 循环依赖检测（仅对虚拟字段）
	if isVirtualFieldType(req.Type) {
		if err := s.checkCircularDependency(ctx, req.TableID, field); err != nil {
			return nil, err
		}
	}

	// 7. 计算字段order值（参考原系统逻辑：查询最大order + 1）
	maxOrder, err := s.fieldRepo.GetMaxOrder(ctx, req.TableID)
	if err != nil {
		// 如果查询失败，使用-1，这样第一个字段order为0
		logger.Warn("获取最大order失败，使用默认值-1", logger.ErrorField(err))
		maxOrder = -1
	}
	nextOrder := maxOrder + 1
	field.SetOrder(nextOrder)

	// 8. ✅ 创建物理表列（完全动态表架构）
	// 参考旧系统：ALTER TABLE ADD COLUMN
	// 注意：虚拟字段也需要创建物理列来存储计算结果
	if s.tableRepo != nil && s.dbProvider != nil {
		// 8.1 获取Table信息（需要Base ID）
		table, err := s.tableRepo.GetByID(ctx, req.TableID)
		if err != nil {
			return nil, pkgerrors.ErrDatabaseOperation.WithDetails(
				fmt.Sprintf("获取Table信息失败: %v", err))
		}
		if table == nil {
			return nil, pkgerrors.ErrNotFound.WithDetails("Table不存在")
		}

		baseID := table.BaseID()
		tableID := table.ID().String()
		dbFieldName := field.DBFieldName().String() // 例如：field_fld_xxx

		// 8.2 使用Field Entity已确定的数据库类型
		// Field Entity中的determineDBFieldType已经处理了类型映射
		dbType := field.DBFieldType()

		logger.Info("正在为字段创建物理表列",
			logger.String("field_id", field.ID().String()),
			logger.String("base_id", baseID),
			logger.String("table_id", tableID),
			logger.String("db_field_name", dbFieldName),
			logger.String("db_type", dbType))

		// 8.3 构建列定义
		columnDef := database.ColumnDefinition{
			Name:    dbFieldName,
			Type:    dbType,
			NotNull: req.Required, // 必填 = NOT NULL
			Unique:  req.Unique,   // 唯一 = UNIQUE
		}

		// 8.4 添加列到物理表
		if err := s.dbProvider.AddColumn(ctx, baseID, tableID, columnDef); err != nil {
			logger.Error("创建物理表列失败",
				logger.String("field_id", field.ID().String()),
				logger.String("db_field_name", dbFieldName),
				logger.ErrorField(err))
			return nil, pkgerrors.ErrDatabaseOperation.WithDetails(
				fmt.Sprintf("创建物理表列失败: %v", err))
		}

		// 8.5 为 JSONB 字段自动创建 GIN 索引
		if dbType == "JSONB" {
			indexName := fmt.Sprintf("idx_%s_%s_gin",
				strings.ReplaceAll(baseID, "-", "_"),
				strings.ReplaceAll(field.ID().String(), "-", "_"))

			fullTableName := fmt.Sprintf("\"%s\".\"%s\"", baseID, tableID)
			createIndexSQL := fmt.Sprintf(
				`CREATE INDEX IF NOT EXISTS %s ON %s USING GIN (%s jsonb_path_ops)`,
				indexName,
				fullTableName,
				dbFieldName,
			)

			logger.Info("创建 JSONB GIN 索引",
				logger.String("field_id", field.ID().String()),
				logger.String("field_name", field.Name().String()),
				logger.String("index_name", indexName))

			// 获取底层数据库连接
			if pgProvider, ok := s.dbProvider.(*database.PostgresProvider); ok {
				db := pgProvider.GetDB()
				if err := db.WithContext(ctx).Exec(createIndexSQL).Error; err != nil {
					logger.Warn("创建 JSONB GIN 索引失败（不影响字段创建）",
						logger.String("field_id", field.ID().String()),
						logger.ErrorField(err))
				} else {
					logger.Info("✅ JSONB GIN 索引创建成功",
						logger.String("field_id", field.ID().String()),
						logger.String("index_name", indexName))
				}
			}
		}

		logger.Info("✅ 物理表列创建成功",
			logger.String("field_id", field.ID().String()),
			logger.String("db_field_name", dbFieldName),
			logger.String("db_type", dbType))
	}

	// 9. 保存字段元数据
	logger.Info("准备保存字段元数据",
		logger.String("field_id", field.ID().String()),
		logger.String("table_id", req.TableID),
		logger.String("name", req.Name),
		logger.String("type", req.Type),
	)

	if err := s.fieldRepo.Save(ctx, field); err != nil {
		// ❌ 回滚：删除已创建的物理表列
		if s.tableRepo != nil && s.dbProvider != nil {
			table, _ := s.tableRepo.GetByID(ctx, req.TableID)
			if table != nil {
				dbFieldName := field.DBFieldName().String()
				if rollbackErr := s.dbProvider.DropColumn(ctx, table.BaseID(), table.ID().String(), dbFieldName); rollbackErr != nil {
					logger.Error("回滚删除物理表列失败",
						logger.String("field_id", field.ID().String()),
						logger.String("db_field_name", dbFieldName),
						logger.ErrorField(rollbackErr))
				}
			}
		}

		logger.Error("保存字段元数据失败",
			logger.String("field_id", field.ID().String()),
			logger.String("table_id", req.TableID),
			logger.ErrorField(err),
		)
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存字段失败: %v", err))
	}

	logger.Info("字段创建成功",
		logger.String("field_id", field.ID().String()),
		logger.String("table_id", req.TableID),
		logger.String("name", req.Name),
		logger.String("type", req.Type),
		logger.Float64("order", nextOrder),
	)

	// 9. ✨ 更新依赖图（如果是虚拟字段）
	if s.depGraphRepo != nil && field.IsComputed() {
		if err := s.depGraphRepo.InvalidateCache(ctx, req.TableID); err != nil {
			logger.Warn("清除依赖图缓存失败（不影响字段创建）",
				logger.String("table_id", req.TableID),
				logger.ErrorField(err),
			)
		} else {
			logger.Info("依赖图缓存已清除 ✨",
				logger.String("table_id", req.TableID),
			)
		}
	}

	// 10. ✨ 实时推送字段创建事件
	if s.broadcaster != nil {
		s.broadcaster.BroadcastFieldCreate(req.TableID, field)
		logger.Info("字段创建事件已广播 ✨",
			logger.String("field_id", field.ID().String()),
		)
	}

	return dto.FromFieldEntity(field), nil
}

// extractChoicesFromOptions 从 Options 中提取 choices（参考原版 Select 字段逻辑）
func (s *FieldService) extractChoicesFromOptions(options map[string]interface{}) []valueobject.SelectChoice {
	if options == nil {
		return nil
	}

	choicesData, ok := options["choices"]
	if !ok {
		return nil
	}

	choicesArray, ok := choicesData.([]interface{})
	if !ok {
		return nil
	}

	choices := make([]valueobject.SelectChoice, 0, len(choicesArray))
	for _, item := range choicesArray {
		choiceMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		choice := valueobject.SelectChoice{}
		if id, ok := choiceMap["id"].(string); ok {
			choice.ID = id
		}
		if name, ok := choiceMap["name"].(string); ok {
			choice.Name = name
		}
		if color, ok := choiceMap["color"].(string); ok {
			choice.Color = color
		}

		choices = append(choices, choice)
	}

	return choices
}

// extractExpressionFromOptions 从 Options 中提取 expression（参考原版 Formula 字段逻辑）
func (s *FieldService) extractExpressionFromOptions(options map[string]interface{}) string {
	if options == nil {
		return ""
	}

	// 支持 formula 和 expression 两种格式（兼容前端SDK）
	if expr, ok := options["formula"].(string); ok && expr != "" {
		return expr
	}

	if expr, ok := options["expression"].(string); ok && expr != "" {
		return expr
	}

	return ""
}

// extractRollupOptionsFromOptions 从 Options 中提取 Rollup 相关参数
func (s *FieldService) extractRollupOptionsFromOptions(options map[string]interface{}) (string, string, string) {
	if options == nil {
		return "", "", ""
	}

	linkFieldID, _ := options["linkFieldId"].(string)
	rollupFieldID, _ := options["rollupFieldId"].(string)
	aggFunc, _ := options["aggregationFunc"].(string)

	return linkFieldID, rollupFieldID, aggFunc
}

// extractLookupOptionsFromOptions 从 Options 中提取 Lookup 相关参数
func (s *FieldService) extractLookupOptionsFromOptions(options map[string]interface{}) (string, string) {
	if options == nil {
		return "", ""
	}

	linkFieldID, _ := options["linkFieldId"].(string)
	lookupFieldID, _ := options["lookupFieldId"].(string)

	return linkFieldID, lookupFieldID
}

// GetField 获取字段详情
func (s *FieldService) GetField(ctx context.Context, fieldID string) (*dto.FieldResponse, error) {
	id := valueobject.NewFieldID(fieldID)

	field, err := s.fieldRepo.FindByID(ctx, id)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找字段失败: %v", err))
	}
	if field == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("字段不存在")
	}

	return dto.FromFieldEntity(field), nil
}

// UpdateField 更新字段
func (s *FieldService) UpdateField(ctx context.Context, fieldID string, req dto.UpdateFieldRequest) (*dto.FieldResponse, error) {
	// 1. 查找字段
	id := valueobject.NewFieldID(fieldID)
	field, err := s.fieldRepo.FindByID(ctx, id)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找字段失败: %v", err))
	}
	if field == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("字段不存在")
	}

	// 2. 更新名称
	if req.Name != nil && *req.Name != "" {
		fieldName, err := valueobject.NewFieldName(*req.Name)
		if err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("字段名称无效: %v", err))
		}

		// 检查名称是否重复
		exists, err := s.fieldRepo.ExistsByName(ctx, field.TableID(), fieldName, &id)
		if err != nil {
			return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("检查字段名称失败: %v", err))
		}
		if exists {
			return nil, pkgerrors.ErrConflict.WithDetails("字段名称已存在")
		}

		if err := field.Rename(fieldName); err != nil {
			return nil, pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("重命名失败: %v", err))
		}
	}

    // 3. 更新Options（如公式表达式等）
    if req.Options != nil && len(req.Options) > 0 || req.DefaultValue != nil {
        // 顶层 defaultValue 兼容：注入到 options 中
        if req.DefaultValue != nil {
            if req.Options == nil {
                req.Options = make(map[string]interface{})
            }
            req.Options["defaultValue"] = req.DefaultValue
        }
		// 根据字段类型更新Options
		switch field.Type().String() {
		case "formula":
			// 更新公式表达式
			if expression, ok := req.Options["expression"].(string); ok && expression != "" {
				options := field.Options()
				if options == nil {
					options = valueobject.NewFieldOptions()
				}
				if options.Formula == nil {
					options.Formula = &valueobject.FormulaOptions{}
				}
				options.Formula.Expression = expression
				field.UpdateOptions(options)

				logger.Info("更新公式表达式",
					logger.String("field_id", fieldID),
					logger.String("old_expression", field.Options().Formula.Expression),
					logger.String("new_expression", expression),
				)
			}
		case "number":
			// 更新数字精度
			if precision, ok := req.Options["precision"].(float64); ok {
				options := field.Options()
				if options == nil {
					options = valueobject.NewFieldOptions()
				}
				if options.Number == nil {
					options.Number = &valueobject.NumberOptions{}
				}
				precisionInt := int(precision)
				options.Number.Precision = &precisionInt
				field.UpdateOptions(options)
			}
		case "singleSelect", "multipleSelect":
			// 更新选项列表
			if choicesData, ok := req.Options["choices"].([]interface{}); ok {
				choices := make([]valueobject.SelectChoice, 0, len(choicesData))
				for _, item := range choicesData {
					if choiceMap, ok := item.(map[string]interface{}); ok {
						choice := valueobject.SelectChoice{}
						if id, ok := choiceMap["id"].(string); ok {
							choice.ID = id
						}
						if name, ok := choiceMap["name"].(string); ok {
							choice.Name = name
						}
						if color, ok := choiceMap["color"].(string); ok {
							choice.Color = color
						}
						choices = append(choices, choice)
					}
				}

				options := field.Options()
				if options == nil {
					options = valueobject.NewFieldOptions()
				}
				if options.Select == nil {
					options.Select = &valueobject.SelectOptions{}
				}
				options.Select.Choices = choices
				field.UpdateOptions(options)
			}
		}

		// ✨ 应用通用字段配置（defaultValue, showAs, formatting 等）
		// 参考 Teable 的优秀设计，补充我们之前缺失的配置
		s.applyCommonFieldOptions(field, req.Options)
	}

	// 4. 更新约束
	if req.Required != nil {
		field.SetRequired(*req.Required)
	}
	if req.Unique != nil {
		field.SetUnique(*req.Unique)
	}

	// 5. 循环依赖检测（如果是虚拟字段且Options被更新）
	if req.Options != nil && len(req.Options) > 0 && isVirtualFieldType(field.Type().String()) {
		logger.Info("🔍 字段更新触发循环依赖检测",
			logger.String("field_id", fieldID),
			logger.String("field_name", field.Name().String()),
			logger.String("field_type", field.Type().String()),
		)

		if err := s.checkCircularDependency(ctx, field.TableID(), field); err != nil {
			return nil, err
		}
	}

	// 6. 保存
	if err := s.fieldRepo.Save(ctx, field); err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存字段失败: %v", err))
	}

	logger.Info("字段更新成功", logger.String("field_id", fieldID))

	// 7. ✨ 清除依赖图缓存（如果是虚拟字段）
	if s.depGraphRepo != nil && field.IsComputed() {
		if err := s.depGraphRepo.InvalidateCache(ctx, field.TableID()); err != nil {
			logger.Warn("清除依赖图缓存失败（不影响字段更新）",
				logger.String("table_id", field.TableID()),
				logger.ErrorField(err),
			)
		}
	}

	// 8. ✨ 实时推送字段更新事件
	if s.broadcaster != nil {
		s.broadcaster.BroadcastFieldUpdate(field.TableID(), field)
		logger.Info("字段更新事件已广播 ✨",
			logger.String("field_id", fieldID),
		)
	}

	return dto.FromFieldEntity(field), nil
}

// DeleteField 删除字段
// ✅ 完全动态表架构：删除Field时删除物理表列
// 严格按照旧系统实现
func (s *FieldService) DeleteField(ctx context.Context, fieldID string) error {
	id := valueobject.NewFieldID(fieldID)

	// 1. 获取字段信息（用于广播、清除缓存和删除物理列）
	field, err := s.fieldRepo.FindByID(ctx, id)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找字段失败: %v", err))
	}
	if field == nil {
		return pkgerrors.ErrNotFound.WithDetails("字段不存在")
	}

	tableID := field.TableID()
	isComputed := field.IsComputed()
	dbFieldName := field.DBFieldName().String()

	logger.Info("正在删除字段",
		logger.String("field_id", fieldID),
		logger.String("table_id", tableID),
		logger.String("db_field_name", dbFieldName))

	// 2. ✅ 删除物理表列（完全动态表架构）
	// 参考旧系统：ALTER TABLE DROP COLUMN
	if s.tableRepo != nil && s.dbProvider != nil {
		// 2.1 获取Table信息（需要Base ID）
		table, err := s.tableRepo.GetByID(ctx, tableID)
		if err != nil {
			return pkgerrors.ErrDatabaseOperation.WithDetails(
				fmt.Sprintf("获取Table信息失败: %v", err))
		}
		if table == nil {
			return pkgerrors.ErrNotFound.WithDetails("Table不存在")
		}

		baseID := table.BaseID()

		logger.Info("正在删除物理表列",
			logger.String("base_id", baseID),
			logger.String("table_id", tableID),
			logger.String("db_field_name", dbFieldName))

		// 2.2 删除列
		if err := s.dbProvider.DropColumn(ctx, baseID, tableID, dbFieldName); err != nil {
			logger.Error("删除物理表列失败",
				logger.String("field_id", fieldID),
				logger.String("db_field_name", dbFieldName),
				logger.ErrorField(err))
			return pkgerrors.ErrDatabaseOperation.WithDetails(
				fmt.Sprintf("删除物理表列失败: %v", err))
		}

		logger.Info("✅ 物理表列删除成功",
			logger.String("field_id", fieldID),
			logger.String("db_field_name", dbFieldName))
	}

	// 3. 删除字段元数据
	if err := s.fieldRepo.Delete(ctx, id); err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("删除字段失败: %v", err))
	}

	logger.Info("✅ 字段删除成功（含物理表列）",
		logger.String("field_id", fieldID),
		logger.String("table_id", tableID))

	// 4. ✨ 清除依赖图缓存（如果是虚拟字段）
	if s.depGraphRepo != nil && isComputed {
		if err := s.depGraphRepo.InvalidateCache(ctx, tableID); err != nil {
			logger.Warn("清除依赖图缓存失败（不影响字段删除）",
				logger.String("table_id", tableID),
				logger.ErrorField(err),
			)
		}
	}

	// 5. ✨ 实时推送字段删除事件
	if s.broadcaster != nil {
		s.broadcaster.BroadcastFieldDelete(tableID, fieldID)
		logger.Info("字段删除事件已广播 ✨",
			logger.String("field_id", fieldID),
		)
	}

	return nil
}

// ListFields 列出表格的所有字段
func (s *FieldService) ListFields(ctx context.Context, tableID string) ([]*dto.FieldResponse, error) {
	fields, err := s.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查询字段列表失败: %v", err))
	}

	fieldList := make([]*dto.FieldResponse, 0, len(fields))
	for _, field := range fields {
		fieldList = append(fieldList, dto.FromFieldEntity(field))
	}

	return fieldList, nil
}

// checkCircularDependency 检测循环依赖
// 在创建或更新虚拟字段（formula, rollup, lookup）时调用
func (s *FieldService) checkCircularDependency(ctx context.Context, tableID string, newField *entity.Field) error {
	// 1. 获取表中所有现有字段
	existingFields, err := s.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		logger.Warn("获取字段列表失败，跳过循环依赖检测", logger.ErrorField(err))
		return nil // 不阻塞字段创建
	}

	// 2. 构建依赖图（包含新字段）
	allFields := append(existingFields, newField)
	graphItems := s.buildDependencyGraphForFields(allFields)

	logger.Info("🔍 循环依赖检测",
		logger.String("new_field_id", newField.ID().String()),
		logger.String("new_field_name", newField.Name().String()),
		logger.String("new_field_type", newField.Type().String()),
		logger.Int("total_fields", len(allFields)),
		logger.Int("graph_edges", len(graphItems)),
	)

	// 3. 检测循环依赖
	hasCycle, cyclePath := dependency.DetectCyclePath(graphItems)
	if hasCycle {
		logger.Error("❌ 检测到循环依赖",
			logger.String("new_field", newField.Name().String()),
			logger.Any("cycle_path", cyclePath),
		)
		return pkgerrors.ErrValidationFailed.WithDetails(map[string]interface{}{
			"message": "检测到循环依赖，无法创建该字段",
			"field":   newField.Name().String(),
			"cycle":   cyclePath,
		})
	}

	logger.Info("✅ 循环依赖检测通过", logger.String("field", newField.Name().String()))
	return nil
}

// buildDependencyGraphForFields 为字段列表构建依赖图
func (s *FieldService) buildDependencyGraphForFields(fields []*entity.Field) []dependency.GraphItem {
	items := make([]dependency.GraphItem, 0)

	for _, field := range fields {
		fieldType := field.Type().String()

		switch fieldType {
		case "formula":
			// Formula 依赖于表达式中的字段
			deps := s.extractFormulaDependencies(field)
			for _, depFieldRef := range deps {
				// 尝试通过名称或ID查找字段
				depField := s.findFieldByNameOrID(fields, depFieldRef)
				if depField != nil {
					items = append(items, dependency.GraphItem{
						FromFieldID: depField.ID().String(),
						ToFieldID:   field.ID().String(),
					})
				}
			}

		case "rollup":
			// Rollup 依赖于 Link 字段
			options := field.Options()
			if options != nil && options.Rollup != nil {
				items = append(items, dependency.GraphItem{
					FromFieldID: options.Rollup.LinkFieldID,
					ToFieldID:   field.ID().String(),
				})
			}

		case "lookup":
			// Lookup 依赖于 Link 字段
			options := field.Options()
			if options != nil && options.Lookup != nil {
				items = append(items, dependency.GraphItem{
					FromFieldID: options.Lookup.LinkFieldID,
					ToFieldID:   field.ID().String(),
				})
			}
		}
	}

	return items
}

// extractFormulaDependencies 提取公式的依赖字段
func (s *FieldService) extractFormulaDependencies(field *entity.Field) []string {
	options := field.Options()
	if options == nil || options.Formula == nil {
		return []string{}
	}

	expression := options.Formula.Expression
	if expression == "" {
		return []string{}
	}

	// 使用正则表达式提取 {fieldName} 引用
	re := regexp.MustCompile(`\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(expression, -1)

	if len(matches) == 0 {
		return []string{}
	}

	dependencies := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			fieldRef := match[1]
			dependencies = append(dependencies, fieldRef)
		}
	}

	return dependencies
}

// findFieldByNameOrID 通过名称或ID查找字段
func (s *FieldService) findFieldByNameOrID(fields []*entity.Field, nameOrID string) *entity.Field {
	// 先尝试按ID查找
	for _, field := range fields {
		if field.ID().String() == nameOrID {
			return field
		}
	}

	// 再尝试按名称查找
	for _, field := range fields {
		if field.Name().String() == nameOrID {
			return field
		}
	}

	return nil
}

// isVirtualFieldType 判断是否为虚拟字段类型
func isVirtualFieldType(fieldType string) bool {
	virtualTypes := map[string]bool{
		"formula": true,
		"rollup":  true,
		"lookup":  true,
		"count":   true,
	}
	return virtualTypes[fieldType]
}

// isComputedFieldType 判断是否为计算字段类型（按照 teable 标准）
func isComputedFieldType(fieldType string) bool {
	computedTypes := map[string]bool{
		"formula": true,
		"rollup":  true,
		"lookup":  true,
		"count":   true,
	}
	return computedTypes[fieldType]
}

// GetFieldIDsByNames 根据字段名称获取字段ID列表
// 用于 UpdateRecord 流程中识别变更的字段
func (s *FieldService) GetFieldIDsByNames(ctx context.Context, tableID string, fieldNames []string) ([]string, error) {
	if len(fieldNames) == 0 {
		return []string{}, nil
	}

	// 获取表的所有字段
	fields, err := s.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	// 构建名称到ID的映射
	nameToID := make(map[string]string)
	for _, field := range fields {
		nameToID[field.Name().String()] = field.ID().String()
	}

	// 查找匹配的字段ID
	fieldIDs := make([]string, 0, len(fieldNames))
	for _, name := range fieldNames {
		if fieldID, exists := nameToID[name]; exists {
			fieldIDs = append(fieldIDs, fieldID)
		} else {
			logger.Warn("字段名称未找到",
				logger.String("field_name", name),
				logger.String("table_id", tableID),
			)
		}
	}

	return fieldIDs, nil
}

// applyCommonFieldOptions 应用通用字段配置（defaultValue, showAs, formatting 等）
// 参考 Teable 的设计，补充我们之前缺失的配置
func (s *FieldService) applyCommonFieldOptions(field *entity.Field, reqOptions map[string]interface{}) {
	if reqOptions == nil || field == nil {
		return
	}

	options := field.Options()
	if options == nil {
		options = valueobject.NewFieldOptions()
	}

	// 1. 应用通用的 ShowAs 配置
	if showAsData, ok := reqOptions["showAs"].(map[string]interface{}); ok {
		options.ShowAs = &valueobject.ShowAsOptions{
			Type:   getStringFromMap(showAsData, "type"),
			Color:  getStringFromMap(showAsData, "color"),
			Config: showAsData,
		}
	}

	// 2. 应用通用的 Formatting 配置
	if formattingData, ok := reqOptions["formatting"].(map[string]interface{}); ok {
		formatting := &valueobject.FormattingOptions{
			Type:       getStringFromMap(formattingData, "type"),
			DateFormat: getStringFromMap(formattingData, "dateFormat"),
			TimeFormat: getStringFromMap(formattingData, "timeFormat"),
			TimeZone:   getStringFromMap(formattingData, "timeZone"),
			Currency:   getStringFromMap(formattingData, "currency"),
			ShowCommas: getBoolFromMap(formattingData, "showCommas"),
		}
		if precision, ok := formattingData["precision"].(float64); ok {
			p := int(precision)
			formatting.Precision = &p
		}
		options.Formatting = formatting
	}

	// 3. 根据字段类型应用特定配置
	fieldType := field.Type().String()

	switch fieldType {
	case "number":
		if options.Number == nil {
			options.Number = &valueobject.NumberOptions{}
		}
		// DefaultValue
		if defaultValue, ok := reqOptions["defaultValue"].(float64); ok {
			options.Number.DefaultValue = &defaultValue
		}
		// ShowAs (字段级别)
		if showAsData, ok := reqOptions["showAs"].(map[string]interface{}); ok {
			options.Number.ShowAs = &valueobject.ShowAsOptions{
				Type:   getStringFromMap(showAsData, "type"),
				Color:  getStringFromMap(showAsData, "color"),
				Config: showAsData,
			}
		}

	case "singleSelect", "multipleSelect":
		if options.Select == nil {
			options.Select = &valueobject.SelectOptions{}
		}
		// DefaultValue
		if defaultValue, ok := reqOptions["defaultValue"]; ok {
			options.Select.DefaultValue = defaultValue
		}
		// PreventAutoNewOptions
		if preventAuto, ok := reqOptions["preventAutoNewOptions"].(bool); ok {
			options.Select.PreventAutoNewOptions = preventAuto
		}

	case "date", "datetime":
		if options.Date == nil {
			options.Date = &valueobject.DateOptions{}
		}
		// DefaultValue
		if defaultValue, ok := reqOptions["defaultValue"].(string); ok {
			options.Date.DefaultValue = &defaultValue
		}

	case "formula":
		if options.Formula != nil {
			// TimeZone
			if timeZone, ok := reqOptions["timeZone"].(string); ok {
				options.Formula.TimeZone = timeZone
			}
			// ShowAs
			if showAsData, ok := reqOptions["showAs"].(map[string]interface{}); ok {
				options.Formula.ShowAs = &valueobject.ShowAsOptions{
					Type:   getStringFromMap(showAsData, "type"),
					Color:  getStringFromMap(showAsData, "color"),
					Config: showAsData,
				}
			}
			// Formatting
			if formattingData, ok := reqOptions["formatting"].(map[string]interface{}); ok {
				formatting := &valueobject.FormattingOptions{
					Type:       getStringFromMap(formattingData, "type"),
					DateFormat: getStringFromMap(formattingData, "dateFormat"),
					TimeFormat: getStringFromMap(formattingData, "timeFormat"),
					TimeZone:   getStringFromMap(formattingData, "timeZone"),
					Currency:   getStringFromMap(formattingData, "currency"),
					ShowCommas: getBoolFromMap(formattingData, "showCommas"),
				}
				if precision, ok := formattingData["precision"].(float64); ok {
					p := int(precision)
					formatting.Precision = &p
				}
				options.Formula.Formatting = formatting
			}
		}

	case "rollup":
		if options.Rollup != nil {
			// TimeZone
			if timeZone, ok := reqOptions["timeZone"].(string); ok {
				options.Rollup.TimeZone = timeZone
			}
			// ShowAs
			if showAsData, ok := reqOptions["showAs"].(map[string]interface{}); ok {
				options.Rollup.ShowAs = &valueobject.ShowAsOptions{
					Type:   getStringFromMap(showAsData, "type"),
					Color:  getStringFromMap(showAsData, "color"),
					Config: showAsData,
				}
			}
			// Formatting
			if formattingData, ok := reqOptions["formatting"].(map[string]interface{}); ok {
				formatting := &valueobject.FormattingOptions{
					Type:       getStringFromMap(formattingData, "type"),
					DateFormat: getStringFromMap(formattingData, "dateFormat"),
					TimeFormat: getStringFromMap(formattingData, "timeFormat"),
					TimeZone:   getStringFromMap(formattingData, "timeZone"),
					Currency:   getStringFromMap(formattingData, "currency"),
					ShowCommas: getBoolFromMap(formattingData, "showCommas"),
				}
				if precision, ok := formattingData["precision"].(float64); ok {
					p := int(precision)
					formatting.Precision = &p
				}
				options.Rollup.Formatting = formatting
			}
		}

	case "lookup":
		if options.Lookup != nil {
			// Formatting
			if formattingData, ok := reqOptions["formatting"].(map[string]interface{}); ok {
				formatting := &valueobject.FormattingOptions{
					Type:       getStringFromMap(formattingData, "type"),
					DateFormat: getStringFromMap(formattingData, "dateFormat"),
					TimeFormat: getStringFromMap(formattingData, "timeFormat"),
					TimeZone:   getStringFromMap(formattingData, "timeZone"),
					Currency:   getStringFromMap(formattingData, "currency"),
					ShowCommas: getBoolFromMap(formattingData, "showCommas"),
				}
				if precision, ok := formattingData["precision"].(float64); ok {
					p := int(precision)
					formatting.Precision = &p
				}
				options.Lookup.Formatting = formatting
			}
			// ShowAs
			if showAsData, ok := reqOptions["showAs"].(map[string]interface{}); ok {
				options.Lookup.ShowAs = &valueobject.ShowAsOptions{
					Type:   getStringFromMap(showAsData, "type"),
					Color:  getStringFromMap(showAsData, "color"),
					Config: showAsData,
				}
			}
		}

	case "link":
		if options.Link == nil {
			options.Link = &valueobject.LinkOptions{}
		}
		// 高级过滤功能（参考 Teable）
		if baseID, ok := reqOptions["baseId"].(string); ok {
			options.Link.BaseID = baseID
		}
		if lookupFieldID, ok := reqOptions["lookupFieldId"].(string); ok {
			options.Link.LookupFieldID = lookupFieldID
		}
		if filterByViewID, ok := reqOptions["filterByViewId"].(string); ok {
			options.Link.FilterByViewID = &filterByViewID
		}
		if visibleFieldIDs, ok := reqOptions["visibleFieldIds"].([]interface{}); ok {
			ids := make([]string, 0, len(visibleFieldIDs))
			for _, id := range visibleFieldIDs {
				if strID, ok := id.(string); ok {
					ids = append(ids, strID)
				}
			}
			options.Link.VisibleFieldIDs = ids
		}
		if filterData, ok := reqOptions["filter"].(map[string]interface{}); ok {
			filter := &valueobject.FilterOptions{
				Conjunction: getStringFromMap(filterData, "conjunction"),
			}
			if conditions, ok := filterData["conditions"].([]interface{}); ok {
				filter.Conditions = make([]valueobject.FilterCondition, 0, len(conditions))
				for _, condData := range conditions {
					if condMap, ok := condData.(map[string]interface{}); ok {
						filter.Conditions = append(filter.Conditions, valueobject.FilterCondition{
							FieldID:  getStringFromMap(condMap, "fieldId"),
							Operator: getStringFromMap(condMap, "operator"),
							Value:    condMap["value"],
						})
					}
				}
			}
			options.Link.Filter = filter
		}
	}

	// 更新字段的 options
	field.UpdateOptions(options)
}

// 辅助函数：从 map 中安全获取字符串
func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// 辅助函数：从 map 中安全获取布尔值
func getBoolFromMap(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}
