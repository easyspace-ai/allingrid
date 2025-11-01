package application

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	tableRepo "github.com/easyspace-ai/luckdb/server/internal/domain/table/repository"
	"github.com/easyspace-ai/luckdb/server/internal/events"
	infraRepository "github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository"
	"github.com/easyspace-ai/luckdb/server/internal/sharedb"
	"github.com/easyspace-ai/luckdb/server/pkg/database"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// RecordService 记录应用服务（集成计算引擎+实时推送）✨
//
// 设计哲学：
//   - 自动计算：Record创建/更新时自动计算虚拟字段
//   - 依赖感知：自动识别受影响的字段
//   - 性能优先：批量计算，拓扑优化
//   - 实时推送：变更实时广播到WebSocket客户端
//
// 集成计算引擎：
//   - 创建Record后自动计算所有虚拟字段
//   - 更新Record后自动计算受影响的字段
//   - 确保数据一致性和实时性
//
// 实时推送：
//   - 记录变更实时推送到前端
//   - 计算字段变更实时推送
//   - 支持多客户端同步
type RecordService struct {
	recordRepo         recordRepo.RecordRepository
	fieldRepo          repository.FieldRepository
	tableRepo          tableRepo.TableRepository     // ✅ 添加表仓储，用于检查表存在性
	calculationService *CalculationService           // ✨ 计算引擎
	broadcaster        Broadcaster                   // ✨ WebSocket广播器
	businessEvents     events.BusinessEventPublisher // ✨ 业务事件发布器
	typecastService    *TypecastService              // ✅ Phase 2: 类型转换和验证
	hookService        *HookService                  // ✨ 钩子服务
	shareDBService     *sharedb.ShareDBService       // ✨ ShareDB 实时协作服务
	logger             *zap.Logger                   // ✨ 日志记录器
}

// Broadcaster WebSocket广播器接口
type Broadcaster interface {
	BroadcastRecordUpdate(tableID, recordID string, fields map[string]interface{})
	BroadcastRecordCreate(tableID, recordID string, fields map[string]interface{})
	BroadcastRecordDelete(tableID, recordID string)
}

// NewRecordService 创建记录服务（集成计算引擎+实时推送+验证）✨
func NewRecordService(
	recordRepo recordRepo.RecordRepository,
	fieldRepo repository.FieldRepository,
	tableRepo tableRepo.TableRepository,
	calculationService *CalculationService,
	broadcaster Broadcaster,
	businessEvents events.BusinessEventPublisher,
	typecastService *TypecastService,
	shareDBService *sharedb.ShareDBService,
) *RecordService {
	return &RecordService{
		recordRepo:         recordRepo,
		fieldRepo:          fieldRepo,
		tableRepo:          tableRepo,
		calculationService: calculationService,
		broadcaster:        broadcaster,
		businessEvents:     businessEvents,
		typecastService:    typecastService,
		shareDBService:     shareDBService,
		logger:             logger.Logger,
	}
}

// SetBroadcaster 设置广播器（用于延迟注入）
func (s *RecordService) SetBroadcaster(broadcaster Broadcaster) {
	s.broadcaster = broadcaster
}

// SetHookService 设置钩子服务（用于延迟注入）
func (s *RecordService) SetHookService(hookService *HookService) {
	s.hookService = hookService
}

// getDBFromRecordRepo 从 RecordRepository 获取数据库连接
// 处理缓存包装器的情况
func (s *RecordService) getDBFromRecordRepo() (*gorm.DB, error) {
	// 尝试类型断言到 CachedRecordRepository
	if cachedRepo, ok := s.recordRepo.(*infraRepository.CachedRecordRepository); ok {
		db := cachedRepo.GetDB()
		if db == nil {
			return nil, fmt.Errorf("无法从缓存仓库获取数据库连接")
		}
		return db, nil
	}
	// 尝试类型断言到 RecordRepositoryDynamic
	if dynamicRepo, ok := s.recordRepo.(*infraRepository.RecordRepositoryDynamic); ok {
		return dynamicRepo.GetDB(), nil
	}
	return nil, fmt.Errorf("不支持的 RecordRepository 类型")
}

// CreateRecord 创建记录（集成自动计算）✨ 事务版
//
// 执行流程：
//  1. 在事务中验证并创建Record实体
//  2. 保存到数据库
//  3. ✨ 自动计算所有虚拟字段（在事务内）
//  4. 收集 WebSocket 事件（不立即发送）
//  5. 事务成功后发布事件
//  6. 返回包含计算结果的Record
//
// 设计考量：
//   - 所有操作在单个事务中（原子性）
//   - 计算失败回滚整个事务
//   - 事务成功后才发布 WebSocket 事件
func (s *RecordService) CreateRecord(ctx context.Context, req dto.CreateRecordRequest, userID string) (*dto.RecordResponse, error) {
	// ✅ 在事务前检查表是否存在
	table, err := s.tableRepo.GetByID(ctx, req.TableID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找表失败: %v", err))
	}
	if table == nil {
		return nil, pkgerrors.ErrTableNotFound.WithDetails(map[string]interface{}{
			"table_id": req.TableID,
		})
	}

	var record *entity.Record
	var finalFields map[string]interface{}

	// ✅ 在事务中执行所有操作
	// 处理缓存包装器的情况
	db, err := s.getDBFromRecordRepo()
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("获取数据库连接失败: %v", err))
	}

	err = database.Transaction(ctx, db, nil, func(txCtx context.Context) error {
		// 1. 数据验证和类型转换
		var validatedData map[string]interface{}
		if s.typecastService != nil {
			var err error
			// ✅ 使用严格模式（typecast=false）进行验证，确保字段存在性和数据类型正确
			validatedData, err = s.typecastService.ValidateAndTypecastRecord(txCtx, req.TableID, req.Data, false)
			if err != nil {
				return err // 直接返回错误，保留具体的错误类型
			}
		} else {
			validatedData = req.Data
		}

		// 2. 验证必填字段
		if err := s.validateRequiredFields(txCtx, req.TableID, validatedData); err != nil {
			return err
		}

		// 3. 创建记录数据值对象
		recordData, err := valueobject.NewRecordData(validatedData)
		if err != nil {
			return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("记录数据无效: %v", err))
		}

		// 4. 创建记录实体
		record, err = entity.NewRecord(req.TableID, recordData, userID)
		if err != nil {
			return pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("创建记录实体失败: %v", err))
		}

		// 5. 保存记录（在事务中）
		if err := s.recordRepo.Save(txCtx, record); err != nil {
			return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存记录失败: %v", err))
		}

		logger.Info("记录创建成功（事务中）",
			logger.String("record_id", record.ID().String()),
			logger.String("table_id", req.TableID))

		// 6. ✨ 自动计算虚拟字段（在事务内）
		if s.calculationService != nil {
			if err := s.calculationService.CalculateRecordFields(txCtx, record); err != nil {
				logger.Error("虚拟字段计算失败（回滚事务）",
					logger.String("record_id", record.ID().String()),
					logger.ErrorField(err))
				return err
			}
			logger.Info("虚拟字段计算成功（事务中）✨",
				logger.String("record_id", record.ID().String()))
		}

		// 7. ✅ 收集事件（不立即发送）
		finalFields = record.Data().ToMap()
		event := &database.RecordEvent{
			EventType: "record.create",
			TID:       req.TableID,
			RID:       record.ID().String(),
			Fields:    finalFields,
			UserID:    userID,
		}
		database.AddEventToTx(txCtx, event)

		// 8. ✨ 添加事务提交后回调（发布 WebSocket 事件）
		database.AddTxCallback(txCtx, func() {
			s.publishRecordEvent(event)
		})

		return nil
	})

	if err != nil {
		logger.Error("记录创建失败",
			logger.String("table_id", req.TableID),
			logger.Any("data", req.Data),
			logger.ErrorField(err))
		return nil, err
	}

	logger.Info("记录创建完成，事件将在事务提交后发布",
		logger.String("record_id", record.ID().String()))

	// 触发记录创建钩子
	if s.hookService != nil {
		s.hookService.TriggerRecordCreateHook(ctx, req.TableID, record.ID().String(), finalFields)
	}

	return dto.FromRecordEntity(record), nil
}

// GetRecord 获取记录详情
func (s *RecordService) GetRecord(ctx context.Context, tableID, recordID string) (*dto.RecordResponse, error) {
	id := valueobject.NewRecordID(recordID)

	record, err := s.recordRepo.FindByTableAndID(ctx, tableID, id)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找记录失败: %v", err))
	}
	if record == nil {
		return nil, pkgerrors.ErrNotFound.WithDetails("记录不存在")
	}

	return dto.FromRecordEntity(record), nil
}

// UpdateRecord 更新记录（集成智能重算）✨ 事务版
//
// 执行流程：
//  1. 在事务中查找并验证Record
//  2. 识别变化的字段
//  3. 更新Record数据并保存
//  4. ✨ 智能重算受影响的虚拟字段（在事务内）
//  5. 收集 WebSocket 事件（不立即发送）
//  6. 事务成功后发布事件
//  7. 返回包含最新计算结果的Record
//
// 设计考量：
//   - 所有操作在单个事务中（原子性）
//   - 计算失败回滚整个事务
//   - 事务成功后才发布 WebSocket 事件
func (s *RecordService) UpdateRecord(ctx context.Context, tableID, recordID string, req dto.UpdateRecordRequest, userID string) (*dto.RecordResponse, error) {
	// 处理 Teable 格式的请求
	var updateData map[string]interface{}
	var version *int

	// 检查是否是 Teable 格式（有 record 字段）
	if req.Record != nil && req.Record.Fields != nil {
		// Teable 格式：使用 record.fields
		updateData = req.Record.Fields
		version = req.Version
		s.logger.Info("使用 Teable 格式更新记录",
			zap.String("fieldKeyType", req.FieldKeyType),
			zap.Any("fields", updateData))
	} else if req.Data != nil {
		// 兼容格式：使用 data 字段
		updateData = req.Data
		version = req.Version
		s.logger.Info("使用兼容格式更新记录", zap.Any("data", updateData))
	} else {
		return nil, pkgerrors.ErrValidationFailed.WithMessage("请求格式无效：必须提供 record.fields 或 data 字段")
	}

	// ✅ 在事务前检查表是否存在
	table, err := s.tableRepo.GetByID(ctx, tableID)
	if err != nil {
		return nil, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找表失败: %v", err))
	}
	if table == nil {
		return nil, pkgerrors.ErrTableNotFound.WithDetails(map[string]interface{}{
			"table_id": tableID,
		})
	}

	var record *entity.Record
	var finalFields map[string]interface{}

	// ✅ 在事务中执行所有操作
	// 处理缓存包装器的情况
	db, err := s.getDBFromRecordRepo()
	if err != nil {
		return nil, pkgerrors.ErrInternalServer.WithDetails(fmt.Sprintf("获取数据库连接失败: %v", err))
	}

	err = database.Transaction(ctx, db, nil, func(txCtx context.Context) error {
		// 1. 查找记录（使用 tableID）
		id := valueobject.NewRecordID(recordID)
		var err error
		records, err := s.recordRepo.FindByIDs(txCtx, tableID, []valueobject.RecordID{id})
		if err != nil {
			return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找记录失败: %v", err))
		}
		if len(records) == 0 {
			return pkgerrors.ErrNotFound.WithDetails("记录不存在")
		}
		record = records[0]

		// ✅ 2. 乐观锁检查：只在明确提供版本号且大于0时才检查
		if version != nil && *version > 0 {
			expectedVersion, err := valueobject.NewRecordVersion(int64(*version))
			if err != nil {
				return pkgerrors.ErrValidationFailed.WithMessage("无效的版本号").WithDetails(map[string]interface{}{
					"version": *version,
				})
			}
			if record.HasChangedSince(expectedVersion) {
				return pkgerrors.ErrConflict.WithMessage("记录已被其他用户修改，请刷新后重试").WithDetails(map[string]interface{}{
					"expected_version": *version,
					"current_version":  record.Version().Value(),
				})
			}
		}

		// 3. 识别变化的字段（用于智能重算）
		oldData := record.Data().ToMap()
		changedFieldIDs := s.identifyChangedFields(oldData, updateData)

		// 4. 创建新数据
		newData, err := valueobject.NewRecordData(updateData)
		if err != nil {
			return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("记录数据无效: %v", err))
		}

		// 5. 更新记录（会递增版本号）
		if err := record.Update(newData, userID); err != nil {
			return pkgerrors.ErrValidationFailed.WithDetails(fmt.Sprintf("更新记录失败: %v", err))
		}

		// 6. ✨ 智能重算受影响的虚拟字段（在事务内，保存之前）
		if s.calculationService != nil && len(changedFieldIDs) > 0 {
			if err := s.calculationService.CalculateAffectedFields(txCtx, record, changedFieldIDs); err != nil {
				logger.Error("受影响字段重算失败（回滚事务）",
					logger.String("record_id", recordID),
					logger.Int("changed_fields", len(changedFieldIDs)),
					logger.ErrorField(err))
				return err
			}
			logger.Info("受影响字段重算成功（事务中）✨",
				logger.String("record_id", recordID),
				logger.Int("changed_fields", len(changedFieldIDs)))
		}

		// 7. 保存（在事务中，包含计算后的字段）
		// 注意：record.Update()已经递增了版本，但Save会用旧版本做乐观锁检查
		if err := s.recordRepo.Save(txCtx, record); err != nil {
			return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("保存记录失败: %v", err))
		}

		logger.Info("记录更新成功（事务中）", logger.String("record_id", recordID))

		// 8. ✅ 收集事件（不立即发送）
		finalFields = record.Data().ToMap()
		event := &database.RecordEvent{
			EventType:  "record.update",
			TID:        record.TableID(),
			RID:        recordID,
			Fields:     finalFields,
			UserID:     userID,
			OldVersion: record.Version().Value() - 1,
			NewVersion: record.Version().Value(),
		}
		database.AddEventToTx(txCtx, event)

		// 9. ✨ 添加事务提交后回调（发布 WebSocket 事件）
		database.AddTxCallback(txCtx, func() {
			s.publishRecordEvent(event)
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	logger.Info("记录更新完成，事件将在事务提交后发布",
		logger.String("record_id", recordID))

	return dto.FromRecordEntity(record), nil
}

// validateRequiredFields 验证必填字段
// 返回 nil 表示验证通过，返回 AppError 表示验证失败
func (s *RecordService) validateRequiredFields(ctx context.Context, tableID string, data map[string]interface{}) error {
	// 1. 获取表的所有字段
	fields, err := s.fieldRepo.FindByTableID(ctx, tableID)
	if err != nil {
		return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("获取字段列表失败: %v", err))
	}

	// 2. 检查每个必填字段
	missingFields := make([]map[string]string, 0)
	for _, field := range fields {
		// 跳过计算字段（只读，不需要用户提供）
		if field.IsComputed() {
			continue
		}

		// 检查是否为必填字段
		if !field.IsRequired() {
			continue
		}

		fieldID := field.ID().String()
		fieldName := field.Name().String()

		// 检查字段是否在数据中
		value, exists := data[fieldID]
		if !exists {
			// 尝试通过字段名查找
			value, exists = data[fieldName]
		}

		// 检查值是否为空
		if !exists || value == nil || value == "" {
			missingFields = append(missingFields, map[string]string{
				"id":   fieldID,
				"name": fieldName,
			})
		}
	}

	if len(missingFields) > 0 {
		return pkgerrors.ErrFieldRequired.WithDetails(map[string]interface{}{
			"missing_fields": missingFields,
			"message":        fmt.Sprintf("必填字段缺失，共 %d 个", len(missingFields)),
		})
	}

	return nil
}

// identifyChangedFields 识别变化的字段ID列表
func (s *RecordService) identifyChangedFields(oldData map[string]interface{}, newData map[string]interface{}) []string {
	changed := make([]string, 0)

	// 检查所有新数据中的字段
	for fieldID, newValue := range newData {
		oldValue, exists := oldData[fieldID]

		// 字段不存在或值发生变化
		if !exists || !s.isValueEqual(oldValue, newValue) {
			changed = append(changed, fieldID)
		}
	}

	return changed
}

// isValueEqual 比较两个值是否相等（简化版）
func (s *RecordService) isValueEqual(a, b interface{}) bool {
	// 简化比较：使用fmt.Sprintf转字符串比较
	// 实际项目中可以使用reflect.DeepEqual或更精确的比较
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// DeleteRecord 删除记录 ✨ 事务版
// ✅ 对齐 Teable：所有记录操作都需要 tableID
func (s *RecordService) DeleteRecord(ctx context.Context, tableID, recordID string) error {
	// ✅ 在事务中执行所有操作
	err := database.Transaction(ctx, s.recordRepo.(*infraRepository.RecordRepositoryDynamic).GetDB(), nil, func(txCtx context.Context) error {
		id := valueobject.NewRecordID(recordID)

		// 1. 先获取记录信息（使用 tableID）
		record, err := s.recordRepo.FindByTableAndID(txCtx, tableID, id)
		if err != nil {
			return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查找记录失败: %v", err))
		}
		if record == nil {
			return pkgerrors.ErrNotFound.WithDetails("记录不存在")
		}

		// 2. 删除记录（使用 tableID）
		if err := s.recordRepo.DeleteByTableAndID(txCtx, tableID, id); err != nil {
			return pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("删除记录失败: %v", err))
		}

		logger.Info("记录删除成功（事务中）", logger.String("record_id", recordID))

		// 3. ✅ 收集事件（不立即发送）
		event := &database.RecordEvent{
			EventType: "record.delete",
			TID:       tableID,
			RID:       recordID,
			Fields:    record.Data().ToMap(), // 保存删除前的数据
		}
		database.AddEventToTx(txCtx, event)

		// 4. ✨ 添加事务提交后回调（发布 WebSocket 事件）
		database.AddTxCallback(txCtx, func() {
			s.publishRecordEvent(event)
		})

		return nil
	})

	if err != nil {
		return err
	}

	logger.Info("记录删除完成，事件将在事务提交后发布",
		logger.String("record_id", recordID))

	return nil
}

// ListRecords 列出表格的所有记录
func (s *RecordService) ListRecords(ctx context.Context, tableID string, limit, offset int) ([]*dto.RecordResponse, int64, error) {
	// 构建过滤器
	filter := recordRepo.RecordFilter{
		TableID: &tableID,
		Limit:   limit,
		Offset:  offset,
	}

	if filter.Limit == 0 {
		filter.Limit = 100 // 默认限制
	}

	// 查询记录列表
	records, total, err := s.recordRepo.List(ctx, filter)
	if err != nil {
		return nil, 0, pkgerrors.ErrDatabaseOperation.WithDetails(fmt.Sprintf("查询记录列表失败: %v", err))
	}

	// ✅ 优化：批量预加载字段，避免N+1查询
	// 一次性获取所有字段，然后在计算时复用
	if s.calculationService != nil && len(records) > 0 {
		logger.Info("开始计算记录列表的虚拟字段",
			logger.String("table_id", tableID),
			logger.Int("record_count", len(records)))

		// 预加载字段（只查询一次）
		fields, err := s.fieldRepo.FindByTableID(ctx, tableID)
		if err != nil {
			logger.Warn("预加载字段失败，跳过虚拟字段计算",
				logger.String("table_id", tableID),
				logger.ErrorField(err))
		} else {
			// 批量计算所有记录的虚拟字段（使用预加载的字段）
			for _, record := range records {
				if err := s.calculationService.CalculateRecordFieldsWithFields(ctx, record, fields); err != nil {
					logger.Warn("计算记录虚拟字段失败",
						logger.String("record_id", record.ID().String()),
						logger.ErrorField(err))
					// 不中断整个列表，继续处理其他记录
				}
			}

			logger.Info("记录列表虚拟字段计算完成",
				logger.String("table_id", tableID),
				logger.Int("record_count", len(records)),
				logger.Int("fields_count", len(fields)))
		}
	}

	// 转换为 DTO
	return dto.FromRecordEntities(records), total, nil
}

// BatchCreateRecords 批量创建记录（严格遵守：返回AppError）
func (s *RecordService) BatchCreateRecords(ctx context.Context, tableID string, req dto.BatchCreateRecordRequest, userID string) (*dto.BatchCreateRecordResponse, error) {
	// ✅ 允许空数组：直接返回成功响应
	if len(req.Records) == 0 {
		return &dto.BatchCreateRecordResponse{
			Records:      []*dto.RecordResponse{},
			SuccessCount: 0,
			FailedCount:  0,
			Errors:       []string{},
		}, nil
	}

	successRecords := make([]*dto.RecordResponse, 0, len(req.Records))
	errorsList := make([]string, 0)

	// 遍历每条记录进行创建
	for i, item := range req.Records {
		// ✅ 对齐单条创建逻辑：使用 typecast service 验证和转换数据
		validatedData, err := s.typecastService.ValidateAndTypecastRecord(ctx, tableID, item.Fields, true)
		if err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%d数据验证失败: %v", i+1, err))
			continue
		}

		// 创建记录数据值对象（使用验证后的数据）
		recordData, err := valueobject.NewRecordData(validatedData)
		if err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%d数据无效: %v", i+1, err))
			continue
		}

		// 创建记录实体
		record, err := entity.NewRecord(tableID, recordData, userID)
		if err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%d创建失败: %v", i+1, err))
			continue
		}

		// 保存记录
		if err := s.recordRepo.Save(ctx, record); err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%d保存失败: %v", i+1, err))
			continue
		}

		// ✨ 自动计算虚拟字段（对齐单条创建逻辑）
		if s.calculationService != nil {
			if err := s.calculationService.CalculateRecordFields(ctx, record); err != nil {
				logger.Warn("记录虚拟字段计算失败（不影响创建）",
					logger.String("record_id", record.ID().String()),
					logger.Int("record_index", i+1),
					logger.ErrorField(err),
				)
				// 计算失败不影响记录创建，继续
			}
		}

		// 添加到成功列表
		successRecords = append(successRecords, dto.FromRecordEntity(record))
	}

	logger.Info("批量创建记录完成",
		logger.String("table_id", tableID),
		logger.Int("total", len(req.Records)),
		logger.Int("success", len(successRecords)),
		logger.Int("failed", len(errorsList)),
	)

	return &dto.BatchCreateRecordResponse{
		Records:      successRecords,
		SuccessCount: len(successRecords),
		FailedCount:  len(errorsList),
		Errors:       errorsList,
	}, nil
}

// BatchUpdateRecords 批量更新记录（严格遵守：返回AppError）
func (s *RecordService) BatchUpdateRecords(ctx context.Context, tableID string, req dto.BatchUpdateRecordRequest, userID string) (*dto.BatchUpdateRecordResponse, error) {
	successRecords := make([]*dto.RecordResponse, 0, len(req.Records))
	errorsList := make([]string, 0)

	// 遍历每条记录进行更新
	for i, item := range req.Records {
		// 查找记录（使用 tableID）
		id := valueobject.NewRecordID(item.ID)
		records, err := s.recordRepo.FindByIDs(ctx, tableID, []valueobject.RecordID{id})
		if err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%s查找失败: %v", item.ID, err))
			continue
		}
		if len(records) == 0 {
			errorsList = append(errorsList, fmt.Sprintf("记录%s不存在", item.ID))
			continue
		}
		record := records[0]

		// 创建新数据
		newData, err := valueobject.NewRecordData(item.Fields)
		if err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%d数据无效: %v", i+1, err))
			continue
		}

		// 更新记录
		if err := record.Update(newData, userID); err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%s更新失败: %v", item.ID, err))
			continue
		}

		// 保存
		if err := s.recordRepo.Save(ctx, record); err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%s保存失败: %v", item.ID, err))
			continue
		}

		// 添加到成功列表
		successRecords = append(successRecords, dto.FromRecordEntity(record))
	}

	logger.Info("批量更新记录完成",
		logger.Int("total", len(req.Records)),
		logger.Int("success", len(successRecords)),
		logger.Int("failed", len(errorsList)),
	)

	return &dto.BatchUpdateRecordResponse{
		Records:      successRecords,
		SuccessCount: len(successRecords),
		FailedCount:  len(errorsList),
		Errors:       errorsList,
	}, nil
}

// BatchDeleteRecords 批量删除记录（严格遵守：返回AppError）
func (s *RecordService) BatchDeleteRecords(ctx context.Context, tableID string, req dto.BatchDeleteRecordRequest) (*dto.BatchDeleteRecordResponse, error) {
	errorsList := make([]string, 0)
	successCount := 0

	// 遍历每条记录进行删除（使用 tableID）
	for _, recordID := range req.RecordIDs {
		id := valueobject.NewRecordID(recordID)

		// 删除记录（使用 tableID）
		if err := s.recordRepo.DeleteByTableAndID(ctx, tableID, id); err != nil {
			errorsList = append(errorsList, fmt.Sprintf("记录%s删除失败: %v", recordID, err))
			continue
		}

		successCount++
	}

	logger.Info("批量删除记录完成",
		logger.Int("total", len(req.RecordIDs)),
		logger.Int("success", successCount),
		logger.Int("failed", len(errorsList)),
	)

	return &dto.BatchDeleteRecordResponse{
		SuccessCount: successCount,
		FailedCount:  len(errorsList),
		Errors:       errorsList,
	}, nil
}

// publishRecordEvent 发布记录事件到 WebSocket
func (s *RecordService) publishRecordEvent(event *database.RecordEvent) {
	// 1. 发布到传统WebSocket广播器（保持向后兼容）
	if s.broadcaster != nil {
		switch event.EventType {
		case "record.create":
			s.broadcaster.BroadcastRecordCreate(event.TID, event.RID, event.Fields)
			logger.Info("WebSocket 事件已发布：创建",
				logger.String("table_id", event.TID),
				logger.String("record_id", event.RID))

		case "record.update":
			s.broadcaster.BroadcastRecordUpdate(event.TID, event.RID, event.Fields)
			logger.Info("WebSocket 事件已发布：更新",
				logger.String("table_id", event.TID),
				logger.String("record_id", event.RID),
				logger.Int64("version", event.NewVersion))

		case "record.delete":
			s.broadcaster.BroadcastRecordDelete(event.TID, event.RID)
			logger.Info("WebSocket 事件已发布：删除",
				logger.String("table_id", event.TID),
				logger.String("record_id", event.RID))

		default:
			logger.Warn("未知的事件类型",
				logger.String("event_type", event.EventType))
		}
	}

	// 2. 发布到ShareDB（实时协作）
	if s.shareDBService != nil && event.EventType == "record.update" {
		// 创建ShareDB操作
		op := sharedb.OTOperation{
			"p": []interface{}{"data", event.Fields}, // 设置路径操作
		}

		// 广播ShareDB操作
		err := s.shareDBService.BroadcastOperation(event.TID, event.RID, []sharedb.OTOperation{op})
		if err != nil {
			logger.Error("ShareDB广播失败", logger.String("error", err.Error()))
		} else {
			logger.Info("✅ ShareDB操作已广播",
				logger.String("table_id", event.TID),
				logger.String("record_id", event.RID))
		}
	}

	// 2. 发布到 ShareDB 实时协作系统 ✨
	if s.shareDBService != nil {
		ctx := context.Background()
		collection := fmt.Sprintf("rec_%s", event.TID)
		docID := event.RID

		// 构建 Teable 风格的 ShareDB EditOp
		// 基于 Teable 的 IOtOperation 格式
		operations := make([]sharedb.OTOperation, 0)

		// 为每个字段变化创建 OT 操作
		for fieldId, fieldValue := range event.Fields {
			operation := sharedb.OTOperation{
				"p":  []interface{}{"fields", fieldId}, // path: ["fields", fieldId] - 修复路径格式
				"oi": fieldValue,                       // object insert: new value
			}
			operations = append(operations, operation)
		}

		// 转换为 opbuilder.Operation 类型
		opBuilderOp := &opbuilder.Operation{
			Path:     []interface{}{operations},
			OldValue: nil,
			NewValue: nil,
			Type:     opbuilder.OpTypeSet,
		}

		// 发布到 ShareDB
		err := s.shareDBService.PublishOp(ctx, collection, docID, opBuilderOp)
		if err != nil {
			logger.Error("ShareDB 操作发布失败",
				logger.String("collection", collection),
				logger.String("doc_id", docID),
				logger.String("event_type", event.EventType),
				logger.ErrorField(err))
		} else {
			logger.Info("ShareDB 操作已发布",
				logger.String("collection", collection),
				logger.String("doc_id", docID),
				logger.String("event_type", event.EventType),
				logger.Int64("version", event.NewVersion),
				logger.Int("operations_count", len(operations)))
		}
	}

	// 3. 发布到统一业务事件系统（支持SSE、WebSocket、Yjs）
	if s.businessEvents != nil {
		ctx := context.Background()
		var businessEventType events.BusinessEventType

		switch event.EventType {
		case "record.create":
			businessEventType = events.BusinessEventTypeRecordCreate
		case "record.update":
			businessEventType = events.BusinessEventTypeRecordUpdate
		case "record.delete":
			businessEventType = events.BusinessEventTypeRecordDelete
		default:
			logger.Warn("未知的业务事件类型",
				logger.String("event_type", event.EventType))
			return
		}

		err := s.businessEvents.PublishRecordEvent(
			ctx,
			businessEventType,
			event.TID,
			event.RID,
			event.Fields,
			event.UserID,
			event.NewVersion,
		)

		if err != nil {
			logger.Error("发布业务事件失败",
				logger.String("event_type", string(businessEventType)),
				logger.String("table_id", event.TID),
				logger.String("record_id", event.RID),
				logger.ErrorField(err))
		} else {
			logger.Info("业务事件已发布",
				logger.String("event_type", string(businessEventType)),
				logger.String("table_id", event.TID),
				logger.String("record_id", event.RID),
				logger.Int64("version", event.NewVersion))
		}
	}
}

// SetShareDBService 设置 ShareDB 服务
func (s *RecordService) SetShareDBService(shareDBService *sharedb.ShareDBService) {
	s.shareDBService = shareDBService
}
