package http

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	infraRepository "github.com/easyspace-ai/luckdb/server/internal/infrastructure/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// RecordHandler 记录HTTP处理器
type RecordHandler struct {
	recordService      *application.RecordService
	fieldService       *application.FieldService       // ✅ 新增
	calculationService *application.CalculationService // ✅ 新增
	recordRepo         recordRepo.RecordRepository     // ✅ 新增
}

// NewRecordHandler 创建记录处理器
func NewRecordHandler(
	recordService *application.RecordService,
	fieldService *application.FieldService, // ✅ 新增参数
	calculationService *application.CalculationService, // ✅ 新增参数
	recordRepo recordRepo.RecordRepository, // ✅ 新增参数
) *RecordHandler {
	return &RecordHandler{
		recordService:      recordService,
		fieldService:       fieldService,       // ✅ 注入
		calculationService: calculationService, // ✅ 注入
		recordRepo:         recordRepo,         // ✅ 注入
	}
}

// CreateRecord 创建记录
func (h *RecordHandler) CreateRecord(c *gin.Context) {
	// 添加详细的请求日志
	logger.Info("开始创建记录",
		logger.String("method", c.Request.Method),
		logger.String("path", c.Request.URL.Path),
		logger.String("ip", c.ClientIP()),
		logger.String("content_type", c.GetHeader("Content-Type")))

	var req dto.CreateRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Error("JSON绑定失败",
			logger.ErrorField(err),
			logger.String("path", c.Request.URL.Path))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	logger.Info("JSON绑定成功",
		logger.String("table_id", req.TableID),
		logger.Any("data", req.Data))

	userID := c.GetString("user_id")
	if userID == "" {
		logger.Error("用户ID为空", logger.String("path", c.Request.URL.Path))
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	logger.Info("开始调用 RecordService.CreateRecord",
		logger.String("table_id", req.TableID),
		logger.String("user_id", userID))

	resp, err := h.recordService.CreateRecord(c.Request.Context(), req, userID)
	if err != nil {
		// 记录详细错误信息
		logger.Error("创建记录失败",
			logger.String("table_id", req.TableID),
			logger.Any("data", req.Data),
			logger.String("user_id", userID),
			logger.ErrorField(err))
		response.Error(c, err)
		return
	}

	logger.Info("创建记录成功",
		logger.String("table_id", req.TableID),
		logger.String("record_id", resp.ID))

	// ✅ 虚拟字段计算已在 Service 事务内完成，无需异步计算
	response.Success(c, resp, "创建记录成功")
}

// GetRecord 获取记录详情
func (h *RecordHandler) GetRecord(c *gin.Context) {
	tableID := c.Param("tableId")
	recordID := c.Param("recordId")

	// ✅ 兼容旧路由：如果没有 tableID，尝试通过 recordID 查找 tableID
	if tableID == "" {
		// 使用临时兼容方法查找 tableID
		recordIDObj := valueobject.NewRecordID(recordID)
		foundTableID, err := h.recordRepo.(*infraRepository.RecordRepositoryDynamic).FindTableIDByRecordID(c.Request.Context(), recordIDObj)
		if err != nil {
			response.Error(c, errors.ErrNotFound.WithDetails("记录不存在"))
			return
		}
		tableID = foundTableID
	}

	resp, err := h.recordService.GetRecord(c.Request.Context(), tableID, recordID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取记录成功")
}

// UpdateRecord 更新记录
func (h *RecordHandler) UpdateRecord(c *gin.Context) {
	ctx := c.Request.Context()
	tableID := c.Param("tableId") // ✅ 从路由获取 tableId（新路由）
	recordID := c.Param("recordId")

	// ✅ 兼容旧路由：如果没有 tableId，尝试从 record 查找
	if tableID == "" {
		// TODO: 从 record 的元数据中获取 tableId（需要额外查询）
		response.Error(c, errors.ErrBadRequest.WithDetails("请使用新路由: PATCH /api/v1/tables/:tableId/records/:recordId"))
		return
	}

	var req dto.UpdateRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	resp, err := h.recordService.UpdateRecord(ctx, tableID, recordID, req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	// ✅ 新增：自动计算受影响的虚拟字段
	// 异步计算，不阻塞响应
	logger.Info("⚡ 即将启动虚拟字段异步计算",
		logger.String("record_id", recordID))
	go h.calculateVirtualFieldsAsync(tableID, recordID, req)
	logger.Info("⚡ 异步计算goroutine已启动",
		logger.String("record_id", recordID))

	response.Success(c, resp, "更新记录成功")
}

// DeleteRecord 删除记录
func (h *RecordHandler) DeleteRecord(c *gin.Context) {
	tableID := c.Param("tableId") // ✅ 从路由获取 tableId（新路由）
	recordID := c.Param("recordId")

	// ✅ 兼容旧路由：如果没有 tableId，返回错误提示使用新路由
	if tableID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("请使用新路由: DELETE /api/v1/tables/:tableId/records/:recordId"))
		return
	}

	if err := h.recordService.DeleteRecord(c.Request.Context(), tableID, recordID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除记录成功")
}

// BatchCreateRecords 批量创建记录
// POST /api/v1/tables/:tableId/records/batch
// ✅ 严格使用 response.Success
func (h *RecordHandler) BatchCreateRecords(c *gin.Context) {
	tableID := c.Param("tableId")

	// 1. 参数绑定
	var req dto.BatchCreateRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 2. 获取用户ID
	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	// 3. 调用Service
	resp, err := h.recordService.BatchCreateRecords(c.Request.Context(), tableID, req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 4. ✅ 严格使用response.Success
	response.Success(c, resp, "批量创建记录成功")
}

// BatchUpdateRecords 批量更新记录
// PATCH /api/v1/records/batch
// ✅ 严格使用 response.Success
func (h *RecordHandler) BatchUpdateRecords(c *gin.Context) {
	// 1. 获取 tableId
	tableID := c.Param("tableId")
	if tableID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("请使用新路由: PATCH /api/v1/tables/:tableId/records/batch"))
		return
	}

	// 2. 参数绑定
	var req dto.BatchUpdateRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 3. 获取用户ID
	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	// 4. 调用Service（传递 tableID）
	resp, err := h.recordService.BatchUpdateRecords(c.Request.Context(), tableID, req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 4. ✅ 严格使用response.Success
	response.Success(c, resp, "批量更新记录成功")
}

// BatchDeleteRecords 批量删除记录
// DELETE /api/v1/tables/:tableId/records/batch
// ✅ 严格使用 response.Success
func (h *RecordHandler) BatchDeleteRecords(c *gin.Context) {
	// 1. 获取 tableId
	tableID := c.Param("tableId")
	if tableID == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("请使用新路由: DELETE /api/v1/tables/:tableId/records/batch"))
		return
	}

	// 2. 参数绑定
	var req dto.BatchDeleteRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 3. 调用Service（传递 tableID）
	resp, err := h.recordService.BatchDeleteRecords(c.Request.Context(), tableID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 4. ✅ 严格使用response.Success
	response.Success(c, resp, "批量删除记录成功")
}

// ListRecords 列出表格的所有记录
func (h *RecordHandler) ListRecords(c *gin.Context) {
	tableID := c.Param("tableId")

    // 解析分页参数：优先支持 page/perPage，其次兼容 limit/offset
    var limit, offset int
    if pageStr := c.Query("page"); pageStr != "" {
        page, _ := strconv.Atoi(pageStr)
        if page < 1 {
            page = 1
        }
        perPage := 20
        if pp := c.Query("perPage"); pp != "" {
            if v, err := strconv.Atoi(pp); err == nil && v > 0 {
                perPage = v
            }
        }
        limit = perPage
        offset = (page - 1) * perPage
    } else {
        // 回退到 limit/offset
        limit, _ = strconv.Atoi(c.DefaultQuery("limit", "100"))
        offset, _ = strconv.Atoi(c.DefaultQuery("offset", "0"))
        if limit <= 0 {
            limit = 100
        }
        if offset < 0 {
            offset = 0
        }
    }

	// 调用 Service 获取记录列表和总数
	records, total, err := h.recordService.ListRecords(c.Request.Context(), tableID, limit, offset)
	if err != nil {
		response.Error(c, err)
		return
	}

	// 计算总页数
    totalPages := int((total + int64(limit) - 1) / int64(limit))
    page := (offset / limit) + 1

	// 使用分页响应（Records 是唯一需要分页的资源）
	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      int(total),
		TotalPages: totalPages,
	}

	response.PaginatedSuccess(c, records, pagination, "获取记录列表成功")
}

// ==================== 辅助方法 ====================

// calculateVirtualFieldsAsync 异步计算虚拟字段
func (h *RecordHandler) calculateVirtualFieldsAsync(tableID, recordID string, req dto.UpdateRecordRequest) {
	// ⚠️ 关键：添加延迟确保主线程的数据库事务已提交
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background() // 使用独立context避免被取消

	logger.Info("🚀 开始异步计算虚拟字段",
		logger.String("record_id", recordID))

	// 1. 提取变更的字段（现在是字段ID）
	changedFields := h.extractChangedFieldNames(req.Data)
	if len(changedFields) == 0 {
		logger.Warn("⚠️ 没有字段变更",
			logger.String("record_id", recordID))
		return
	}

	logger.Info("📝 检测到字段变更",
		logger.String("record_id", recordID),
		logger.Strings("changed_fields", changedFields))

	// 2. ✅ 修复：检查是字段ID还是字段名
	// 现在前端发送的是字段ID，直接使用
	fieldIDs := make([]string, 0, len(changedFields))

	// 检查第一个字段是否为字段ID格式（fld_开头）
	isFieldID := len(changedFields) > 0 && strings.HasPrefix(changedFields[0], "fld_")

	if isFieldID {
		// ✅ 已经是字段ID，直接使用
		fieldIDs = changedFields
		logger.Info("🔍 检测到字段ID格式，直接使用",
			logger.Int("field_count", len(fieldIDs)))
	} else {
		// 旧格式：字段名，需要转换
		var err error
		fieldIDs, err = h.fieldService.GetFieldIDsByNames(ctx, tableID, changedFields)
		if err != nil {
			logger.Error("❌ 获取字段ID失败",
				logger.String("table_id", tableID),
				logger.ErrorField(err))
			return
		}
		logger.Info("🔍 字段名称转换完成",
			logger.Int("field_count", len(fieldIDs)))
	}

	// 4. 通过RecordRepository获取Record entity（包含最新数据）
	recordIDObj := valueobject.NewRecordID(recordID)
	recordEntity, err := h.recordRepo.FindByTableAndID(ctx, tableID, recordIDObj)
	if err != nil {
		logger.Error("❌ 获取Record entity失败",
			logger.String("record_id", recordID),
			logger.String("table_id", tableID),
			logger.ErrorField(err))
		return
	}

	// 📊 诊断：输出当前record的数据
	logger.Info("🔍 当前Record数据",
		logger.String("record_id", recordID),
		logger.Any("data_keys", h.getDataKeys(recordEntity.Data().ToMap())))

	// 5. 调用CalculationService进行虚拟字段重算
	if err := h.calculationService.CalculateAffectedFields(ctx, recordEntity, fieldIDs); err != nil {
		logger.Error("❌ 虚拟字段计算失败",
			logger.String("record_id", recordID),
			logger.ErrorField(err))
		return
	}

	logger.Info("✅ 虚拟字段计算完成",
		logger.String("record_id", recordID))
}

// getDataKeys 获取data的keys用于诊断
func (h *RecordHandler) getDataKeys(data map[string]interface{}) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	return keys
}

// extractChangedFieldNames 提取变更的字段名称
func (h *RecordHandler) extractChangedFieldNames(fields map[string]interface{}) []string {
	if fields == nil {
		return []string{}
	}

	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}

	return names
}
