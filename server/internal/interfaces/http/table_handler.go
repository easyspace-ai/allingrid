package http

import (
	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// TableHandler 表格HTTP处理器
type TableHandler struct {
	tableService *application.TableService
}

// NewTableHandler 创建表格处理器
func NewTableHandler(tableService *application.TableService) *TableHandler {
	return &TableHandler{
		tableService: tableService,
	}
}

// CreateTable 创建表格
func (h *TableHandler) CreateTable(c *gin.Context) {
	var req dto.CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	userID := c.GetString("user_id")
	if userID == "" {
		response.Error(c, pkgerrors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	resp, err := h.tableService.CreateTable(c.Request.Context(), req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "创建表格成功")
}

// GetTable 获取表格详情
func (h *TableHandler) GetTable(c *gin.Context) {
	tableID := c.Param("tableId")

	resp, err := h.tableService.GetTable(c.Request.Context(), tableID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取表格成功")
}

// UpdateTable 更新表格
func (h *TableHandler) UpdateTable(c *gin.Context) {
	tableID := c.Param("tableId")

	var req dto.UpdateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	resp, err := h.tableService.UpdateTable(c.Request.Context(), tableID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "更新表格成功")
}

// DeleteTable 删除表格
func (h *TableHandler) DeleteTable(c *gin.Context) {
	tableID := c.Param("tableId")

	if err := h.tableService.DeleteTable(c.Request.Context(), tableID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除表格成功")
}

// ListTables 列出Base下的所有表格
func (h *TableHandler) ListTables(c *gin.Context) {
	baseID := c.Param("baseId")

	resp, err := h.tableService.ListTables(c.Request.Context(), baseID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, resp, "获取表格列表成功")
}

// RenameTable 重命名表
// PUT /api/v1/tables/:tableId/rename
func (h *TableHandler) RenameTable(c *gin.Context) {
	tableID := c.Param("tableId")
	if tableID == "" {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails("表格ID不能为空"))
		return
	}

	var req dto.RenameTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 调用服务层重命名表
	table, err := h.tableService.RenameTable(c.Request.Context(), tableID, req)
	if err != nil {
		logger.Error("重命名表失败",
			logger.String("table_id", tableID),
			logger.String("new_name", req.Name),
			logger.ErrorField(err))
		response.Error(c, err)
		return
	}

	logger.Info("表重命名成功",
		logger.String("table_id", tableID),
		logger.String("new_name", req.Name))

	response.Success(c, table, "表格重命名成功")
}

// DuplicateTable 复制表
// POST /api/v1/tables/:tableId/duplicate
func (h *TableHandler) DuplicateTable(c *gin.Context) {
	tableID := c.Param("tableId")
	if tableID == "" {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails("表格ID不能为空"))
		return
	}

	var req dto.DuplicateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	// 获取用户ID
	userID := getUserIDFromContext(c)
	if userID == "" {
		response.Error(c, pkgerrors.ErrUnauthorized.WithDetails("用户未认证"))
		return
	}

	// 调用服务层复制表
	table, err := h.tableService.DuplicateTable(c.Request.Context(), tableID, req, userID)
	if err != nil {
		logger.Error("复制表失败",
			logger.String("table_id", tableID),
			logger.String("new_name", req.Name),
			logger.ErrorField(err))
		response.Error(c, err)
		return
	}

	logger.Info("表复制成功",
		logger.String("original_table_id", tableID),
		logger.String("new_table_id", table.ID),
		logger.String("new_name", req.Name),
		logger.Bool("with_data", req.WithData),
		logger.Bool("with_views", req.WithViews),
		logger.Bool("with_fields", req.WithFields))

	response.Success(c, table, "表格复制成功")
}

// GetTableUsage 获取表用量信息
// GET /api/v1/tables/:tableId/usage
func (h *TableHandler) GetTableUsage(c *gin.Context) {
	tableID := c.Param("tableId")
	if tableID == "" {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails("表格ID不能为空"))
		return
	}

	// 调用服务层获取表用量
	usage, err := h.tableService.GetTableUsage(c.Request.Context(), tableID)
	if err != nil {
		logger.Error("获取表用量失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
		response.Error(c, err)
		return
	}

	logger.Info("获取表用量成功",
		logger.String("table_id", tableID),
		logger.Int64("record_count", usage.RecordCount),
		logger.Float64("usage_percentage", usage.UsagePercentage))

	response.Success(c, usage, "获取表用量成功")
}

// GetTableManagementMenu 获取表管理菜单信息
// GET /api/v1/tables/:tableId/menu
func (h *TableHandler) GetTableManagementMenu(c *gin.Context) {
	tableID := c.Param("tableId")
	if tableID == "" {
		response.Error(c, pkgerrors.ErrBadRequest.WithDetails("表格ID不能为空"))
		return
	}

	// 获取表基本信息
	table, err := h.tableService.GetTable(c.Request.Context(), tableID)
	if err != nil {
		logger.Error("获取表信息失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
		response.Error(c, err)
		return
	}

	// 获取表用量信息
	usage, err := h.tableService.GetTableUsage(c.Request.Context(), tableID)
	if err != nil {
		logger.Warn("获取表用量失败，使用默认值",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
		// 使用默认用量信息
		usage = &dto.TableUsageResponse{
			RecordCount:     0,
			MaxRecords:      20000,
			UsagePercentage: 0,
			StorageSize:     0,
			MaxStorageSize:  100 * 1024 * 1024, // 100MB
		}
	}

	// 构建菜单响应
	menu := gin.H{
		"table": table,
		"usage": usage,
		"actions": gin.H{
			"rename": gin.H{
				"enabled": true,
				"label":   "重命名",
				"icon":    "edit",
			},
			"duplicate": gin.H{
				"enabled": true,
				"label":   "复制数据表",
				"icon":    "copy",
			},
			"move": gin.H{
				"enabled": false, // 暂时禁用
				"label":   "移动至",
				"icon":    "move",
			},
			"delete": gin.H{
				"enabled": true,
				"label":   "删除数据表",
				"icon":    "trash",
				"danger":  true, // 危险操作
			},
		},
	}

	response.Success(c, menu, "获取表管理菜单成功")
}

// getUserIDFromContext 从上下文获取用户ID
// 这是一个辅助函数，实际实现需要根据你的认证系统来调整
func getUserIDFromContext(c *gin.Context) string {
	// 方式1：从JWT token中获取
	if userID, exists := c.Get("user_id"); exists {
		if id, ok := userID.(string); ok {
			return id
		}
	}

	// 方式2：从请求头中获取
	if userID := c.GetHeader("X-User-ID"); userID != "" {
		return userID
	}

	// 方式3：从查询参数中获取（用于测试）
	if userID := c.Query("user_id"); userID != "" {
		return userID
	}

	// 默认返回测试用户ID
	return "test_user_001"
}
