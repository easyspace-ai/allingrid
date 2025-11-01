package http

import (
	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// RecordHistoryHandler 记录历史处理器
type RecordHistoryHandler struct {
	historyService *application.RecordHistoryService
}

// NewRecordHistoryHandler 创建记录历史处理器
func NewRecordHistoryHandler(historyService *application.RecordHistoryService) *RecordHistoryHandler {
	return &RecordHistoryHandler{
		historyService: historyService,
	}
}

// GetRecordHistory 获取记录历史
// GET /api/v1/tables/:tableId/records/:recordId/history
func (h *RecordHistoryHandler) GetRecordHistory(c *gin.Context) {
	tableID := c.Param("tableId")
	recordID := c.Param("recordId")

	histories, err := h.historyService.GetRecordHistory(c.Request.Context(), tableID, recordID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, histories, "获取历史记录成功")
}
