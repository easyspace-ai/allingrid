package http

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// WorkflowHandler 工作流处理器
type WorkflowHandler struct {
	service *application.WorkflowService
}

// NewWorkflowHandler 创建工作流处理器
func NewWorkflowHandler(service *application.WorkflowService) *WorkflowHandler {
	return &WorkflowHandler{
		service: service,
	}
}

// CreateWorkflow 创建工作流
func (h *WorkflowHandler) CreateWorkflow(c *gin.Context) {
	var workflow models.Workflow
	if err := c.ShouldBindJSON(&workflow); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 从上下文中获取用户ID
	userID, exists := c.Get("user_id")
	if !exists {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	workflow.CreatedBy = userID.(string)

	// 调用服务创建工作流
	if err := h.service.Create(c.Request.Context(), &workflow); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, workflow, "创建成功")
}

// GetWorkflow 获取工作流
func (h *WorkflowHandler) GetWorkflow(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务获取工作流
	workflow, err := h.service.GetByID(c.Request.Context(), id)
	if err != nil {
		response.Error(c, errors.ErrNotFound)
		return
	}

	response.Success(c, workflow, "操作成功")
}

// ListWorkflows 列出工作流
func (h *WorkflowHandler) ListWorkflows(c *gin.Context) {
	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	// 调用服务获取列表
	workflows, total, err := h.service.List(c.Request.Context(), page, limit)
	if err != nil {
		response.Error(c, err)
		return
	}

	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      int(total),
		TotalPages: (int(total) + limit - 1) / limit,
	}
	response.PaginatedSuccess(c, workflows, pagination, "获取工作流列表成功")
}

// UpdateWorkflow 更新工作流
func (h *WorkflowHandler) UpdateWorkflow(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	var workflow models.Workflow
	if err := c.ShouldBindJSON(&workflow); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	workflow.ID = id

	// 调用服务更新工作流
	if err := h.service.Update(c.Request.Context(), id, &workflow); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, workflow, "操作成功")
}

// DeleteWorkflow 删除工作流
func (h *WorkflowHandler) DeleteWorkflow(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务删除工作流
	if err := h.service.Delete(c.Request.Context(), id); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "删除成功")
}

// RunWorkflow 运行工作流
func (h *WorkflowHandler) RunWorkflow(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	var input map[string]interface{}
	if err := c.ShouldBindJSON(&input); err != nil && err.Error() != "EOF" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 从上下文中获取用户ID
	userID, exists := c.Get("user_id")
	if !exists {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权"))
		return
	}

	// 调用服务运行工作流
	run, err := h.service.Run(c.Request.Context(), id, userID.(string), input)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, run, "操作成功")
}

// GetWorkflowRun 获取工作流运行
func (h *WorkflowHandler) GetWorkflowRun(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务获取运行记录
	run, err := h.service.GetRun(c.Request.Context(), id)
	if err != nil {
		response.Error(c, errors.ErrNotFound)
		return
	}

	response.Success(c, run, "操作成功")
}

// ListWorkflowRuns 列出工作流运行
func (h *WorkflowHandler) ListWorkflowRuns(c *gin.Context) {
	workflowID := c.Query("workflow_id")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	// 调用服务获取运行列表
	runs, total, err := h.service.ListRuns(c.Request.Context(), workflowID, page, limit)
	if err != nil {
		response.Error(c, err)
		return
	}

	pagination := response.Pagination{
		Page:       page,
		Limit:      limit,
		Total:      int(total),
		TotalPages: (int(total) + limit - 1) / limit,
	}
	response.PaginatedSuccess(c, runs, pagination, "获取运行历史成功")
}

// StopWorkflowRun 停止工作流运行
func (h *WorkflowHandler) StopWorkflowRun(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务停止运行
	if err := h.service.StopRun(c.Request.Context(), id); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, gin.H{
		"id":     id,
		"status": "stopped",
	}, "工作流已停止")
}

// GetWorkflowStats 获取工作流统计信息
func (h *WorkflowHandler) GetWorkflowStats(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("Invalid request"))
		return
	}

	// 调用服务获取统计信息
	stats, err := h.service.GetStats(c.Request.Context(), id)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, stats, "操作成功")
}
