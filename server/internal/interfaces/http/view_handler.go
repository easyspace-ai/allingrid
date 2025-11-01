package http

import (
	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// ViewHandler 视图HTTP处理器
type ViewHandler struct {
	viewService *application.ViewService
}

// NewViewHandler 创建视图处理器
func NewViewHandler(viewService *application.ViewService) *ViewHandler {
	return &ViewHandler{
		viewService: viewService,
	}
}

// CreateView 创建视图
// @Summary 创建视图
// @Tags View
// @Accept json
// @Produce json
// @Param tableId path string true "表格ID"
// @Param request body dto.CreateViewRequest true "创建请求"
// @Success 200 {object} dto.ViewResponse
// @Router /api/v1/tables/{tableId}/views [post]
func (h *ViewHandler) CreateView(c *gin.Context) {
	tableID := c.Param("tableId")

	var req dto.CreateViewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	req.TableID = tableID
	userID := c.GetString("user_id")

	view, err := h.viewService.CreateView(c.Request.Context(), req, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, view, "操作成功")
}

// GetView 获取视图详情
// @Summary 获取视图
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} dto.ViewResponse
// @Router /api/v1/views/{viewId} [get]
func (h *ViewHandler) GetView(c *gin.Context) {
	viewID := c.Param("viewId")

	view, err := h.viewService.GetView(c.Request.Context(), viewID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, view, "操作成功")
}

// ListViews 获取表格的所有视图
// @Summary 获取表格视图列表
// @Tags View
// @Produce json
// @Param tableId path string true "表格ID"
// @Success 200 {object} []dto.ViewResponse
// @Router /api/v1/tables/{tableId}/views [get]
func (h *ViewHandler) ListViews(c *gin.Context) {
	tableID := c.Param("tableId")

	views, err := h.viewService.ListViewsByTable(c.Request.Context(), tableID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, views, "操作成功")
}

// UpdateView 更新视图
// @Summary 更新视图
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewRequest true "更新请求"
// @Success 200 {object} dto.ViewResponse
// @Router /api/v1/views/{viewId} [put]
func (h *ViewHandler) UpdateView(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	view, err := h.viewService.UpdateView(c.Request.Context(), viewID, req)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, view, "操作成功")
}

// DeleteView 删除视图
// @Summary 删除视图
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId} [delete]
func (h *ViewHandler) DeleteView(c *gin.Context) {
	viewID := c.Param("viewId")

	if err := h.viewService.DeleteView(c.Request.Context(), viewID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "视图删除成功")
}

// UpdateViewFilter 更新视图过滤器
// @Summary 更新视图过滤器
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewFilterRequest true "过滤器请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/filter [put]
func (h *ViewHandler) UpdateViewFilter(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewFilterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateViewFilter(c.Request.Context(), viewID, req.Filter); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "过滤器更新成功")
}

// UpdateViewSort 更新视图排序
// @Summary 更新视图排序
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewSortRequest true "排序请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/sort [put]
func (h *ViewHandler) UpdateViewSort(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewSortRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateViewSort(c.Request.Context(), viewID, req.Sort); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "排序更新成功")
}

// UpdateViewGroup 更新视图分组
// @Summary 更新视图分组
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewGroupRequest true "分组请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/group [put]
func (h *ViewHandler) UpdateViewGroup(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateViewGroup(c.Request.Context(), viewID, req.Group); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "分组更新成功")
}

// UpdateViewColumnMeta 更新视图列配置
// @Summary 更新视图列配置
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewColumnMetaRequest true "列配置请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/column-meta [put]
func (h *ViewHandler) UpdateViewColumnMeta(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewColumnMetaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateViewColumnMeta(c.Request.Context(), viewID, req.ColumnMeta); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "列配置更新成功")
}

// UpdateViewOptions 更新视图选项（完全替换）
// @Summary 更新视图选项
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewOptionsRequest true "选项请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/options [put]
func (h *ViewHandler) UpdateViewOptions(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewOptionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateViewOptions(c.Request.Context(), viewID, req.Options); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "选项更新成功")
}

// PatchViewOptions 部分更新视图选项
// @Summary 部分更新视图选项
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.PatchViewOptionsRequest true "选项请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/options [patch]
func (h *ViewHandler) PatchViewOptions(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.PatchViewOptionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.PatchViewOptions(c.Request.Context(), viewID, req.Options); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "选项部分更新成功")
}

// UpdateViewOrder 更新视图排序位置
// @Summary 更新视图排序位置
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateViewOrderRequest true "排序位置请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/order [put]
func (h *ViewHandler) UpdateViewOrder(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateViewOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateViewOrder(c.Request.Context(), viewID, req.Order); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "排序位置更新成功")
}

// EnableShare 启用视图分享
// @Summary 启用视图分享
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} dto.EnableShareResponse
// @Router /api/v1/views/{viewId}/enable-share [post]
func (h *ViewHandler) EnableShare(c *gin.Context) {
	viewID := c.Param("viewId")

	shareID, err := h.viewService.EnableShare(c.Request.Context(), viewID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, dto.EnableShareResponse{ShareID: shareID}, "分享已启用")
}

// DisableShare 禁用视图分享
// @Summary 禁用视图分享
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/disable-share [post]
func (h *ViewHandler) DisableShare(c *gin.Context) {
	viewID := c.Param("viewId")

	if err := h.viewService.DisableShare(c.Request.Context(), viewID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "分享已禁用")
}

// RefreshShareID 刷新分享ID
// @Summary 刷新分享ID
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} dto.RefreshShareIDResponse
// @Router /api/v1/views/{viewId}/refresh-share-id [post]
func (h *ViewHandler) RefreshShareID(c *gin.Context) {
	viewID := c.Param("viewId")

	shareID, err := h.viewService.RefreshShareID(c.Request.Context(), viewID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, dto.RefreshShareIDResponse{ShareID: shareID}, "分享ID已刷新")
}

// UpdateShareMeta 更新分享元数据
// @Summary 更新分享元数据
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.UpdateShareMetaRequest true "分享元数据请求"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/share-meta [put]
func (h *ViewHandler) UpdateShareMeta(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.UpdateShareMetaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	if err := h.viewService.UpdateShareMeta(c.Request.Context(), viewID, req.ShareMeta); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "分享元数据更新成功")
}

// LockView 锁定视图
// @Summary 锁定视图
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/lock [post]
func (h *ViewHandler) LockView(c *gin.Context) {
	viewID := c.Param("viewId")

	if err := h.viewService.LockView(c.Request.Context(), viewID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "视图已锁定")
}

// UnlockView 解锁视图
// @Summary 解锁视图
// @Tags View
// @Produce json
// @Param viewId path string true "视图ID"
// @Success 200 {object} gin.H
// @Router /api/v1/views/{viewId}/unlock [post]
func (h *ViewHandler) UnlockView(c *gin.Context) {
	viewID := c.Param("viewId")

	if err := h.viewService.UnlockView(c.Request.Context(), viewID); err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, nil, "视图已解锁")
}

// DuplicateView 复制视图
// @Summary 复制视图
// @Tags View
// @Accept json
// @Produce json
// @Param viewId path string true "视图ID"
// @Param request body dto.DuplicateViewRequest true "复制请求"
// @Success 200 {object} dto.ViewResponse
// @Router /api/v1/views/{viewId}/duplicate [post]
func (h *ViewHandler) DuplicateView(c *gin.Context) {
	viewID := c.Param("viewId")

	var req dto.DuplicateViewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	userID := c.GetString("user_id")

	view, err := h.viewService.DuplicateView(c.Request.Context(), viewID, req.Name, userID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, view, "操作成功")
}

// GetViewByShareID 通过分享ID获取视图
// @Summary 通过分享ID获取视图
// @Tags View
// @Produce json
// @Param shareId path string true "分享ID"
// @Success 200 {object} dto.ViewResponse
// @Router /api/v1/share/views/{shareId} [get]
func (h *ViewHandler) GetViewByShareID(c *gin.Context) {
	shareID := c.Param("shareId")

	view, err := h.viewService.GetViewByShareID(c.Request.Context(), shareID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, view, "操作成功")
}

// CountViews 统计表格的视图数量
// @Summary 统计表格视图数量
// @Tags View
// @Produce json
// @Param tableId path string true "表格ID"
// @Success 200 {object} dto.ViewCountResponse
// @Router /api/v1/tables/{tableId}/views/count [get]
func (h *ViewHandler) CountViews(c *gin.Context) {
	tableID := c.Param("tableId")

	count, err := h.viewService.CountViews(c.Request.Context(), tableID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.Success(c, dto.ViewCountResponse{Count: count}, "获取计数成功")
}
