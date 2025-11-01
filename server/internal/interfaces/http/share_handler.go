package http

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/share"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// ShareHandler 分享HTTP处理器
type ShareHandler struct {
	shareService share.Service
	logger       *zap.Logger
}

// NewShareHandler 创建分享HTTP处理器
func NewShareHandler(shareService share.Service, logger *zap.Logger) *ShareHandler {
	return &ShareHandler{
		shareService: shareService,
		logger:       logger,
	}
}

// CreateShareView 创建分享视图
// @Summary 创建分享视图
// @Description 为指定视图创建分享链接
// @Tags Share
// @Accept json
// @Produce json
// @Param view_id path string true "视图ID"
// @Param table_id path string true "表格ID"
// @Success 200 {object} Response{data=share.ShareView}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/views/{view_id}/share [post]
func (h *ShareHandler) CreateShareView(c *gin.Context) {
	viewID := c.Param("view_id")
	tableID := c.Param("table_id")
	userID := c.GetString("user_id")

	shareView, err := h.shareService.CreateShareView(c.Request.Context(), viewID, tableID, userID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, shareView, "")
}

// GetShareView 获取分享视图
// @Summary 获取分享视图
// @Description 通过分享ID获取分享视图信息
// @Tags Share
// @Produce json
// @Param share_id path string true "分享ID"
// @Success 200 {object} Response{data=share.ShareViewInfo}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/share/{share_id}/view [get]
func (h *ShareHandler) GetShareView(c *gin.Context) {
	shareID := c.Param("share_id")

	shareInfo, err := h.shareService.GetShareViewInfo(c.Request.Context(), shareID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, shareInfo, "")
}

// EnableShareView 启用分享视图
// @Summary 启用分享视图
// @Description 启用指定视图的分享功能
// @Tags Share
// @Accept json
// @Produce json
// @Param share_id path string true "分享ID"
// @Param request body share.ShareViewMeta true "分享元数据"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/share/{share_id}/enable [post]
func (h *ShareHandler) EnableShareView(c *gin.Context) {
	shareID := c.Param("share_id")

	var meta share.ShareViewMeta
	if err := c.ShouldBindJSON(&meta); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	err := h.shareService.EnableShareView(c.Request.Context(), shareID, &meta)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// DisableShareView 禁用分享视图
// @Summary 禁用分享视图
// @Description 禁用指定视图的分享功能
// @Tags Share
// @Produce json
// @Param share_id path string true "分享ID"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/share/{share_id}/disable [post]
func (h *ShareHandler) DisableShareView(c *gin.Context) {
	shareID := c.Param("share_id")

	err := h.shareService.DisableShareView(c.Request.Context(), shareID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// UpdateShareMeta 更新分享元数据
// @Summary 更新分享元数据
// @Description 更新指定分享视图的元数据
// @Tags Share
// @Accept json
// @Produce json
// @Param share_id path string true "分享ID"
// @Param request body share.ShareViewMeta true "分享元数据"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/share/{share_id}/meta [put]
func (h *ShareHandler) UpdateShareMeta(c *gin.Context) {
	shareID := c.Param("share_id")

	var meta share.ShareViewMeta
	if err := c.ShouldBindJSON(&meta); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	err := h.shareService.UpdateShareMeta(c.Request.Context(), shareID, &meta)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// ShareAuth 分享认证
// @Summary 分享认证
// @Description 验证分享访问权限
// @Tags Share
// @Accept json
// @Produce json
// @Param request body share.ShareAuthRequest true "认证请求"
// @Success 200 {object} Response{data=share.ShareAuthResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 401 {object} ErrorResponse
// @Router /api/share/auth [post]
func (h *ShareHandler) ShareAuth(c *gin.Context) {
	var req share.ShareAuthRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	_, err := h.shareService.ValidateShareAccess(c.Request.Context(), req.ShareID, req.Password)
	if err != nil {
		h.handleError(c, err)
		return
	}

	// 生成临时token（这里简化处理，实际应该使用JWT）
	token := "temp_share_token_" + req.ShareID
	authResp := &share.ShareAuthResponse{
		Token:   token,
		Expires: time.Now().Add(24 * time.Hour).Unix(),
	}

	response.SuccessWithMessage(c, authResp, "")
}

// SubmitForm 提交表单
// @Summary 提交表单
// @Description 通过分享链接提交表单数据
// @Tags Share
// @Accept json
// @Produce json
// @Param share_id path string true "分享ID"
// @Param request body share.ShareFormSubmitRequest true "表单提交请求"
// @Success 200 {object} Response{data=share.ShareFormSubmitResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 403 {object} ErrorResponse
// @Router /api/share/{share_id}/form-submit [post]
func (h *ShareHandler) SubmitForm(c *gin.Context) {
	shareID := c.Param("share_id")

	var req share.ShareFormSubmitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	formResp, err := h.shareService.SubmitForm(c.Request.Context(), shareID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, formResp, "")
}

// CopyData 复制数据
// @Summary 复制数据
// @Description 通过分享链接复制数据
// @Tags Share
// @Accept json
// @Produce json
// @Param share_id path string true "分享ID"
// @Param request body share.ShareCopyRequest true "复制请求"
// @Success 200 {object} Response{data=share.ShareCopyResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 403 {object} ErrorResponse
// @Router /api/share/{share_id}/copy [post]
func (h *ShareHandler) CopyData(c *gin.Context) {
	shareID := c.Param("share_id")

	var req share.ShareCopyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	copyResp, err := h.shareService.CopyData(c.Request.Context(), shareID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, copyResp, "")
}

// GetCollaborators 获取协作者
// @Summary 获取协作者
// @Description 获取分享视图的协作者列表
// @Tags Share
// @Produce json
// @Param share_id path string true "分享ID"
// @Param view_id query string true "视图ID"
// @Success 200 {object} Response{data=share.ShareCollaboratorsResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 403 {object} ErrorResponse
// @Router /api/share/{share_id}/collaborators [get]
func (h *ShareHandler) GetCollaborators(c *gin.Context) {
	shareID := c.Param("share_id")

	var req share.ShareCollaboratorsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	collabResp, err := h.shareService.GetCollaborators(c.Request.Context(), shareID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, collabResp, "")
}

// GetLinkRecords 获取链接记录
// @Summary 获取链接记录
// @Description 获取分享视图的链接记录
// @Tags Share
// @Produce json
// @Param share_id path string true "分享ID"
// @Param field_id query string true "字段ID"
// @Param type query string true "类型"
// @Param search query string false "搜索关键词"
// @Success 200 {object} Response{data=share.ShareLinkRecordsResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 403 {object} ErrorResponse
// @Router /api/share/{share_id}/link-records [get]
func (h *ShareHandler) GetLinkRecords(c *gin.Context) {
	shareID := c.Param("share_id")

	var req share.ShareLinkRecordsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	linkResp, err := h.shareService.GetLinkRecords(c.Request.Context(), shareID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, linkResp, "")
}

// GetShareStats 获取分享统计
// @Summary 获取分享统计
// @Description 获取指定表格的分享统计信息
// @Tags Share
// @Produce json
// @Param table_id path string true "表格ID"
// @Success 200 {object} Response{data=share.ShareStats}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/tables/{table_id}/share/stats [get]
func (h *ShareHandler) GetShareStats(c *gin.Context) {
	tableID := c.Param("table_id")

	stats, err := h.shareService.GetShareStats(c.Request.Context(), tableID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	response.SuccessWithMessage(c, stats, "")
}

func (h *ShareHandler) handleError(c *gin.Context, err error) {
	response.Error(c, err)
}
