package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/attachment"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	resp "github.com/easyspace-ai/luckdb/server/pkg/response"
)

// AttachmentHandler 附件HTTP处理器
type AttachmentHandler struct {
	attachmentService attachment.Service
	logger            *zap.Logger
}

// NewAttachmentHandler 创建附件HTTP处理器
func NewAttachmentHandler(attachmentService attachment.Service, logger *zap.Logger) *AttachmentHandler {
	return &AttachmentHandler{
		attachmentService: attachmentService,
		logger:            logger,
	}
}

// GenerateSignature 生成上传签名
// @Summary 生成上传签名
// @Description 为文件上传生成签名令牌
// @Tags Attachments
// @Accept json
// @Produce json
// @Param request body attachment.SignatureRequest true "签名请求"
// @Success 200 {object} Response{data=attachment.SignatureResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/attachments/signature [post]
func (h *AttachmentHandler) GenerateSignature(c *gin.Context) {
	var req attachment.SignatureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	userID := c.GetString("user_id")
	if userID == "" {
		h.handleError(c, errors.ErrUnauthorized.WithDetails("User ID not found"))
		return
	}

	respData, err := h.attachmentService.GenerateSignature(c.Request.Context(), userID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, respData, "")
}

// UploadFile 上传文件
// @Summary 上传文件
// @Description 使用令牌上传文件
// @Tags Attachments
// @Accept multipart/form-data
// @Produce json
// @Param token path string true "上传令牌"
// @Param file formData file true "文件"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/attachments/upload/{token} [post]
func (h *AttachmentHandler) UploadFile(c *gin.Context) {
	token := c.Param("token")
	if token == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("Token is required"))
		return
	}

	// 获取上传的文件
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		h.handleError(c, errors.ErrBadRequest.WithDetails("File is required"))
		return
	}
	defer file.Close()

	// 获取文件大小
	fileSize := header.Size
	if fileSize <= 0 {
		h.handleError(c, errors.ErrBadRequest.WithDetails("Invalid file size"))
		return
	}

	// 上传文件
	err = h.attachmentService.UploadFile(c.Request.Context(), token, file, header.Filename, fileSize)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// NotifyUpload 通知上传完成
// @Summary 通知上传完成
// @Description 通知服务器文件上传完成
// @Tags Attachments
// @Accept json
// @Produce json
// @Param token path string true "上传令牌"
// @Param filename query string false "文件名"
// @Success 200 {object} Response{data=attachment.NotifyResponse}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/attachments/notify/{token} [post]
func (h *AttachmentHandler) NotifyUpload(c *gin.Context) {
	token := c.Param("token")
	if token == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("Token is required"))
		return
	}

	// 优先从请求体获取 filename，如果没有则从 query 参数获取
	var filename string
	var requestBody map[string]interface{}
	if err := c.ShouldBindJSON(&requestBody); err == nil {
		if fn, ok := requestBody["filename"].(string); ok {
			filename = fn
		}
	}
	
	// 如果请求体中获取不到，从 query 参数获取
	if filename == "" {
		filename = c.Query("filename")
	}

	respData2, err := h.attachmentService.NotifyUpload(c.Request.Context(), token, filename)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, respData2, "")
}

// ReadFile 读取文件
// @Summary 读取文件
// @Description 通过路径读取文件内容
// @Tags Attachments
// @Produce application/octet-stream
// @Param path path string true "文件路径"
// @Param token query string false "访问令牌"
// @Param response-content-disposition query string false "响应内容配置"
// @Success 200 {file} binary
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/attachments/read/{path} [get]
func (h *AttachmentHandler) ReadFile(c *gin.Context) {
	// 使用 *path 通配符路由，支持路径中的斜杠
	path := c.Param("path")
	// 移除开头的斜杠（如果存在）
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	if path == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("Path is required"))
		return
	}

	token := c.Query("token")
	responseContentDisposition := c.Query("response-content-disposition")

	// 读取文件
	response, err := h.attachmentService.ReadFile(c.Request.Context(), path, token)
	if err != nil {
		h.handleError(c, err)
		return
	}

	// 设置响应头
	for key, value := range response.Headers {
		c.Header(key, value)
	}

	// 处理响应内容配置
	if responseContentDisposition != "" {
		c.Header("Content-Disposition", responseContentDisposition)
	}

	// 设置缓存控制
	c.Header("Cache-Control", "public, max-age=31536000")
	c.Header("Cross-Origin-Resource-Policy", "unsafe-none")
	c.Header("Content-Security-Policy", "")

	// 返回文件内容
	c.Data(http.StatusOK, response.MimeType, response.Data)
}

// DeleteFile 删除文件
// @Summary 删除文件
// @Description 删除指定的附件文件
// @Tags Attachments
// @Produce json
// @Param id path string true "附件ID"
// @Success 200 {object} Response{success=boolean}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/attachments/{id} [delete]
func (h *AttachmentHandler) DeleteFile(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("ID is required"))
		return
	}

	err := h.attachmentService.DeleteFile(c.Request.Context(), id)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

// GetAttachment 获取附件信息
// @Summary 获取附件信息
// @Description 获取指定附件的详细信息
// @Tags Attachments
// @Produce json
// @Param id path string true "附件ID"
// @Success 200 {object} Response{data=attachment.AttachmentItem}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/attachments/{id} [get]
func (h *AttachmentHandler) GetAttachment(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("ID is required"))
		return
	}

	attachment, err := h.attachmentService.GetAttachment(c.Request.Context(), id)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, attachment, "")
}

// ListAttachments 列出附件
// @Summary 列出附件
// @Description 列出指定条件下的附件
// @Tags Attachments
// @Produce json
// @Param table_id query string true "表格ID"
// @Param field_id query string false "字段ID"
// @Param record_id query string false "记录ID"
// @Success 200 {object} Response{data=[]attachment.AttachmentItem}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/attachments [get]
func (h *AttachmentHandler) ListAttachments(c *gin.Context) {
	tableID := c.Query("table_id")
	if tableID == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("table_id is required"))
		return
	}

	fieldID := c.Query("field_id")
	recordID := c.Query("record_id")

	attachments, err := h.attachmentService.ListAttachments(c.Request.Context(), tableID, fieldID, recordID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, attachments, "")
}

// GetAttachmentStats 获取附件统计
// @Summary 获取附件统计
// @Description 获取指定表格的附件统计信息
// @Tags Attachments
// @Produce json
// @Param table_id path string true "表格ID"
// @Success 200 {object} Response{data=attachment.AttachmentStats}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/tables/{table_id}/attachments/stats [get]
func (h *AttachmentHandler) GetAttachmentStats(c *gin.Context) {
	tableID := c.Param("tableId")
	if tableID == "" {
		h.handleError(c, errors.ErrBadRequest.WithDetails("table_id is required"))
		return
	}

	stats, err := h.attachmentService.GetAttachmentStats(c.Request.Context(), tableID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, stats, "")
}

// CleanupExpiredTokens 清理过期令牌
// @Summary 清理过期令牌
// @Description 清理过期的上传令牌
// @Tags Attachments
// @Produce json
// @Success 200 {object} Response{success=boolean}
// @Failure 500 {object} ErrorResponse
// @Router /api/attachments/cleanup [post]
func (h *AttachmentHandler) CleanupExpiredTokens(c *gin.Context) {
	err := h.attachmentService.CleanupExpiredTokens(c.Request.Context())
	if err != nil {
		h.handleError(c, err)
		return
	}

	resp.SuccessWithMessage(c, map[string]bool{"success": true}, "")
}

func (h *AttachmentHandler) handleError(c *gin.Context, err error) {
	resp.Error(c, err)
}
