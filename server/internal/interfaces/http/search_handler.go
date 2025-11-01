package http

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/domain/search"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// SearchHandler 搜索处理器
type SearchHandler struct {
	service search.Service
	logger  *zap.Logger
}

// NewSearchHandler 创建搜索处理器
func NewSearchHandler(service search.Service, logger *zap.Logger) *SearchHandler {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	return &SearchHandler{
		service: service,
		logger:  logger,
	}
}

// Search 搜索
// @Summary 搜索
// @Description 执行搜索操作
// @Tags 搜索管理
// @Accept json
// @Produce json
// @Param query query string true "搜索关键词"
// @Param type query string false "搜索类型"
// @Param scope query string false "搜索范围"
// @Param source_id query string false "来源ID"
// @Param source_type query string false "来源类型"
// @Param user_id query string false "用户ID"
// @Param space_id query string false "空间ID"
// @Param table_id query string false "表格ID"
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(20)
// @Param sort_by query string false "排序字段" default(score)
// @Param sort_order query string false "排序顺序" default(desc)
// @Param highlight query bool false "是否高亮" default(false)
// @Success 200 {object} response.Response{data=search.SearchResponse} "搜索成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search [get]
func (h *SearchHandler) Search(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("搜索关键词不能为空"))
		return
	}

	req := &search.SearchRequest{
		Query: query,
	}

	// 解析查询参数
	if typeStr := c.Query("type"); typeStr != "" {
		req.Type = search.SearchType(typeStr)
	}
	if scopeStr := c.Query("scope"); scopeStr != "" {
		req.Scope = search.SearchScope(scopeStr)
	}
	req.SourceID = c.Query("source_id")
	req.SourceType = c.Query("source_type")
	req.UserID = c.Query("user_id")
	req.SpaceID = c.Query("space_id")
	req.TableID = c.Query("table_id")

	// 分页参数
	if pageStr := c.Query("page"); pageStr != "" {
		if page, err := strconv.Atoi(pageStr); err == nil {
			req.Page = page
		}
	}
	if pageStr := c.Query("page_size"); pageStr != "" {
		if pageSize, err := strconv.Atoi(pageStr); err == nil {
			req.PageSize = pageSize
		}
	}

	req.SortBy = c.DefaultQuery("sort_by", "score")
	req.SortOrder = c.DefaultQuery("sort_order", "desc")

	// 高亮参数
	if highlightStr := c.Query("highlight"); highlightStr != "" {
		if highlight, err := strconv.ParseBool(highlightStr); err == nil {
			req.Highlight = highlight
		}
	}

	searchResponse, err := h.service.Search(c.Request.Context(), req)
	if err != nil {
		h.logger.Error("Failed to search",
			zap.String("query", query),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, searchResponse, "搜索完成")
}

// AdvancedSearch 高级搜索
// @Summary 高级搜索
// @Description 执行高级搜索操作
// @Tags 搜索管理
// @Accept json
// @Produce json
// @Param request body search.AdvancedSearchRequest true "高级搜索请求"
// @Success 200 {object} response.Response{data=search.SearchResponse} "搜索成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/advanced [post]
func (h *SearchHandler) AdvancedSearch(c *gin.Context) {
	var req search.AdvancedSearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind advanced search request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	searchResponse, err := h.service.AdvancedSearch(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to perform advanced search",
			zap.Strings("queries", req.Queries),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, searchResponse, "高级搜索完成")
}

// SearchSuggestions 搜索建议
// @Summary 搜索建议
// @Description 获取搜索建议
// @Tags 搜索管理
// @Accept json
// @Produce json
// @Param query query string true "搜索关键词"
// @Param limit query int false "建议数量" default(10)
// @Success 200 {object} response.Response{data=[]search.SearchSuggestion} "获取成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/suggestions [get]
func (h *SearchHandler) SearchSuggestions(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("搜索关键词不能为空"))
		return
	}

	limit := 10
	if limitStr := c.Query("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	suggestions, err := h.service.SearchSuggestions(c.Request.Context(), query, limit)
	if err != nil {
		h.logger.Error("Failed to get search suggestions",
			zap.String("query", query),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, suggestions, "获取搜索建议成功")
}

// GetPopularQueries 获取热门查询
// @Summary 获取热门查询
// @Description 获取热门搜索查询
// @Tags 搜索管理
// @Accept json
// @Produce json
// @Param limit query int false "查询数量" default(10)
// @Success 200 {object} response.Response{data=[]search.SearchSuggestion} "获取成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/popular [get]
func (h *SearchHandler) GetPopularQueries(c *gin.Context) {
	limit := 10
	if limitStr := c.Query("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	queries, err := h.service.GetPopularQueries(c.Request.Context(), limit)
	if err != nil {
		h.logger.Error("Failed to get popular queries", zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, queries, "获取热门查询成功")
}

// GetSearchStats 获取搜索统计
// @Summary 获取搜索统计
// @Description 获取搜索系统统计信息
// @Tags 搜索管理
// @Accept json
// @Produce json
// @Success 200 {object} response.Response{data=search.SearchStats} "获取成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/stats [get]
func (h *SearchHandler) GetSearchStats(c *gin.Context) {
	stats, err := h.service.GetSearchStats(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to get search stats", zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, stats, "获取搜索统计成功")
}

// CreateIndex 创建搜索索引
// @Summary 创建搜索索引
// @Description 创建新的搜索索引
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param request body search.SearchIndexRequest true "创建索引请求"
// @Success 200 {object} response.Response{data=search.SearchIndex} "创建成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes [post]
func (h *SearchHandler) CreateIndex(c *gin.Context) {
	var req search.SearchIndexRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind create index request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	index, err := h.service.CreateIndex(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to create search index",
			zap.String("type", string(req.Type)),
			zap.String("source_id", req.SourceID),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, index, "搜索索引创建成功")
}

// GetIndex 获取搜索索引
// @Summary 获取搜索索引
// @Description 根据ID获取搜索索引
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param id path string true "索引ID"
// @Success 200 {object} response.Response{data=search.SearchIndex} "获取成功"
// @Failure 404 {object} response.Response "索引不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/{id} [get]
func (h *SearchHandler) GetIndex(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("索引ID不能为空"))
		return
	}

	index, err := h.service.GetIndex(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to get search index",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, index, "获取搜索索引成功")
}

// UpdateIndex 更新搜索索引
// @Summary 更新搜索索引
// @Description 更新搜索索引信息
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param id path string true "索引ID"
// @Param request body search.UpdateSearchIndexRequest true "更新索引请求"
// @Success 200 {object} response.Response{data=search.SearchIndex} "更新成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 404 {object} response.Response "索引不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/{id} [put]
func (h *SearchHandler) UpdateIndex(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("索引ID不能为空"))
		return
	}

	var req search.UpdateSearchIndexRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Failed to bind update index request", zap.Error(err))
		response.Error(c, errors.ErrBadRequest.WithDetails(err.Error()))
		return
	}

	index, err := h.service.UpdateIndex(c.Request.Context(), id, &req)
	if err != nil {
		h.logger.Error("Failed to update search index",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, index, "搜索索引更新成功")
}

// DeleteIndex 删除搜索索引
// @Summary 删除搜索索引
// @Description 根据ID删除搜索索引
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param id path string true "索引ID"
// @Success 200 {object} response.Response "删除成功"
// @Failure 404 {object} response.Response "索引不存在"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/{id} [delete]
func (h *SearchHandler) DeleteIndex(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("索引ID不能为空"))
		return
	}

	err := h.service.DeleteIndex(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Failed to delete search index",
			zap.String("id", id),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "搜索索引删除成功")
}

// DeleteIndexesBySource 根据来源删除搜索索引
// @Summary 根据来源删除搜索索引
// @Description 根据来源ID和类型删除相关搜索索引
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param source_id query string true "来源ID"
// @Param source_type query string true "来源类型"
// @Success 200 {object} response.Response "删除成功"
// @Failure 400 {object} response.Response "请求参数错误"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/by-source [delete]
func (h *SearchHandler) DeleteIndexesBySource(c *gin.Context) {
	sourceID := c.Query("source_id")
	sourceType := c.Query("source_type")

	if sourceID == "" || sourceType == "" {
		response.Error(c, errors.ErrBadRequest.WithDetails("来源ID和来源类型不能为空"))
		return
	}

	err := h.service.DeleteIndexesBySource(c.Request.Context(), sourceID, sourceType)
	if err != nil {
		h.logger.Error("Failed to delete search indexes by source",
			zap.String("source_id", sourceID),
			zap.String("source_type", sourceType),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "搜索索引删除成功")
}

// ListIndexes 列出搜索索引
// @Summary 列出搜索索引
// @Description 获取搜索索引列表
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param type query string false "搜索类型"
// @Param source_id query string false "来源ID"
// @Param source_type query string false "来源类型"
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(20)
// @Success 200 {object} response.Response{data=ListIndexesResponse} "获取成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes [get]
func (h *SearchHandler) ListIndexes(c *gin.Context) {
	var searchType *search.SearchType
	if typeStr := c.Query("type"); typeStr != "" {
		st := search.SearchType(typeStr)
		searchType = &st
	}

	sourceID := c.Query("source_id")
	sourceType := c.Query("source_type")

	page := 1
	if pageStr := c.Query("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil {
			page = p
		}
	}

	pageSize := 20
	if pageSizeStr := c.Query("page_size"); pageSizeStr != "" {
		if ps, err := strconv.Atoi(pageSizeStr); err == nil {
			pageSize = ps
		}
	}

	indexes, total, err := h.service.ListIndexes(c.Request.Context(), searchType, sourceID, sourceType, page, pageSize)
	if err != nil {
		h.logger.Error("Failed to list search indexes",
			zap.String("source_id", sourceID),
			zap.String("source_type", sourceType),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	totalPages := int((total + int64(pageSize) - 1) / int64(pageSize))
	result := ListIndexesResponse{
		Indexes:    indexes,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}

	response.SuccessWithMessage(c, result, "获取搜索索引列表成功")
}

// RebuildIndex 重建索引
// @Summary 重建索引
// @Description 重建搜索索引
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Param type query string false "搜索类型"
// @Success 200 {object} response.Response "重建成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/rebuild [post]
func (h *SearchHandler) RebuildIndex(c *gin.Context) {
	var searchType *search.SearchType
	if typeStr := c.Query("type"); typeStr != "" {
		st := search.SearchType(typeStr)
		searchType = &st
	}

	err := h.service.RebuildIndex(c.Request.Context(), searchType)
	if err != nil {
		h.logger.Error("Failed to rebuild search index",
			zap.String("type", string(*searchType)),
			zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "搜索索引重建成功")
}

// OptimizeIndex 优化索引
// @Summary 优化索引
// @Description 优化搜索索引
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Success 200 {object} response.Response "优化成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/optimize [post]
func (h *SearchHandler) OptimizeIndex(c *gin.Context) {
	err := h.service.OptimizeIndex(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to optimize search index", zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, nil, "搜索索引优化成功")
}

// GetIndexStats 获取索引统计
// @Summary 获取索引统计
// @Description 获取搜索索引统计信息
// @Tags 搜索索引管理
// @Accept json
// @Produce json
// @Success 200 {object} response.Response{data=map[string]interface{}} "获取成功"
// @Failure 500 {object} response.Response "服务器内部错误"
// @Router /api/search/indexes/stats [get]
func (h *SearchHandler) GetIndexStats(c *gin.Context) {
	stats, err := h.service.GetIndexStats(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to get index stats", zap.Error(err))
		response.Error(c, err)
		return
	}

	response.SuccessWithMessage(c, stats, "获取索引统计成功")
}

// ListIndexesResponse 列出搜索索引响应
type ListIndexesResponse struct {
	Indexes    []*search.SearchIndex `json:"indexes"`
	Total      int64                 `json:"total"`
	Page       int                   `json:"page"`
	PageSize   int                   `json:"page_size"`
	TotalPages int                   `json:"total_pages"`
}
