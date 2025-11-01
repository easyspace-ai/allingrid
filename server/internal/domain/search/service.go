package search

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	pkgErrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// Service 搜索服务接口
type Service interface {
	// Index management
	// CreateIndex 创建搜索索引
	CreateIndex(ctx context.Context, req *SearchIndexRequest) (*SearchIndex, error)
	// GetIndex 获取搜索索引
	GetIndex(ctx context.Context, id string) (*SearchIndex, error)
	// UpdateIndex 更新搜索索引
	UpdateIndex(ctx context.Context, id string, req *UpdateSearchIndexRequest) (*SearchIndex, error)
	// DeleteIndex 删除搜索索引
	DeleteIndex(ctx context.Context, id string) error
	// DeleteIndexesBySource 根据来源删除搜索索引
	DeleteIndexesBySource(ctx context.Context, sourceID, sourceType string) error
	// ListIndexes 列出搜索索引
	ListIndexes(ctx context.Context, searchType *SearchType, sourceID, sourceType string, page, pageSize int) ([]*SearchIndex, int64, error)

	// Search operations
	// Search 搜索
	Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error)
	// AdvancedSearch 高级搜索
	AdvancedSearch(ctx context.Context, req *AdvancedSearchRequest) (*SearchResponse, error)
	// SearchSuggestions 搜索建议
	SearchSuggestions(ctx context.Context, query string, limit int) ([]*SearchSuggestion, error)
	// GetPopularQueries 获取热门查询
	GetPopularQueries(ctx context.Context, limit int) ([]*SearchSuggestion, error)

	// Statistics
	// GetSearchStats 获取搜索统计
	GetSearchStats(ctx context.Context) (*SearchStats, error)

	// Index maintenance
	// RebuildIndex 重建索引
	RebuildIndex(ctx context.Context, searchType *SearchType) error
	// OptimizeIndex 优化索引
	OptimizeIndex(ctx context.Context) error
	// GetIndexStats 获取索引统计
	GetIndexStats(ctx context.Context) (map[string]interface{}, error)
}

// service 搜索服务实现
type service struct {
	repo   Repository
	logger *zap.Logger
}

// NewService 创建搜索服务
func NewService(repo Repository, logger *zap.Logger) Service {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	return &service{
		repo:   repo,
		logger: logger,
	}
}

// CreateIndex 创建搜索索引
func (s *service) CreateIndex(ctx context.Context, req *SearchIndexRequest) (*SearchIndex, error) {
	index := NewSearchIndex(req.Type, req.Title, req.Content)

	// 设置来源信息
	index.SetSource(req.SourceID, req.SourceType, req.SourceURL)

	// 设置上下文
	index.SetContext(req.UserID, req.SpaceID, req.TableID, req.FieldID)

	// 添加关键词
	if req.Keywords != nil {
		index.AddKeywords(req.Keywords)
	}

	// 添加元数据
	if req.Metadata != nil {
		for key, value := range req.Metadata {
			index.SetMetadata(key, value)
		}
	}

	// 添加权限
	if req.Permissions != nil {
		for _, permission := range req.Permissions {
			index.AddPermission(permission)
		}
	}

	// 添加标签
	if req.Tags != nil {
		for _, tag := range req.Tags {
			index.AddTag(tag)
		}
	}

	// 自动提取关键词
	s.extractKeywords(index)

	if err := s.repo.CreateIndex(ctx, index); err != nil {
		s.logger.Error("Failed to create search index",
			zap.String("type", string(req.Type)),
			zap.String("source_id", req.SourceID),
			zap.String("source_type", req.SourceType),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return index, nil
}

// GetIndex 获取搜索索引
func (s *service) GetIndex(ctx context.Context, id string) (*SearchIndex, error) {
	index, err := s.repo.GetIndex(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Search index not found")
		}
		s.logger.Error("Failed to get search index",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return index, nil
}

// UpdateIndex 更新搜索索引
func (s *service) UpdateIndex(ctx context.Context, id string, req *UpdateSearchIndexRequest) (*SearchIndex, error) {
	index, err := s.repo.GetIndex(ctx, id)
	if err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return nil, pkgErrors.ErrNotFound.WithDetails("Search index not found")
		}
		s.logger.Error("Failed to get search index for update",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	// 更新字段
	if req.Title != "" {
		index.Title = req.Title
	}
	if req.Content != "" {
		index.Content = req.Content
	}
	if req.Keywords != nil {
		index.Keywords = req.Keywords
	}
	if req.SourceURL != "" {
		index.SourceURL = req.SourceURL
	}
	if req.Metadata != nil {
		index.Metadata = req.Metadata
	}
	if req.Permissions != nil {
		index.Permissions = req.Permissions
	}
	if req.Tags != nil {
		index.Tags = req.Tags
	}

	index.UpdatedTime = time.Now()

	// 重新提取关键词
	s.extractKeywords(index)

	if err := s.repo.UpdateIndex(ctx, index); err != nil {
		s.logger.Error("Failed to update search index",
			zap.String("id", id),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return index, nil
}

// DeleteIndex 删除搜索索引
func (s *service) DeleteIndex(ctx context.Context, id string) error {
	if err := s.repo.DeleteIndex(ctx, id); err != nil {
		if errors.Is(err, pkgErrors.ErrNotFound) {
			return pkgErrors.ErrNotFound.WithDetails("Search index not found")
		}
		s.logger.Error("Failed to delete search index",
			zap.String("id", id),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// DeleteIndexesBySource 根据来源删除搜索索引
func (s *service) DeleteIndexesBySource(ctx context.Context, sourceID, sourceType string) error {
	if err := s.repo.DeleteIndexesBySource(ctx, sourceID, sourceType); err != nil {
		s.logger.Error("Failed to delete search indexes by source",
			zap.String("source_id", sourceID),
			zap.String("source_type", sourceType),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// ListIndexes 列出搜索索引
func (s *service) ListIndexes(ctx context.Context, searchType *SearchType, sourceID, sourceType string, page, pageSize int) ([]*SearchIndex, int64, error) {
	indexes, total, err := s.repo.ListIndexes(ctx, searchType, sourceID, sourceType, page, pageSize)
	if err != nil {
		s.logger.Error("Failed to list search indexes",
			zap.String("source_id", sourceID),
			zap.String("source_type", sourceType),
			zap.Error(err))
		return nil, 0, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return indexes, total, nil
}

// Search 搜索
func (s *service) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	startTime := time.Now()

	// 设置默认值
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}
	if req.SortBy == "" {
		req.SortBy = "score"
	}
	if req.SortOrder == "" {
		req.SortOrder = "desc"
	}
	if req.Type == "" {
		req.Type = SearchTypeGlobal
	}
	if req.Scope == "" {
		req.Scope = SearchScopeAll
	}

	// 预处理查询
	req.Query = s.preprocessQuery(req.Query)

	// 执行搜索
	response, err := s.repo.Search(ctx, req)
	if err != nil {
		s.logger.Error("Failed to search",
			zap.String("query", req.Query),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	// 记录搜索统计
	go func() {
		ctx := context.Background()
		if err := s.repo.IncrementSearchCount(ctx, req.Query, req.Type, req.Scope); err != nil {
			s.logger.Error("Failed to increment search count", zap.Error(err))
		}
	}()

	// 计算查询时间
	response.QueryTime = time.Since(startTime).Milliseconds()
	response.SearchType = req.Type
	response.SearchScope = req.Scope

	return response, nil
}

// AdvancedSearch 高级搜索
func (s *service) AdvancedSearch(ctx context.Context, req *AdvancedSearchRequest) (*SearchResponse, error) {
	startTime := time.Now()

	// 设置默认值
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}
	if req.Type == "" {
		req.Type = SearchTypeGlobal
	}
	if req.Scope == "" {
		req.Scope = SearchScopeAll
	}

	// 预处理查询
	for i, query := range req.Queries {
		req.Queries[i] = s.preprocessQuery(query)
	}

	// 执行高级搜索
	response, err := s.repo.AdvancedSearch(ctx, req)
	if err != nil {
		s.logger.Error("Failed to perform advanced search",
			zap.Strings("queries", req.Queries),
			zap.String("type", string(req.Type)),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	// 计算查询时间
	response.QueryTime = time.Since(startTime).Milliseconds()
	response.SearchType = req.Type
	response.SearchScope = req.Scope

	return response, nil
}

// SearchSuggestions 搜索建议
func (s *service) SearchSuggestions(ctx context.Context, query string, limit int) ([]*SearchSuggestion, error) {
	if query == "" {
		return []*SearchSuggestion{}, nil
	}

	// 设置默认限制
	if limit <= 0 {
		limit = 10
	}

	// 预处理查询
	query = s.preprocessQuery(query)

	suggestions, err := s.repo.SearchSuggestions(ctx, query, limit)
	if err != nil {
		s.logger.Error("Failed to get search suggestions",
			zap.String("query", query),
			zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return suggestions, nil
}

// GetPopularQueries 获取热门查询
func (s *service) GetPopularQueries(ctx context.Context, limit int) ([]*SearchSuggestion, error) {
	// 设置默认限制
	if limit <= 0 {
		limit = 10
	}

	queries, err := s.repo.GetPopularQueries(ctx, limit)
	if err != nil {
		s.logger.Error("Failed to get popular queries", zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return queries, nil
}

// GetSearchStats 获取搜索统计
func (s *service) GetSearchStats(ctx context.Context) (*SearchStats, error) {
	stats, err := s.repo.GetSearchStats(ctx)
	if err != nil {
		s.logger.Error("Failed to get search stats", zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return stats, nil
}

// RebuildIndex 重建索引
func (s *service) RebuildIndex(ctx context.Context, searchType *SearchType) error {
	if err := s.repo.RebuildIndex(ctx, searchType); err != nil {
		s.logger.Error("Failed to rebuild search index",
			zap.String("type", string(*searchType)),
			zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// OptimizeIndex 优化索引
func (s *service) OptimizeIndex(ctx context.Context) error {
	if err := s.repo.OptimizeIndex(ctx); err != nil {
		s.logger.Error("Failed to optimize search index", zap.Error(err))
		return pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return nil
}

// GetIndexStats 获取索引统计
func (s *service) GetIndexStats(ctx context.Context) (map[string]interface{}, error) {
	stats, err := s.repo.GetIndexStats(ctx)
	if err != nil {
		s.logger.Error("Failed to get index stats", zap.Error(err))
		return nil, pkgErrors.ErrInternalServer.WithDetails(err.Error())
	}

	return stats, nil
}

// extractKeywords 提取关键词
func (s *service) extractKeywords(index *SearchIndex) {
	// 简单的关键词提取逻辑
	// 在实际应用中，可以使用更复杂的NLP算法

	text := fmt.Sprintf("%s %s", index.Title, index.Content)
	words := strings.Fields(strings.ToLower(text))

	// 过滤停用词和短词
	stopWords := map[string]bool{
		"the": true, "a": true, "an": true, "and": true, "or": true, "but": true,
		"in": true, "on": true, "at": true, "to": true, "for": true, "of": true,
		"with": true, "by": true, "is": true, "are": true, "was": true, "were": true,
		"be": true, "been": true, "have": true, "has": true, "had": true,
		"do": true, "does": true, "did": true, "will": true, "would": true,
		"could": true, "should": true, "may": true, "might": true, "must": true,
	}

	keywords := make([]string, 0)
	for _, word := range words {
		// 清理单词（移除标点符号）
		word = strings.Trim(word, ".,!?;:\"'()[]{}")
		if len(word) >= 2 && !stopWords[word] {
			keywords = append(keywords, word)
		}
	}

	index.Keywords = keywords
}

// preprocessQuery 预处理查询
func (s *service) preprocessQuery(query string) string {
	// 清理查询字符串
	query = strings.TrimSpace(query)

	// 移除多余的空格
	query = strings.Join(strings.Fields(query), " ")

	return query
}
