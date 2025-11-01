package search

import (
	"context"
)

// Repository 搜索仓储接口
type Repository interface {
	// Index management
	// CreateIndex 创建搜索索引
	CreateIndex(ctx context.Context, index *SearchIndex) error
	// GetIndex 获取搜索索引
	GetIndex(ctx context.Context, id string) (*SearchIndex, error)
	// UpdateIndex 更新搜索索引
	UpdateIndex(ctx context.Context, index *SearchIndex) error
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
	// IncrementSearchCount 增加搜索计数
	IncrementSearchCount(ctx context.Context, query string, searchType SearchType, scope SearchScope) error

	// Index maintenance
	// RebuildIndex 重建索引
	RebuildIndex(ctx context.Context, searchType *SearchType) error
	// OptimizeIndex 优化索引
	OptimizeIndex(ctx context.Context) error
	// GetIndexStats 获取索引统计
	GetIndexStats(ctx context.Context) (map[string]interface{}, error)
}
