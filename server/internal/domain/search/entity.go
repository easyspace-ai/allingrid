package search

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// SearchType 搜索类型
type SearchType string

const (
	SearchTypeGlobal     SearchType = "global"     // 全局搜索
	SearchTypeSpace      SearchType = "space"      // 空间内搜索
	SearchTypeTable      SearchType = "table"      // 表格内搜索
	SearchTypeRecord     SearchType = "record"     // 记录搜索
	SearchTypeField      SearchType = "field"      // 字段搜索
	SearchTypeUser       SearchType = "user"       // 用户搜索
	SearchTypeComment    SearchType = "comment"    // 评论搜索
	SearchTypeAttachment SearchType = "attachment" // 附件搜索
)

// SearchScope 搜索范围
type SearchScope string

const (
	SearchScopeAll         SearchScope = "all"         // 全部
	SearchScopeTitle       SearchScope = "title"       // 标题
	SearchScopeContent     SearchScope = "content"     // 内容
	SearchScopeMetadata    SearchScope = "metadata"    // 元数据
	SearchScopeComments    SearchScope = "comments"    // 评论
	SearchScopeAttachments SearchScope = "attachments" // 附件
)

// SearchResult 搜索结果
type SearchResult struct {
	ID          string                 `json:"id"`
	Type        SearchType             `json:"type"`
	Title       string                 `json:"title"`
	Content     string                 `json:"content"`
	Highlight   map[string]interface{} `json:"highlight,omitempty"`
	Score       float64                `json:"score"`
	SourceID    string                 `json:"source_id,omitempty"`
	SourceType  string                 `json:"source_type,omitempty"`
	SourceURL   string                 `json:"source_url,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedTime time.Time              `json:"created_time"`
	UpdatedTime time.Time              `json:"updated_time"`
}

// SearchRequest 搜索请求
type SearchRequest struct {
	Query       string                 `json:"query" binding:"required"`
	Type        SearchType             `json:"type,omitempty"`
	Scope       SearchScope            `json:"scope,omitempty"`
	SourceID    string                 `json:"source_id,omitempty"`
	SourceType  string                 `json:"source_type,omitempty"`
	UserID      string                 `json:"user_id,omitempty"`
	SpaceID     string                 `json:"space_id,omitempty"`
	TableID     string                 `json:"table_id,omitempty"`
	FieldIDs    []string               `json:"field_ids,omitempty"`
	Filters     map[string]interface{} `json:"filters,omitempty"`
	SortBy      string                 `json:"sort_by,omitempty"`
	SortOrder   string                 `json:"sort_order,omitempty"`
	Page        int                    `json:"page,omitempty"`
	PageSize    int                    `json:"page_size,omitempty"`
	Highlight   bool                   `json:"highlight,omitempty"`
	Suggestions bool                   `json:"suggestions,omitempty"`
}

// SearchResponse 搜索响应
type SearchResponse struct {
	Results     []*SearchResult        `json:"results"`
	Total       int64                  `json:"total"`
	Page        int                    `json:"page"`
	PageSize    int                    `json:"page_size"`
	TotalPages  int                    `json:"total_pages"`
	Suggestions []string               `json:"suggestions,omitempty"`
	Facets      map[string]interface{} `json:"facets,omitempty"`
	QueryTime   int64                  `json:"query_time"` // 查询时间(毫秒)
	SearchType  SearchType             `json:"search_type"`
	SearchScope SearchScope            `json:"search_scope"`
}

// SearchIndex 搜索索引
type SearchIndex struct {
	ID          string                 `json:"id"`
	Type        SearchType             `json:"type"`
	Title       string                 `json:"title"`
	Content     string                 `json:"content"`
	Keywords    []string               `json:"keywords,omitempty"`
	SourceID    string                 `json:"source_id"`
	SourceType  string                 `json:"source_type"`
	SourceURL   string                 `json:"source_url,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	UserID      string                 `json:"user_id,omitempty"`
	SpaceID     string                 `json:"space_id,omitempty"`
	TableID     string                 `json:"table_id,omitempty"`
	FieldID     string                 `json:"field_id,omitempty"`
	Permissions []string               `json:"permissions,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
	CreatedTime time.Time              `json:"created_time"`
	UpdatedTime time.Time              `json:"updated_time"`
}

// NewSearchIndex 创建搜索索引
func NewSearchIndex(searchType SearchType, title, content string) *SearchIndex {
	return &SearchIndex{
		ID:          utils.GenerateNanoID(10),
		Type:        searchType,
		Title:       title,
		Content:     content,
		Keywords:    make([]string, 0),
		Metadata:    make(map[string]interface{}),
		Permissions: make([]string, 0),
		Tags:        make([]string, 0),
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// AddKeyword 添加关键词
func (s *SearchIndex) AddKeyword(keyword string) {
	for _, k := range s.Keywords {
		if k == keyword {
			return // 已存在
		}
	}
	s.Keywords = append(s.Keywords, keyword)
	s.UpdatedTime = time.Now()
}

// AddKeywords 批量添加关键词
func (s *SearchIndex) AddKeywords(keywords []string) {
	for _, keyword := range keywords {
		s.AddKeyword(keyword)
	}
}

// SetSource 设置来源
func (s *SearchIndex) SetSource(sourceID, sourceType, sourceURL string) {
	s.SourceID = sourceID
	s.SourceType = sourceType
	s.SourceURL = sourceURL
	s.UpdatedTime = time.Now()
}

// SetMetadata 设置元数据
func (s *SearchIndex) SetMetadata(key string, value interface{}) {
	if s.Metadata == nil {
		s.Metadata = make(map[string]interface{})
	}
	s.Metadata[key] = value
	s.UpdatedTime = time.Now()
}

// SetContext 设置上下文
func (s *SearchIndex) SetContext(userID, spaceID, tableID, fieldID string) {
	s.UserID = userID
	s.SpaceID = spaceID
	s.TableID = tableID
	s.FieldID = fieldID
	s.UpdatedTime = time.Now()
}

// AddPermission 添加权限
func (s *SearchIndex) AddPermission(permission string) {
	for _, p := range s.Permissions {
		if p == permission {
			return // 已存在
		}
	}
	s.Permissions = append(s.Permissions, permission)
	s.UpdatedTime = time.Now()
}

// AddTag 添加标签
func (s *SearchIndex) AddTag(tag string) {
	for _, t := range s.Tags {
		if t == tag {
			return // 已存在
		}
	}
	s.Tags = append(s.Tags, tag)
	s.UpdatedTime = time.Now()
}

// SearchSuggestion 搜索建议
type SearchSuggestion struct {
	ID          string     `json:"id"`
	Query       string     `json:"query"`
	Count       int64      `json:"count"`
	Type        SearchType `json:"type,omitempty"`
	SourceID    string     `json:"source_id,omitempty"`
	SourceType  string     `json:"source_type,omitempty"`
	CreatedTime time.Time  `json:"created_time"`
	UpdatedTime time.Time  `json:"updated_time"`
}

// NewSearchSuggestion 创建搜索建议
func NewSearchSuggestion(query string, count int64) *SearchSuggestion {
	return &SearchSuggestion{
		ID:          utils.GenerateNanoID(10),
		Query:       query,
		Count:       count,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// SearchStats 搜索统计
type SearchStats struct {
	TotalSearches    int64                 `json:"total_searches"`
	TotalIndexes     int64                 `json:"total_indexes"`
	PopularQueries   []*SearchSuggestion   `json:"popular_queries"`
	SearchByType     map[SearchType]int64  `json:"search_by_type"`
	SearchByScope    map[SearchScope]int64 `json:"search_by_scope"`
	AverageQueryTime float64               `json:"average_query_time"`
	TopResults       []*SearchResult       `json:"top_results"`
	RecentSearches   []*SearchRequest      `json:"recent_searches"`
}

// SearchFilter 搜索过滤器
type SearchFilter struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // eq, ne, gt, gte, lt, lte, in, nin, like, regex
	Value    interface{} `json:"value"`
}

// SearchSort 搜索排序
type SearchSort struct {
	Field string `json:"field"`
	Order string `json:"order"` // asc, desc
}

// AdvancedSearchRequest 高级搜索请求
type AdvancedSearchRequest struct {
	Queries    []string       `json:"queries" binding:"required"`
	Filters    []SearchFilter `json:"filters,omitempty"`
	Sorts      []SearchSort   `json:"sorts,omitempty"`
	Type       SearchType     `json:"type,omitempty"`
	Scope      SearchScope    `json:"scope,omitempty"`
	SourceID   string         `json:"source_id,omitempty"`
	SourceType string         `json:"source_type,omitempty"`
	UserID     string         `json:"user_id,omitempty"`
	SpaceID    string         `json:"space_id,omitempty"`
	TableID    string         `json:"table_id,omitempty"`
	FieldIDs   []string       `json:"field_ids,omitempty"`
	Page       int            `json:"page,omitempty"`
	PageSize   int            `json:"page_size,omitempty"`
	Highlight  bool           `json:"highlight,omitempty"`
	Facets     []string       `json:"facets,omitempty"`
}

// SearchIndexRequest 创建搜索索引请求
type SearchIndexRequest struct {
	Type        SearchType             `json:"type" binding:"required"`
	Title       string                 `json:"title" binding:"required"`
	Content     string                 `json:"content" binding:"required"`
	Keywords    []string               `json:"keywords,omitempty"`
	SourceID    string                 `json:"source_id" binding:"required"`
	SourceType  string                 `json:"source_type" binding:"required"`
	SourceURL   string                 `json:"source_url,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	UserID      string                 `json:"user_id,omitempty"`
	SpaceID     string                 `json:"space_id,omitempty"`
	TableID     string                 `json:"table_id,omitempty"`
	FieldID     string                 `json:"field_id,omitempty"`
	Permissions []string               `json:"permissions,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
}

// UpdateSearchIndexRequest 更新搜索索引请求
type UpdateSearchIndexRequest struct {
	Title       string                 `json:"title,omitempty"`
	Content     string                 `json:"content,omitempty"`
	Keywords    []string               `json:"keywords,omitempty"`
	SourceURL   string                 `json:"source_url,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Permissions []string               `json:"permissions,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
}
