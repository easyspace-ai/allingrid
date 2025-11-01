package models

import (
	"time"

	"gorm.io/gorm"
)

// SearchIndex 搜索索引模型
type SearchIndex struct {
	ID          string    `gorm:"primaryKey;type:varchar(20)" json:"id"`
	Type        string    `gorm:"type:varchar(50);not null;index" json:"type"`
	Title       string    `gorm:"type:varchar(500);not null;index" json:"title"`
	Content     string    `gorm:"type:text;not null" json:"content"`
	Keywords    string    `gorm:"type:json" json:"keywords"` // JSON格式存储
	SourceID    string    `gorm:"type:varchar(20);not null;index" json:"source_id"`
	SourceType  string    `gorm:"type:varchar(50);not null;index" json:"source_type"`
	SourceURL   string    `gorm:"type:varchar(1000)" json:"source_url"`
	Metadata    string    `gorm:"type:json" json:"metadata"` // JSON格式存储
	UserID      string    `gorm:"type:varchar(20);index" json:"user_id"`
	SpaceID     string    `gorm:"type:varchar(20);index" json:"space_id"`
	TableID     string    `gorm:"type:varchar(20);index" json:"table_id"`
	FieldID     string    `gorm:"type:varchar(20);index" json:"field_id"`
	Permissions string    `gorm:"type:json" json:"permissions"` // JSON格式存储
	Tags        string    `gorm:"type:json" json:"tags"`        // JSON格式存储
	CreatedTime time.Time `gorm:"autoCreateTime;index" json:"created_time"`
	UpdatedTime time.Time `gorm:"autoUpdateTime" json:"updated_time"`
}

// TableName 返回表名
func (SearchIndex) TableName() string {
	return "search_indexes"
}

// SearchSuggestion 搜索建议模型
type SearchSuggestion struct {
	ID          string    `gorm:"primaryKey;type:varchar(20)" json:"id"`
	Query       string    `gorm:"type:varchar(500);not null;uniqueIndex" json:"query"`
	Count       int64     `gorm:"not null;default:1;index" json:"count"`
	Type        string    `gorm:"type:varchar(50);index" json:"type"`
	SourceID    string    `gorm:"type:varchar(20);index" json:"source_id"`
	SourceType  string    `gorm:"type:varchar(50);index" json:"source_type"`
	CreatedTime time.Time `gorm:"autoCreateTime" json:"created_time"`
	UpdatedTime time.Time `gorm:"autoUpdateTime" json:"updated_time"`
}

// TableName 返回表名
func (SearchSuggestion) TableName() string {
	return "search_suggestions"
}

// SearchStats 搜索统计模型
type SearchStats struct {
	ID               string    `gorm:"primaryKey;type:varchar(20)" json:"id"`
	TotalSearches    int64     `gorm:"not null;default:0" json:"total_searches"`
	TotalIndexes     int64     `gorm:"not null;default:0" json:"total_indexes"`
	AverageQueryTime float64   `gorm:"not null;default:0" json:"average_query_time"`
	LastUpdated      time.Time `gorm:"autoUpdateTime" json:"last_updated"`
}

// TableName 返回表名
func (SearchStats) TableName() string {
	return "search_stats"
}

// BeforeCreate 创建前钩子
func (s *SearchIndex) BeforeCreate(tx *gorm.DB) error {
	if s.ID == "" {
		// 这里应该生成ID，但通常由应用层处理
	}
	return nil
}

// BeforeCreate 创建前钩子
func (s *SearchSuggestion) BeforeCreate(tx *gorm.DB) error {
	if s.ID == "" {
		// 这里应该生成ID，但通常由应用层处理
	}
	return nil
}

// BeforeCreate 创建前钩子
func (s *SearchStats) BeforeCreate(tx *gorm.DB) error {
	if s.ID == "" {
		// 这里应该生成ID，但通常由应用层处理
	}
	return nil
}
