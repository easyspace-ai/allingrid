package valueobject

import (
	"encoding/json"
	"fmt"
)

// SortOrder 排序方向
type SortOrder string

const (
	SortOrderAsc  SortOrder = "asc"  // 升序
	SortOrderDesc SortOrder = "desc" // 降序
)

// Sort 排序值对象
type Sort struct {
	SortItems []SortItem `json:"sortItems"`
}

// SortItem 排序项
type SortItem struct {
	FieldID string    `json:"fieldId"` // 字段ID
	Order   SortOrder `json:"order"`   // 排序方向
}

// NewSort 创建排序值对象
func NewSort(data []map[string]interface{}) (*Sort, error) {
	if data == nil || len(data) == 0 {
		return nil, nil
	}

	// 序列化再反序列化，确保数据格式正确
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("invalid sort data: %w", err)
	}

	var sortItems []SortItem
	if err := json.Unmarshal(jsonData, &sortItems); err != nil {
		return nil, fmt.Errorf("failed to parse sort: %w", err)
	}

	sort := &Sort{
		SortItems: sortItems,
	}

	// 验证
	if err := sort.Validate(); err != nil {
		return nil, err
	}

	return sort, nil
}

// Validate 验证排序
func (s *Sort) Validate() error {
	if s == nil {
		return nil
	}

	if len(s.SortItems) == 0 {
		return fmt.Errorf("sort must have at least one sort item")
	}

	for i, item := range s.SortItems {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("invalid sort item at index %d: %w", i, err)
		}
	}

	return nil
}

// Validate 验证排序项
func (si *SortItem) Validate() error {
	// 验证字段ID
	if si.FieldID == "" {
		return fmt.Errorf("field ID is required")
	}

	// 验证排序方向
	if si.Order != SortOrderAsc && si.Order != SortOrderDesc {
		return fmt.Errorf("invalid sort order: %s, must be 'asc' or 'desc'", si.Order)
	}

	return nil
}

// ToSlice 转换为切片
func (s *Sort) ToSlice() []map[string]interface{} {
	if s == nil || len(s.SortItems) == 0 {
		return nil
	}

	result := make([]map[string]interface{}, len(s.SortItems))
	for i, item := range s.SortItems {
		result[i] = map[string]interface{}{
			"fieldId": item.FieldID,
			"order":   item.Order,
		}
	}

	return result
}

// IsEmpty 检查排序是否为空
func (s *Sort) IsEmpty() bool {
	return s == nil || len(s.SortItems) == 0
}

// GetFieldIDs 获取所有涉及的字段ID
func (s *Sort) GetFieldIDs() []string {
	if s == nil {
		return []string{}
	}

	fieldIDs := make([]string, len(s.SortItems))
	for i, item := range s.SortItems {
		fieldIDs[i] = item.FieldID
	}

	return fieldIDs
}

// AddSortItem 添加排序项
func (s *Sort) AddSortItem(fieldID string, order SortOrder) error {
	item := SortItem{
		FieldID: fieldID,
		Order:   order,
	}

	if err := item.Validate(); err != nil {
		return err
	}

	s.SortItems = append(s.SortItems, item)
	return nil
}

// RemoveSortItem 移除排序项
func (s *Sort) RemoveSortItem(fieldID string) {
	if s == nil {
		return
	}

	newItems := make([]SortItem, 0, len(s.SortItems))
	for _, item := range s.SortItems {
		if item.FieldID != fieldID {
			newItems = append(newItems, item)
		}
	}

	s.SortItems = newItems
}

// GetSortItem 获取指定字段的排序项
func (s *Sort) GetSortItem(fieldID string) *SortItem {
	if s == nil {
		return nil
	}

	for _, item := range s.SortItems {
		if item.FieldID == fieldID {
			return &item
		}
	}

	return nil
}

// HasSortItem 检查是否有指定字段的排序
func (s *Sort) HasSortItem(fieldID string) bool {
	return s.GetSortItem(fieldID) != nil
}
