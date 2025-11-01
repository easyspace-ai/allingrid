package valueobject

import (
	"encoding/json"
	"fmt"
)

// Group 分组值对象
type Group struct {
	GroupItems []GroupItem `json:"groupItems"`
}

// GroupItem 分组项
type GroupItem struct {
	FieldID string    `json:"fieldId"` // 字段ID
	Order   SortOrder `json:"order"`   // 排序方向
}

// NewGroup 创建分组值对象
func NewGroup(data []map[string]interface{}) (*Group, error) {
	if data == nil || len(data) == 0 {
		return nil, nil
	}

	// 序列化再反序列化，确保数据格式正确
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("invalid group data: %w", err)
	}

	var groupItems []GroupItem
	if err := json.Unmarshal(jsonData, &groupItems); err != nil {
		return nil, fmt.Errorf("failed to parse group: %w", err)
	}

	group := &Group{
		GroupItems: groupItems,
	}

	// 验证
	if err := group.Validate(); err != nil {
		return nil, err
	}

	return group, nil
}

// Validate 验证分组
func (g *Group) Validate() error {
	if g == nil {
		return nil
	}

	if len(g.GroupItems) == 0 {
		return fmt.Errorf("group must have at least one group item")
	}

	// 分组最多3层
	if len(g.GroupItems) > 3 {
		return fmt.Errorf("group can have at most 3 levels, got %d", len(g.GroupItems))
	}

	for i, item := range g.GroupItems {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("invalid group item at index %d: %w", i, err)
		}
	}

	return nil
}

// Validate 验证分组项
func (gi *GroupItem) Validate() error {
	// 验证字段ID
	if gi.FieldID == "" {
		return fmt.Errorf("field ID is required")
	}

	// 验证排序方向
	if gi.Order != SortOrderAsc && gi.Order != SortOrderDesc {
		return fmt.Errorf("invalid sort order: %s, must be 'asc' or 'desc'", gi.Order)
	}

	return nil
}

// ToSlice 转换为切片
func (g *Group) ToSlice() []map[string]interface{} {
	if g == nil || len(g.GroupItems) == 0 {
		return nil
	}

	result := make([]map[string]interface{}, len(g.GroupItems))
	for i, item := range g.GroupItems {
		result[i] = map[string]interface{}{
			"fieldId": item.FieldID,
			"order":   item.Order,
		}
	}

	return result
}

// IsEmpty 检查分组是否为空
func (g *Group) IsEmpty() bool {
	return g == nil || len(g.GroupItems) == 0
}

// GetFieldIDs 获取所有涉及的字段ID
func (g *Group) GetFieldIDs() []string {
	if g == nil {
		return []string{}
	}

	fieldIDs := make([]string, len(g.GroupItems))
	for i, item := range g.GroupItems {
		fieldIDs[i] = item.FieldID
	}

	return fieldIDs
}

// GetLevelCount 获取分组层级数
func (g *Group) GetLevelCount() int {
	if g == nil {
		return 0
	}
	return len(g.GroupItems)
}

// AddGroupItem 添加分组项
func (g *Group) AddGroupItem(fieldID string, order SortOrder) error {
	// 检查是否已达到最大层级
	if len(g.GroupItems) >= 3 {
		return fmt.Errorf("group can have at most 3 levels")
	}

	item := GroupItem{
		FieldID: fieldID,
		Order:   order,
	}

	if err := item.Validate(); err != nil {
		return err
	}

	g.GroupItems = append(g.GroupItems, item)
	return nil
}

// RemoveGroupItem 移除分组项
func (g *Group) RemoveGroupItem(fieldID string) {
	if g == nil {
		return
	}

	newItems := make([]GroupItem, 0, len(g.GroupItems))
	for _, item := range g.GroupItems {
		if item.FieldID != fieldID {
			newItems = append(newItems, item)
		}
	}

	g.GroupItems = newItems
}

// GetGroupItem 获取指定字段的分组项
func (g *Group) GetGroupItem(fieldID string) *GroupItem {
	if g == nil {
		return nil
	}

	for _, item := range g.GroupItems {
		if item.FieldID == fieldID {
			return &item
		}
	}

	return nil
}

// HasGroupItem 检查是否有指定字段的分组
func (g *Group) HasGroupItem(fieldID string) bool {
	return g.GetGroupItem(fieldID) != nil
}

// GetGroupLevel 获取指定字段的分组层级（从1开始）
func (g *Group) GetGroupLevel(fieldID string) int {
	if g == nil {
		return 0
	}

	for i, item := range g.GroupItems {
		if item.FieldID == fieldID {
			return i + 1
		}
	}

	return 0
}
