package valueobject

import (
	"encoding/json"
	"fmt"
)

// ColumnMeta 列配置
type ColumnMeta struct {
	FieldID string  `json:"fieldId"` // 字段ID
	Width   int     `json:"width"`   // 列宽（像素）
	Visible bool    `json:"visible"` // 是否可见
	Order   float64 `json:"order"`   // 排序位置
}

// ColumnMetaList 列配置列表
type ColumnMetaList struct {
	Columns []ColumnMeta `json:"columnMeta"`
}

// NewColumnMetaList 创建列配置列表值对象
func NewColumnMetaList(data []map[string]interface{}) (*ColumnMetaList, error) {
	if data == nil || len(data) == 0 {
		return &ColumnMetaList{Columns: []ColumnMeta{}}, nil
	}

	// 序列化再反序列化，确保数据格式正确
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("invalid column meta data: %w", err)
	}

	var columns []ColumnMeta
	if err := json.Unmarshal(jsonData, &columns); err != nil {
		return nil, fmt.Errorf("failed to parse column meta: %w", err)
	}

	columnMetaList := &ColumnMetaList{
		Columns: columns,
	}

	// 验证
	if err := columnMetaList.Validate(); err != nil {
		return nil, err
	}

	return columnMetaList, nil
}

// Validate 验证列配置列表
func (cml *ColumnMetaList) Validate() error {
	if cml == nil {
		return nil
	}

	// 检查字段ID重复
	fieldIDSet := make(map[string]bool)
	for i, col := range cml.Columns {
		if err := col.Validate(); err != nil {
			return fmt.Errorf("invalid column meta at index %d: %w", i, err)
		}

		if fieldIDSet[col.FieldID] {
			return fmt.Errorf("duplicate field ID: %s", col.FieldID)
		}
		fieldIDSet[col.FieldID] = true
	}

	return nil
}

// Validate 验证列配置
func (cm *ColumnMeta) Validate() error {
	// 验证字段ID
	if cm.FieldID == "" {
		return fmt.Errorf("field ID is required")
	}

	// 验证列宽
	if cm.Width < 0 {
		return fmt.Errorf("column width must be non-negative, got %d", cm.Width)
	}

	// 列宽合理范围检查（50-1000像素）
	if cm.Width > 0 && (cm.Width < 50 || cm.Width > 1000) {
		return fmt.Errorf("column width should be between 50 and 1000 pixels, got %d", cm.Width)
	}

	return nil
}

// ToSlice 转换为切片
func (cml *ColumnMetaList) ToSlice() []map[string]interface{} {
	if cml == nil || len(cml.Columns) == 0 {
		return []map[string]interface{}{}
	}

	result := make([]map[string]interface{}, len(cml.Columns))
	for i, col := range cml.Columns {
		result[i] = map[string]interface{}{
			"fieldId": col.FieldID,
			"width":   col.Width,
			"visible": col.Visible,
			"order":   col.Order,
		}
	}

	return result
}

// IsEmpty 检查列配置是否为空
func (cml *ColumnMetaList) IsEmpty() bool {
	return cml == nil || len(cml.Columns) == 0
}

// GetFieldIDs 获取所有字段ID
func (cml *ColumnMetaList) GetFieldIDs() []string {
	if cml == nil {
		return []string{}
	}

	fieldIDs := make([]string, len(cml.Columns))
	for i, col := range cml.Columns {
		fieldIDs[i] = col.FieldID
	}

	return fieldIDs
}

// GetVisibleFieldIDs 获取所有可见字段ID
func (cml *ColumnMetaList) GetVisibleFieldIDs() []string {
	if cml == nil {
		return []string{}
	}

	visibleIDs := make([]string, 0, len(cml.Columns))
	for _, col := range cml.Columns {
		if col.Visible {
			visibleIDs = append(visibleIDs, col.FieldID)
		}
	}

	return visibleIDs
}

// GetColumn 获取指定字段的列配置
func (cml *ColumnMetaList) GetColumn(fieldID string) *ColumnMeta {
	if cml == nil {
		return nil
	}

	for _, col := range cml.Columns {
		if col.FieldID == fieldID {
			return &col
		}
	}

	return nil
}

// HasColumn 检查是否有指定字段的列配置
func (cml *ColumnMetaList) HasColumn(fieldID string) bool {
	return cml.GetColumn(fieldID) != nil
}

// SetColumnWidth 设置列宽
func (cml *ColumnMetaList) SetColumnWidth(fieldID string, width int) error {
	if width < 50 || width > 1000 {
		return fmt.Errorf("column width should be between 50 and 1000 pixels, got %d", width)
	}

	for i, col := range cml.Columns {
		if col.FieldID == fieldID {
			cml.Columns[i].Width = width
			return nil
		}
	}

	return fmt.Errorf("column not found: %s", fieldID)
}

// SetColumnVisible 设置列可见性
func (cml *ColumnMetaList) SetColumnVisible(fieldID string, visible bool) error {
	for i, col := range cml.Columns {
		if col.FieldID == fieldID {
			cml.Columns[i].Visible = visible
			return nil
		}
	}

	return fmt.Errorf("column not found: %s", fieldID)
}

// AddColumn 添加列配置
func (cml *ColumnMetaList) AddColumn(fieldID string, width int, visible bool, order float64) error {
	// 检查是否已存在
	if cml.HasColumn(fieldID) {
		return fmt.Errorf("column already exists: %s", fieldID)
	}

	col := ColumnMeta{
		FieldID: fieldID,
		Width:   width,
		Visible: visible,
		Order:   order,
	}

	if err := col.Validate(); err != nil {
		return err
	}

	cml.Columns = append(cml.Columns, col)
	return nil
}

// RemoveColumn 移除列配置
func (cml *ColumnMetaList) RemoveColumn(fieldID string) {
	if cml == nil {
		return
	}

	newColumns := make([]ColumnMeta, 0, len(cml.Columns))
	for _, col := range cml.Columns {
		if col.FieldID != fieldID {
			newColumns = append(newColumns, col)
		}
	}

	cml.Columns = newColumns
}

// ReorderColumns 重新排序列
func (cml *ColumnMetaList) ReorderColumns(fieldIDs []string) error {
	if len(fieldIDs) != len(cml.Columns) {
		return fmt.Errorf("field IDs count mismatch")
	}

	// 创建字段ID到列配置的映射
	columnMap := make(map[string]ColumnMeta)
	for _, col := range cml.Columns {
		columnMap[col.FieldID] = col
	}

	// 按新顺序重建列配置
	newColumns := make([]ColumnMeta, 0, len(fieldIDs))
	for i, fieldID := range fieldIDs {
		col, exists := columnMap[fieldID]
		if !exists {
			return fmt.Errorf("field ID not found: %s", fieldID)
		}
		col.Order = float64(i)
		newColumns = append(newColumns, col)
	}

	cml.Columns = newColumns
	return nil
}

// GetVisibleCount 获取可见列数量
func (cml *ColumnMetaList) GetVisibleCount() int {
	if cml == nil {
		return 0
	}

	count := 0
	for _, col := range cml.Columns {
		if col.Visible {
			count++
		}
	}

	return count
}

// GetHiddenCount 获取隐藏列数量
func (cml *ColumnMetaList) GetHiddenCount() int {
	if cml == nil {
		return 0
	}

	return len(cml.Columns) - cml.GetVisibleCount()
}
