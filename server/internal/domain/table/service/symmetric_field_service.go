package service

import (
	"context"
	"fmt"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
)

// SymmetricFieldService 对称字段服务
type SymmetricFieldService struct {
	batchService BatchService
	fieldRepo    FieldRepository
}

// IsVirtualField 检查字段类型是否为虚拟字段
func IsVirtualField(fieldType string) bool {
	switch fieldType {
	case "formula", "rollup", "lookup", "ai":
		return true
	default:
		return false
	}
}

// SymmetricFieldService 对称字段同步服务
// 处理Link字段的双向关联关系自动同步

func NewSymmetricFieldService(
	fieldRepo FieldRepository,
	batchService BatchService,
) *SymmetricFieldService {
	return &SymmetricFieldService{
		fieldRepo:    fieldRepo,
		batchService: batchService,
	}
}

// LinkCellValue Link单元格值
type LinkCellValue struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
}

// LinkCellChange Link字段变更
type LinkCellChange struct {
	RecordID      string
	OldValue      []string // 旧的关联记录IDs
	NewValue      []string // 新的关联记录IDs
	ExpectedValue []string // 预期的对称字段值（用于冲突检测）
}

// Conflict 同步冲突
type Conflict struct {
	RecordID      string
	FieldID       string
	ExpectedValue interface{}
	ActualValue   interface{}
}

// ConflictResolutionStrategy 冲突解决策略
type ConflictResolutionStrategy string

const (
	StrategyLastWriteWins ConflictResolutionStrategy = "last_write_wins" // 最后写入胜出
	StrategyMerge         ConflictResolutionStrategy = "merge"           // 合并值
	StrategyAbort         ConflictResolutionStrategy = "abort"           // 中止操作
)

// SyncSymmetricField 同步对称字段
// 当一个Link字段变更时，自动更新对称字段
func (s *SymmetricFieldService) SyncSymmetricField(
	ctx context.Context,
	linkField *fieldEntity.Field,
	changes []LinkCellChange,
) error {
	if linkField.Options == nil || linkField.Options().Link.SymmetricFieldID == "" {
		return nil // 没有对称字段，无需同步
	}

	symmetricFieldID := linkField.Options().Link.SymmetricFieldID
	linkedTableID := linkField.Options().Link.LinkedTableID
	if linkedTableID == "" {
		linkedTableID = linkField.Options().Link.LinkedTableID
	}

	// 收集所有需要更新的对称字段变更
	var symmetricUpdates []RecordUpdate

	for _, change := range changes {
		// 计算需要添加和移除的关联
		toRemove := difference(change.OldValue, change.NewValue)
		toAdd := difference(change.NewValue, change.OldValue)

		// 从旧关联中移除当前记录
		for _, linkedRecordID := range toRemove {
			update, err := s.removeFromSymmetricField(
				ctx, linkedTableID, symmetricFieldID,
				linkedRecordID, change.RecordID,
			)
			if err != nil {
				return err
			}
			if update != nil {
				symmetricUpdates = append(symmetricUpdates, *update)
			}
		}

		// 添加到新关联
		for _, linkedRecordID := range toAdd {
			update, err := s.addToSymmetricField(
				ctx, linkedTableID, symmetricFieldID,
				linkedRecordID, change.RecordID,
			)
			if err != nil {
				return err
			}
			if update != nil {
				symmetricUpdates = append(symmetricUpdates, *update)
			}
		}
	}

	// 批量更新对称字段
	if len(symmetricUpdates) > 0 {
		return s.batchService.BatchUpdateRecords(ctx, symmetricUpdates)
	}

	return nil
}

// removeFromSymmetricField 从对称字段中移除记录引用
func (s *SymmetricFieldService) removeFromSymmetricField(
	ctx context.Context,
	tableID string,
	fieldID string,
	linkedRecordID string,
	recordIDToRemove string,
) (*RecordUpdate, error) {
	// 查询当前的Link字段值
	currentValue, err := s.getFieldValue(ctx, tableID, linkedRecordID, fieldID)
	if err != nil {
		return nil, err
	}

	// 从值中移除recordIDToRemove
	currentIDs := s.valueToRecordIDs(currentValue)
	newIDs := remove(currentIDs, recordIDToRemove)

	// 如果值没有变化，不需要更新
	if len(currentIDs) == len(newIDs) {
		return nil, nil
	}

	return &RecordUpdate{
		TableID:  tableID,
		RecordID: linkedRecordID,
		FieldID:  fieldID,
		NewValue: newIDs,
	}, nil
}

// addToSymmetricField 向对称字段添加记录引用
func (s *SymmetricFieldService) addToSymmetricField(
	ctx context.Context,
	tableID string,
	fieldID string,
	linkedRecordID string,
	recordIDToAdd string,
) (*RecordUpdate, error) {
	// 查询当前的Link字段值
	currentValue, err := s.getFieldValue(ctx, tableID, linkedRecordID, fieldID)
	if err != nil {
		return nil, err
	}

	// 添加recordIDToAdd到值中
	currentIDs := s.valueToRecordIDs(currentValue)
	if sliceContainsString(currentIDs, recordIDToAdd) {
		return nil, nil // 已存在，不需要更新
	}

	newIDs := append(currentIDs, recordIDToAdd)

	return &RecordUpdate{
		TableID:  tableID,
		RecordID: linkedRecordID,
		FieldID:  fieldID,
		NewValue: newIDs,
	}, nil
}

// getFieldValue 获取字段值
func (s *SymmetricFieldService) getFieldValue(
	ctx context.Context,
	tableID string,
	recordID string,
	fieldID string,
) (interface{}, error) {
	records, err := s.batchService.BatchQueryRecords(
		ctx, tableID, []string{recordID}, []string{fieldID},
	)
	if err != nil {
		return nil, err
	}

	if len(records) > 0 {
		return records[0][fieldID], nil
	}

	return nil, nil
}

// UpdateLinkCellTitles 更新Link Cell的标题
func (s *SymmetricFieldService) UpdateLinkCellTitles(
	ctx context.Context,
	linkField *fieldEntity.Field,
	recordID string,
	linkValues []LinkCellValue,
) ([]LinkCellValue, error) {
	if len(linkValues) == 0 {
		return linkValues, nil
	}

	// 获取标题字段ID
	titleFieldID := linkField.Options().Lookup.LookupFieldID
	if titleFieldID == "" {
		// 默认使用主字段（第一个字段）
		titleFieldID, _ = s.getPrimaryFieldID(ctx, linkField.Options().Link.LinkedTableID)
	}

	// 批量查询关联记录的标题
	linkedRecordIDs := make([]string, len(linkValues))
	for i, v := range linkValues {
		linkedRecordIDs[i] = v.ID
	}

	records, err := s.batchService.BatchQueryRecords(
		ctx,
		linkField.Options().Link.LinkedTableID,
		linkedRecordIDs,
		[]string{titleFieldID},
	)
	if err != nil {
		return nil, err
	}

	// 更新标题
	updatedValues := make([]LinkCellValue, len(linkValues))
	for i, v := range linkValues {
		recordMap := make(map[string]map[string]interface{})
		for _, rec := range records {
			if id, ok := rec["id"].(string); ok {
				recordMap[id] = rec
			}
		}
		if recordData, exists := recordMap[v.ID]; exists {
			title := recordData[titleFieldID]
			updatedValues[i] = LinkCellValue{
				ID:    v.ID,
				Title: fmt.Sprintf("%v", title),
			}
		} else {
			// 记录不存在，保留原值
			updatedValues[i] = v
		}
	}

	return updatedValues, nil
}

// getPrimaryFieldID 获取表的主字段ID
func (s *SymmetricFieldService) getPrimaryFieldID(
	ctx context.Context,
	tableID string,
) (string, error) {
	fields, err := s.fieldRepo.GetByTableID(ctx, tableID)
	if err != nil {
		return "", err
	}

	// 返回第一个非虚拟字段
	for _, fld := range fields {
		if !IsVirtualField(fld.Type().String()) {
			return fld.ID().String(), nil
		}
	}

	// 如果没有普通字段，返回第一个字段
	if len(fields) > 0 {
		return fields[0].ID().String(), nil
	}

	return "", fmt.Errorf("no fields found in table %s", tableID)
}

// DetectConflicts 检测对称字段同步冲突
func (s *SymmetricFieldService) DetectConflicts(
	ctx context.Context,
	linkField *fieldEntity.Field,
	changes []LinkCellChange,
) ([]Conflict, error) {
	if linkField.Options == nil || linkField.Options().Link.SymmetricFieldID == "" {
		return nil, nil
	}

	var conflicts []Conflict
	symmetricFieldID := linkField.Options().Link.SymmetricFieldID
	linkedTableID := linkField.Options().Link.LinkedTableID

	// 检查每个变更的对称字段
	for _, change := range changes {
		if len(change.ExpectedValue) == 0 {
			continue // 没有预期值，跳过冲突检测
		}

		for _, linkedRecordID := range change.NewValue {
			// 查询对称字段的当前值
			currentValue, err := s.getFieldValue(
				ctx, linkedTableID, linkedRecordID, symmetricFieldID,
			)
			if err != nil {
				return nil, err
			}

			currentIDs := s.valueToRecordIDs(currentValue)

			// 检查是否包含当前记录
			if !sliceContainsString(change.ExpectedValue, change.RecordID) &&
				sliceContainsString(currentIDs, change.RecordID) {
				// 冲突：对称字段中已经有了这个引用，但不在预期中
				conflicts = append(conflicts, Conflict{
					RecordID:      linkedRecordID,
					FieldID:       symmetricFieldID,
					ExpectedValue: change.ExpectedValue,
					ActualValue:   currentIDs,
				})
			}
		}
	}

	return conflicts, nil
}

// ResolveConflicts 解决冲突
func (s *SymmetricFieldService) ResolveConflicts(
	ctx context.Context,
	conflicts []Conflict,
	strategy ConflictResolutionStrategy,
) error {
	for _, conflict := range conflicts {
		switch strategy {
		case StrategyLastWriteWins:
			// 最后写入胜出，不做任何处理
			continue

		case StrategyMerge:
			// 合并两边的值
			expectedIDs := s.valueToRecordIDs(conflict.ExpectedValue)
			actualIDs := s.valueToRecordIDs(conflict.ActualValue)
			mergedIDs := unique(append(expectedIDs, actualIDs...))

			// 更新为合并后的值
			update := RecordUpdate{
				TableID:  "", // 需要从上下文获取
				RecordID: conflict.RecordID,
				FieldID:  conflict.FieldID,
				NewValue: mergedIDs,
			}
			if err := s.batchService.BatchUpdateRecords(ctx, []RecordUpdate{update}); err != nil {
				return err
			}

		case StrategyAbort:
			// 中止操作，报错
			return fmt.Errorf("conflict detected for record %s field %s: expected %v, got %v",
				conflict.RecordID, conflict.FieldID,
				conflict.ExpectedValue, conflict.ActualValue)
		}
	}

	return nil
}

// valueToRecordIDs 将字段值转换为记录ID数组
func (s *SymmetricFieldService) valueToRecordIDs(value interface{}) []string {
	if value == nil {
		return []string{}
	}

	switch v := value.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		ids := make([]string, 0, len(v))
		for _, item := range v {
			if id, ok := item.(string); ok {
				ids = append(ids, id)
			} else if m, ok := item.(map[string]interface{}); ok {
				if id, ok := m["id"].(string); ok {
					ids = append(ids, id)
				}
			}
		}
		return ids
	default:
		return []string{}
	}
}

// 工具函数

// difference 计算数组差集 a - b
func difference(a, b []string) []string {
	mb := make(map[string]bool, len(b))
	for _, x := range b {
		mb[x] = true
	}

	var diff []string
	for _, x := range a {
		if !mb[x] {
			diff = append(diff, x)
		}
	}
	return diff
}

// contains 检查数组是否包含元素
func sliceContainsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// remove 从数组中移除元素
func remove(slice []string, item string) []string {
	var result []string
	for _, s := range slice {
		if s != item {
			result = append(result, s)
		}
	}
	return result
}

// unique 去重
func unique(slice []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}
