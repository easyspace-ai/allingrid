package service

import (
	"context"

	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
)

// RecordRepositoryAdapter 适配器，将主仓储接口适配到计算服务的接口
type RecordRepositoryAdapter struct {
	recordRepo recordRepo.RecordRepository
}

// NewRecordRepositoryAdapter 创建记录仓储适配器
func NewRecordRepositoryAdapter(recordRepo recordRepo.RecordRepository) *RecordRepositoryAdapter {
	return &RecordRepositoryAdapter{
		recordRepo: recordRepo,
	}
}

// FindByID 根据表ID和记录ID查找记录，返回map格式
func (a *RecordRepositoryAdapter) FindByID(ctx context.Context, tableID, recordID string) (map[string]interface{}, error) {
	recordIDObj := valueobject.NewRecordID(recordID)
	record, err := a.recordRepo.FindByTableAndID(ctx, tableID, recordIDObj)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}

	// 转换为map格式
	result := make(map[string]interface{})
	result["id"] = record.ID().String()
	result["table_id"] = record.TableID()
	result["data"] = record.Data().ToMap()
	result["created_by"] = record.CreatedBy()
	result["updated_by"] = record.UpdatedBy()
	result["created_at"] = record.CreatedAt()
	result["updated_at"] = record.UpdatedAt()
	result["version"] = record.Version().Value()

	return result, nil
}

// FindByIDs 根据表ID和记录ID列表查找记录，返回map格式列表
func (a *RecordRepositoryAdapter) FindByIDs(ctx context.Context, tableID string, recordIDs []string) ([]map[string]interface{}, error) {
	if len(recordIDs) == 0 {
		return []map[string]interface{}{}, nil
	}

	// 转换字符串ID为RecordID对象
	recordIDObjects := make([]valueobject.RecordID, len(recordIDs))
	for i, id := range recordIDs {
		recordIDObjects[i] = valueobject.NewRecordID(id)
	}

	records, err := a.recordRepo.FindByIDs(ctx, tableID, recordIDObjects)
	if err != nil {
		return nil, err
	}

	// 转换为map格式列表
	result := make([]map[string]interface{}, len(records))
	for i, record := range records {
		result[i] = make(map[string]interface{})
		result[i]["id"] = record.ID().String()
		result[i]["table_id"] = record.TableID()
		result[i]["data"] = record.Data().ToMap()
		result[i]["created_by"] = record.CreatedBy()
		result[i]["updated_by"] = record.UpdatedBy()
		result[i]["created_at"] = record.CreatedAt()
		result[i]["updated_at"] = record.UpdatedAt()
		result[i]["version"] = record.Version().Value()
	}

	return result, nil
}

// UpdateFields 更新记录的字段值
func (a *RecordRepositoryAdapter) UpdateFields(ctx context.Context, tableID, recordID string, fields map[string]interface{}) error {
	recordIDObj := valueobject.NewRecordID(recordID)
	record, err := a.recordRepo.FindByTableAndID(ctx, tableID, recordIDObj)
	if err != nil {
		return err
	}
	if record == nil {
		return nil
	}

	// 更新记录数据
	recordData := record.Data().ToMap()
	for key, value := range fields {
		recordData[key] = value
	}

	// 创建新的记录数据
	newData, err := valueobject.NewRecordData(recordData)
	if err != nil {
		return err
	}

	// 更新记录
	record.Update(newData, "system") // TODO: 从上下文获取实际的更新者

	// 保存记录
	return a.recordRepo.Save(ctx, record)
}
