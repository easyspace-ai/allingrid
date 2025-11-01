package mapper

import (
	"encoding/json"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
)

// ToRecordEntity 将数据库模型转换为领域实体
func ToRecordEntity(dbRecord *models.Record) (*entity.Record, error) {
	if dbRecord == nil {
		return nil, nil
	}

	// 解析 JSON 数据
	var dataMap map[string]interface{}
	if dbRecord.Data != "" {
		if err := json.Unmarshal([]byte(dbRecord.Data), &dataMap); err != nil {
			return nil, err
		}
	}

	// 创建 RecordData 值对象
	recordData, err := valueobject.NewRecordData(dataMap)
	if err != nil {
		return nil, err
	}

	// 创建 RecordID
	recordID := valueobject.NewRecordID(dbRecord.ID)

	// 创建 RecordVersion
	version := valueobject.InitialVersion()

	// 处理 UpdatedAt
	updatedAt := dbRecord.CreatedTime
	if dbRecord.LastModifiedTime != nil {
		updatedAt = *dbRecord.LastModifiedTime
	}

	// 处理 DeletedAt
	var deletedAt *time.Time
	if dbRecord.DeletedTime.Valid {
		deletedAt = &dbRecord.DeletedTime.Time
	}

	// 重建实体
	record := entity.ReconstructRecord(
		recordID,
		dbRecord.TableID,
		recordData,
		version,
		dbRecord.CreatedBy,
		dbRecord.CreatedBy, // updatedBy 使用 createdBy
		dbRecord.CreatedTime,
		updatedAt,
		deletedAt,
	)

	return record, nil
}

// ToRecordModel 将领域实体转换为数据库模型
func ToRecordModel(record *entity.Record) (*models.Record, error) {
	if record == nil {
		return nil, nil
	}

	// 序列化数据
	dataJSON, err := json.Marshal(record.Data().ToMap())
	if err != nil {
		return nil, err
	}

	updatedAt := record.UpdatedAt()

	return &models.Record{
		ID:               record.ID().String(),
		TableID:          record.TableID(),
		Data:             string(dataJSON),
		CreatedBy:        record.CreatedBy(),
		CreatedTime:      record.CreatedAt(),
		LastModifiedTime: &updatedAt,
	}, nil
}

// ToRecordList 批量转换
func ToRecordList(dbRecords []*models.Record) ([]*entity.Record, error) {
	records := make([]*entity.Record, 0, len(dbRecords))
	for _, dbRecord := range dbRecords {
		record, err := ToRecordEntity(dbRecord)
		if err != nil {
			return nil, err
		}
		if record != nil {
			records = append(records, record)
		}
	}
	return records, nil
}
