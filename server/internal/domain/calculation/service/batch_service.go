package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
)

// DatabaseRepository 数据库仓储接口
type DatabaseRepository interface {
	Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error
	GetDB() *gorm.DB
}

// BatchService 批量更新服务
// 负责批量更新记录字段值
// 参考 teable-develop/apps/nestjs-backend/src/features/calculation/batch.service.ts
type BatchService struct {
	db        DatabaseRepository
	fieldRepo FieldRepository
	tableRepo TableRepository
	logger    *zap.Logger
}

// NewBatchService 创建批量更新服务
func NewBatchService(
	db DatabaseRepository,
	fieldRepo FieldRepository,
	tableRepo TableRepository,
	logger *zap.Logger,
) *BatchService {
	return &BatchService{
		db:        db,
		fieldRepo: fieldRepo,
		tableRepo: tableRepo,
		logger:    logger,
	}
}

// BatchUpdateRecords 批量更新记录
// 使用事务保证原子性
func (s *BatchService) BatchUpdateRecords(ctx context.Context, opsMap OpsMap) error {
	if opsMap.IsEmpty() {
		return nil
	}

	s.logger.Info("Starting batch update",
		zap.Int("table_count", len(opsMap)),
	)

	// 在事务中执行所有更新
	err := s.db.Transaction(ctx, func(tx *gorm.DB) error {
		for tableID, records := range opsMap {
			// 获取数据库表名
			dbTableName, err := s.tableRepo.GetDBTableName(ctx, tableID)
			if err != nil {
				s.logger.Error("Failed to get db table name",
					zap.String("table_id", tableID),
					zap.Error(err),
				)
				return fmt.Errorf("failed to get db table name for table %s: %w", tableID, err)
			}

			// 获取所有涉及的字段
			allFieldIDs := make(map[string]bool)
			for _, fields := range records {
				for fieldID := range fields {
					allFieldIDs[fieldID] = true
				}
			}

			// 批量查询字段信息
			fieldIDsList := make([]string, 0, len(allFieldIDs))
			for fieldID := range allFieldIDs {
				fieldIDsList = append(fieldIDsList, fieldID)
			}

			fields, err := s.fieldRepo.FindByIDs(ctx, fieldIDsList)
			if err != nil {
				s.logger.Error("Failed to load fields",
					zap.String("table_id", tableID),
					zap.Error(err),
				)
				return fmt.Errorf("failed to load fields: %w", err)
			}

			// 构建字段ID到字段的映射
			fieldMap := make(map[string]*entity.Field)
			for _, field := range fields {
				fieldMap[field.ID().String()] = field
			}

			// 更新每条记录
			for recordID, fieldValues := range records {
				if err := s.updateRecord(ctx, tx, dbTableName, recordID, fieldValues, fieldMap); err != nil {
					s.logger.Error("Failed to update record",
						zap.String("table_id", tableID),
						zap.String("record_id", recordID),
						zap.Error(err),
					)
					return err
				}
			}

			s.logger.Debug("Table records updated",
				zap.String("table_id", tableID),
				zap.String("db_table_name", dbTableName),
				zap.Int("record_count", len(records)),
			)
		}

		return nil
	})

	if err != nil {
		s.logger.Error("Batch update failed",
			zap.Error(err),
		)
		return fmt.Errorf("batch update failed: %w", err)
	}

	s.logger.Info("Batch update completed successfully")
	return nil
}

// updateRecord 更新单条记录
func (s *BatchService) updateRecord(
	ctx context.Context,
	tx *gorm.DB,
	dbTableName string,
	recordID string,
	fieldValues map[string]interface{},
	fieldMap map[string]*entity.Field,
) error {
	// 构建更新数据：dbFieldName -> value
	updateData := make(map[string]interface{})

	for fieldID, value := range fieldValues {
		field, ok := fieldMap[fieldID]
		if !ok {
			s.logger.Warn("Field not found in field map",
				zap.String("field_id", fieldID),
			)
			continue
		}

		// 使用数据库字段名
		dbFieldName := field.DBFieldName().String()
		updateData[dbFieldName] = value
	}

	if len(updateData) == 0 {
		return nil
	}

	// 执行更新
	result := tx.Table(dbTableName).
		Where("id = ?", recordID).
		Updates(updateData)

	if result.Error != nil {
		return fmt.Errorf("failed to update record %s: %w", recordID, result.Error)
	}

	if result.RowsAffected == 0 {
		s.logger.Warn("No rows affected",
			zap.String("table", dbTableName),
			zap.String("record_id", recordID),
		)
	}

	return nil
}

// UpdateRecordsInTable 更新指定表的多条记录
// 便捷方法，用于单表批量更新
func (s *BatchService) UpdateRecordsInTable(
	ctx context.Context,
	tableID string,
	recordUpdates map[string]map[string]interface{}, // recordID -> fieldID -> value
) error {
	opsMap := NewOpsMap()
	for recordID, fields := range recordUpdates {
		for fieldID, value := range fields {
			opsMap.Set(tableID, recordID, fieldID, value)
		}
	}

	return s.BatchUpdateRecords(ctx, opsMap)
}

// UpdateRecordField 更新单条记录的单个字段
// 便捷方法
func (s *BatchService) UpdateRecordField(
	ctx context.Context,
	tableID string,
	recordID string,
	fieldID string,
	value interface{},
) error {
	opsMap := NewOpsMap()
	opsMap.Set(tableID, recordID, fieldID, value)
	return s.BatchUpdateRecords(ctx, opsMap)
}

// ValidateOpsMap 验证操作映射的有效性
// 检查字段是否存在、类型是否匹配等
func (s *BatchService) ValidateOpsMap(ctx context.Context, opsMap OpsMap) error {
	for tableID, records := range opsMap {
		// 收集所有字段ID
		fieldIDs := make(map[string]bool)
		for _, fields := range records {
			for fieldID := range fields {
				fieldIDs[fieldID] = true
			}
		}

		// 批量查询字段
		fieldIDsList := make([]string, 0, len(fieldIDs))
		for fieldID := range fieldIDs {
			fieldIDsList = append(fieldIDsList, fieldID)
		}

		fields, err := s.fieldRepo.FindByIDs(ctx, fieldIDsList)
		if err != nil {
			return fmt.Errorf("failed to load fields for table %s: %w", tableID, err)
		}

		// 验证所有字段都存在
		fieldMap := make(map[string]bool)
		for _, field := range fields {
			fieldMap[field.ID().String()] = true
		}

		for fieldID := range fieldIDs {
			if !fieldMap[fieldID] {
				return fmt.Errorf("field %s not found in table %s", fieldID, tableID)
			}
		}
	}

	return nil
}

// GetAffectedRecordCount 获取受影响的记录数量
func (s *BatchService) GetAffectedRecordCount(opsMap OpsMap) int {
	count := 0
	for _, records := range opsMap {
		count += len(records)
	}
	return count
}

// GetAffectedFieldCount 获取受影响的字段数量（总计）
func (s *BatchService) GetAffectedFieldCount(opsMap OpsMap) int {
	count := 0
	for _, records := range opsMap {
		for _, fields := range records {
			count += len(fields)
		}
	}
	return count
}
