package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	fieldRepo "github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	pkgerrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"

	"gorm.io/gorm"
)

// RecordHistoryService 记录历史服务
// 参考旧系统: teable-develop/apps/nestjs-backend/src/features/record/record-history.service.ts
type RecordHistoryService struct {
	db        *gorm.DB
	fieldRepo fieldRepo.FieldRepository
}

// NewRecordHistoryService 创建历史记录服务
func NewRecordHistoryService(db *gorm.DB, fieldRepo fieldRepo.FieldRepository) *RecordHistoryService {
	return &RecordHistoryService{
		db:        db,
		fieldRepo: fieldRepo,
	}
}

// RecordUpdate 记录更新后的历史
// 参考旧系统: recordHistory
func (s *RecordHistoryService) RecordUpdate(
	ctx context.Context,
	record *entity.Record,
	changedFieldIDs []string,
	beforeData map[string]interface{},
	afterData map[string]interface{},
	userID string,
) error {
	if len(changedFieldIDs) == 0 {
		return nil
	}

	logger.Info("记录历史变更",
		logger.String("record_id", record.ID().String()),
		logger.String("table_id", record.TableID()),
		logger.Int("changed_fields", len(changedFieldIDs)))

	// 获取字段信息
	fieldMap := make(map[string]*fieldEntity.Field)
	for _, fieldID := range changedFieldIDs {
		fieldIDVO := valueobject.NewFieldID(fieldID)
		field, err := s.fieldRepo.FindByID(ctx, fieldIDVO)
		if err != nil || field == nil {
			continue
		}
		fieldMap[fieldID] = field
	}

	// 为每个变更的字段创建历史记录
	histories := make([]models.RecordHistory, 0, len(changedFieldIDs))

	for _, fieldID := range changedFieldIDs {
		field, exists := fieldMap[fieldID]
		if !exists {
			continue
		}

		// 构建 before 和 after 状态
		before := s.buildFieldState(field, beforeData[fieldID])
		after := s.buildFieldState(field, afterData[fieldID])

		history := models.RecordHistory{
			ID:          fmt.Sprintf("his_%d", time.Now().UnixNano()),
			TableID:     record.TableID(),
			RecordID:    record.ID().String(),
			FieldID:     fieldID,
			Before:      before,
			After:       after,
			CreatedTime: time.Now(),
			CreatedBy:   userID,
		}

		histories = append(histories, history)
	}

	// 批量插入历史记录
	if len(histories) > 0 {
		if err := s.db.WithContext(ctx).Create(&histories).Error; err != nil {
			logger.Error("保存历史记录失败", logger.ErrorField(err))
			return fmt.Errorf("保存历史记录失败: %w", err)
		}

		logger.Info("✅ 历史记录保存成功",
			logger.Int("count", len(histories)))
	}

	return nil
}

// buildFieldState 构建字段状态
func (s *RecordHistoryService) buildFieldState(field *fieldEntity.Field, value interface{}) *models.RecordHistoryState {
	if value == nil {
		return nil
	}

	// 最小化 field options（参考旧系统）
	var options interface{}
	if field.Options() != nil {
		// TODO: 实现 minimizeFieldOptions
		options = field.Options()
	}

	return &models.RecordHistoryState{
		Meta: models.FieldMeta{
			Type:          field.Type().String(),
			Name:          field.Name().String(),
			CellValueType: s.getCellValueType(field),
			Options:       options,
		},
		Data: value,
	}
}

// getCellValueType 获取单元格值类型
func (s *RecordHistoryService) getCellValueType(field *fieldEntity.Field) string {
	// TODO: 根据字段类型返回 cellValueType
	return "string"
}

// GetRecordHistory 获取记录历史
func (s *RecordHistoryService) GetRecordHistory(
	ctx context.Context,
	tableID string,
	recordID string,
) ([]dto.RecordHistoryResponse, error) {
	var histories []models.RecordHistory

	err := s.db.WithContext(ctx).
		Where("table_id = ? AND record_id = ?", tableID, recordID).
		Order("created_time DESC").
		Find(&histories).Error

	if err != nil {
		return nil, pkgerrors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	// 转换为 DTO
	responses := make([]dto.RecordHistoryResponse, len(histories))
	for i, h := range histories {
		responses[i] = dto.RecordHistoryResponse{
			ID:          h.ID,
			TableID:     h.TableID,
			RecordID:    h.RecordID,
			FieldID:     h.FieldID,
			Before:      s.stateToJSON(h.Before),
			After:       s.stateToJSON(h.After),
			CreatedTime: h.CreatedTime,
			CreatedBy:   h.CreatedBy,
		}
	}

	return responses, nil
}

// stateToJSON 将状态转换为JSON
func (s *RecordHistoryService) stateToJSON(state *models.RecordHistoryState) interface{} {
	if state == nil {
		return nil
	}

	data, _ := json.Marshal(state)
	var result interface{}
	json.Unmarshal(data, &result)
	return result
}
