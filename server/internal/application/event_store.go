package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/events"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
	"gorm.io/gorm"
)

// EventStore 事件存储实现
// 负责事件的持久化存储和查询
type EventStore struct {
	db     *gorm.DB
	config *EventStoreConfig
}

// EventStoreConfig 事件存储配置
type EventStoreConfig struct {
	// 表名配置
	EventsTableName string

	// 批量操作配置
	BatchSize     int
	FlushInterval time.Duration

	// 清理配置
	RetentionDays  int
	CleanupEnabled bool
}

// DefaultEventStoreConfig 默认配置
func DefaultEventStoreConfig() *EventStoreConfig {
	return &EventStoreConfig{
		EventsTableName: "domain_events",
		BatchSize:       100,
		FlushInterval:   1 * time.Second,
		RetentionDays:   30,
		CleanupEnabled:  true,
	}
}

// EventRecord 事件记录（数据库模型）
type EventRecord struct {
	ID            uint      `gorm:"primaryKey"`
	EventID       string    `gorm:"uniqueIndex;not null"`
	EventType     string    `gorm:"index;not null"`
	AggregateID   string    `gorm:"index;not null"`
	AggregateType string    `gorm:"index;not null"`
	Version       int64     `gorm:"not null"`
	Data          string    `gorm:"type:text;not null"`
	Metadata      string    `gorm:"type:text"`
	OccurredAt    time.Time `gorm:"index;not null"`
	CreatedAt     time.Time `gorm:"autoCreateTime"`
}

// TableName 指定表名
func (EventRecord) TableName() string {
	return "domain_events"
}

// NewEventStore 创建事件存储
func NewEventStore(db *gorm.DB, config *EventStoreConfig) *EventStore {
	if config == nil {
		config = DefaultEventStoreConfig()
	}

	store := &EventStore{
		db:     db,
		config: config,
	}

	// 自动迁移表结构
	if err := store.migrate(); err != nil {
		logger.Error("事件存储表迁移失败", logger.ErrorField(err))
	}

	return store
}

// migrate 迁移表结构
func (es *EventStore) migrate() error {
	return es.db.AutoMigrate(&EventRecord{})
}

// Save 保存单个事件
func (es *EventStore) Save(ctx context.Context, event events.DomainEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	record, err := es.convertToRecord(event)
	if err != nil {
		return fmt.Errorf("convert event to record failed: %w", err)
	}

	if err := es.db.WithContext(ctx).Create(record).Error; err != nil {
		return fmt.Errorf("save event failed: %w", err)
	}

	logger.Debug("事件已保存",
		logger.String("event_id", event.EventID()),
		logger.String("event_type", event.EventType()),
		logger.String("aggregate_id", event.AggregateID()))

	return nil
}

// SaveBatch 批量保存事件
func (es *EventStore) SaveBatch(ctx context.Context, eventList []events.DomainEvent) error {
	if len(eventList) == 0 {
		return nil
	}

	records := make([]*EventRecord, 0, len(eventList))
	for _, event := range eventList {
		record, err := es.convertToRecord(event)
		if err != nil {
			return fmt.Errorf("convert event to record failed: %w", err)
		}
		records = append(records, record)
	}

	// 批量插入
	if err := es.db.WithContext(ctx).CreateInBatches(records, es.config.BatchSize).Error; err != nil {
		return fmt.Errorf("batch save events failed: %w", err)
	}

	logger.Debug("批量事件已保存",
		logger.Int("event_count", len(eventList)))

	return nil
}

// GetEvents 获取聚合根的所有事件
func (es *EventStore) GetEvents(ctx context.Context, aggregateID string, fromVersion int64) ([]events.DomainEvent, error) {
	var records []EventRecord

	query := es.db.WithContext(ctx).Where("aggregate_id = ?", aggregateID)
	if fromVersion > 0 {
		query = query.Where("version > ?", fromVersion)
	}

	if err := query.Order("version ASC").Find(&records).Error; err != nil {
		return nil, fmt.Errorf("get events failed: %w", err)
	}

	events := make([]events.DomainEvent, 0, len(records))
	for _, record := range records {
		event, err := es.convertToEvent(&record)
		if err != nil {
			logger.Warn("转换事件记录失败",
				logger.String("event_id", record.EventID),
				logger.ErrorField(err))
			continue
		}
		events = append(events, event)
	}

	return events, nil
}

// GetEventsByType 根据类型获取事件
func (es *EventStore) GetEventsByType(ctx context.Context, eventType string, limit int) ([]events.DomainEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	var records []EventRecord
	if err := es.db.WithContext(ctx).
		Where("event_type = ?", eventType).
		Order("occurred_at DESC").
		Limit(limit).
		Find(&records).Error; err != nil {
		return nil, fmt.Errorf("get events by type failed: %w", err)
	}

	events := make([]events.DomainEvent, 0, len(records))
	for _, record := range records {
		event, err := es.convertToEvent(&record)
		if err != nil {
			logger.Warn("转换事件记录失败",
				logger.String("event_id", record.EventID),
				logger.ErrorField(err))
			continue
		}
		events = append(events, event)
	}

	return events, nil
}

// GetEventsByTimeRange 根据时间范围获取事件
func (es *EventStore) GetEventsByTimeRange(ctx context.Context, startTime, endTime time.Time, limit int) ([]events.DomainEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	var records []EventRecord
	if err := es.db.WithContext(ctx).
		Where("occurred_at BETWEEN ? AND ?", startTime, endTime).
		Order("occurred_at DESC").
		Limit(limit).
		Find(&records).Error; err != nil {
		return nil, fmt.Errorf("get events by time range failed: %w", err)
	}

	events := make([]events.DomainEvent, 0, len(records))
	for _, record := range records {
		event, err := es.convertToEvent(&record)
		if err != nil {
			logger.Warn("转换事件记录失败",
				logger.String("event_id", record.EventID),
				logger.ErrorField(err))
			continue
		}
		events = append(events, event)
	}

	return events, nil
}

// CleanupOldEvents 清理过期事件
func (es *EventStore) CleanupOldEvents(ctx context.Context) error {
	if !es.config.CleanupEnabled {
		return nil
	}

	cutoffTime := time.Now().AddDate(0, 0, -es.config.RetentionDays)

	result := es.db.WithContext(ctx).
		Where("occurred_at < ?", cutoffTime).
		Delete(&EventRecord{})

	if result.Error != nil {
		return fmt.Errorf("cleanup old events failed: %w", result.Error)
	}

	if result.RowsAffected > 0 {
		logger.Info("清理过期事件完成",
			logger.Int64("deleted_count", result.RowsAffected),
			logger.String("cutoff_time", cutoffTime.Format(time.RFC3339)))
	}

	return nil
}

// GetStats 获取事件存储统计信息
func (es *EventStore) GetStats(ctx context.Context) (map[string]interface{}, error) {
	var totalCount int64
	var typeCounts []struct {
		EventType string `json:"event_type"`
		Count     int64  `json:"count"`
	}

	// 总事件数
	if err := es.db.WithContext(ctx).Model(&EventRecord{}).Count(&totalCount).Error; err != nil {
		return nil, fmt.Errorf("get total count failed: %w", err)
	}

	// 按类型统计
	if err := es.db.WithContext(ctx).
		Model(&EventRecord{}).
		Select("event_type, COUNT(*) as count").
		Group("event_type").
		Find(&typeCounts).Error; err != nil {
		return nil, fmt.Errorf("get type counts failed: %w", err)
	}

	stats := map[string]interface{}{
		"total_events": totalCount,
		"event_types":  typeCounts,
		"config": map[string]interface{}{
			"table_name":      es.config.EventsTableName,
			"batch_size":      es.config.BatchSize,
			"retention_days":  es.config.RetentionDays,
			"cleanup_enabled": es.config.CleanupEnabled,
		},
	}

	return stats, nil
}

// convertToRecord 将领域事件转换为数据库记录
func (es *EventStore) convertToRecord(event events.DomainEvent) (*EventRecord, error) {
	// 序列化事件数据
	dataBytes, err := json.Marshal(event.Data())
	if err != nil {
		return nil, fmt.Errorf("marshal event data failed: %w", err)
	}

	// 序列化元数据
	var metadataBytes []byte
	if event.Metadata() != nil {
		metadataBytes, err = json.Marshal(event.Metadata())
		if err != nil {
			return nil, fmt.Errorf("marshal event metadata failed: %w", err)
		}
	}

	return &EventRecord{
		EventID:       event.EventID(),
		EventType:     event.EventType(),
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		Version:       event.Version(),
		Data:          string(dataBytes),
		Metadata:      string(metadataBytes),
		OccurredAt:    event.OccurredAt(),
	}, nil
}

// convertToEvent 将数据库记录转换为领域事件
func (es *EventStore) convertToEvent(record *EventRecord) (events.DomainEvent, error) {
	// 反序列化事件数据
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(record.Data), &data); err != nil {
		return nil, fmt.Errorf("unmarshal event data failed: %w", err)
	}

	// 反序列化元数据
	var metadata map[string]interface{}
	if record.Metadata != "" {
		if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
			return nil, fmt.Errorf("unmarshal event metadata failed: %w", err)
		}
	}

	// 创建基础领域事件
	event := &BaseDomainEvent{
		eventID:       record.EventID,
		eventType:     record.EventType,
		aggregateID:   record.AggregateID,
		aggregateType: record.AggregateType,
		version:       record.Version,
		data:          data,
		metadata:      metadata,
		occurredAt:    record.OccurredAt,
	}

	return event, nil
}

// BaseDomainEvent 基础领域事件实现
type BaseDomainEvent struct {
	eventID       string
	eventType     string
	aggregateID   string
	aggregateType string
	version       int64
	data          map[string]interface{}
	metadata      map[string]interface{}
	occurredAt    time.Time
}

func (e *BaseDomainEvent) EventID() string                  { return e.eventID }
func (e *BaseDomainEvent) EventType() string                { return e.eventType }
func (e *BaseDomainEvent) AggregateID() string              { return e.aggregateID }
func (e *BaseDomainEvent) AggregateType() string            { return e.aggregateType }
func (e *BaseDomainEvent) Version() int64                   { return e.version }
func (e *BaseDomainEvent) Data() map[string]interface{}     { return e.data }
func (e *BaseDomainEvent) Metadata() map[string]interface{} { return e.metadata }
func (e *BaseDomainEvent) OccurredAt() time.Time            { return e.occurredAt }
