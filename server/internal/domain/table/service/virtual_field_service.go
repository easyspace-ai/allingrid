package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	tableAggregate "github.com/easyspace-ai/luckdb/server/internal/domain/table/aggregate"
	tableEntity "github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
)

// FieldTypeHandler 字段类型处理器接口
type FieldTypeHandler interface {
	// ComputeValue calculates the field value
	ComputeValue(ctx context.Context, field *fieldEntity.Field, record map[string]interface{}, relatedRecords map[string][]map[string]interface{}) (interface{}, error)
	// GetDependencies returns field dependencies
	GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error)
}

// AIProvider AI提供者接口
type AIProvider interface {
	// AI相关的方法
	GenerateFieldValue(ctx context.Context, prompt string, config interface{}) (interface{}, error)
}

// IsVirtualField 检查字段类型是否为虚拟字段
// VirtualFieldService manages virtual field calculations
type VirtualFieldService struct {
	tableService     TableService
	recordService    RecordService
	fieldHandlers    map[string]FieldTypeHandler // 使用 string 作为 key
	aiProvider       AIProvider
	cache            VirtualFieldCache
	formulaEvaluator FormulaEvaluator // 定义在同一个包中
	mu               sync.RWMutex
}

// TableService 表格服务接口（临时定义，后续可能需要调整）
type TableService interface {
	GetTable(ctx context.Context, tableID string) (*tableEntity.Table, error)
	GetFieldsByTableID(ctx context.Context, tableID string) ([]*fieldEntity.Field, error)
}

// VirtualFieldCache caches computed virtual field values
type VirtualFieldCache interface {
	Get(recordID, fieldID string) (interface{}, bool)
	Set(recordID, fieldID string, value interface{}, ttl time.Duration)
	Delete(recordID, fieldID string)
	DeleteByRecord(recordID string)
	DeleteByField(fieldID string)
}

// RecordService interface for accessing record data
type RecordService interface {
	GetRecord(ctx context.Context, tableID, recordID string) (map[string]interface{}, error)
	GetLinkedRecords(ctx context.Context, tableID, recordID, linkFieldID string) ([]map[string]interface{}, error)
}

// NewVirtualFieldService creates a new virtual field service
func NewVirtualFieldService(
	tableService TableService,
	recordService RecordService,
	aiProvider AIProvider,
	cache VirtualFieldCache,
) *VirtualFieldService {
	service := &VirtualFieldService{
		tableService:  tableService,
		recordService: recordService,
		fieldHandlers: make(map[string]FieldTypeHandler),
		aiProvider:    aiProvider,
		cache:         cache,
	}

	// Create formula evaluator（使用同包中的实现）
	formulaEvaluator := NewDefaultFormulaEvaluator()
	service.formulaEvaluator = formulaEvaluator

	// Register virtual field handlers
	service.registerDefaultHandlers()

	return service
}

// RegisterHandler registers a field type handler
func (s *VirtualFieldService) RegisterHandler(fieldType string, handler FieldTypeHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fieldHandlers[fieldType] = handler
}

// registerDefaultHandlers registers all default virtual field handlers
func (s *VirtualFieldService) registerDefaultHandlers() {
	// Register Formula handler
	s.RegisterHandler("formula", &FormulaHandler{
		evaluator: s.formulaEvaluator,
	})

	// Register Lookup handler
	s.RegisterHandler("lookup", &LookupHandler{
		recordService: s.recordService,
	})

	// Register Rollup handler
	s.RegisterHandler("rollup", &RollupHandler{
		recordService: s.recordService,
		evaluator:     s.formulaEvaluator,
	})

	// Register AI handler if provider is available
	if s.aiProvider != nil {
		s.RegisterHandler("ai", &AIHandler{
			aiProvider: s.aiProvider,
		})
	}

	// Register Count handler
	s.RegisterHandler("count", &CountHandler{
		recordService: s.recordService,
	})
}

// CalculateVirtualFields calculates all virtual fields for a record
func (s *VirtualFieldService) CalculateVirtualFields(
	ctx context.Context,
	tableAgg *tableAggregate.TableAggregate,
	recordData map[string]interface{},
	fields []string, // specific fields to calculate, empty for all
) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Get fields to calculate
	fieldsToCalc := s.getFieldsToCalculate(tableAgg, fields)

	// Calculate each virtual field
	for _, field := range fieldsToCalc {
		if !IsVirtualField(field.Type().String()) {
			continue
		}

		value, err := s.CalculateField(ctx, tableAgg, field, recordData)
		if err != nil {
			// Store error in result but continue with other fields
			result[field.Name().String()] = map[string]interface{}{
				"error": err.Error(),
				"value": nil,
			}
		} else {
			result[field.Name().String()] = value
		}
	}

	return result, nil
}

// CalculateField calculates a single virtual field value
func (s *VirtualFieldService) CalculateField(
	ctx context.Context,
	table *tableAggregate.TableAggregate,
	field *fieldEntity.Field,
	recordData map[string]interface{},
) (interface{}, error) {
	if !IsVirtualField(field.Type().String()) {
		return nil, fmt.Errorf("field %s is not a virtual field", field.Name().String())
	}

	// Check cache first
	recordID, _ := recordData["id"].(string)
	if recordID != "" && s.cache != nil {
		if cachedValue, found := s.cache.Get(recordID, field.ID().String()); found {
			return cachedValue, nil
		}
	}

	// Get handler for field type
	s.mu.RLock()
	handler, exists := s.fieldHandlers[field.Type().String()]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no handler found for field type: %s", field.Type().String())
	}

	// Calculate value using handler
	var value interface{}
	var err error

	// Get related records if this is a relational virtual field
	relatedRecords := make(map[string][]map[string]interface{})
	if needsRelatedRecords(field.Type().String()) {
		relatedRecords, err = s.getRelatedRecords(ctx, table, field, recordData)
		if err != nil {
			return nil, fmt.Errorf("failed to get related records: %w", err)
		}
	}

	// Compute value using handler
	value, err = handler.ComputeValue(ctx, field, recordData, relatedRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to compute field value: %w", err)
	}

	// Cache the result
	if recordID != "" && s.cache != nil && value != nil {
		// Cache for 5 minutes by default
		s.cache.Set(recordID, field.ID().String(), value, 5*time.Minute)
	}

	return value, nil
}

// InvalidateCache invalidates cached values
func (s *VirtualFieldService) InvalidateCache(recordID, fieldID string) {
	if s.cache == nil {
		return
	}

	if recordID != "" && fieldID != "" {
		s.cache.Delete(recordID, fieldID)
	} else if recordID != "" {
		s.cache.DeleteByRecord(recordID)
	} else if fieldID != "" {
		s.cache.DeleteByField(fieldID)
	}
}

// GetFieldDependencies returns the fields that a virtual field depends on
func (s *VirtualFieldService) GetFieldDependencies(field *fieldEntity.Field) ([]string, error) {
	if !IsVirtualField(field.Type().String()) {
		return nil, nil
	}

	// Get handler for field type
	s.mu.RLock()
	handler, exists := s.fieldHandlers[field.Type().String()]
	s.mu.RUnlock()

	if !exists {
		return []string{}, nil
	}

	// Try to get dependencies from handler
	ctx := context.Background()
	dependencies, err := handler.GetDependencies(ctx, field)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependencies: %w", err)
	}

	return dependencies, nil
}

// UpdateDependentFields updates virtual fields that depend on changed fields
func (s *VirtualFieldService) UpdateDependentFields(
	ctx context.Context,
	tableAgg *tableAggregate.TableAggregate,
	recordID string,
	changedFields []string,
) error {
	// Find virtual fields that depend on the changed fields
	dependentFields := s.findDependentFields(tableAgg, changedFields)

	if len(dependentFields) == 0 {
		return nil
	}

	// Get the record data
	recordData, err := s.recordService.GetRecord(ctx, tableAgg.Table().ID().String(), recordID)
	if err != nil {
		return fmt.Errorf("failed to get record: %w", err)
	}

	// Invalidate cache for dependent fields
	for _, field := range dependentFields {
		s.InvalidateCache(recordID, field.ID().String())
	}

	// Recalculate dependent fields
	fieldCodes := make([]string, len(dependentFields))

	_, err = s.CalculateVirtualFields(ctx, tableAgg, recordData, fieldCodes)
	return err
}

// Helper methods

func (s *VirtualFieldService) getFieldsToCalculate(tableAgg *tableAggregate.TableAggregate, requestedFields []string) []*fieldEntity.Field {
	if len(requestedFields) == 0 {
		// Return all virtual fields
		var virtualFields []*fieldEntity.Field
		for _, field := range tableAgg.Fields() {
			if IsVirtualField(field.Type().String()) {
				virtualFields = append(virtualFields, field)
			}
		}
		return virtualFields
	}

	// Return only requested fields that are virtual
	fieldMap := make(map[string]*fieldEntity.Field)
	for _, field := range tableAgg.Fields() {
		fieldMap[field.Name().String()] = field
	}

	var virtualFields []*fieldEntity.Field
	for _, code := range requestedFields {
		if field, exists := fieldMap[code]; exists && IsVirtualField(field.Type().String()) {
			virtualFields = append(virtualFields, field)
		}
	}

	return virtualFields
}

func (s *VirtualFieldService) findDependentFields(tableAgg *tableAggregate.TableAggregate, changedFields []string) []*fieldEntity.Field {
	changedSet := make(map[string]bool)
	for _, field := range changedFields {
		changedSet[field] = true
	}

	var dependentFields []*fieldEntity.Field

	for _, field := range tableAgg.Fields() {
		if !IsVirtualField(field.Type().String()) {
			continue
		}

		// Get dependencies for this virtual field
		deps, err := s.GetFieldDependencies(field)
		if err != nil {
			continue
		}

		// Check if any dependency was changed
		for _, dep := range deps {
			if changedSet[dep] {
				dependentFields = append(dependentFields, field)
				break
			}
		}
	}

	return dependentFields
}

func getUserIDFromContext(ctx context.Context) string {
	// Extract user ID from context using auth context
	if userID, ok := ctx.Value("user_id").(string); ok {
		return userID
	}
	// Try alternative key
	if userID, ok := ctx.Value("userID").(string); ok {
		return userID
	}
	return ""
}

// InMemoryVirtualFieldCache is a simple in-memory cache implementation
type InMemoryVirtualFieldCache struct {
	data map[string]map[string]cacheEntry
	mu   sync.RWMutex
}

type cacheEntry struct {
	value     interface{}
	expiresAt time.Time
}

// NewInMemoryVirtualFieldCache creates a new in-memory cache
func NewInMemoryVirtualFieldCache() *InMemoryVirtualFieldCache {
	cache := &InMemoryVirtualFieldCache{
		data: make(map[string]map[string]cacheEntry),
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

func (c *InMemoryVirtualFieldCache) Get(recordID, fieldID string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if recordCache, exists := c.data[recordID]; exists {
		if entry, exists := recordCache[fieldID]; exists {
			if time.Now().Before(entry.expiresAt) {
				return entry.value, true
			}
		}
	}

	return nil, false
}

func (c *InMemoryVirtualFieldCache) Set(recordID, fieldID string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.data[recordID]; !exists {
		c.data[recordID] = make(map[string]cacheEntry)
	}

	c.data[recordID][fieldID] = cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

func (c *InMemoryVirtualFieldCache) Delete(recordID, fieldID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if recordCache, exists := c.data[recordID]; exists {
		delete(recordCache, fieldID)
		if len(recordCache) == 0 {
			delete(c.data, recordID)
		}
	}
}

func (c *InMemoryVirtualFieldCache) DeleteByRecord(recordID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, recordID)
}

func (c *InMemoryVirtualFieldCache) DeleteByField(fieldID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for recordID, recordCache := range c.data {
		delete(recordCache, fieldID)
		if len(recordCache) == 0 {
			delete(c.data, recordID)
		}
	}
}

func (c *InMemoryVirtualFieldCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()

		for recordID, recordCache := range c.data {
			for fieldID, entry := range recordCache {
				if now.After(entry.expiresAt) {
					delete(recordCache, fieldID)
				}
			}
			if len(recordCache) == 0 {
				delete(c.data, recordID)
			}
		}

		c.mu.Unlock()
	}
}

// needsRelatedRecords checks if a field type needs related records for calculation
func needsRelatedRecords(fieldType string) bool {
	relationalTypes := map[string]bool{
		"lookup": true,
		"rollup": true,
		"count":  true,
	}
	return relationalTypes[fieldType]
}

// getRelatedRecords retrieves related records for relational virtual fields
func (s *VirtualFieldService) getRelatedRecords(
	ctx context.Context,
	table *tableAggregate.TableAggregate,
	field *fieldEntity.Field,
	recordData map[string]interface{},
) (map[string][]map[string]interface{}, error) {
	result := make(map[string][]map[string]interface{})

	// Get link field ID from field options
	var linkFieldID string
	if field.Options() != nil {
		if field.Options().Lookup != nil {
			linkFieldID = field.Options().Lookup.LinkFieldID
		} else if field.Options().Rollup != nil {
			linkFieldID = field.Options().Rollup.LinkFieldID
		}
	}

	if linkFieldID == "" {
		return result, nil
	}

	// Get linked records
	recordID, _ := recordData["id"].(string)
	if recordID == "" {
		return result, nil
	}

	linkedRecords, err := s.recordService.GetLinkedRecords(
		ctx,
		table.Table().ID().String(),
		recordID,
		linkFieldID,
	)
	if err != nil {
		return nil, err
	}

	result[linkFieldID] = linkedRecords
	return result, nil
}

// Virtual field handler implementations

// VirtualFieldHandler interface for handlers
type VirtualFieldHandler interface {
	ComputeValue(ctx context.Context, field *fieldEntity.Field, record map[string]interface{}, relatedRecords map[string][]map[string]interface{}) (interface{}, error)
	GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error)
}

// FormulaHandler handles formula field calculations
type FormulaHandler struct {
	evaluator FormulaEvaluator
}

func (h *FormulaHandler) ComputeValue(
	ctx context.Context,
	field *fieldEntity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	if field.Options() == nil || field.Options().Formula == nil {
		return nil, fmt.Errorf("formula field missing expression")
	}

	expression := field.Options().Formula.Expression
	return h.evaluator.Evaluate(expression, make(FieldInstanceMap), record)
}

func (h *FormulaHandler) GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error) {
	if field.Options() == nil || field.Options().Formula == nil {
		return []string{}, nil
	}

	expression := field.Options().Formula.Expression
	// Parse formula to extract field references
	// This is a simplified implementation
	return parseFormulaReferences(expression), nil
}

// LookupHandler handles lookup field calculations
type LookupHandler struct {
	recordService RecordService
}

func (h *LookupHandler) ComputeValue(
	ctx context.Context,
	field *fieldEntity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	if field.Options() == nil || field.Options().Lookup == nil {
		return nil, fmt.Errorf("lookup field missing configuration")
	}

	lookupOpts := field.Options().Lookup
	linkedRecords, exists := relatedRecords[lookupOpts.LinkFieldID]
	if !exists || len(linkedRecords) == 0 {
		return nil, nil
	}

	// Extract values from linked records
	values := make([]interface{}, 0, len(linkedRecords))
	for _, linkedRecord := range linkedRecords {
		if val, ok := linkedRecord[lookupOpts.LookupFieldID]; ok && val != nil {
			values = append(values, val)
		}
	}

	if len(values) == 0 {
		return nil, nil
	}

	// Return single value or array
	if len(values) == 1 {
		return values[0], nil
	}
	return values, nil
}

func (h *LookupHandler) GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error) {
	if field.Options() == nil || field.Options().Lookup == nil {
		return []string{}, nil
	}

	lookupOpts := field.Options().Lookup
	return []string{lookupOpts.LinkFieldID, lookupOpts.LookupFieldID}, nil
}

// RollupHandler handles rollup field calculations
type RollupHandler struct {
	recordService RecordService
	evaluator     FormulaEvaluator
}

func (h *RollupHandler) ComputeValue(
	ctx context.Context,
	field *fieldEntity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	if field.Options() == nil || field.Options().Rollup == nil {
		return nil, fmt.Errorf("rollup field missing configuration")
	}

	rollupOpts := field.Options().Rollup
	linkedRecords, exists := relatedRecords[rollupOpts.LinkFieldID]
	if !exists || len(linkedRecords) == 0 {
		return getEmptyRollupValue(rollupOpts.AggregationFunction), nil
	}

	// Extract values
	values := make([]interface{}, 0, len(linkedRecords))
	for _, linkedRecord := range linkedRecords {
		if val, ok := linkedRecord[rollupOpts.RollupFieldID]; ok {
			values = append(values, val)
		}
	}

	// Perform aggregation
	return aggregateValues(rollupOpts.AggregationFunction, values), nil
}

func (h *RollupHandler) GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error) {
	if field.Options() == nil || field.Options().Rollup == nil {
		return []string{}, nil
	}

	rollupOpts := field.Options().Rollup
	return []string{rollupOpts.LinkFieldID, rollupOpts.RollupFieldID}, nil
}

// AIHandler handles AI field calculations
type AIHandler struct {
	aiProvider AIProvider
}

func (h *AIHandler) ComputeValue(
	ctx context.Context,
	field *fieldEntity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	if field.Options() == nil || field.Options().AI == nil {
		return nil, fmt.Errorf("AI field missing configuration")
	}

	aiConfig := field.Options().AI
	prompt := buildAIPrompt(aiConfig, record)

	return h.aiProvider.GenerateFieldValue(ctx, prompt, aiConfig)
}

func (h *AIHandler) GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error) {
	// AI fields can depend on any referenced fields in their prompts
	// This is a simplified implementation
	return []string{}, nil
}

// CountHandler handles count field calculations
type CountHandler struct {
	recordService RecordService
}

func (h *CountHandler) ComputeValue(
	ctx context.Context,
	field *fieldEntity.Field,
	record map[string]interface{},
	relatedRecords map[string][]map[string]interface{},
) (interface{}, error) {
	// Count fields typically count linked records
	// For now, return a simple implementation
	// You can extend this based on your Count field configuration

	// If you have a link field, count those records
	linkedRecordsCount := 0
	for _, records := range relatedRecords {
		linkedRecordsCount += len(records)
	}

	return linkedRecordsCount, nil
}

func (h *CountHandler) GetDependencies(ctx context.Context, field *fieldEntity.Field) ([]string, error) {
	// Count fields typically depend on link fields
	// This is a simplified implementation
	return []string{}, nil
}

// Helper functions

func parseFormulaReferences(expression string) []string {
	// Simplified implementation - extract field references from formula
	// In real implementation, should use proper parser
	return []string{}
}

func getEmptyRollupValue(function string) interface{} {
	switch function {
	case "count", "countall", "counta":
		return 0
	case "sum", "average", "avg":
		return 0.0
	case "min", "max":
		return nil
	case "and":
		return true
	case "or", "xor":
		return false
	case "array_join", "concatenate":
		return ""
	case "array_unique", "array_compact":
		return []interface{}{}
	default:
		return nil
	}
}

func aggregateValues(function string, values []interface{}) interface{} {
	if len(values) == 0 {
		return getEmptyRollupValue(function)
	}

	switch function {
	case "count", "countall":
		return len(values)
	case "counta":
		count := 0
		for _, v := range values {
			if v != nil {
				count++
			}
		}
		return count
	case "sum":
		sum := 0.0
		for _, v := range values {
			if num, ok := toFloat64(v); ok {
				sum += num
			}
		}
		return sum
	case "average", "avg":
		sum := 0.0
		count := 0
		for _, v := range values {
			if num, ok := toFloat64(v); ok {
				sum += num
				count++
			}
		}
		if count == 0 {
			return 0.0
		}
		return sum / float64(count)
	case "min":
		var min *float64
		for _, v := range values {
			if num, ok := toFloat64(v); ok {
				if min == nil || num < *min {
					min = &num
				}
			}
		}
		if min == nil {
			return nil
		}
		return *min
	case "max":
		var max *float64
		for _, v := range values {
			if num, ok := toFloat64(v); ok {
				if max == nil || num > *max {
					max = &num
				}
			}
		}
		if max == nil {
			return nil
		}
		return *max
	default:
		return nil
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	default:
		return 0, false
	}
}

func buildAIPrompt(config interface{}, record map[string]interface{}) string {
	// Simplified implementation
	// You can extend this to properly extract prompt from AI config
	return "Generate value for this record"
}
