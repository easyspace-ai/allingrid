package service

import (
	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
)

// CalculationContext 计算上下文
// 包含计算所需的所有数据和状态
type CalculationContext struct {
	// 表ID
	TableID string

	// 起始字段ID列表（触发计算的字段）
	StartFieldIDs []string

	// 起始记录ID映射：fieldID -> recordIDs
	// 表示哪些记录的哪些字段发生了变化
	StartRecordIDs map[string][]string

	// 依赖图
	DependencyGraph []dependency.GraphItem

	// 拓扑排序结果
	TopologicalOrder []dependency.TopoItem

	// 字段映射：fieldID -> Field
	FieldMap map[string]*entity.Field

	// 记录数据缓存：recordID -> fieldData
	// fieldData 是 map[fieldID]value
	RecordDataCache map[string]map[string]interface{}

	// 表ID到数据库表名的映射
	TableID2DBTableName map[string]string

	// 字段ID到表ID的映射
	FieldID2TableID map[string]string

	// 字段ID到数据库字段名的映射
	FieldID2DBFieldName map[string]string

	// 数据库表名到字段列表的映射
	DBTableName2Fields map[string][]*entity.Field

	// 用户信息映射（用于计算CreatedBy/ModifiedBy字段）
	UserMap map[string]interface{}

	// 跨表记录映射（用于Lookup/Rollup计算）
	ForeignKeyRecordMap map[string]map[string]interface{}
}

// NewCalculationContext 创建计算上下文
func NewCalculationContext(tableID string) *CalculationContext {
	return &CalculationContext{
		TableID:             tableID,
		StartFieldIDs:       []string{},
		StartRecordIDs:      make(map[string][]string),
		DependencyGraph:     []dependency.GraphItem{},
		TopologicalOrder:    []dependency.TopoItem{},
		FieldMap:            make(map[string]*entity.Field),
		RecordDataCache:     make(map[string]map[string]interface{}),
		TableID2DBTableName: make(map[string]string),
		FieldID2TableID:     make(map[string]string),
		FieldID2DBFieldName: make(map[string]string),
		DBTableName2Fields:  make(map[string][]*entity.Field),
		UserMap:             make(map[string]interface{}),
		ForeignKeyRecordMap: make(map[string]map[string]interface{}),
	}
}

// AddStartField 添加起始字段
func (ctx *CalculationContext) AddStartField(fieldID string, recordIDs []string) {
	ctx.StartFieldIDs = append(ctx.StartFieldIDs, fieldID)
	ctx.StartRecordIDs[fieldID] = recordIDs
}

// GetRecordData 获取记录数据
func (ctx *CalculationContext) GetRecordData(recordID string) (map[string]interface{}, bool) {
	data, ok := ctx.RecordDataCache[recordID]
	return data, ok
}

// SetRecordData 设置记录数据
func (ctx *CalculationContext) SetRecordData(recordID string, data map[string]interface{}) {
	ctx.RecordDataCache[recordID] = data
}

// GetField 获取字段
func (ctx *CalculationContext) GetField(fieldID string) (*entity.Field, bool) {
	field, ok := ctx.FieldMap[fieldID]
	return field, ok
}

// GetDBTableName 获取表的数据库表名
func (ctx *CalculationContext) GetDBTableName(tableID string) (string, bool) {
	dbTableName, ok := ctx.TableID2DBTableName[tableID]
	return dbTableName, ok
}

// GetFieldTableID 获取字段所属的表ID
func (ctx *CalculationContext) GetFieldTableID(fieldID string) (string, bool) {
	tableID, ok := ctx.FieldID2TableID[fieldID]
	return tableID, ok
}

// CellChange 单元格变更
// 用于记录字段值的变化
type CellChange struct {
	TableID  string
	RecordID string
	FieldID  string
	OldValue interface{}
	NewValue interface{}
}

// OpsMap 操作映射
// tableID -> recordID -> fieldID -> value
type OpsMap map[string]map[string]map[string]interface{}

// NewOpsMap 创建操作映射
func NewOpsMap() OpsMap {
	return make(OpsMap)
}

// Set 设置操作
func (om OpsMap) Set(tableID, recordID, fieldID string, value interface{}) {
	if om[tableID] == nil {
		om[tableID] = make(map[string]map[string]interface{})
	}
	if om[tableID][recordID] == nil {
		om[tableID][recordID] = make(map[string]interface{})
	}
	om[tableID][recordID][fieldID] = value
}

// Get 获取操作值
func (om OpsMap) Get(tableID, recordID, fieldID string) (interface{}, bool) {
	if om[tableID] == nil {
		return nil, false
	}
	if om[tableID][recordID] == nil {
		return nil, false
	}
	value, ok := om[tableID][recordID][fieldID]
	return value, ok
}

// GetRecordOps 获取记录的所有操作
func (om OpsMap) GetRecordOps(tableID, recordID string) (map[string]interface{}, bool) {
	if om[tableID] == nil {
		return nil, false
	}
	ops, ok := om[tableID][recordID]
	return ops, ok
}

// GetTableOps 获取表的所有操作
func (om OpsMap) GetTableOps(tableID string) (map[string]map[string]interface{}, bool) {
	ops, ok := om[tableID]
	return ops, ok
}

// Merge 合并两个操作映射
func (om OpsMap) Merge(other OpsMap) {
	for tableID, records := range other {
		for recordID, fields := range records {
			for fieldID, value := range fields {
				om.Set(tableID, recordID, fieldID, value)
			}
		}
	}
}

// IsEmpty 判断操作映射是否为空
func (om OpsMap) IsEmpty() bool {
	return len(om) == 0
}

// GetRecordIDs 获取所有记录ID
func (om OpsMap) GetRecordIDs(tableID string) []string {
	if om[tableID] == nil {
		return []string{}
	}
	recordIDs := make([]string, 0, len(om[tableID]))
	for recordID := range om[tableID] {
		recordIDs = append(recordIDs, recordID)
	}
	return recordIDs
}

// GetFieldIDs 获取记录的所有字段ID
func (om OpsMap) GetFieldIDs(tableID, recordID string) []string {
	if om[tableID] == nil || om[tableID][recordID] == nil {
		return []string{}
	}
	fieldIDs := make([]string, 0, len(om[tableID][recordID]))
	for fieldID := range om[tableID][recordID] {
		fieldIDs = append(fieldIDs, fieldID)
	}
	return fieldIDs
}
