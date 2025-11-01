package link

// ForeignKeyRecordMap 外键记录映射
// fieldID -> recordID -> 关联记录数据
type ForeignKeyRecordMap map[string]map[string]interface{}

// RecordItem 记录项
type RecordItem struct {
	RecordID     string
	Dependencies []string // 依赖的记录ID
}

// RelatedRecordItem 关联记录项
type RelatedRecordItem struct {
	ToID   string // 目标记录ID
	FromID string // 源记录ID（可选）
}
