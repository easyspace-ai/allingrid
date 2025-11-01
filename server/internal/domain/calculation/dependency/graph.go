package dependency

// GraphItem 依赖图的边（对齐原版 IGraphItem）
// 表示字段之间的依赖关系：fromFieldId 依赖 toFieldId
type GraphItem struct {
	FromFieldID string `json:"fromFieldId"` // 依赖字段（计算字段）
	ToFieldID   string `json:"toFieldId"`   // 被依赖字段（数据字段）
}

// TopoItem 拓扑排序项（对齐原版 ITopoItem）
// 表示字段及其所有依赖
type TopoItem struct {
	ID           string   `json:"id"`           // 字段ID
	Dependencies []string `json:"dependencies"` // 依赖的字段ID列表
}

// TopoItemWithRecords 带记录的拓扑项（对齐原版）
type TopoItemWithRecords struct {
	TopoItem
	RecordItemMap map[string]*RecordItem `json:"recordItemMap,omitempty"`
}

// RecordItem 记录项（对齐原版）
type RecordItem struct {
	RecordID     string   `json:"recordId"`
	Dependencies []string `json:"dependencies,omitempty"` // 依赖的记录ID
}

// Graph 依赖图
type Graph struct {
	Edges []GraphItem // 边集合
}

// NewGraph 创建依赖图
func NewGraph(edges []GraphItem) *Graph {
	return &Graph{
		Edges: edges,
	}
}

// GetAllNodes 获取图中的所有节点
func (g *Graph) GetAllNodes() []string {
	nodeSet := make(map[string]bool)

	for _, edge := range g.Edges {
		nodeSet[edge.FromFieldID] = true
		nodeSet[edge.ToFieldID] = true
	}

	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}

	return nodes
}

// BuildAdjacencyList 构建邻接表
func (g *Graph) BuildAdjacencyList() map[string][]string {
	adjList := make(map[string][]string)

	for _, edge := range g.Edges {
		adjList[edge.FromFieldID] = append(adjList[edge.FromFieldID], edge.ToFieldID)
	}

	return adjList
}

// BuildReverseAdjacencyList 构建反向邻接表（对齐原版）
// 用于拓扑排序：toFieldId -> []fromFieldId
func (g *Graph) BuildReverseAdjacencyList() map[string][]string {
	reverseAdjList := make(map[string][]string)

	for _, edge := range g.Edges {
		reverseAdjList[edge.ToFieldID] = append(reverseAdjList[edge.ToFieldID], edge.FromFieldID)
	}

	return reverseAdjList
}

// FlatGraph 展平图，获取所有唯一的字段ID（对齐原版）
func FlatGraph(graph []GraphItem) []string {
	fieldIDSet := make(map[string]bool)

	for _, edge := range graph {
		fieldIDSet[edge.FromFieldID] = true
		fieldIDSet[edge.ToFieldID] = true
	}

	fieldIDs := make([]string, 0, len(fieldIDSet))
	for fieldID := range fieldIDSet {
		fieldIDs = append(fieldIDs, fieldID)
	}

	return fieldIDs
}
