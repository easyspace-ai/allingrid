package dependency

// FilterDirectedGraph 过滤有向图，返回与指定字段相关的所有关系（完全对齐原版）
// 使用DFS遍历，找出所有与指定字段相关的边
func FilterDirectedGraph(undirectedGraph []GraphItem, fieldIDs []string) []GraphItem {
	result := []GraphItem{}
	visited := make(map[string]bool)
	addedEdges := make(map[string]bool) // 用于存储已添加的边（对齐原版）

	// 构建邻接表（对齐原版）
	outgoingAdjList := make(map[string][]GraphItem)
	incomingAdjList := make(map[string][]GraphItem)

	for _, item := range undirectedGraph {
		// 出边
		outgoingAdjList[item.FromFieldID] = append(outgoingAdjList[item.FromFieldID], item)

		// 入边
		incomingAdjList[item.ToFieldID] = append(incomingAdjList[item.ToFieldID], item)
	}

	// 添加边（去重）
	addEdgeIfNotExists := func(edge GraphItem) {
		edgeKey := edge.FromFieldID + "-" + edge.ToFieldID
		if !addedEdges[edgeKey] {
			addedEdges[edgeKey] = true
			result = append(result, edge)
		}
	}

	// DFS遍历（对齐原版）
	var dfs func(currentNode string)
	dfs = func(currentNode string) {
		visited[currentNode] = true

		// 添加与当前节点相关的入边（对齐原版）
		if incomingEdges := incomingAdjList[currentNode]; incomingEdges != nil {
			for _, edge := range incomingEdges {
				addEdgeIfNotExists(edge)
			}
		}

		// 处理从当前节点出发的边（对齐原版）
		if outgoingEdges := outgoingAdjList[currentNode]; outgoingEdges != nil {
			for _, item := range outgoingEdges {
				if !visited[item.ToFieldID] {
					addEdgeIfNotExists(item)
					dfs(item.ToFieldID)
				}
			}
		}
	}

	// 对每个指定的字段运行DFS（对齐原版）
	for _, fieldID := range fieldIDs {
		if !visited[fieldID] {
			dfs(fieldID)
		}
	}

	return result
}

// PruneGraph 修剪图，只保留与指定节点相关的部分（对齐原版）
func PruneGraph(node string, graph []GraphItem) []GraphItem {
	relatedNodes := make(map[string]bool)
	prunedGraph := []GraphItem{}

	// DFS查找相关节点
	var dfs func(currentNode string)
	dfs = func(currentNode string) {
		relatedNodes[currentNode] = true

		for _, edge := range graph {
			if edge.FromFieldID == currentNode && !relatedNodes[edge.ToFieldID] {
				dfs(edge.ToFieldID)
			}
		}
	}

	dfs(node)

	// 收集相关的边
	for _, edge := range graph {
		if relatedNodes[edge.FromFieldID] || relatedNodes[edge.ToFieldID] {
			prunedGraph = append(prunedGraph, edge)

			// 确保所有相关节点都被访问
			if !relatedNodes[edge.FromFieldID] {
				dfs(edge.FromFieldID)
			}
			if !relatedNodes[edge.ToFieldID] {
				dfs(edge.ToFieldID)
			}
		}
	}

	return prunedGraph
}

// GetDependentFields 获取字段的所有依赖字段（递归）
func GetDependentFields(fieldID string, graph []GraphItem) []string {
	visited := make(map[string]bool)
	result := []string{}

	// 构建反向邻接表
	reverseAdjList := make(map[string][]string)
	for _, edge := range graph {
		reverseAdjList[edge.ToFieldID] = append(reverseAdjList[edge.ToFieldID], edge.FromFieldID)
	}

	// DFS收集依赖
	var dfs func(node string)
	dfs = func(node string) {
		if visited[node] {
			return
		}

		visited[node] = true

		if dependencies := reverseAdjList[node]; dependencies != nil {
			for _, dep := range dependencies {
				result = append(result, dep)
				dfs(dep)
			}
		}
	}

	dfs(fieldID)

	return result
}

// GetAffectedFields 获取受字段影响的所有字段（递归）
func GetAffectedFields(fieldID string, graph []GraphItem) []string {
	visited := make(map[string]bool)
	result := []string{}

	// 构建邻接表
	adjList := make(map[string][]string)
	for _, edge := range graph {
		adjList[edge.FromFieldID] = append(adjList[edge.FromFieldID], edge.ToFieldID)
	}

	// DFS收集受影响的字段
	var dfs func(node string)
	dfs = func(node string) {
		if visited[node] {
			return
		}

		visited[node] = true

		if affected := adjList[node]; affected != nil {
			for _, fieldID := range affected {
				result = append(result, fieldID)
				dfs(fieldID)
			}
		}
	}

	dfs(fieldID)

	return result
}
