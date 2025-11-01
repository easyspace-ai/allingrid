package dependency

// HasCycle 检测是否有循环依赖（完全对齐原版 hasCycle）
// 使用DFS算法检测有向图中的环
func HasCycle(graphItems []GraphItem) bool {
	adjList := make(map[string][]string)
	visiting := make(map[string]bool)
	visited := make(map[string]bool)

	// 构建邻接表（对齐原版）
	for _, item := range graphItems {
		adjList[item.FromFieldID] = append(adjList[item.FromFieldID], item.ToFieldID)
	}

	// DFS检测环（对齐原版）
	var dfs func(node string) bool
	dfs = func(node string) bool {
		// 正在访问中，发现环
		if visiting[node] {
			return true
		}

		// 已经访问过，没有环
		if visited[node] {
			return false
		}

		visiting[node] = true

		// 访问所有邻居节点
		if neighbors := adjList[node]; neighbors != nil {
			for _, neighbor := range neighbors {
				if dfs(neighbor) {
					return true
				}
			}
		}

		delete(visiting, node)
		visited[node] = true

		return false
	}

	// 检查所有节点（对齐原版）
	for node := range adjList {
		if !visited[node] && dfs(node) {
			return true
		}
	}

	return false
}

// DetectCyclePath 检测循环依赖并返回路径
func DetectCyclePath(graphItems []GraphItem) (bool, []string) {
	adjList := make(map[string][]string)
	visiting := make(map[string]bool)
	visited := make(map[string]bool)
	path := []string{}
	cyclePath := []string{}

	// 构建邻接表
	for _, item := range graphItems {
		adjList[item.FromFieldID] = append(adjList[item.FromFieldID], item.ToFieldID)
	}

	var dfs func(node string) bool
	dfs = func(node string) bool {
		if visiting[node] {
			// 找到环，记录路径
			foundCycle := false
			for _, n := range path {
				if foundCycle || n == node {
					foundCycle = true
					cyclePath = append(cyclePath, n)
				}
			}
			cyclePath = append(cyclePath, node)
			return true
		}

		if visited[node] {
			return false
		}

		visiting[node] = true
		path = append(path, node)

		if neighbors := adjList[node]; neighbors != nil {
			for _, neighbor := range neighbors {
				if dfs(neighbor) {
					return true
				}
			}
		}

		delete(visiting, node)
		visited[node] = true
		if len(path) > 0 {
			path = path[:len(path)-1]
		}

		return false
	}

	for node := range adjList {
		if !visited[node] && dfs(node) {
			return true, cyclePath
		}
	}

	return false, nil
}
