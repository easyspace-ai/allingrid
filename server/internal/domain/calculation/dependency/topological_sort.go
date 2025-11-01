package dependency

import "fmt"

// GetTopoOrders 拓扑排序（完全对齐原版 getTopoOrders）
// 使用DFS算法进行拓扑排序，确保依赖的字段先计算
func GetTopoOrders(graph []GraphItem) ([]TopoItem, error) {
	visitedNodes := make(map[string]bool)
	visitingNodes := make(map[string]bool)
	sortedNodes := []TopoItem{}
	allNodes := make(map[string]bool)

	// 构建邻接表和反向邻接表（对齐原版）
	adjList := make(map[string][]string)
	reverseAdjList := make(map[string][]string)

	for _, edge := range graph {
		// 邻接表: fromFieldId -> []toFieldId
		adjList[edge.FromFieldID] = append(adjList[edge.FromFieldID], edge.ToFieldID)

		// 反向邻接表: toFieldId -> []fromFieldId
		reverseAdjList[edge.ToFieldID] = append(reverseAdjList[edge.ToFieldID], edge.FromFieldID)

		// 收集所有节点
		allNodes[edge.FromFieldID] = true
		allNodes[edge.ToFieldID] = true
	}

	// DFS访问函数（对齐原版）
	var visit func(node string) error
	visit = func(node string) error {
		// 检测循环依赖
		if visitingNodes[node] {
			return fmt.Errorf("detected a cycle: %s is part of a circular dependency", node)
		}

		if !visitedNodes[node] {
			visitingNodes[node] = true

			// 获取依赖（反向边）
			dependencies := reverseAdjList[node]
			if dependencies == nil {
				dependencies = []string{}
			}

			// 先处理依赖
			for _, dep := range dependencies {
				if !visitedNodes[dep] {
					if err := visit(dep); err != nil {
						return err
					}
				}
			}

			delete(visitingNodes, node)
			visitedNodes[node] = true

			// 添加到排序结果
			sortedNodes = append(sortedNodes, TopoItem{
				ID:           node,
				Dependencies: dependencies,
			})
		}

		return nil
	}

	// 从叶子节点开始（没有出边的节点）（对齐原版）
	startNodes := []string{}
	for node := range allNodes {
		if len(adjList[node]) == 0 {
			startNodes = append(startNodes, node)
		}
	}

	for _, node := range startNodes {
		if !visitedNodes[node] {
			if err := visit(node); err != nil {
				return nil, err
			}
		}
	}

	// 处理剩余节点
	for node := range allNodes {
		if !visitedNodes[node] {
			if err := visit(node); err != nil {
				return nil, err
			}
		}
	}

	return sortedNodes, nil
}

// PrependStartFieldIDs 在拓扑排序结果前添加起始字段（对齐原版）
func PrependStartFieldIDs(topoOrders []TopoItem, startFieldIDs []string) []TopoItem {
	// 收集已存在的字段ID
	existFieldIDs := make(map[string]bool)
	for _, item := range topoOrders {
		existFieldIDs[item.ID] = true
	}

	// 创建新的拓扑项（不在原结果中的起始字段）
	newTopoOrders := []TopoItem{}
	for _, fieldID := range startFieldIDs {
		if !existFieldIDs[fieldID] {
			newTopoOrders = append(newTopoOrders, TopoItem{
				ID:           fieldID,
				Dependencies: []string{},
			})
		}
	}

	// 合并结果（对齐原版：[...newTopoOrders, ...topoOrders]）
	result := make([]TopoItem, 0, len(newTopoOrders)+len(topoOrders))
	result = append(result, newTopoOrders...)
	result = append(result, topoOrders...)

	return result
}

// TopoOrderWithStart 从起始节点生成拓扑排序（对齐原版）
func TopoOrderWithStart(startNodeID string, graph []GraphItem) []string {
	visitedNodes := make(map[string]bool)
	sortedNodes := []string{}

	// 构建邻接表和反向邻接表
	adjList := make(map[string][]string)
	reverseAdjList := make(map[string][]string)

	for _, edge := range graph {
		adjList[edge.FromFieldID] = append(adjList[edge.FromFieldID], edge.ToFieldID)
		reverseAdjList[edge.ToFieldID] = append(reverseAdjList[edge.ToFieldID], edge.FromFieldID)
	}

	// DFS访问函数
	var visit func(node string)
	visit = func(node string) {
		if !visitedNodes[node] {
			visitedNodes[node] = true

			// 处理出边
			if neighbors := adjList[node]; neighbors != nil {
				for _, neighbor := range neighbors {
					visit(neighbor)
				}
			}

			sortedNodes = append(sortedNodes, node)
		}
	}

	visit(startNodeID)

	// 反转结果（对齐原版）
	for i, j := 0, len(sortedNodes)-1; i < j; i, j = i+1, j-1 {
		sortedNodes[i], sortedNodes[j] = sortedNodes[j], sortedNodes[i]
	}

	return sortedNodes
}
