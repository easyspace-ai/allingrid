package calculation

import (
	"container/heap"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/event"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// PriorityQueue 优先级队列
// 支持按优先级和创建时间排序的计算任务队列
type PriorityQueue struct {
	items *ItemHeap
	mu    sync.RWMutex
}

// QueueItem 队列项
type QueueItem struct {
	Task      *CalculationTask
	Priority  int
	CreatedAt time.Time
	Index     int // 堆索引
}

// ItemHeap 堆实现
type ItemHeap []*QueueItem

// NewPriorityQueue 创建优先级队列
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: &ItemHeap{},
	}
	heap.Init(pq.items)
	return pq
}

// Push 添加任务到队列
func (pq *PriorityQueue) Push(task *CalculationTask) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item := &QueueItem{
		Task:      task,
		Priority:  task.Priority,
		CreatedAt: time.Now(),
	}

	heap.Push(pq.items, item)

	logger.Debug("任务已添加到优先级队列",
		logger.String("task_id", task.ID),
		logger.String("record_id", task.RecordID.String()),
		logger.Int("priority", task.Priority))
}

// Pop 从队列中取出最高优先级的任务
func (pq *PriorityQueue) Pop() *CalculationTask {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.items.Len() == 0 {
		return nil
	}

	item := heap.Pop(pq.items).(*QueueItem)
	return item.Task
}

// Peek 查看队列中的最高优先级任务（不移除）
func (pq *PriorityQueue) Peek() *CalculationTask {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if pq.items.Len() == 0 {
		return nil
	}

	return (*pq.items)[0].Task
}

// Size 获取队列大小
func (pq *PriorityQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	return pq.items.Len()
}

// IsEmpty 检查队列是否为空
func (pq *PriorityQueue) IsEmpty() bool {
	return pq.Size() == 0
}

// Clear 清空队列
func (pq *PriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.items = &ItemHeap{}
	heap.Init(pq.items)
}

// GetStats 获取队列统计信息
func (pq *PriorityQueue) GetStats() *QueueStats {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	stats := &QueueStats{
		Size:                 pq.items.Len(),
		OldestTask:           nil,
		NewestTask:           nil,
		PriorityDistribution: make(map[int]int),
	}

	if pq.items.Len() == 0 {
		return stats
	}

	// 统计优先级分布
	oldestTime := time.Now()
	newestTime := time.Time{}

	for _, item := range *pq.items {
		// 优先级分布
		stats.PriorityDistribution[item.Priority]++

		// 最老任务
		if item.CreatedAt.Before(oldestTime) {
			oldestTime = item.CreatedAt
			stats.OldestTask = item.Task
		}

		// 最新任务
		if item.CreatedAt.After(newestTime) {
			newestTime = item.CreatedAt
			stats.NewestTask = item.Task
		}
	}

	return stats
}

// QueueStats 队列统计信息
type QueueStats struct {
	Size                 int              `json:"size"`
	OldestTask           *CalculationTask `json:"oldest_task"`
	NewestTask           *CalculationTask `json:"newest_task"`
	PriorityDistribution map[int]int      `json:"priority_distribution"`
}

// 堆接口实现

// Len 返回堆的长度
func (h ItemHeap) Len() int {
	return len(h)
}

// Less 比较函数，决定堆的顺序
func (h ItemHeap) Less(i, j int) bool {
	// 首先按优先级比较（数字越小优先级越高）
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	// 优先级相同时，按创建时间比较（越早创建的优先级越高）
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

// Swap 交换两个元素
func (h ItemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

// Push 添加元素到堆
func (h *ItemHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*QueueItem)
	item.Index = n
	*h = append(*h, item)
}

// Pop 从堆中移除并返回最小元素
func (h *ItemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // 避免内存泄漏
	item.Index = -1 // 安全
	*h = old[0 : n-1]
	return item
}

// Update 更新堆中的元素
func (h *ItemHeap) Update(item *QueueItem, task *CalculationTask, priority int) {
	item.Task = task
	item.Priority = priority
	heap.Fix(h, item.Index)
}

// BatchPriorityQueue 批量优先级队列
// 支持批量操作和批量处理
type BatchPriorityQueue struct {
	*PriorityQueue
	batchSize    int
	batchTimeout time.Duration
}

// NewBatchPriorityQueue 创建批量优先级队列
func NewBatchPriorityQueue(batchSize int, batchTimeout time.Duration) *BatchPriorityQueue {
	return &BatchPriorityQueue{
		PriorityQueue: NewPriorityQueue(),
		batchSize:     batchSize,
		batchTimeout:  batchTimeout,
	}
}

// PopBatch 批量取出任务
func (bpq *BatchPriorityQueue) PopBatch() []*CalculationTask {
	tasks := make([]*CalculationTask, 0, bpq.batchSize)

	for i := 0; i < bpq.batchSize; i++ {
		task := bpq.Pop()
		if task == nil {
			break
		}
		tasks = append(tasks, task)
	}

	return tasks
}

// PopBatchWithTimeout 带超时的批量取出任务
func (bpq *BatchPriorityQueue) PopBatchWithTimeout() []*CalculationTask {
	tasks := make([]*CalculationTask, 0, bpq.batchSize)
	timeout := time.After(bpq.batchTimeout)

	for i := 0; i < bpq.batchSize; i++ {
		select {
		case <-timeout:
			// 超时，返回已收集的任务
			if len(tasks) > 0 {
				return tasks
			}
			// 如果还没有任务，继续等待
			timeout = time.After(bpq.batchTimeout)
		default:
			task := bpq.Pop()
			if task == nil {
				// 队列为空，等待新任务
				time.Sleep(10 * time.Millisecond)
				continue
			}
			tasks = append(tasks, task)
		}
	}

	return tasks
}

// GetBatchStats 获取批量队列统计信息
func (bpq *BatchPriorityQueue) GetBatchStats() *BatchQueueStats {
	baseStats := bpq.GetStats()

	return &BatchQueueStats{
		QueueStats:        *baseStats,
		BatchSize:         bpq.batchSize,
		BatchTimeout:      bpq.batchTimeout,
		OptimalBatchCount: bpq.calculateOptimalBatchCount(),
	}
}

// calculateOptimalBatchCount 计算最优批量数量
func (bpq *BatchPriorityQueue) calculateOptimalBatchCount() int {
	size := bpq.Size()
	if size == 0 {
		return 0
	}

	// 根据队列大小和批量大小计算最优批量数量
	optimalCount := size / bpq.batchSize
	if size%bpq.batchSize > 0 {
		optimalCount++
	}

	return optimalCount
}

// BatchQueueStats 批量队列统计信息
type BatchQueueStats struct {
	QueueStats
	BatchSize         int           `json:"batch_size"`
	BatchTimeout      time.Duration `json:"batch_timeout"`
	OptimalBatchCount int           `json:"optimal_batch_count"`
}

// TaskScheduler 任务调度器
// 负责管理多个优先级队列，支持不同优先级的任务调度
type TaskScheduler struct {
	queues map[int]*PriorityQueue
	mu     sync.RWMutex
}

// NewTaskScheduler 创建任务调度器
func NewTaskScheduler() *TaskScheduler {
	return &TaskScheduler{
		queues: make(map[int]*PriorityQueue),
	}
}

// GetQueue 获取指定优先级的队列
func (ts *TaskScheduler) GetQueue(priority int) *PriorityQueue {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if queue, exists := ts.queues[priority]; exists {
		return queue
	}

	// 创建新队列
	queue := NewPriorityQueue()
	ts.queues[priority] = queue
	return queue
}

// Push 添加任务到对应优先级的队列
func (ts *TaskScheduler) Push(task *CalculationTask) {
	queue := ts.GetQueue(task.Priority)
	queue.Push(task)
}

// Pop 从最高优先级队列中取出任务
func (ts *TaskScheduler) Pop() *CalculationTask {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	// 按优先级从高到低查找
	priorities := []int{event.UrgentPriority, event.HighPriority, event.NormalPriority, event.LowPriority}

	for _, priority := range priorities {
		if queue, exists := ts.queues[priority]; exists && !queue.IsEmpty() {
			return queue.Pop()
		}
	}

	return nil
}

// GetTotalSize 获取所有队列的总大小
func (ts *TaskScheduler) GetTotalSize() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	total := 0
	for _, queue := range ts.queues {
		total += queue.Size()
	}

	return total
}

// GetSchedulerStats 获取调度器统计信息
func (ts *TaskScheduler) GetSchedulerStats() *SchedulerStats {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	stats := &SchedulerStats{
		TotalSize:    0,
		QueueStats:   make(map[int]*QueueStats),
		ActiveQueues: 0,
	}

	for priority, queue := range ts.queues {
		queueStats := queue.GetStats()
		stats.QueueStats[priority] = queueStats
		stats.TotalSize += queueStats.Size

		if queueStats.Size > 0 {
			stats.ActiveQueues++
		}
	}

	return stats
}

// SchedulerStats 调度器统计信息
type SchedulerStats struct {
	TotalSize    int                 `json:"total_size"`
	QueueStats   map[int]*QueueStats `json:"queue_stats"`
	ActiveQueues int                 `json:"active_queues"`
}
