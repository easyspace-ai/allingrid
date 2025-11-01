package calculation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	calculationEvent "github.com/easyspace-ai/luckdb/server/internal/domain/calculation/event"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CalculationWorker 计算工作器
// 负责异步处理计算任务
type CalculationWorker struct {
	// 配置
	config *CalculationWorkerConfig

	// 依赖注入
	recordRepo recordRepo.RecordRepository
	eventBus   *application.EventBus

	// 工作池
	workerPool chan chan *CalculationTask
	workers    []*Worker

	// 任务队列
	taskQueue chan *CalculationTask

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计
	stats *WorkerStats
	mu    sync.RWMutex
}

// CalculationWorkerConfig 计算工作器配置
type CalculationWorkerConfig struct {
	// 工作池配置
	WorkerCount int `json:"worker_count"`

	// 队列配置
	QueueSize int `json:"queue_size"`

	// 超时配置
	TaskTimeout time.Duration `json:"task_timeout"`

	// 重试配置
	MaxRetries int           `json:"max_retries"`
	RetryDelay time.Duration `json:"retry_delay"`

	// 批处理配置
	BatchSize    int           `json:"batch_size"`
	BatchTimeout time.Duration `json:"batch_timeout"`
}

// DefaultCalculationWorkerConfig 默认配置
func DefaultCalculationWorkerConfig() *CalculationWorkerConfig {
	return &CalculationWorkerConfig{
		WorkerCount:  5,
		QueueSize:    1000,
		TaskTimeout:  30 * time.Second,
		MaxRetries:   3,
		RetryDelay:   1 * time.Second,
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
	}
}

// CalculationTask 计算任务
type CalculationTask struct {
	ID          string
	TableID     string
	RecordID    valueobject.RecordID
	FieldIDs    []string
	Priority    int
	RequestedBy string
	CreatedAt   time.Time
	RetryCount  int
}

// Worker 工作器
type Worker struct {
	ID         int
	WorkerPool chan chan *CalculationTask
	TaskQueue  chan *CalculationTask
	Quit       chan bool
	Worker     *CalculationWorker
}

// WorkerStats 工作器统计
type WorkerStats struct {
	TotalTasks     int64     `json:"total_tasks"`
	CompletedTasks int64     `json:"completed_tasks"`
	FailedTasks    int64     `json:"failed_tasks"`
	ActiveWorkers  int       `json:"active_workers"`
	QueueSize      int       `json:"queue_size"`
	LastActivity   time.Time `json:"last_activity"`
}

// NewCalculationWorker 创建计算工作器
func NewCalculationWorker(
	config *CalculationWorkerConfig,
	recordRepo recordRepo.RecordRepository,
	eventBus *application.EventBus,
) *CalculationWorker {
	if config == nil {
		config = DefaultCalculationWorkerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &CalculationWorker{
		config:     config,
		recordRepo: recordRepo,
		eventBus:   eventBus,
		ctx:        ctx,
		cancel:     cancel,
		stats: &WorkerStats{
			LastActivity: time.Now(),
		},
	}
}

// Start 启动计算工作器
func (w *CalculationWorker) Start() error {
	logger.Info("启动计算工作器",
		logger.Int("worker_count", w.config.WorkerCount),
		logger.Int("queue_size", w.config.QueueSize))

	// 初始化工作池
	w.workerPool = make(chan chan *CalculationTask, w.config.WorkerCount)
	w.taskQueue = make(chan *CalculationTask, w.config.QueueSize)

	// 启动工作器
	w.workers = make([]*Worker, w.config.WorkerCount)
	for i := 0; i < w.config.WorkerCount; i++ {
		worker := &Worker{
			ID:         i,
			WorkerPool: w.workerPool,
			TaskQueue:  make(chan *CalculationTask),
			Quit:       make(chan bool),
			Worker:     w,
		}
		w.workers[i] = worker

		w.wg.Add(1)
		go worker.Start()
	}

	// 启动任务分发器
	w.wg.Add(1)
	go w.dispatch()

	// 启动统计更新器
	w.wg.Add(1)
	go w.updateStats()

	logger.Info("计算工作器启动成功")
	return nil
}

// Stop 停止计算工作器
func (w *CalculationWorker) Stop() error {
	logger.Info("停止计算工作器")

	// 取消上下文
	w.cancel()

	// 停止所有工作器
	for _, worker := range w.workers {
		worker.Quit <- true
	}

	// 关闭任务队列
	close(w.taskQueue)

	// 等待所有goroutine结束
	w.wg.Wait()

	logger.Info("计算工作器已停止")
	return nil
}

// SubmitTask 提交计算任务
func (w *CalculationWorker) SubmitTask(task *CalculationTask) error {
	select {
	case w.taskQueue <- task:
		w.mu.Lock()
		w.stats.TotalTasks++
		w.stats.LastActivity = time.Now()
		w.mu.Unlock()

		logger.Debug("计算任务已提交",
			logger.String("task_id", task.ID),
			logger.String("record_id", task.RecordID.String()),
			logger.Int("priority", task.Priority))

		return nil
	case <-w.ctx.Done():
		return fmt.Errorf("计算工作器已停止")
	default:
		return fmt.Errorf("任务队列已满")
	}
}

// dispatch 任务分发器
func (w *CalculationWorker) dispatch() {
	defer w.wg.Done()

	for {
		select {
		case task := <-w.taskQueue:
			// 获取可用的工作器
			workerQueue := <-w.workerPool
			// 将任务分配给工作器
			workerQueue <- task

		case <-w.ctx.Done():
			logger.Info("任务分发器已停止")
			return
		}
	}
}

// updateStats 更新统计信息
func (w *CalculationWorker) updateStats() {
	defer w.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			w.stats.QueueSize = len(w.taskQueue)
			w.stats.ActiveWorkers = len(w.workers)
			w.mu.Unlock()

		case <-w.ctx.Done():
			logger.Info("统计更新器已停止")
			return
		}
	}
}

// Start 启动工作器
func (worker *Worker) Start() {
	defer worker.Worker.wg.Done()

	for {
		// 将工作器注册到工作池
		worker.WorkerPool <- worker.TaskQueue

		select {
		case task := <-worker.TaskQueue:
			// 处理任务
			worker.processTask(task)

		case <-worker.Quit:
			logger.Info("工作器已停止",
				logger.Int("worker_id", worker.ID))
			return
		}
	}
}

// processTask 处理计算任务
func (worker *Worker) processTask(task *CalculationTask) {
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(worker.Worker.ctx, worker.Worker.config.TaskTimeout)
	defer cancel()

	logger.Info("开始处理计算任务",
		logger.Int("worker_id", worker.ID),
		logger.String("task_id", task.ID),
		logger.String("record_id", task.RecordID.String()),
		logger.Strings("field_ids", task.FieldIDs))

	// 发布计算开始事件
	startEvent := calculationEvent.NewCalculationRequested(
		task.TableID,
		task.RecordID,
		task.FieldIDs,
		task.Priority,
		task.RequestedBy,
	)
	worker.Worker.eventBus.Publish(ctx, startEvent)

	// 执行计算
	err := worker.executeCalculation(ctx, task)
	duration := time.Since(startTime)

	// 更新统计
	worker.Worker.mu.Lock()
	if err != nil {
		worker.Worker.stats.FailedTasks++
	} else {
		worker.Worker.stats.CompletedTasks++
	}
	worker.Worker.stats.LastActivity = time.Now()
	worker.Worker.mu.Unlock()

	// 发布计算完成或失败事件
	if err != nil {
		logger.Error("计算任务失败",
			logger.Int("worker_id", worker.ID),
			logger.String("task_id", task.ID),
			logger.String("record_id", task.RecordID.String()),
			logger.ErrorField(err))

		// 发布计算失败事件
		failedEvent := calculationEvent.NewCalculationFailed(
			task.TableID,
			task.RecordID,
			task.FieldIDs,
			err.Error(),
			"calculation_worker",
			task.RetryCount,
		)
		worker.Worker.eventBus.Publish(ctx, failedEvent)

		// 重试逻辑
		if task.RetryCount < worker.Worker.config.MaxRetries {
			task.RetryCount++
			time.Sleep(worker.Worker.config.RetryDelay)
			worker.Worker.SubmitTask(task)
		}
	} else {
		logger.Info("计算任务完成",
			logger.Int("worker_id", worker.ID),
			logger.String("task_id", task.ID),
			logger.String("record_id", task.RecordID.String()),
			logger.Duration("duration", duration))

		// 发布计算完成事件
		completedEvent := calculationEvent.NewCalculationCompleted(
			task.TableID,
			task.RecordID,
			make(map[string]interface{}), // 这里应该包含计算结果
			"calculation_worker",
			duration,
		)
		worker.Worker.eventBus.Publish(ctx, completedEvent)
	}
}

// executeCalculation 执行计算
func (worker *Worker) executeCalculation(ctx context.Context, task *CalculationTask) error {
	// 1. 查找记录
	records, err := worker.Worker.recordRepo.FindByIDs(ctx, task.TableID, []valueobject.RecordID{task.RecordID})
	if err != nil {
		return fmt.Errorf("查找记录失败: %v", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("记录不存在")
	}
	record := records[0]

	// 2. 执行计算逻辑
	// 这里应该调用实际的计算服务
	// 为了演示，我们模拟计算过程
	time.Sleep(100 * time.Millisecond)

	// 3. 更新记录（如果需要）
	// 这里应该根据计算结果更新记录
	// 为了演示，我们只是保存记录
	if err := worker.Worker.recordRepo.Save(ctx, record); err != nil {
		return fmt.Errorf("保存记录失败: %v", err)
	}

	return nil
}

// GetStats 获取统计信息
func (w *CalculationWorker) GetStats() *WorkerStats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// 返回统计信息的副本
	return &WorkerStats{
		TotalTasks:     w.stats.TotalTasks,
		CompletedTasks: w.stats.CompletedTasks,
		FailedTasks:    w.stats.FailedTasks,
		ActiveWorkers:  w.stats.ActiveWorkers,
		QueueSize:      w.stats.QueueSize,
		LastActivity:   w.stats.LastActivity,
	}
}

// GetConfig 获取配置信息
func (w *CalculationWorker) GetConfig() *CalculationWorkerConfig {
	return w.config
}
