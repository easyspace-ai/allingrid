package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Job 工作任务接口
type Job interface {
	Execute(ctx context.Context) error
	Name() string
	Priority() int
}

// Result 任务执行结果
type Result struct {
	JobName string
	Error   error
	Elapsed time.Duration
}

// WorkerPool 工作池
type WorkerPool struct {
	name        string
	maxWorkers  int
	jobQueue    chan Job
	resultQueue chan Result
	workers     []*worker
	wg          sync.WaitGroup
	logger      *zap.Logger

	// 统计信息
	stats *PoolStats

	// 控制
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
}

// PoolStats 工作池统计信息
type PoolStats struct {
	TotalJobs     atomic.Uint64
	CompletedJobs atomic.Uint64
	FailedJobs    atomic.Uint64
	ActiveWorkers atomic.Int32
	QueuedJobs    atomic.Int32
	TotalDuration atomic.Uint64 // 纳秒
}

// worker 工作者
type worker struct {
	id         int
	pool       *WorkerPool
	jobChannel chan Job
}

// PoolOption 工作池选项
type PoolOption func(*WorkerPool)

// WithLogger 设置日志记录器
func WithLogger(logger *zap.Logger) PoolOption {
	return func(p *WorkerPool) {
		p.logger = logger
	}
}

// WithResultQueue 设置结果队列
func WithResultQueue(size int) PoolOption {
	return func(p *WorkerPool) {
		p.resultQueue = make(chan Result, size)
	}
}

// NewWorkerPool 创建新的工作池
func NewWorkerPool(name string, maxWorkers int, queueSize int, opts ...PoolOption) *WorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		name:       name,
		maxWorkers: maxWorkers,
		jobQueue:   make(chan Job, queueSize),
		workers:    make([]*worker, maxWorkers),
		logger:     zap.L().Named("worker_pool"),
		stats:      &PoolStats{},
		ctx:        ctx,
		cancel:     cancel,
	}

	// 应用选项
	for _, opt := range opts {
		opt(pool)
	}

	// 创建工作者
	for i := 0; i < maxWorkers; i++ {
		pool.workers[i] = &worker{
			id:         i,
			pool:       pool,
			jobChannel: make(chan Job),
		}
	}

	return pool
}

// Start 启动工作池
func (p *WorkerPool) Start() error {
	if p.started.Load() {
		return fmt.Errorf("worker pool already started")
	}

	p.started.Store(true)

	// 启动分发器
	go p.dispatcher()

	// 启动所有工作者
	for _, w := range p.workers {
		go w.start()
	}

	p.logger.Info("Worker pool started",
		zap.String("name", p.name),
		zap.Int("workers", p.maxWorkers),
	)

	return nil
}

// Stop 停止工作池
func (p *WorkerPool) Stop() error {
	if !p.started.Load() {
		return fmt.Errorf("worker pool not started")
	}

	// 停止接收新任务
	close(p.jobQueue)

	// 等待所有任务完成
	p.wg.Wait()

	// 取消上下文
	p.cancel()

	// 关闭结果队列
	if p.resultQueue != nil {
		close(p.resultQueue)
	}

	p.started.Store(false)

	p.logger.Info("Worker pool stopped",
		zap.String("name", p.name),
		zap.Uint64("total_jobs", p.stats.TotalJobs.Load()),
		zap.Uint64("completed_jobs", p.stats.CompletedJobs.Load()),
		zap.Uint64("failed_jobs", p.stats.FailedJobs.Load()),
	)

	return nil
}

// Submit 提交任务
func (p *WorkerPool) Submit(job Job) error {
	if !p.started.Load() {
		return fmt.Errorf("worker pool not started")
	}

	select {
	case p.jobQueue <- job:
		p.stats.TotalJobs.Add(1)
		p.stats.QueuedJobs.Add(1)
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		return fmt.Errorf("job queue is full")
	}
}

// SubmitWithTimeout 提交任务（带超时）
func (p *WorkerPool) SubmitWithTimeout(job Job, timeout time.Duration) error {
	if !p.started.Load() {
		return fmt.Errorf("worker pool not started")
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case p.jobQueue <- job:
		p.stats.TotalJobs.Add(1)
		p.stats.QueuedJobs.Add(1)
		return nil
	case <-timer.C:
		return fmt.Errorf("submit timeout")
	case <-p.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	}
}

// GetStats 获取统计信息
func (p *WorkerPool) GetStats() PoolStats {
	return PoolStats{
		TotalJobs:     atomic.Uint64{},
		CompletedJobs: atomic.Uint64{},
		FailedJobs:    atomic.Uint64{},
		ActiveWorkers: atomic.Int32{},
		QueuedJobs:    atomic.Int32{},
	}
}

// Results 获取结果通道
func (p *WorkerPool) Results() <-chan Result {
	return p.resultQueue
}

// dispatcher 任务分发器
func (p *WorkerPool) dispatcher() {
	// 创建优先级队列（简化实现）
	for {
		select {
		case job, ok := <-p.jobQueue:
			if !ok {
				// 队列已关闭，通知所有工作者停止
				for _, w := range p.workers {
					close(w.jobChannel)
				}
				return
			}

			// 找到空闲的工作者
			p.assignJob(job)

		case <-p.ctx.Done():
			return
		}
	}
}

// assignJob 分配任务给工作者
func (p *WorkerPool) assignJob(job Job) {
	// 使用简单的轮询方式分配任务
	// 实际使用中可以实现更复杂的负载均衡算法
	for {
		for _, w := range p.workers {
			select {
			case w.jobChannel <- job:
				p.stats.QueuedJobs.Add(-1)
				return
			default:
				// 工作者忙碌，尝试下一个
			}
		}

		// 所有工作者都忙，等待一会儿再试
		time.Sleep(10 * time.Millisecond)
	}
}

// worker.start 工作者开始工作
func (w *worker) start() {
	w.pool.wg.Add(1)
	defer w.pool.wg.Done()

	for {
		select {
		case job, ok := <-w.jobChannel:
			if !ok {
				// 通道已关闭
				return
			}

			w.executeJob(job)

		case <-w.pool.ctx.Done():
			return
		}
	}
}

// executeJob 执行任务
func (w *worker) executeJob(job Job) {
	w.pool.stats.ActiveWorkers.Add(1)
	defer w.pool.stats.ActiveWorkers.Add(-1)

	start := time.Now()

	// 创建任务上下文（可以设置超时）
	ctx := context.WithValue(w.pool.ctx, "worker_id", w.id)

	// 执行任务
	err := job.Execute(ctx)

	elapsed := time.Since(start)
	w.pool.stats.TotalDuration.Add(uint64(elapsed.Nanoseconds()))

	if err != nil {
		w.pool.stats.FailedJobs.Add(1)
		w.pool.logger.Error("Job execution failed",
			zap.Int("worker_id", w.id),
			zap.String("job_name", job.Name()),
			zap.Error(err),
			zap.Duration("elapsed", elapsed),
		)
	} else {
		w.pool.stats.CompletedJobs.Add(1)
		w.pool.logger.Debug("Job completed",
			zap.Int("worker_id", w.id),
			zap.String("job_name", job.Name()),
			zap.Duration("elapsed", elapsed),
		)
	}

	// 发送结果
	if w.pool.resultQueue != nil {
		select {
		case w.pool.resultQueue <- Result{
			JobName: job.Name(),
			Error:   err,
			Elapsed: elapsed,
		}:
		default:
			// 结果队列满，记录但不阻塞
			w.pool.logger.Warn("Result queue is full", zap.String("job_name", job.Name()))
		}
	}
}

// SimpleJob 简单任务实现
type SimpleJob struct {
	name     string
	priority int
	fn       func(ctx context.Context) error
}

// NewSimpleJob 创建简单任务
func NewSimpleJob(name string, priority int, fn func(ctx context.Context) error) Job {
	return &SimpleJob{
		name:     name,
		priority: priority,
		fn:       fn,
	}
}

func (j *SimpleJob) Execute(ctx context.Context) error {
	return j.fn(ctx)
}

func (j *SimpleJob) Name() string {
	return j.name
}

func (j *SimpleJob) Priority() int {
	return j.priority
}

// BatchProcessor 批处理器
type BatchProcessor struct {
	pool      *WorkerPool
	batchSize int
	timeout   time.Duration
}

// NewBatchProcessor 创建批处理器
func NewBatchProcessor(pool *WorkerPool, batchSize int, timeout time.Duration) *BatchProcessor {
	return &BatchProcessor{
		pool:      pool,
		batchSize: batchSize,
		timeout:   timeout,
	}
}

// ProcessBatch 处理批量任务
func (bp *BatchProcessor) ProcessBatch(jobs []Job) ([]Result, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	results := make([]Result, 0, len(jobs))

	// 提交所有任务
	for _, job := range jobs {
		if err := bp.pool.Submit(job); err != nil {
			return nil, fmt.Errorf("failed to submit job %s: %w", job.Name(), err)
		}
	}

	// 收集结果
	timer := time.NewTimer(bp.timeout)
	defer timer.Stop()

	for i := 0; i < len(jobs); i++ {
		select {
		case result := <-bp.pool.Results():
			results = append(results, result)
		case <-timer.C:
			return results, fmt.Errorf("batch processing timeout")
		}
	}

	return results, nil
}
