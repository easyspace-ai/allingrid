package application

import (
	"context"
	"sync"

	"github.com/easyspace-ai/luckdb/server/internal/domain/calculation/dependency"
	fieldEntity "github.com/easyspace-ai/luckdb/server/internal/domain/fields/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/fields/repository"
	"github.com/easyspace-ai/luckdb/server/internal/domain/record/entity"
	recordRepo "github.com/easyspace-ai/luckdb/server/internal/domain/record/repository"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// CalculationOrchestrator 计算编排器
// 负责协调各种计算服务，管理计算流程
type CalculationOrchestrator struct {
	// 依赖服务
	dependencyService *DependencyService
	formulaService    *FormulaService
	rollupService     *RollupService
	lookupService     *LookupService
	countService      *CountService

	// 仓储
	fieldRepo  repository.FieldRepository
	recordRepo recordRepo.RecordRepository

	// 错误处理
	errorService *ErrorService

	// 并发控制
	workerPool *WorkerPool
}

// NewCalculationOrchestrator 创建计算编排器
func NewCalculationOrchestrator(
	dependencyService *DependencyService,
	formulaService *FormulaService,
	rollupService *RollupService,
	lookupService *LookupService,
	countService *CountService,
	fieldRepo repository.FieldRepository,
	recordRepo recordRepo.RecordRepository,
	errorService *ErrorService,
) *CalculationOrchestrator {
	return &CalculationOrchestrator{
		dependencyService: dependencyService,
		formulaService:    formulaService,
		rollupService:     rollupService,
		lookupService:     lookupService,
		countService:      countService,
		fieldRepo:         fieldRepo,
		recordRepo:        recordRepo,
		errorService:      errorService,
		workerPool:        NewWorkerPool(10), // 默认10个worker
	}
}

// CalculateRecordFields 计算记录的所有虚拟字段
func (o *CalculationOrchestrator) CalculateRecordFields(ctx context.Context, record *entity.Record) error {
	// 1. 获取表格的所有字段
	fields, err := o.fieldRepo.FindByTableID(ctx, record.TableID())
	if err != nil {
		return o.errorService.HandleDatabaseError(ctx, "FindByTableID", err)
	}

	// 2. 过滤虚拟字段
	virtualFields := o.filterVirtualFields(fields)
	if len(virtualFields) == 0 {
		logger.Debug("no virtual fields to calculate",
			logger.String("table_id", record.TableID()),
			logger.String("record_id", record.ID().String()),
		)
		return nil
	}

	logger.Info("calculating virtual fields",
		logger.String("record_id", record.ID().String()),
		logger.Int("virtual_fields_count", len(virtualFields)),
		logger.Int("total_fields_count", len(fields)),
	)

	// 3. 构建依赖图
	depGraph := o.dependencyService.BuildDependencyGraph(fields)

	// 4. 检查循环依赖
	if dependency.HasCycle(depGraph) {
		return o.errorService.HandleBusinessLogicError(ctx, "CalculateRecordFields", "circular dependency detected in fields")
	}

	// 5. 拓扑排序
	sortedFields, err := dependency.GetTopoOrders(depGraph)
	if err != nil {
		return o.errorService.HandleBusinessLogicError(ctx, "CalculateRecordFields", "topological sort failed")
	}

	// 6. 并发计算虚拟字段
	return o.calculateFieldsConcurrently(ctx, record, sortedFields, virtualFields)
}

// CalculateAffectedFields 计算受影响的字段
func (o *CalculationOrchestrator) CalculateAffectedFields(ctx context.Context, record *entity.Record, changedFieldIDs []string) error {
	if len(changedFieldIDs) == 0 {
		return nil
	}

	// 1. 获取所有字段
	fields, err := o.fieldRepo.FindByTableID(ctx, record.TableID())
	if err != nil {
		return o.errorService.HandleDatabaseError(ctx, "FindByTableID", err)
	}

	// 2. 构建依赖图
	depGraph := o.dependencyService.BuildDependencyGraph(fields)

	// 3. 传播依赖：找出所有受影响的虚拟字段
	affectedFieldIDs := o.dependencyService.PropagateDependencies(changedFieldIDs, depGraph, fields)

	if len(affectedFieldIDs) == 0 {
		logger.Info("no affected virtual fields",
			logger.String("record_id", record.ID().String()),
			logger.Strings("changed_field_ids", changedFieldIDs))
		return nil
	}

	// 4. 拓扑排序
	sortedFields, err := dependency.GetTopoOrders(depGraph)
	if err != nil {
		return o.errorService.HandleBusinessLogicError(ctx, "CalculateAffectedFields", "topological sort failed")
	}

	// 5. 过滤出需要计算的字段
	fieldsToCalculate := o.filterFieldsToCalculate(sortedFields, affectedFieldIDs)

	// 6. 并发计算
	return o.calculateFieldsConcurrently(ctx, record, fieldsToCalculate, fields)
}

// calculateFieldsConcurrently 并发计算字段
func (o *CalculationOrchestrator) calculateFieldsConcurrently(ctx context.Context, record *entity.Record, sortedFields []dependency.TopoItem, allFields []*fieldEntity.Field) error {
	// 创建计算任务
	tasks := make([]func() error, 0, len(sortedFields))

	for _, item := range sortedFields {
		field := o.getFieldByID(allFields, item.ID)
		if field == nil || !o.isVirtualField(field) {
			continue
		}

		task := func(f *fieldEntity.Field) func() error {
			return func() error {
				return o.calculateSingleField(ctx, record, f)
			}
		}(field)

		tasks = append(tasks, task)
	}

	// 并发执行任务
	return o.workerPool.Execute(ctx, tasks)
}

// calculateSingleField 计算单个字段
func (o *CalculationOrchestrator) calculateSingleField(ctx context.Context, record *entity.Record, field *fieldEntity.Field) error {
	switch field.Type().String() {
	case "formula":
		return o.formulaService.Calculate(ctx, record, field)
	case "rollup":
		return o.rollupService.Calculate(ctx, record, field)
	case "lookup":
		return o.lookupService.Calculate(ctx, record, field)
	case "count":
		return o.countService.Calculate(ctx, record, field)
	default:
		logger.Warn("unsupported virtual field type",
			logger.String("field_id", field.ID().String()),
			logger.String("field_type", field.Type().String()))
		return nil
	}
}

// filterVirtualFields 过滤虚拟字段
func (o *CalculationOrchestrator) filterVirtualFields(fields []*fieldEntity.Field) []*fieldEntity.Field {
	virtualTypes := map[string]bool{
		"formula": true,
		"rollup":  true,
		"lookup":  true,
		"count":   true,
	}

	result := make([]*fieldEntity.Field, 0, len(fields))
	for _, field := range fields {
		if virtualTypes[field.Type().String()] && !field.IsDeleted() {
			result = append(result, field)
		}
	}
	return result
}

// filterFieldsToCalculate 过滤需要计算的字段
func (o *CalculationOrchestrator) filterFieldsToCalculate(sortedFields []dependency.TopoItem, affectedFieldIDs []string) []dependency.TopoItem {
	affectedSet := make(map[string]bool)
	for _, id := range affectedFieldIDs {
		affectedSet[id] = true
	}

	result := make([]dependency.TopoItem, 0, len(sortedFields))
	for _, item := range sortedFields {
		if affectedSet[item.ID] {
			result = append(result, item)
		}
	}
	return result
}

// getFieldByID 根据ID获取字段
func (o *CalculationOrchestrator) getFieldByID(fields []*fieldEntity.Field, fieldID string) *fieldEntity.Field {
	for _, field := range fields {
		if field.ID().String() == fieldID {
			return field
		}
	}
	return nil
}

// isVirtualField 检查是否为虚拟字段
func (o *CalculationOrchestrator) isVirtualField(field *fieldEntity.Field) bool {
	virtualTypes := map[string]bool{
		"formula": true,
		"rollup":  true,
		"lookup":  true,
		"count":   true,
	}
	return virtualTypes[field.Type().String()]
}

// WorkerPool 工作池
type WorkerPool struct {
	workers    int
	taskChan   chan func() error
	resultChan chan error
	wg         sync.WaitGroup
}

// NewWorkerPool 创建工作池
func NewWorkerPool(workers int) *WorkerPool {
	return &WorkerPool{
		workers:    workers,
		taskChan:   make(chan func() error, workers*2),
		resultChan: make(chan error, workers*2),
	}
}

// Execute 执行任务
func (wp *WorkerPool) Execute(ctx context.Context, tasks []func() error) error {
	// 启动worker
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx)
	}

	// 发送任务
	go func() {
		defer close(wp.taskChan)
		for _, task := range tasks {
			select {
			case wp.taskChan <- task:
			case <-ctx.Done():
				return
			}
		}
	}()

	// 等待所有任务完成
	go func() {
		wp.wg.Wait()
		close(wp.resultChan)
	}()

	// 收集结果
	var lastError error
	for err := range wp.resultChan {
		if err != nil {
			lastError = err
		}
	}

	return lastError
}

// worker 工作协程
func (wp *WorkerPool) worker(ctx context.Context) {
	defer wp.wg.Done()

	for {
		select {
		case task, ok := <-wp.taskChan:
			if !ok {
				return
			}

			err := task()
			select {
			case wp.resultChan <- err:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
