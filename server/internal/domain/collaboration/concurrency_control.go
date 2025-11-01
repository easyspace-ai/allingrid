package collaboration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// ConcurrencyControlService 并发控制服务
type ConcurrencyControlService struct {
	lockManager      *LockManager
	conflictDetector *ConflictDetector
	operationQueue   *OperationQueue
	logger           *zap.Logger
	mu               sync.RWMutex
}

// NewConcurrencyControlService 创建并发控制服务
func NewConcurrencyControlService(logger *zap.Logger) *ConcurrencyControlService {
	return &ConcurrencyControlService{
		lockManager:      NewLockManager(),
		conflictDetector: NewConflictDetector(),
		operationQueue:   NewOperationQueue(),
		logger:           logger,
	}
}

// LockManager 锁管理器
type LockManager struct {
	locks map[string]*ResourceLock // resourceID -> lock
	mu    sync.RWMutex
}

// NewLockManager 创建锁管理器
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*ResourceLock),
	}
}

// ResourceLock 资源锁
type ResourceLock struct {
	ResourceID   string
	ResourceType string // record, table, field, view
	LockType     LockType
	OwnerID      string
	SessionID    string
	AcquiredAt   time.Time
	ExpiresAt    time.Time
	mu           sync.RWMutex
}

// LockType 锁类型
type LockType string

const (
	LockTypeRead      LockType = "read"      // 读锁
	LockTypeWrite     LockType = "write"     // 写锁
	LockTypeExclusive LockType = "exclusive" // 排他锁
)

// LockRequest 锁请求
type LockRequest struct {
	ResourceID   string
	ResourceType string
	LockType     LockType
	UserID       string
	SessionID    string
	Timeout      time.Duration
}

// AcquireLock 获取锁
func (lm *LockManager) AcquireLock(ctx context.Context, req *LockRequest) (*ResourceLock, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	resourceKey := fmt.Sprintf("%s:%s", req.ResourceType, req.ResourceID)

	// 检查是否已存在锁
	if existingLock, exists := lm.locks[resourceKey]; exists {
		// 检查锁是否过期
		if time.Now().After(existingLock.ExpiresAt) {
			delete(lm.locks, resourceKey)
		} else {
			// 检查锁兼容性
			if !lm.isLockCompatible(existingLock, req) {
				return nil, fmt.Errorf("resource %s is locked by user %s", resourceKey, existingLock.OwnerID)
			}
		}
	}

	// 创建新锁
	lock := &ResourceLock{
		ResourceID:   req.ResourceID,
		ResourceType: req.ResourceType,
		LockType:     req.LockType,
		OwnerID:      req.UserID,
		SessionID:    req.SessionID,
		AcquiredAt:   time.Now(),
		ExpiresAt:    time.Now().Add(req.Timeout),
	}

	lm.locks[resourceKey] = lock
	return lock, nil
}

// ReleaseLock 释放锁
func (lm *LockManager) ReleaseLock(resourceType, resourceID, userID, sessionID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	resourceKey := fmt.Sprintf("%s:%s", resourceType, resourceID)

	if lock, exists := lm.locks[resourceKey]; exists {
		if lock.OwnerID == userID && lock.SessionID == sessionID {
			delete(lm.locks, resourceKey)
			return nil
		}
		return fmt.Errorf("lock not owned by user %s session %s", userID, sessionID)
	}

	return fmt.Errorf("lock not found for resource %s", resourceKey)
}

// isLockCompatible 检查锁兼容性
func (lm *LockManager) isLockCompatible(existingLock *ResourceLock, req *LockRequest) bool {
	// 同一用户同一会话的锁总是兼容
	if existingLock.OwnerID == req.UserID && existingLock.SessionID == req.SessionID {
		return true
	}

	// 排他锁不兼容任何其他锁
	if existingLock.LockType == LockTypeExclusive || req.LockType == LockTypeExclusive {
		return false
	}

	// 写锁不兼容任何其他锁
	if existingLock.LockType == LockTypeWrite || req.LockType == LockTypeWrite {
		return false
	}

	// 读锁之间兼容
	return existingLock.LockType == LockTypeRead && req.LockType == LockTypeRead
}

// CleanupExpiredLocks 清理过期锁
func (lm *LockManager) CleanupExpiredLocks() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	for key, lock := range lm.locks {
		if now.After(lock.ExpiresAt) {
			delete(lm.locks, key)
		}
	}
}

// GetActiveLocks 获取活跃锁
func (lm *LockManager) GetActiveLocks() map[string]*ResourceLock {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make(map[string]*ResourceLock)
	now := time.Now()

	for key, lock := range lm.locks {
		if now.Before(lock.ExpiresAt) {
			result[key] = lock
		}
	}

	return result
}

// ConflictDetector 冲突检测器
type ConflictDetector struct {
	mu sync.RWMutex
}

// NewConflictDetector 创建冲突检测器
func NewConflictDetector() *ConflictDetector {
	return &ConflictDetector{}
}

// Operation 操作定义
type Operation struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"` // create, update, delete
	ResourceType string                 `json:"resource_type"`
	ResourceID   string                 `json:"resource_id"`
	UserID       string                 `json:"user_id"`
	SessionID    string                 `json:"session_id"`
	Data         map[string]interface{} `json:"data"`
	Timestamp    time.Time              `json:"timestamp"`
	Version      int64                  `json:"version"`
}

// ConflictResult 冲突检测结果
type ConflictResult struct {
	HasConflict   bool                   `json:"has_conflict"`
	ConflictType  string                 `json:"conflict_type"`
	ConflictingOp *Operation             `json:"conflicting_op,omitempty"`
	Resolution    map[string]interface{} `json:"resolution,omitempty"`
}

// DetectConflict 检测操作冲突
func (cd *ConflictDetector) DetectConflict(ctx context.Context, op *Operation, existingOps []*Operation) *ConflictResult {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	for _, existingOp := range existingOps {
		if cd.hasConflict(op, existingOp) {
			conflictType := cd.determineConflictType(op, existingOp)
			resolution := cd.generateResolution(op, existingOp, conflictType)

			return &ConflictResult{
				HasConflict:   true,
				ConflictType:  conflictType,
				ConflictingOp: existingOp,
				Resolution:    resolution,
			}
		}
	}

	return &ConflictResult{
		HasConflict: false,
	}
}

// hasConflict 检查两个操作是否冲突
func (cd *ConflictDetector) hasConflict(op1, op2 *Operation) bool {
	// 不同资源不冲突
	if op1.ResourceType != op2.ResourceType || op1.ResourceID != op2.ResourceID {
		return false
	}

	// 同一用户同一会话不冲突
	if op1.UserID == op2.UserID && op1.SessionID == op2.SessionID {
		return false
	}

	// 检查时间窗口冲突
	timeDiff := op1.Timestamp.Sub(op2.Timestamp)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}

	// 5秒内的操作可能冲突
	if timeDiff > 5*time.Second {
		return false
	}

	// 检查数据字段冲突
	return cd.hasDataConflict(op1.Data, op2.Data)
}

// hasDataConflict 检查数据字段冲突
func (cd *ConflictDetector) hasDataConflict(data1, data2 map[string]interface{}) bool {
	for key := range data1 {
		if _, exists := data2[key]; exists {
			return true // 修改了相同字段
		}
	}
	return false
}

// determineConflictType 确定冲突类型
func (cd *ConflictDetector) determineConflictType(op1, op2 *Operation) string {
	if op1.Type == "delete" || op2.Type == "delete" {
		return "delete_conflict"
	}

	if op1.Type == "update" && op2.Type == "update" {
		return "concurrent_update"
	}

	if op1.Type == "create" && op2.Type == "create" {
		return "duplicate_create"
	}

	return "unknown_conflict"
}

// generateResolution 生成冲突解决方案
func (cd *ConflictDetector) generateResolution(op1, op2 *Operation, conflictType string) map[string]interface{} {
	resolution := make(map[string]interface{})

	switch conflictType {
	case "concurrent_update":
		// 合并更新策略
		resolution["strategy"] = "merge"
		resolution["merged_data"] = cd.mergeData(op1.Data, op2.Data)
		resolution["winner"] = cd.selectWinner(op1, op2)

	case "delete_conflict":
		// 删除冲突策略
		resolution["strategy"] = "delete_wins"
		resolution["action"] = "confirm_delete"

	case "duplicate_create":
		// 重复创建策略
		resolution["strategy"] = "latest_wins"
		resolution["winner"] = cd.selectWinner(op1, op2)

	default:
		resolution["strategy"] = "manual_resolve"
	}

	return resolution
}

// mergeData 合并数据
func (cd *ConflictDetector) mergeData(data1, data2 map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})

	// 复制第一个操作的数据
	for key, value := range data1 {
		merged[key] = value
	}

	// 合并第二个操作的数据（后者优先）
	for key, value := range data2 {
		merged[key] = value
	}

	return merged
}

// selectWinner 选择获胜操作
func (cd *ConflictDetector) selectWinner(op1, op2 *Operation) string {
	// 版本号高的获胜
	if op1.Version > op2.Version {
		return op1.ID
	}
	if op2.Version > op1.Version {
		return op2.ID
	}

	// 时间戳晚的获胜
	if op1.Timestamp.After(op2.Timestamp) {
		return op1.ID
	}

	return op2.ID
}

// OperationQueue 操作队列
type OperationQueue struct {
	operations map[string][]*Operation // resourceKey -> operations
	mu         sync.RWMutex
}

// NewOperationQueue 创建操作队列
func NewOperationQueue() *OperationQueue {
	return &OperationQueue{
		operations: make(map[string][]*Operation),
	}
}

// AddOperation 添加操作到队列
func (oq *OperationQueue) AddOperation(op *Operation) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	resourceKey := fmt.Sprintf("%s:%s", op.ResourceType, op.ResourceID)
	oq.operations[resourceKey] = append(oq.operations[resourceKey], op)
}

// GetOperations 获取资源的操作列表
func (oq *OperationQueue) GetOperations(resourceType, resourceID string) []*Operation {
	oq.mu.RLock()
	defer oq.mu.RUnlock()

	resourceKey := fmt.Sprintf("%s:%s", resourceType, resourceID)
	ops := oq.operations[resourceKey]

	// 返回副本
	result := make([]*Operation, len(ops))
	copy(result, ops)
	return result
}

// RemoveOperation 移除操作
func (oq *OperationQueue) RemoveOperation(resourceType, resourceID, operationID string) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	resourceKey := fmt.Sprintf("%s:%s", resourceType, resourceID)
	ops := oq.operations[resourceKey]

	for i, op := range ops {
		if op.ID == operationID {
			oq.operations[resourceKey] = append(ops[:i], ops[i+1:]...)
			break
		}
	}
}

// CleanupOldOperations 清理旧操作
func (oq *OperationQueue) CleanupOldOperations(maxAge time.Duration) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)

	for resourceKey, ops := range oq.operations {
		var filtered []*Operation
		for _, op := range ops {
			if op.Timestamp.After(cutoff) {
				filtered = append(filtered, op)
			}
		}
		oq.operations[resourceKey] = filtered
	}
}

// ExecuteWithConcurrencyControl 在并发控制下执行操作
func (ccs *ConcurrencyControlService) ExecuteWithConcurrencyControl(
	ctx context.Context,
	op *Operation,
	executor func(context.Context, *Operation) error,
) error {
	// 1. 尝试获取锁
	lockReq := &LockRequest{
		ResourceID:   op.ResourceID,
		ResourceType: op.ResourceType,
		LockType:     LockTypeWrite,
		UserID:       op.UserID,
		SessionID:    op.SessionID,
		Timeout:      30 * time.Second,
	}

	_, err := ccs.lockManager.AcquireLock(ctx, lockReq)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	defer func() {
		if releaseErr := ccs.lockManager.ReleaseLock(
			op.ResourceType, op.ResourceID, op.UserID, op.SessionID,
		); releaseErr != nil {
			ccs.logger.Error("Failed to release lock",
				logger.String("resource_type", op.ResourceType),
				logger.String("resource_id", op.ResourceID),
				logger.ErrorField(releaseErr),
			)
		}
	}()

	// 2. 检测冲突
	existingOps := ccs.operationQueue.GetOperations(op.ResourceType, op.ResourceID)
	conflictResult := ccs.conflictDetector.DetectConflict(ctx, op, existingOps)

	if conflictResult.HasConflict {
		ccs.logger.Warn("Operation conflict detected",
			logger.String("operation_id", op.ID),
			logger.String("conflict_type", conflictResult.ConflictType),
			logger.String("conflicting_op_id", conflictResult.ConflictingOp.ID),
		)

		// 根据冲突类型处理
		if err := ccs.handleConflict(ctx, op, conflictResult); err != nil {
			return fmt.Errorf("failed to handle conflict: %w", err)
		}
	}

	// 3. 添加操作到队列
	ccs.operationQueue.AddOperation(op)

	// 4. 执行操作
	if err := executor(ctx, op); err != nil {
		// 执行失败，从队列中移除
		ccs.operationQueue.RemoveOperation(op.ResourceType, op.ResourceID, op.ID)
		return fmt.Errorf("failed to execute operation: %w", err)
	}

	ccs.logger.Info("Operation executed successfully",
		logger.String("operation_id", op.ID),
		logger.String("resource_type", op.ResourceType),
		logger.String("resource_id", op.ResourceID),
		logger.String("user_id", op.UserID),
	)

	return nil
}

// handleConflict 处理冲突
func (ccs *ConcurrencyControlService) handleConflict(ctx context.Context, op *Operation, conflict *ConflictResult) error {
	strategy, ok := conflict.Resolution["strategy"].(string)
	if !ok {
		return fmt.Errorf("invalid conflict resolution strategy")
	}

	switch strategy {
	case "merge":
		// 合并数据
		if mergedData, ok := conflict.Resolution["merged_data"].(map[string]interface{}); ok {
			op.Data = mergedData
		}

	case "delete_wins":
		// 删除操作获胜，确认删除
		if op.Type != "delete" {
			return fmt.Errorf("operation cancelled due to delete conflict")
		}

	case "latest_wins":
		// 最新操作获胜
		if winner, ok := conflict.Resolution["winner"].(string); ok && winner != op.ID {
			return fmt.Errorf("operation cancelled, latest operation wins")
		}

	case "manual_resolve":
		// 需要手动解决
		return fmt.Errorf("manual conflict resolution required")

	default:
		return fmt.Errorf("unknown conflict resolution strategy: %s", strategy)
	}

	return nil
}

// StartCleanupTasks 启动清理任务
func (ccs *ConcurrencyControlService) StartCleanupTasks(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ccs.lockManager.CleanupExpiredLocks()
			ccs.operationQueue.CleanupOldOperations(10 * time.Minute)
		}
	}
}

// GetConcurrencyStats 获取并发控制统计信息
func (ccs *ConcurrencyControlService) GetConcurrencyStats() map[string]interface{} {
	activeLocks := ccs.lockManager.GetActiveLocks()

	return map[string]interface{}{
		"active_locks": len(activeLocks),
		"lock_details": activeLocks,
	}
}
