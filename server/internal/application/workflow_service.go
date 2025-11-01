package application

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"

	"gorm.io/gorm"
)

// WorkflowService 工作流服务
type WorkflowService struct {
	db *gorm.DB
}

// NewWorkflowService 创建工作流服务
func NewWorkflowService(db *gorm.DB) *WorkflowService {
	return &WorkflowService{db: db}
}

// Create 创建工作流
func (s *WorkflowService) Create(ctx context.Context, workflow *models.Workflow) error {
	workflow.ID = utils.GenerateIDWithPrefix("wfl")
	workflow.CreatedTime = time.Now()
	// LastModifiedTime 将由 BeforeCreate 钩子自动设置

	return s.db.WithContext(ctx).Create(workflow).Error
}

// GetByID 根据ID获取工作流
func (s *WorkflowService) GetByID(ctx context.Context, id string) (*models.Workflow, error) {
	var workflow models.Workflow
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&workflow).Error
	if err != nil {
		return nil, err
	}
	return &workflow, nil
}

// List 列出工作流
func (s *WorkflowService) List(ctx context.Context, page, limit int) ([]*models.Workflow, int64, error) {
	var workflows []*models.Workflow
	var total int64

	// 计数
	if err := s.db.WithContext(ctx).Model(&models.Workflow{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 分页查询
	offset := (page - 1) * limit
	err := s.db.WithContext(ctx).
		Order("created_time DESC").
		Offset(offset).
		Limit(limit).
		Find(&workflows).Error

	return workflows, total, err
}

// Update 更新工作流
func (s *WorkflowService) Update(ctx context.Context, id string, workflow *models.Workflow) error {
	// LastModifiedTime 将由 BeforeUpdate 钩子自动更新
	return s.db.WithContext(ctx).
		Model(&models.Workflow{}).
		Where("id = ?", id).
		Updates(workflow).Error
}

// Delete 删除工作流
func (s *WorkflowService) Delete(ctx context.Context, id string) error {
	return s.db.WithContext(ctx).Delete(&models.Workflow{}, "id = ?", id).Error
}

// Run 运行工作流
func (s *WorkflowService) Run(ctx context.Context, workflowID, userID string, input map[string]interface{}) (*models.WorkflowRun, error) {
	now := time.Now()
	run := &models.WorkflowRun{
		ID:          utils.GenerateIDWithPrefix("wfr"),
		WorkflowID:  workflowID,
		CreatedBy:   userID,
		Status:      "running",
		StartedTime: &now,
	}

	if err := s.db.WithContext(ctx).Create(run).Error; err != nil {
		return nil, err
	}

	// 工作流执行逻辑应该在后台异步执行（参考 teable-develop 使用 BullMQ）
	// 这里只是创建了运行记录

	return run, nil
}

// GetRun 获取工作流运行记录
func (s *WorkflowService) GetRun(ctx context.Context, runID string) (*models.WorkflowRun, error) {
	var run models.WorkflowRun
	err := s.db.WithContext(ctx).Where("id = ?", runID).First(&run).Error
	if err != nil {
		return nil, err
	}
	return &run, nil
}

// ListRuns 列出工作流运行记录
func (s *WorkflowService) ListRuns(ctx context.Context, workflowID string, page, limit int) ([]*models.WorkflowRun, int64, error) {
	var runs []*models.WorkflowRun
	var total int64

	query := s.db.WithContext(ctx).Model(&models.WorkflowRun{})
	if workflowID != "" {
		query = query.Where("workflow_id = ?", workflowID)
	}

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	err := query.Order("started_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&runs).Error

	return runs, total, err
}

// StopRun 停止工作流运行
func (s *WorkflowService) StopRun(ctx context.Context, runID string) error {
	now := time.Now()
	return s.db.WithContext(ctx).
		Model(&models.WorkflowRun{}).
		Where("id = ?", runID).
		Updates(map[string]interface{}{
			"status":       "stopped",
			"completed_at": &now,
		}).Error
}

// GetStats 获取工作流统计信息
func (s *WorkflowService) GetStats(ctx context.Context, workflowID string) (map[string]interface{}, error) {
	var totalRuns int64
	var successRuns int64

	// 总运行次数
	s.db.WithContext(ctx).Model(&models.WorkflowRun{}).
		Where("workflow_id = ?", workflowID).
		Count(&totalRuns)

	// 成功运行次数
	s.db.WithContext(ctx).Model(&models.WorkflowRun{}).
		Where("workflow_id = ? AND status = ?", workflowID, "completed").
		Count(&successRuns)

	successRate := 0.0
	if totalRuns > 0 {
		successRate = float64(successRuns) / float64(totalRuns)
	}

	return map[string]interface{}{
		"total_runs":   totalRuns,
		"success_runs": successRuns,
		"success_rate": successRate,
	}, nil
}
