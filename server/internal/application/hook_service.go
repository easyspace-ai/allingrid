package application

import (
	"context"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/jsvm"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// HookService 钩子服务
// 负责在业务逻辑中触发 JSVM 钩子
type HookService struct {
	jsvmManager *jsvm.RuntimeManager
}

// NewHookService 创建钩子服务
func NewHookService(jsvmManager *jsvm.RuntimeManager) *HookService {
	return &HookService{
		jsvmManager: jsvmManager,
	}
}

// TriggerUserCreateHook 触发用户创建钩子
func (s *HookService) TriggerUserCreateHook(ctx context.Context, userID, email, name string) {
	if s.jsvmManager == nil {
		return
	}

	data := map[string]interface{}{
		"user_id":   userID,
		"email":     email,
		"name":      name,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onUserCreate", data); err != nil {
		logger.Error("用户创建钩子执行失败",
			logger.String("user_id", userID),
			logger.ErrorField(err))
	}
}

// TriggerUserUpdateHook 触发用户更新钩子
func (s *HookService) TriggerUserUpdateHook(ctx context.Context, userID string, updates map[string]interface{}) {
	if s.jsvmManager == nil {
		return
	}

	data := map[string]interface{}{
		"user_id":   userID,
		"updates":   updates,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onUserUpdate", data); err != nil {
		logger.Error("用户更新钩子执行失败",
			logger.String("user_id", userID),
			logger.ErrorField(err))
	}
}

// TriggerUserDeleteHook 触发用户删除钩子
func (s *HookService) TriggerUserDeleteHook(ctx context.Context, userID string) {
	if s.jsvmManager == nil {
		return
	}

	data := map[string]interface{}{
		"user_id":   userID,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onUserDelete", data); err != nil {
		logger.Error("用户删除钩子执行失败",
			logger.String("user_id", userID),
			logger.ErrorField(err))
	}
}

// TriggerRecordCreateHook 触发记录创建钩子
func (s *HookService) TriggerRecordCreateHook(ctx context.Context, tableID, recordID string, data map[string]interface{}) {
	if s.jsvmManager == nil {
		return
	}

	hookData := map[string]interface{}{
		"table_id":  tableID,
		"record_id": recordID,
		"data":      data,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onRecordCreate", hookData); err != nil {
		logger.Error("记录创建钩子执行失败",
			logger.String("table_id", tableID),
			logger.String("record_id", recordID),
			logger.ErrorField(err))
	}
}

// TriggerRecordUpdateHook 触发记录更新钩子
func (s *HookService) TriggerRecordUpdateHook(ctx context.Context, tableID, recordID string, data map[string]interface{}) {
	if s.jsvmManager == nil {
		return
	}

	hookData := map[string]interface{}{
		"table_id":  tableID,
		"record_id": recordID,
		"data":      data,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onRecordUpdate", hookData); err != nil {
		logger.Error("记录更新钩子执行失败",
			logger.String("table_id", tableID),
			logger.String("record_id", recordID),
			logger.ErrorField(err))
	}
}

// TriggerRecordDeleteHook 触发记录删除钩子
func (s *HookService) TriggerRecordDeleteHook(ctx context.Context, tableID, recordID string) {
	if s.jsvmManager == nil {
		return
	}

	hookData := map[string]interface{}{
		"table_id":  tableID,
		"record_id": recordID,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onRecordDelete", hookData); err != nil {
		logger.Error("记录删除钩子执行失败",
			logger.String("table_id", tableID),
			logger.String("record_id", recordID),
			logger.ErrorField(err))
	}
}

// TriggerTableCreateHook 触发表格创建钩子
func (s *HookService) TriggerTableCreateHook(ctx context.Context, tableID, tableName string) {
	if s.jsvmManager == nil {
		return
	}

	data := map[string]interface{}{
		"table_id":   tableID,
		"table_name": tableName,
		"timestamp":  time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onTableCreate", data); err != nil {
		logger.Error("表格创建钩子执行失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
	}
}

// TriggerTableUpdateHook 触发表格更新钩子
func (s *HookService) TriggerTableUpdateHook(ctx context.Context, tableID string, updates map[string]interface{}) {
	if s.jsvmManager == nil {
		return
	}

	data := map[string]interface{}{
		"table_id":  tableID,
		"updates":   updates,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onTableUpdate", data); err != nil {
		logger.Error("表格更新钩子执行失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
	}
}

// TriggerTableDeleteHook 触发表格删除钩子
func (s *HookService) TriggerTableDeleteHook(ctx context.Context, tableID string) {
	if s.jsvmManager == nil {
		return
	}

	data := map[string]interface{}{
		"table_id":  tableID,
		"timestamp": time.Now().Unix(),
	}

	if err := s.jsvmManager.TriggerHook("onTableDelete", data); err != nil {
		logger.Error("表格删除钩子执行失败",
			logger.String("table_id", tableID),
			logger.ErrorField(err))
	}
}
