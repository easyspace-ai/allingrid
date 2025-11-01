package jsvm

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dop251/goja"
	"go.uber.org/zap"
)

// HookManager 钩子管理器
// 借鉴 PocketBase 的钩子系统，但简化实现
type HookManager struct {
	hooks  map[string][]*Hook
	mu     sync.RWMutex
	logger *zap.Logger
}

// Hook 钩子定义
type Hook struct {
	Name     string
	Function func(...interface{}) interface{}
	Priority int
	Runtime  *goja.Runtime
	FilePath string
}

// NewHookManager 创建钩子管理器
func NewHookManager(logger *zap.Logger) *HookManager {
	return &HookManager{
		hooks:  make(map[string][]*Hook),
		logger: logger,
	}
}

// RegisterHook 注册钩子
func (hm *HookManager) RegisterHook(hook *Hook) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.hooks[hook.Name] = append(hm.hooks[hook.Name], hook)

	// 按优先级排序
	hm.sortHooks(hook.Name)

	hm.logger.Info("Hook registered",
		zap.String("name", hook.Name),
		zap.String("file", hook.FilePath),
		zap.Int("priority", hook.Priority))
}

// RegisterHooksFromRuntime 从运行时注册钩子
func (hm *HookManager) RegisterHooksFromRuntime(vm *goja.Runtime, filePath string) {
	// 常见的钩子名称
	hookNames := []string{
		"onUserCreate",
		"onUserUpdate",
		"onUserDelete",
		"onTableCreate",
		"onTableUpdate",
		"onTableDelete",
		"onRecordCreate",
		"onRecordUpdate",
		"onRecordDelete",
		"onRequest",
		"onResponse",
		"onError",
	}

	for _, hookName := range hookNames {
		if hookFunc := vm.Get(hookName); hookFunc != nil {
			if callable, ok := hookFunc.Export().(func(...interface{}) interface{}); ok {
				hook := &Hook{
					Name:     hookName,
					Function: callable,
					Priority: 0, // 默认优先级
					Runtime:  vm,
					FilePath: filePath,
				}
				hm.RegisterHook(hook)
			}
		}
	}
}

// TriggerHook 触发钩子
func (hm *HookManager) TriggerHook(hookName string, data interface{}) error {
	hm.mu.RLock()
	hooks := hm.hooks[hookName]
	hm.mu.RUnlock()

	if len(hooks) == 0 {
		return nil // 没有钩子，直接返回
	}

	hm.logger.Debug("Triggering hook",
		zap.String("name", hookName),
		zap.Int("count", len(hooks)))

	var errors []error

	for _, hook := range hooks {
		if err := hm.executeHook(hook, data); err != nil {
			errors = append(errors, fmt.Errorf("hook %s failed: %w", hook.Name, err))
			hm.logger.Error("Hook execution failed",
				zap.String("name", hook.Name),
				zap.String("file", hook.FilePath),
				zap.Error(err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("hook execution errors: %v", errors)
	}

	return nil
}

// executeHook 执行单个钩子
func (hm *HookManager) executeHook(hook *Hook, data interface{}) error {
	// 序列化数据
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// 执行钩子函数
	hook.Function(string(dataJSON))

	return nil
}

// sortHooks 按优先级排序钩子
func (hm *HookManager) sortHooks(hookName string) {
	hooks := hm.hooks[hookName]

	// 简单的冒泡排序（按优先级降序）
	for i := 0; i < len(hooks)-1; i++ {
		for j := 0; j < len(hooks)-i-1; j++ {
			if hooks[j].Priority < hooks[j+1].Priority {
				hooks[j], hooks[j+1] = hooks[j+1], hooks[j]
			}
		}
	}
}

// GetHooks 获取指定名称的钩子
func (hm *HookManager) GetHooks(hookName string) []*Hook {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	hooks := hm.hooks[hookName]
	result := make([]*Hook, len(hooks))
	copy(result, hooks)

	return result
}

// GetAllHooks 获取所有钩子
func (hm *HookManager) GetAllHooks() map[string][]*Hook {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	result := make(map[string][]*Hook)
	for name, hooks := range hm.hooks {
		result[name] = make([]*Hook, len(hooks))
		copy(result[name], hooks)
	}

	return result
}

// RemoveHooksFromFile 移除指定文件的钩子
func (hm *HookManager) RemoveHooksFromFile(filePath string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	for hookName, hooks := range hm.hooks {
		var newHooks []*Hook
		for _, hook := range hooks {
			if hook.FilePath != filePath {
				newHooks = append(newHooks, hook)
			}
		}
		hm.hooks[hookName] = newHooks
	}

	hm.logger.Info("Hooks removed from file", zap.String("file", filePath))
}

// ClearHooks 清空所有钩子
func (hm *HookManager) ClearHooks() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.hooks = make(map[string][]*Hook)
	hm.logger.Info("All hooks cleared")
}

// GetHookCount 获取钩子数量
func (hm *HookManager) GetHookCount() int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	count := 0
	for _, hooks := range hm.hooks {
		count += len(hooks)
	}

	return count
}

// GetHookNames 获取所有钩子名称
func (hm *HookManager) GetHookNames() []string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	names := make([]string, 0, len(hm.hooks))
	for name := range hm.hooks {
		names = append(names, name)
	}

	return names
}
