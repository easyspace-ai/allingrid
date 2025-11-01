package jsvm

import (
	"fmt"
	"sync"

	"github.com/dop251/goja"
	"go.uber.org/zap"
)

// PluginManager 插件管理器
// 借鉴 PocketBase 的插件系统，但简化实现
type PluginManager struct {
	plugins map[string]*Plugin
	mu      sync.RWMutex
	logger  *zap.Logger
}

// Plugin 插件定义
type Plugin struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Version     string                 `json:"version"`
	Description string                 `json:"description"`
	Author      string                 `json:"author"`
	Config      map[string]interface{} `json:"config"`
	Runtime     *goja.Runtime          `json:"-"`
	FilePath    string                 `json:"file_path"`
	IsEnabled   bool                   `json:"is_enabled"`
	Hooks       []string               `json:"hooks"`
}

// NewPluginManager 创建插件管理器
func NewPluginManager(logger *zap.Logger) *PluginManager {
	return &PluginManager{
		plugins: make(map[string]*Plugin),
		logger:  logger,
	}
}

// RegisterPlugin 注册插件
func (pm *PluginManager) RegisterPlugin(plugin *Plugin) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.plugins[plugin.ID] = plugin
	pm.logger.Info("Plugin registered",
		zap.String("id", plugin.ID),
		zap.String("name", plugin.Name),
		zap.String("version", plugin.Version))
}

// RegisterPluginFromRuntime 从运行时注册插件
func (pm *PluginManager) RegisterPluginFromRuntime(vm *goja.Runtime, filePath string) {
	// 尝试从运行时获取插件配置
	pluginConfig := vm.Get("pluginConfig")
	if pluginConfig == nil {
		// 如果没有插件配置，创建一个默认的
		plugin := &Plugin{
			ID:          "default_" + filePath,
			Name:        "Default Plugin",
			Version:     "1.0.0",
			Description: "Auto-generated plugin",
			Author:      "Unknown",
			Config:      make(map[string]interface{}),
			Runtime:     vm,
			FilePath:    filePath,
			IsEnabled:   true,
			Hooks:       []string{},
		}
		pm.RegisterPlugin(plugin)
		return
	}

	// 解析插件配置
	if configObj, ok := pluginConfig.(*goja.Object); ok {
		plugin := &Plugin{
			Runtime:   vm,
			FilePath:  filePath,
			IsEnabled: true,
			Config:    make(map[string]interface{}),
			Hooks:     []string{},
		}

		// 提取配置信息
		if id := configObj.Get("id"); id != nil {
			plugin.ID = id.String()
		}
		if name := configObj.Get("name"); name != nil {
			plugin.Name = name.String()
		}
		if version := configObj.Get("version"); version != nil {
			plugin.Version = version.String()
		}
		if description := configObj.Get("description"); description != nil {
			plugin.Description = description.String()
		}
		if author := configObj.Get("author"); author != nil {
			plugin.Author = author.String()
		}

		// 提取钩子列表
		if hooks := configObj.Get("hooks"); hooks != nil {
			if hooksArray, ok := hooks.Export().([]interface{}); ok {
				for _, hook := range hooksArray {
					if hookStr, ok := hook.(string); ok {
						plugin.Hooks = append(plugin.Hooks, hookStr)
					}
				}
			}
		}

		// 提取配置
		if config := configObj.Get("config"); config != nil {
			if configMap, ok := config.Export().(map[string]interface{}); ok {
				plugin.Config = configMap
			}
		}

		pm.RegisterPlugin(plugin)
	}
}

// GetPlugin 获取插件
func (pm *PluginManager) GetPlugin(id string) (*Plugin, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugin, exists := pm.plugins[id]
	if !exists {
		return nil, fmt.Errorf("plugin not found: %s", id)
	}

	return plugin, nil
}

// ListPlugins 获取插件列表
func (pm *PluginManager) ListPlugins() []*Plugin {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugins := make([]*Plugin, 0, len(pm.plugins))
	for _, plugin := range pm.plugins {
		plugins = append(plugins, plugin)
	}

	return plugins
}

// EnablePlugin 启用插件
func (pm *PluginManager) EnablePlugin(id string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	plugin, exists := pm.plugins[id]
	if !exists {
		return fmt.Errorf("plugin not found: %s", id)
	}

	plugin.IsEnabled = true
	pm.logger.Info("Plugin enabled", zap.String("id", id))
	return nil
}

// DisablePlugin 禁用插件
func (pm *PluginManager) DisablePlugin(id string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	plugin, exists := pm.plugins[id]
	if !exists {
		return fmt.Errorf("plugin not found: %s", id)
	}

	plugin.IsEnabled = false
	pm.logger.Info("Plugin disabled", zap.String("id", id))
	return nil
}

// RemovePlugin 移除插件
func (pm *PluginManager) RemovePlugin(id string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	_, exists := pm.plugins[id]
	if !exists {
		return fmt.Errorf("plugin not found: %s", id)
	}

	delete(pm.plugins, id)
	pm.logger.Info("Plugin removed", zap.String("id", id))
	return nil
}

// UpdatePluginConfig 更新插件配置
func (pm *PluginManager) UpdatePluginConfig(id string, config map[string]interface{}) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	plugin, exists := pm.plugins[id]
	if !exists {
		return fmt.Errorf("plugin not found: %s", id)
	}

	plugin.Config = config
	pm.logger.Info("Plugin config updated", zap.String("id", id))
	return nil
}

// ExecutePluginMethod 执行插件方法
func (pm *PluginManager) ExecutePluginMethod(pluginID, methodName string, args ...interface{}) (interface{}, error) {
	pm.mu.RLock()
	plugin, exists := pm.plugins[pluginID]
	pm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("plugin not found: %s", pluginID)
	}

	if !plugin.IsEnabled {
		return nil, fmt.Errorf("plugin is disabled: %s", pluginID)
	}

	// 获取方法
	method := plugin.Runtime.Get(methodName)
	if method == nil {
		return nil, fmt.Errorf("method not found: %s", methodName)
	}

	// 转换为可调用对象
	callable, ok := method.Export().(func(...interface{}) interface{})
	if !ok {
		return nil, fmt.Errorf("method is not callable: %s", methodName)
	}

	// 执行方法
	result := callable(args...)

	return result, nil
}

// GetPluginCount 获取插件数量
func (pm *PluginManager) GetPluginCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return len(pm.plugins)
}

// GetEnabledPluginCount 获取启用的插件数量
func (pm *PluginManager) GetEnabledPluginCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	count := 0
	for _, plugin := range pm.plugins {
		if plugin.IsEnabled {
			count++
		}
	}

	return count
}

// GetPluginStats 获取插件统计信息
func (pm *PluginManager) GetPluginStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	stats := map[string]interface{}{
		"total":    len(pm.plugins),
		"enabled":  0,
		"disabled": 0,
	}

	for _, plugin := range pm.plugins {
		if plugin.IsEnabled {
			stats["enabled"] = stats["enabled"].(int) + 1
		} else {
			stats["disabled"] = stats["disabled"].(int) + 1
		}
	}

	return stats
}
