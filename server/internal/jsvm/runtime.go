package jsvm

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/dop251/goja"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

// RuntimeManager JavaScript 运行时管理器
// 借鉴 PocketBase JSVM 的优点，但完全独立实现
type RuntimeManager struct {
	config        *Config
	runtimePool   *RuntimePool
	hookManager   *HookManager
	pluginManager *PluginManager
	watcher       *fsnotify.Watcher
	logger        *zap.Logger
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
}

// Config JSVM 配置
type Config struct {
	// HooksDir 钩子文件目录
	HooksDir string

	// HooksWatch 是否启用热重载
	HooksWatch bool

	// HooksPoolSize 运行时池大小
	HooksPoolSize int

	// PluginsDir 插件目录
	PluginsDir string

	// HooksFilesPattern 钩子文件匹配模式
	HooksFilesPattern string

	// PluginsFilesPattern 插件文件匹配模式
	PluginsFilesPattern string

	// OnInit 运行时初始化回调
	OnInit func(vm *goja.Runtime)
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		HooksDir:            "./hooks",
		HooksWatch:          true,
		HooksPoolSize:       10,
		PluginsDir:          "./plugins",
		HooksFilesPattern:   `^.*\.js$`,
		PluginsFilesPattern: `^.*\.js$`,
	}
}

// NewRuntimeManager 创建运行时管理器
func NewRuntimeManager(config *Config, logger *zap.Logger) (*RuntimeManager, error) {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	rm := &RuntimeManager{
		config: config,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// 创建运行时池
	rm.runtimePool = NewRuntimePool(config.HooksPoolSize, config.OnInit, logger)

	// 创建钩子管理器
	rm.hookManager = NewHookManager(logger)

	// 创建插件管理器
	rm.pluginManager = NewPluginManager(logger)

	// 初始化文件监控
	if config.HooksWatch {
		if err := rm.initWatcher(); err != nil {
			logger.Warn("Failed to initialize file watcher", zap.Error(err))
		}
	}

	return rm, nil
}

// initWatcher 初始化文件监控
func (rm *RuntimeManager) initWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	rm.watcher = watcher

	// 监控钩子目录
	if err := rm.watchDirectory(rm.config.HooksDir); err != nil {
		rm.logger.Warn("Failed to watch hooks directory", zap.Error(err))
	}

	// 监控插件目录
	if err := rm.watchDirectory(rm.config.PluginsDir); err != nil {
		rm.logger.Warn("Failed to watch plugins directory", zap.Error(err))
	}

	// 启动监控协程
	go rm.watchFiles()

	return nil
}

// watchDirectory 监控目录
func (rm *RuntimeManager) watchDirectory(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// 创建目录
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	return rm.watcher.Add(dir)
}

// watchFiles 监控文件变化
func (rm *RuntimeManager) watchFiles() {
	defer rm.watcher.Close()

	for {
		select {
		case event, ok := <-rm.watcher.Events:
			if !ok {
				return
			}

			if event.Op&fsnotify.Write == fsnotify.Write {
				rm.handleFileChange(event.Name)
			}

		case err, ok := <-rm.watcher.Errors:
			if !ok {
				return
			}
			rm.logger.Error("File watcher error", zap.Error(err))

		case <-rm.ctx.Done():
			return
		}
	}
}

// handleFileChange 处理文件变化
func (rm *RuntimeManager) handleFileChange(filename string) {
	rm.logger.Info("File changed, reloading", zap.String("file", filename))

	// 判断是钩子文件还是插件文件
	if rm.isHookFile(filename) {
		rm.reloadHooks()
	} else if rm.isPluginFile(filename) {
		rm.reloadPlugins()
	}
}

// isHookFile 判断是否为钩子文件
func (rm *RuntimeManager) isHookFile(filename string) bool {
	pattern, err := regexp.Compile(rm.config.HooksFilesPattern)
	if err != nil {
		return false
	}

	baseName := filepath.Base(filename)
	return pattern.MatchString(baseName)
}

// isPluginFile 判断是否为插件文件
func (rm *RuntimeManager) isPluginFile(filename string) bool {
	pattern, err := regexp.Compile(rm.config.PluginsFilesPattern)
	if err != nil {
		return false
	}

	baseName := filepath.Base(filename)
	return pattern.MatchString(baseName)
}

// LoadHooks 加载钩子
func (rm *RuntimeManager) LoadHooks() error {
	return rm.loadHooksFromDir(rm.config.HooksDir)
}

// loadHooksFromDir 从目录加载钩子
func (rm *RuntimeManager) loadHooksFromDir(dir string) error {
	pattern, err := regexp.Compile(rm.config.HooksFilesPattern)
	if err != nil {
		return err
	}

	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && pattern.MatchString(d.Name()) {
			if err := rm.loadHookFile(path); err != nil {
				rm.logger.Error("Failed to load hook file",
					zap.String("file", path),
					zap.Error(err))
			}
		}

		return nil
	})
}

// loadHookFile 加载钩子文件
func (rm *RuntimeManager) loadHookFile(filePath string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// 获取运行时
	vm := rm.runtimePool.Get()
	defer rm.runtimePool.Put(vm)

	// 执行钩子代码
	_, err = vm.RunString(string(content))
	if err != nil {
		return err
	}

	// 注册钩子
	rm.hookManager.RegisterHooksFromRuntime(vm, filePath)

	rm.logger.Info("Hook file loaded", zap.String("file", filePath))
	return nil
}

// LoadPlugins 加载插件
func (rm *RuntimeManager) LoadPlugins() error {
	return rm.loadPluginsFromDir(rm.config.PluginsDir)
}

// loadPluginsFromDir 从目录加载插件
func (rm *RuntimeManager) loadPluginsFromDir(dir string) error {
	pattern, err := regexp.Compile(rm.config.PluginsFilesPattern)
	if err != nil {
		return err
	}

	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && pattern.MatchString(d.Name()) {
			if err := rm.loadPluginFile(path); err != nil {
				rm.logger.Error("Failed to load plugin file",
					zap.String("file", path),
					zap.Error(err))
			}
		}

		return nil
	})
}

// loadPluginFile 加载插件文件
func (rm *RuntimeManager) loadPluginFile(filePath string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// 获取运行时
	vm := rm.runtimePool.Get()
	defer rm.runtimePool.Put(vm)

	// 执行插件代码
	_, err = vm.RunString(string(content))
	if err != nil {
		return err
	}

	// 注册插件
	rm.pluginManager.RegisterPluginFromRuntime(vm, filePath)

	rm.logger.Info("Plugin file loaded", zap.String("file", filePath))
	return nil
}

// TriggerHook 触发钩子
func (rm *RuntimeManager) TriggerHook(hookName string, data interface{}) error {
	return rm.hookManager.TriggerHook(hookName, data)
}

// GetHookManager 获取钩子管理器
func (rm *RuntimeManager) GetHookManager() *HookManager {
	return rm.hookManager
}

// GetPluginManager 获取插件管理器
func (rm *RuntimeManager) GetPluginManager() *PluginManager {
	return rm.pluginManager
}

// GetRuntimePool 获取运行时池
func (rm *RuntimeManager) GetRuntimePool() *RuntimePool {
	return rm.runtimePool
}

// reloadHooks 重新加载钩子
func (rm *RuntimeManager) reloadHooks() {
	rm.logger.Info("Reloading hooks...")
	if err := rm.LoadHooks(); err != nil {
		rm.logger.Error("Failed to reload hooks", zap.Error(err))
	}
}

// reloadPlugins 重新加载插件
func (rm *RuntimeManager) reloadPlugins() {
	rm.logger.Info("Reloading plugins...")
	if err := rm.LoadPlugins(); err != nil {
		rm.logger.Error("Failed to reload plugins", zap.Error(err))
	}
}

// Shutdown 关闭运行时管理器
func (rm *RuntimeManager) Shutdown() error {
	rm.cancel()

	if rm.watcher != nil {
		rm.watcher.Close()
	}

	if rm.runtimePool != nil {
		rm.runtimePool.Close()
	}

	rm.logger.Info("Runtime manager shutdown completed")
	return nil
}
