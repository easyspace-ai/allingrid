package jsvm

import (
	"fmt"
	"os"
	"sync"

	"github.com/dop251/goja"
	"go.uber.org/zap"
)

// RuntimePool JavaScript 运行时池
// 借鉴 PocketBase 的运行时池设计，但简化实现
type RuntimePool struct {
	pool   chan *goja.Runtime
	config *RuntimeConfig
	logger *zap.Logger
	mu     sync.RWMutex
	closed bool
}

// RuntimeConfig 运行时配置
type RuntimeConfig struct {
	Size   int
	OnInit func(vm *goja.Runtime)
}

// NewRuntimePool 创建运行时池
func NewRuntimePool(size int, onInit func(vm *goja.Runtime), logger *zap.Logger) *RuntimePool {
	if size <= 0 {
		size = 10 // 默认大小
	}

	pool := &RuntimePool{
		pool: make(chan *goja.Runtime, size),
		config: &RuntimeConfig{
			Size:   size,
			OnInit: onInit,
		},
		logger: logger,
	}

	// 预创建运行时实例
	for i := 0; i < size; i++ {
		vm := pool.createRuntime()
		pool.pool <- vm
	}

	return pool
}

// createRuntime 创建新的运行时实例
func (p *RuntimePool) createRuntime() *goja.Runtime {
	vm := goja.New()

	// 设置基础 API（简化版本）
	vm.Set("console", map[string]interface{}{
		"log": func(args ...interface{}) {
			fmt.Println(args...)
		},
		"error": func(args ...interface{}) {
			fmt.Fprintln(os.Stderr, args...)
		},
	})

	vm.Set("process", map[string]interface{}{
		"env": map[string]string{},
	})

	// 设置全局变量
	vm.Set("global", vm.GlobalObject())
	vm.Set("globalThis", vm.GlobalObject())

	// 设置自定义 API
	p.setupCustomAPI(vm)

	// 调用初始化回调
	if p.config.OnInit != nil {
		p.config.OnInit(vm)
	}

	return vm
}

// setupCustomAPI 设置自定义 API
func (p *RuntimePool) setupCustomAPI(vm *goja.Runtime) {
	// 设置日志 API
	vm.Set("log", map[string]interface{}{
		"info": func(args ...interface{}) {
			p.logger.Info("JS log", zap.Any("args", args))
		},
		"error": func(args ...interface{}) {
			p.logger.Error("JS error", zap.Any("args", args))
		},
		"warn": func(args ...interface{}) {
			p.logger.Warn("JS warn", zap.Any("args", args))
		},
		"debug": func(args ...interface{}) {
			p.logger.Debug("JS debug", zap.Any("args", args))
		},
	})

	// 设置工具 API
	vm.Set("utils", map[string]interface{}{
		"uuid": func() string {
			// 简单的 UUID 生成
			return "uuid-" + string(rune(len(p.pool)))
		},
		"now": func() int64 {
			// 返回当前时间戳
			return 0 // 这里应该返回实际的时间戳
		},
	})

	// 设置事件 API
	vm.Set("events", map[string]interface{}{
		"emit": func(event string, data interface{}) {
			// 这里可以触发 Go 端的事件
			p.logger.Info("JS event emitted",
				zap.String("event", event),
				zap.Any("data", data))
		},
	})
}

// Get 获取运行时实例
func (p *RuntimePool) Get() *goja.Runtime {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		// 如果池已关闭，创建新的运行时
		return p.createRuntime()
	}
	p.mu.RUnlock()

	select {
	case vm := <-p.pool:
		return vm
	default:
		// 如果池为空，创建新的运行时
		return p.createRuntime()
	}
}

// Put 归还运行时实例
func (p *RuntimePool) Put(vm *goja.Runtime) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	select {
	case p.pool <- vm:
		// 成功归还
	default:
		// 池已满，丢弃运行时
	}
}

// Close 关闭运行时池
func (p *RuntimePool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closed = true
	close(p.pool)

	// 清空池中的所有运行时
	for vm := range p.pool {
		_ = vm // 忽略运行时
	}

	p.logger.Info("Runtime pool closed")
}

// Size 获取池大小
func (p *RuntimePool) Size() int {
	return len(p.pool)
}

// Capacity 获取池容量
func (p *RuntimePool) Capacity() int {
	return cap(p.pool)
}
