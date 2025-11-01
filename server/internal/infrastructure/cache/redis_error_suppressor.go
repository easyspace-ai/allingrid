package cache

import (
	"go.uber.org/zap"
)

// SuppressRedisErrors 抑制Redis关闭时的错误输出
// 注意：这个方法目前只是包装函数执行，实际的错误抑制需要在应用层面处理
func SuppressRedisErrors(logger *zap.Logger, fn func() error) error {
	// 执行函数，错误抑制在应用层面处理
	return fn()
}
