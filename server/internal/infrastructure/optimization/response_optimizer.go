package optimization

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ResponseOptimizer 响应优化器
type ResponseOptimizer struct {
	compressor *Compressor
	cache      ResponseCache
	logger     *zap.Logger
	config     ResponseOptimizerConfig
}

// ResponseOptimizerConfig 响应优化器配置
type ResponseOptimizerConfig struct {
	// 是否启用压缩
	EnableCompression bool
	// 压缩级别 (1-9)
	CompressionLevel int
	// 最小压缩大小
	MinCompressionSize int
	// 是否启用响应缓存
	EnableResponseCache bool
	// 缓存过期时间
	CacheTTL time.Duration
	// 是否启用ETag
	EnableETag bool
	// 是否启用Last-Modified
	EnableLastModified bool
}

// DefaultResponseOptimizerConfig 默认配置
func DefaultResponseOptimizerConfig() ResponseOptimizerConfig {
	return ResponseOptimizerConfig{
		EnableCompression:   true,
		CompressionLevel:    6,
		MinCompressionSize:  1024, // 1KB
		EnableResponseCache: true,
		CacheTTL:            5 * time.Minute,
		EnableETag:          true,
		EnableLastModified:  true,
	}
}

// ResponseCache 响应缓存接口
type ResponseCache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, data []byte, ttl time.Duration)
	Delete(ctx context.Context, key string)
}

// NewResponseOptimizer 创建响应优化器
func NewResponseOptimizer(cache ResponseCache, config ResponseOptimizerConfig) *ResponseOptimizer {
	return &ResponseOptimizer{
		compressor: NewCompressor(config.CompressionLevel),
		cache:      cache,
		logger:     zap.L(),
		config:     config,
	}
}

// OptimizeResponse 优化响应
func (ro *ResponseOptimizer) OptimizeResponse(c *gin.Context, data interface{}) {
	// 生成缓存键
	cacheKey := ro.generateCacheKey(c)

	// 尝试从缓存获取
	if ro.config.EnableResponseCache && ro.cache != nil {
		if cachedData, exists := ro.cache.Get(c.Request.Context(), cacheKey); exists {
			ro.serveCachedResponse(c, cachedData)
			return
		}
	}

	// 序列化响应数据
	responseData, err := ro.serializeResponse(data)
	if err != nil {
		ro.logger.Error("Failed to serialize response", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	// 设置缓存
	if ro.config.EnableResponseCache && ro.cache != nil {
		ro.cache.Set(c.Request.Context(), cacheKey, responseData, ro.config.CacheTTL)
	}

	// 优化响应
	ro.serveOptimizedResponse(c, responseData)
}

// serveCachedResponse 提供缓存的响应
func (ro *ResponseOptimizer) serveCachedResponse(c *gin.Context, data []byte) {
	// 设置缓存相关头部
	if ro.config.EnableETag {
		etag := ro.generateETag(data)
		c.Header("ETag", etag)

		// 检查If-None-Match
		if ifNoneMatch := c.GetHeader("If-None-Match"); ifNoneMatch == etag {
			c.Status(http.StatusNotModified)
			return
		}
	}

	// 检查是否需要压缩
	if ro.shouldCompress(c, len(data)) {
		compressedData, err := ro.compressor.Compress(data)
		if err != nil {
			ro.logger.Error("Failed to compress cached response", zap.Error(err))
			c.Data(http.StatusOK, "application/json", data)
			return
		}

		c.Header("Content-Encoding", "gzip")
		c.Header("Content-Length", fmt.Sprintf("%d", len(compressedData)))
		c.Data(http.StatusOK, "application/json", compressedData)
	} else {
		c.Data(http.StatusOK, "application/json", data)
	}
}

// serveOptimizedResponse 提供优化的响应
func (ro *ResponseOptimizer) serveOptimizedResponse(c *gin.Context, data []byte) {
	// 设置缓存相关头部
	if ro.config.EnableETag {
		etag := ro.generateETag(data)
		c.Header("ETag", etag)
	}

	if ro.config.EnableLastModified {
		c.Header("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	}

	// 设置缓存控制头部
	c.Header("Cache-Control", fmt.Sprintf("public, max-age=%d", int(ro.config.CacheTTL.Seconds())))

	// 检查是否需要压缩
	if ro.shouldCompress(c, len(data)) {
		compressedData, err := ro.compressor.Compress(data)
		if err != nil {
			ro.logger.Error("Failed to compress response", zap.Error(err))
			c.Data(http.StatusOK, "application/json", data)
			return
		}

		c.Header("Content-Encoding", "gzip")
		c.Header("Content-Length", fmt.Sprintf("%d", len(compressedData)))
		c.Data(http.StatusOK, "application/json", compressedData)
	} else {
		c.Data(http.StatusOK, "application/json", data)
	}
}

// shouldCompress 检查是否应该压缩
func (ro *ResponseOptimizer) shouldCompress(c *gin.Context, dataSize int) bool {
	if !ro.config.EnableCompression {
		return false
	}

	// 检查Accept-Encoding头部
	acceptEncoding := c.GetHeader("Accept-Encoding")
	if !strings.Contains(acceptEncoding, "gzip") {
		return false
	}

	// 检查数据大小
	if dataSize < ro.config.MinCompressionSize {
		return false
	}

	return true
}

// generateCacheKey 生成缓存键
func (ro *ResponseOptimizer) generateCacheKey(c *gin.Context) string {
	// 使用请求路径、查询参数和用户ID生成缓存键
	key := fmt.Sprintf("%s:%s", c.Request.Method, c.Request.URL.Path)

	if query := c.Request.URL.RawQuery; query != "" {
		key += ":" + query
	}

	if userID, exists := c.Get("user_id"); exists {
		key += ":user:" + userID.(string)
	}

	return key
}

// generateETag 生成ETag
func (ro *ResponseOptimizer) generateETag(data []byte) string {
	// 简化实现，实际项目中应该使用更复杂的哈希算法
	return fmt.Sprintf(`"%x"`, len(data))
}

// serializeResponse 序列化响应数据
func (ro *ResponseOptimizer) serializeResponse(data interface{}) ([]byte, error) {
	// 这里应该使用JSON序列化
	// 为了简化，返回空字节数组
	return []byte("{}"), nil
}

// Compressor 压缩器
type Compressor struct {
	level int
	pool  sync.Pool
}

// NewCompressor 创建压缩器
func NewCompressor(level int) *Compressor {
	if level < 1 || level > 9 {
		level = 6
	}

	return &Compressor{
		level: level,
		pool: sync.Pool{
			New: func() interface{} {
				writer, _ := gzip.NewWriterLevel(nil, level)
				return writer
			},
		},
	}
}

// Compress 压缩数据
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	writer := c.pool.Get().(*gzip.Writer)
	defer c.pool.Put(writer)

	var buf bytes.Buffer
	writer.Reset(&buf)

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decompress 解压缩数据
func (c *Compressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// ResponseCacheImpl 响应缓存实现
type ResponseCacheImpl struct {
	cache map[string]CacheItem
	mu    sync.RWMutex
}

// CacheItem 缓存项
type CacheItem struct {
	Data      []byte
	ExpiresAt time.Time
}

// NewResponseCache 创建响应缓存
func NewResponseCache() *ResponseCacheImpl {
	cache := &ResponseCacheImpl{
		cache: make(map[string]CacheItem),
	}

	// 启动清理goroutine
	go cache.cleanup()

	return cache
}

// Get 获取缓存
func (rc *ResponseCacheImpl) Get(ctx context.Context, key string) ([]byte, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	item, exists := rc.cache[key]
	if !exists {
		return nil, false
	}

	if time.Now().After(item.ExpiresAt) {
		delete(rc.cache, key)
		return nil, false
	}

	return item.Data, true
}

// Set 设置缓存
func (rc *ResponseCacheImpl) Set(ctx context.Context, key string, data []byte, ttl time.Duration) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.cache[key] = CacheItem{
		Data:      data,
		ExpiresAt: time.Now().Add(ttl),
	}
}

// Delete 删除缓存
func (rc *ResponseCacheImpl) Delete(ctx context.Context, key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	delete(rc.cache, key)
}

// cleanup 清理过期缓存
func (rc *ResponseCacheImpl) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rc.mu.Lock()
		now := time.Now()
		for key, item := range rc.cache {
			if now.After(item.ExpiresAt) {
				delete(rc.cache, key)
			}
		}
		rc.mu.Unlock()
	}
}

// OptimizedResponseWriter 优化的响应写入器
type OptimizedResponseWriter struct {
	gin.ResponseWriter
	optimizer *ResponseOptimizer
	buffer    *bytes.Buffer
}

// NewOptimizedResponseWriter 创建优化的响应写入器
func NewOptimizedResponseWriter(w gin.ResponseWriter, optimizer *ResponseOptimizer) *OptimizedResponseWriter {
	return &OptimizedResponseWriter{
		ResponseWriter: w,
		optimizer:      optimizer,
		buffer:         &bytes.Buffer{},
	}
}

// Write 写入数据
func (w *OptimizedResponseWriter) Write(data []byte) (int, error) {
	return w.buffer.Write(data)
}

// WriteString 写入字符串
func (w *OptimizedResponseWriter) WriteString(s string) (int, error) {
	return w.buffer.WriteString(s)
}

// Flush 刷新缓冲区
func (w *OptimizedResponseWriter) Flush() error {
	// 优化响应数据
	optimizedData := w.buffer.Bytes()

	// 检查是否需要压缩
	if w.optimizer.shouldCompress(nil, len(optimizedData)) {
		compressedData, err := w.optimizer.compressor.Compress(optimizedData)
		if err == nil {
			w.ResponseWriter.Header().Set("Content-Encoding", "gzip")
			optimizedData = compressedData
		}
	}

	_, err := w.ResponseWriter.Write(optimizedData)
	w.ResponseWriter.Flush()
	return err
}
