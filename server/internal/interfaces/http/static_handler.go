package http

import (
	"embed"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/gin-gonic/gin"
)

// StaticHandler 静态文件处理器
type StaticHandler struct {
	fileSystem http.FileSystem
}

// NewStaticHandler 创建静态文件处理器
func NewStaticHandler(embedFS embed.FS, rootDir string) (*StaticHandler, error) {
	// 从 embed.FS 中提取子目录
	subFS, err := fs.Sub(embedFS, rootDir)
	if err != nil {
		return nil, err
	}

	return &StaticHandler{
		fileSystem: http.FS(subFS),
	}, nil
}

// ServeStatic 服务静态文件
// 这个方法处理前端单页应用(SPA)的路由
func (h *StaticHandler) ServeStatic() gin.HandlerFunc {
	fileServer := http.FileServer(h.fileSystem)

	return func(c *gin.Context) {
		// 获取请求路径
		requestPath := c.Request.URL.Path

		// 如果是API路径，跳过
		if strings.HasPrefix(requestPath, "/api/") ||
			strings.HasPrefix(requestPath, "/ws") ||
			strings.HasPrefix(requestPath, "/realtime/") ||
			strings.HasPrefix(requestPath, "/yjs/") ||
			strings.HasPrefix(requestPath, "/health") {
			c.Next()
			return
		}

		// 尝试打开文件
		file, err := h.fileSystem.Open(strings.TrimPrefix(requestPath, "/"))
		if err != nil {
			// 文件不存在，返回 index.html（用于 SPA 路由）
			c.Request.URL.Path = "/"
			fileServer.ServeHTTP(c.Writer, c.Request)
			return
		}
		defer file.Close()

		// 检查是否是目录
		stat, err := file.Stat()
		if err != nil {
			c.String(http.StatusInternalServerError, "Internal Server Error")
			return
		}

		if stat.IsDir() {
			// 如果是目录，尝试返回 index.html
			indexPath := path.Join(requestPath, "index.html")
			indexFile, err := h.fileSystem.Open(strings.TrimPrefix(indexPath, "/"))
			if err != nil {
				// 目录中没有 index.html，返回根 index.html
				c.Request.URL.Path = "/"
				fileServer.ServeHTTP(c.Writer, c.Request)
				return
			}
			indexFile.Close()
			c.Request.URL.Path = indexPath
		}

		// 服务文件
		fileServer.ServeHTTP(c.Writer, c.Request)
	}
}

// ServeSPA 服务单页应用（简化版本）
// 所有非API请求都返回 index.html，让前端路由处理
func (h *StaticHandler) ServeSPA() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取请求路径
		requestPath := c.Request.URL.Path

		// 如果是API路径或特殊路径，跳过
		if strings.HasPrefix(requestPath, "/api/") ||
			strings.HasPrefix(requestPath, "/ws") ||
			strings.HasPrefix(requestPath, "/health") {
			c.Next()
			return
		}

		// 尝试打开文件
		filePath := strings.TrimPrefix(requestPath, "/")
		if filePath == "" {
			filePath = "index.html"
		}

		file, err := h.fileSystem.Open(filePath)
		if err == nil {
			defer file.Close()
			stat, err := file.Stat()
			if err == nil && !stat.IsDir() {
				// 文件存在且不是目录，直接服务
				http.FileServer(h.fileSystem).ServeHTTP(c.Writer, c.Request)
				return
			}
		}

		// 文件不存在或是目录，返回 index.html（SPA 路由）
		indexFile, err := h.fileSystem.Open("index.html")
		if err != nil {
			c.String(http.StatusNotFound, "index.html not found")
			return
		}
		defer indexFile.Close()

		indexStat, err := indexFile.Stat()
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to read index.html")
			return
		}

		c.Header("Content-Type", "text/html; charset=utf-8")
		// 将 fs.File 转换为 io.ReadSeeker
		if seeker, ok := indexFile.(io.ReadSeeker); ok {
			http.ServeContent(c.Writer, c.Request, "index.html", indexStat.ModTime(), seeker)
		} else {
			c.String(http.StatusInternalServerError, "Failed to serve index.html")
		}
	}
}
