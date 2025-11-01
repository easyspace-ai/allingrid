package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// CORSConfig CORS配置
type CORSConfig struct {
	AllowOrigins     []string
	AllowMethods     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	AllowCredentials bool
	MaxAge           int
}

// DefaultCORSConfig 默认CORS配置
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
			http.MethodHead,
			http.MethodOptions,
		},
		AllowHeaders: []string{
			"Origin",
			"Content-Length",
			"Content-Type",
			"Authorization",
			"X-Requested-With",
			"Accept",
			"Accept-Encoding",
			"Accept-Language",
			"Cache-Control",
			"Connection",
			"Host",
			"Pragma",
			"Referer",
			"User-Agent",
		},
		ExposeHeaders: []string{
			"Content-Length",
			"Content-Type",
			"Content-Disposition",
		},
		AllowCredentials: true,
		MaxAge:           86400, // 24 hours
	}
}

// CORS 跨域中间件
func CORS(config ...CORSConfig) gin.HandlerFunc {
	var cfg CORSConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultCORSConfig()
	}

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// 检查Origin是否被允许
		if isOriginAllowed(origin, cfg.AllowOrigins) {
			c.Header("Access-Control-Allow-Origin", origin)
		} else if len(cfg.AllowOrigins) == 1 && cfg.AllowOrigins[0] == "*" {
			c.Header("Access-Control-Allow-Origin", "*")
		}

		// 设置允许的方法
		if len(cfg.AllowMethods) > 0 {
			c.Header("Access-Control-Allow-Methods", joinStrings(cfg.AllowMethods, ", "))
		}

		// 设置允许的头部
		if len(cfg.AllowHeaders) > 0 {
			c.Header("Access-Control-Allow-Headers", joinStrings(cfg.AllowHeaders, ", "))
		}

		// 设置暴露的头部
		if len(cfg.ExposeHeaders) > 0 {
			c.Header("Access-Control-Expose-Headers", joinStrings(cfg.ExposeHeaders, ", "))
		}

		// 设置是否允许凭据
		if cfg.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		// 设置预检请求缓存时间
		if cfg.MaxAge > 0 {
			c.Header("Access-Control-Max-Age", string(rune(cfg.MaxAge)))
		}

		// 处理预检请求
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// isOriginAllowed 检查Origin是否被允许
func isOriginAllowed(origin string, allowOrigins []string) bool {
	for _, allowed := range allowOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

// joinStrings 连接字符串
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
