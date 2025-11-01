package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"

	appErrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// SecurityConfig 安全配置
type SecurityConfig struct {
	// CORS配置
	AllowOrigins     []string
	AllowMethods     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	AllowCredentials bool
	MaxAge           time.Duration

	// 限流配置
	RateLimit struct {
		RequestsPerSecond int
		BurstSize         int
		Enabled           bool
	}

	// 安全头配置
	SecurityHeaders struct {
		ContentTypeNosniff      bool
		FrameOptions            string // DENY, SAMEORIGIN, ALLOW-FROM
		XSSProtection           string // 1; mode=block
		ContentSecurityPolicy   string
		StrictTransportSecurity string
		ReferrerPolicy          string
	}

	// IP白名单/黑名单
	IPWhitelist []string
	IPBlacklist []string

	// 请求大小限制
	MaxRequestSize int64 // bytes
}

// DefaultSecurityConfig 默认安全配置
func DefaultSecurityConfig() SecurityConfig {
	return SecurityConfig{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With", "API-Version"},
		ExposeHeaders:    []string{"API-Version", "API-Current-Version", "API-Deprecated"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
		RateLimit: struct {
			RequestsPerSecond int
			BurstSize         int
			Enabled           bool
		}{
			RequestsPerSecond: 100,
			BurstSize:         200,
			Enabled:           true,
		},
		SecurityHeaders: struct {
			ContentTypeNosniff      bool
			FrameOptions            string
			XSSProtection           string
			ContentSecurityPolicy   string
			StrictTransportSecurity string
			ReferrerPolicy          string
		}{
			ContentTypeNosniff:      true,
			FrameOptions:            "DENY",
			XSSProtection:           "1; mode=block",
			ContentSecurityPolicy:   "default-src 'self'",
			StrictTransportSecurity: "max-age=31536000; includeSubDomains",
			ReferrerPolicy:          "strict-origin-when-cross-origin",
		},
		MaxRequestSize: 10 * 1024 * 1024, // 10MB
	}
}

// SecurityMiddleware 安全中间件
func SecurityMiddleware(config SecurityConfig) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		// 设置安全头
		setSecurityHeaders(c, config.SecurityHeaders)

		// IP白名单/黑名单检查
		if !isIPAllowed(c.ClientIP(), config.IPWhitelist, config.IPBlacklist) {
			response.Error(c, appErrors.ErrForbidden.WithDetails("Access denied from this IP address"))
			c.Abort()
			return
		}

		// 请求大小限制
		if c.Request.ContentLength > config.MaxRequestSize {
			response.Error(c, appErrors.ErrBadRequest.WithDetails(map[string]interface{}{
				"message":  "Request entity too large",
				"max_size": config.MaxRequestSize,
				"received": c.Request.ContentLength,
			}))
			c.Abort()
			return
		}

		c.Next()
	})
}

// CORSMiddleware CORS中间件
func CORSMiddleware(config SecurityConfig) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// 检查Origin是否被允许
		if isOriginAllowedForSecurity(origin, config.AllowOrigins) {
			c.Header("Access-Control-Allow-Origin", origin)
		}

		c.Header("Access-Control-Allow-Methods", strings.Join(config.AllowMethods, ", "))
		c.Header("Access-Control-Allow-Headers", strings.Join(config.AllowHeaders, ", "))
		c.Header("Access-Control-Expose-Headers", strings.Join(config.ExposeHeaders, ", "))

		if config.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		if config.MaxAge > 0 {
			c.Header("Access-Control-Max-Age", strconv.Itoa(int(config.MaxAge.Seconds())))
		}

		// 处理预检请求
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	})
}

// RateLimitMiddleware 限流中间件
func RateLimitMiddleware(config SecurityConfig) gin.HandlerFunc {
	if !config.RateLimit.Enabled {
		return func(c *gin.Context) { c.Next() }
	}

	// 创建限流器映射（实际应用中应该使用Redis等分布式存储）
	limiters := make(map[string]*rate.Limiter)

	return gin.HandlerFunc(func(c *gin.Context) {
		// 使用IP作为限流键
		key := c.ClientIP()

		// 获取或创建限流器
		limiter, exists := limiters[key]
		if !exists {
			limiter = rate.NewLimiter(
				rate.Limit(config.RateLimit.RequestsPerSecond),
				config.RateLimit.BurstSize,
			)
			limiters[key] = limiter
		}

		// 检查是否超过限制
		if !limiter.Allow() {
			c.Header("Retry-After", "60")
			response.Error(c, appErrors.ErrTooManyRequests.WithDetails(map[string]interface{}{
				"message":     "Rate limit exceeded",
				"limit":       config.RateLimit.RequestsPerSecond,
				"burst_size":  config.RateLimit.BurstSize,
				"retry_after": 60, // seconds
			}))
			c.Abort()
			return
		}

		c.Next()
	})
}

// RequestSizeLimitMiddleware 请求大小限制中间件
func RequestSizeLimitMiddleware(maxSize int64) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		if c.Request.ContentLength > maxSize {
			response.Error(c, appErrors.ErrBadRequest.WithDetails(map[string]interface{}{
				"message":  "Request entity too large",
				"max_size": maxSize,
				"received": c.Request.ContentLength,
			}))
			c.Abort()
			return
		}

		c.Next()
	})
}

// CSRFMiddleware CSRF保护中间件
func CSRFMiddleware() gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		// 对于非安全方法，检查CSRF令牌
		if c.Request.Method != "GET" && c.Request.Method != "HEAD" && c.Request.Method != "OPTIONS" {
			token := c.GetHeader("X-CSRF-Token")
			if token == "" {
				token = c.PostForm("_csrf_token")
			}

			// 这里应该验证CSRF令牌的有效性
			// 简化实现，实际应用中需要更复杂的验证逻辑
			if token == "" {
				response.Error(c, appErrors.ErrForbidden.WithDetails("CSRF token required"))
				c.Abort()
				return
			}
		}

		c.Next()
	})
}

// ContentTypeValidationMiddleware 内容类型验证中间件
func ContentTypeValidationMiddleware(allowedTypes []string) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		if c.Request.Method == "POST" || c.Request.Method == "PUT" || c.Request.Method == "PATCH" {
			contentType := c.GetHeader("Content-Type")
			if contentType == "" {
				response.Error(c, appErrors.ErrBadRequest.WithDetails("Content-Type header is required"))
				c.Abort()
				return
			}

			// 检查内容类型是否被允许
			allowed := false
			for _, allowedType := range allowedTypes {
				if strings.HasPrefix(contentType, allowedType) {
					allowed = true
					break
				}
			}

			if !allowed {
				response.Error(c, appErrors.ErrBadRequest.WithDetails(map[string]interface{}{
					"message":  "Unsupported content type",
					"received": contentType,
					"allowed":  allowedTypes,
				}))
				c.Abort()
				return
			}
		}

		c.Next()
	})
}

// setSecurityHeaders 设置安全头
func setSecurityHeaders(c *gin.Context, config struct {
	ContentTypeNosniff      bool
	FrameOptions            string
	XSSProtection           string
	ContentSecurityPolicy   string
	StrictTransportSecurity string
	ReferrerPolicy          string
}) {
	if config.ContentTypeNosniff {
		c.Header("X-Content-Type-Options", "nosniff")
	}

	if config.FrameOptions != "" {
		c.Header("X-Frame-Options", config.FrameOptions)
	}

	if config.XSSProtection != "" {
		c.Header("X-XSS-Protection", config.XSSProtection)
	}

	if config.ContentSecurityPolicy != "" {
		c.Header("Content-Security-Policy", config.ContentSecurityPolicy)
	}

	if config.StrictTransportSecurity != "" {
		c.Header("Strict-Transport-Security", config.StrictTransportSecurity)
	}

	if config.ReferrerPolicy != "" {
		c.Header("Referrer-Policy", config.ReferrerPolicy)
	}
}

// isOriginAllowedForSecurity 检查Origin是否被允许（安全中间件）
func isOriginAllowedForSecurity(origin string, allowedOrigins []string) bool {
	if len(allowedOrigins) == 0 {
		return false
	}

	for _, allowed := range allowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
		// 支持通配符匹配
		if strings.HasSuffix(allowed, "*") {
			prefix := strings.TrimSuffix(allowed, "*")
			if strings.HasPrefix(origin, prefix) {
				return true
			}
		}
	}

	return false
}

// isIPAllowed 检查IP是否被允许
func isIPAllowed(ip string, whitelist, blacklist []string) bool {
	// 如果在黑名单中，直接拒绝
	for _, blockedIP := range blacklist {
		if ip == blockedIP || matchIPPattern(ip, blockedIP) {
			return false
		}
	}

	// 如果没有白名单，允许所有（除了黑名单）
	if len(whitelist) == 0 {
		return true
	}

	// 检查是否在白名单中
	for _, allowedIP := range whitelist {
		if ip == allowedIP || matchIPPattern(ip, allowedIP) {
			return true
		}
	}

	return false
}

// matchIPPattern 匹配IP模式（简化实现）
func matchIPPattern(ip, pattern string) bool {
	// 支持CIDR表示法的简化实现
	// 实际应用中应该使用更完善的IP匹配库
	if strings.Contains(pattern, "/") {
		// CIDR匹配逻辑
		return false // 简化实现，返回false
	}

	// 支持通配符匹配
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(ip, prefix)
	}

	return ip == pattern
}

// SecurityAuditMiddleware 安全审计中间件
func SecurityAuditMiddleware() gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		// 记录安全相关的请求信息
		securityInfo := map[string]interface{}{
			"ip":         c.ClientIP(),
			"user_agent": c.GetHeader("User-Agent"),
			"referer":    c.GetHeader("Referer"),
			"method":     c.Request.Method,
			"path":       c.Request.URL.Path,
			"timestamp":  time.Now(),
		}

		// 检查可疑活动
		if isSuspiciousRequest(c) {
			securityInfo["suspicious"] = true
			// 这里可以记录到安全日志或发送告警
		}

		// 将安全信息添加到上下文
		c.Set("security_info", securityInfo)

		c.Next()
	})
}

// isSuspiciousRequest 检查是否为可疑请求
func isSuspiciousRequest(c *gin.Context) bool {
	// 检查常见的攻击模式
	userAgent := c.GetHeader("User-Agent")
	path := c.Request.URL.Path

	// 检查恶意User-Agent
	suspiciousUserAgents := []string{
		"sqlmap", "nikto", "nmap", "masscan", "nessus",
		"burpsuite", "owasp", "w3af", "acunetix",
	}

	for _, suspicious := range suspiciousUserAgents {
		if strings.Contains(strings.ToLower(userAgent), suspicious) {
			return true
		}
	}

	// 检查可疑路径
	suspiciousPaths := []string{
		"../", "..\\", "/etc/passwd", "/proc/", "cmd.exe",
		"<script", "javascript:", "vbscript:", "onload=",
		"union select", "drop table", "insert into",
	}

	pathLower := strings.ToLower(path)
	for _, suspicious := range suspiciousPaths {
		if strings.Contains(pathLower, suspicious) {
			return true
		}
	}

	return false
}

// HoneypotMiddleware 蜜罐中间件
func HoneypotMiddleware() gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		// 检查是否访问了蜜罐路径
		honeypotPaths := []string{
			"/admin", "/administrator", "/wp-admin", "/phpmyadmin",
			"/.env", "/config.php", "/database.php",
		}

		for _, honeypot := range honeypotPaths {
			if c.Request.URL.Path == honeypot {
				// 记录蜜罐访问
				c.Set("honeypot_triggered", true)

				// 返回假的响应以迷惑攻击者
				response.Error(c, appErrors.ErrNotFound.WithDetails("Not Found"))
				c.Abort()
				return
			}
		}

		c.Next()
	})
}
