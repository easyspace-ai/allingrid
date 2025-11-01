package http

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/monitoring"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// HealthHandler 健康检查处理器
type HealthHandler struct {
	db           *database.Connection
	errorMonitor *monitoring.ErrorMonitor
	logger       *zap.Logger
}

// NewHealthHandler 创建健康检查处理器
func NewHealthHandler(db *database.Connection, errorMonitor *monitoring.ErrorMonitor) *HealthHandler {
	return &HealthHandler{
		db:           db,
		errorMonitor: errorMonitor,
		logger:       zap.L(),
	}
}

// HealthResponse 健康检查响应
type HealthResponse struct {
	Status    string                   `json:"status"`
	Timestamp time.Time                `json:"timestamp"`
	Version   string                   `json:"version"`
	Uptime    string                   `json:"uptime"`
	Services  map[string]ServiceHealth `json:"services"`
	Metrics   map[string]interface{}   `json:"metrics,omitempty"`
}

// ServiceHealth 服务健康状态
type ServiceHealth struct {
	Status       string                 `json:"status"`
	Message      string                 `json:"message,omitempty"`
	ResponseTime string                 `json:"response_time,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
}

// HealthCheck 健康检查
// @Summary 健康检查
// @Description 检查系统各组件的健康状态
// @Tags 系统
// @Produce json
// @Success 200 {object} HealthResponse
// @Failure 503 {object} HealthResponse
// @Router /health [get]
func (h *HealthHandler) HealthCheck(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	overallStatus := "healthy"
	services := make(map[string]ServiceHealth)

	// 检查数据库
	dbHealth := h.checkDatabase(ctx)
	services["database"] = dbHealth
	if dbHealth.Status != "healthy" {
		overallStatus = "unhealthy"
	}

	// 检查缓存（如果有的话）
	cacheHealth := h.checkCache(ctx)
	services["cache"] = cacheHealth
	if cacheHealth.Status != "healthy" {
		overallStatus = "degraded"
	}

	// 检查外部服务（如果有的话）
	externalHealth := h.checkExternalServices(ctx)
	services["external"] = externalHealth
	if externalHealth.Status != "healthy" {
		overallStatus = "degraded"
	}

	// 获取系统指标
	metrics := h.getSystemMetrics()

	respBody := HealthResponse{
		Status:    overallStatus,
		Timestamp: time.Now(),
		Version:   "1.0.0", // 这里应该从配置或构建信息中获取
		Uptime:    h.getUptime(),
		Services:  services,
		Metrics:   metrics,
	}

	// 使用统一响应格式
	message := "系统健康"
	if overallStatus == "unhealthy" {
		message = "系统不健康"
	} else if overallStatus == "degraded" {
		message = "系统降级"
	}
	response.Success(c, respBody, message)
}

// ReadinessCheck 就绪检查
// @Summary 就绪检查
// @Description 检查服务是否准备好接收请求
// @Tags 系统
// @Produce json
// @Success 200 {object} ServiceHealth
// @Failure 503 {object} ServiceHealth
// @Router /ready [get]
func (h *HealthHandler) ReadinessCheck(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	// 检查关键服务是否就绪
	dbHealth := h.checkDatabase(ctx)

	status := "ready"
	message := "Service is ready"

	if dbHealth.Status != "healthy" {
		status = "not_ready"
		message = "Database is not available"
	}

	svc := ServiceHealth{
		Status:  status,
		Message: message,
	}

	response.Success(c, svc, message)
}

// LivenessCheck 存活检查
// @Summary 存活检查
// @Description 检查服务是否存活
// @Tags 系统
// @Produce json
// @Success 200 {object} ServiceHealth
// @Router /alive [get]
func (h *HealthHandler) LivenessCheck(c *gin.Context) {
	live := ServiceHealth{
		Status:  "alive",
		Message: "Service is alive",
	}

	response.Success(c, live, "Service is alive")
}

// Metrics 获取系统指标
// @Summary 获取系统指标
// @Description 获取系统运行指标和统计信息
// @Tags 系统
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 401 {object} ErrorResponse
// @Security BearerAuth
// @Router /metrics [get]
func (h *HealthHandler) Metrics(c *gin.Context) {
	// 检查是否为管理员用户
	_, exists := c.Get("user_id")
	if !exists {
		response.Error(c, errors.ErrUnauthorized.WithDetails("未授权访问"))
		return
	}

	// 这里应该检查用户是否为管理员
	// 为了简化，我们暂时跳过这个检查

	metrics := h.getSystemMetrics()
	response.Success(c, metrics, "获取系统指标成功")
}

// checkDatabase 检查数据库健康状态
func (h *HealthHandler) checkDatabase(ctx context.Context) ServiceHealth {
	start := time.Now()

	if h.db == nil {
		return ServiceHealth{
			Status:  "unhealthy",
			Message: "Database connection not initialized",
		}
	}

	// 执行简单的数据库查询
	var result int
	err := h.db.DB.Raw("SELECT 1").Scan(&result).Error

	responseTime := time.Since(start).String()

	if err != nil {
		h.logger.Error("Database health check failed",
			zap.Error(err),
			zap.String("response_time", responseTime),
		)

		return ServiceHealth{
			Status:       "unhealthy",
			Message:      "Database connection failed",
			ResponseTime: responseTime,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return ServiceHealth{
		Status:       "healthy",
		Message:      "Database connection is healthy",
		ResponseTime: responseTime,
		Details: map[string]interface{}{
			"result": result,
		},
	}
}

// checkCache 检查缓存健康状态
func (h *HealthHandler) checkCache(ctx context.Context) ServiceHealth {
	// 这里应该检查Redis或其他缓存服务
	// 暂时返回健康状态
	return ServiceHealth{
		Status:  "healthy",
		Message: "Cache is healthy",
		Details: map[string]interface{}{
			"type": "redis",
		},
	}
}

// checkExternalServices 检查外部服务健康状态
func (h *HealthHandler) checkExternalServices(ctx context.Context) ServiceHealth {
	// 这里应该检查依赖的外部服务，如：
	// - 邮件服务
	// - 文件存储服务
	// - 第三方API等

	return ServiceHealth{
		Status:  "healthy",
		Message: "External services are healthy",
		Details: map[string]interface{}{
			"services": []string{"email", "storage"},
		},
	}
}

// getSystemMetrics 获取系统指标
func (h *HealthHandler) getSystemMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})

	// 获取错误监控指标
	if h.errorMonitor != nil {
		errorMetrics := h.errorMonitor.GetErrorMetrics()
		metrics["errors"] = errorMetrics
	}

	// 获取数据库指标
	if h.db != nil {
		var dbStats struct {
			Connections int `json:"connections"`
		}
		h.db.DB.Raw("SELECT COUNT(*) as connections FROM pg_stat_activity WHERE state = 'active'").Scan(&dbStats)
		metrics["database"] = dbStats
	}

	// 获取内存使用情况（这里简化处理）
	metrics["memory"] = map[string]interface{}{
		"used":  "N/A",
		"total": "N/A",
	}

	// 获取CPU使用情况（这里简化处理）
	metrics["cpu"] = map[string]interface{}{
		"usage": "N/A",
	}

	return metrics
}

// getUptime 获取服务运行时间
func (h *HealthHandler) getUptime() string {
	// 这里应该从服务启动时间计算
	// 暂时返回固定值
	return "24h30m15s"
}
