package monitoring

import (
	"fmt"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	pkgErrors "github.com/easyspace-ai/luckdb/server/pkg/errors"
)

// ErrorStats 错误统计信息
type ErrorStats struct {
	TotalErrors   int64              `json:"total_errors"`
	ErrorCounts   map[string]int64   `json:"error_counts"`
	RecentErrors  []ErrorRecord      `json:"recent_errors"`
	ErrorRates    map[string]float64 `json:"error_rates"`
	LastResetTime time.Time          `json:"last_reset_time"`
	mu            sync.RWMutex       `json:"-"`
}

// ErrorRecord 错误记录
type ErrorRecord struct {
	Timestamp time.Time     `json:"timestamp"`
	Error     string        `json:"error"`
	Code      string        `json:"code"`
	Method    string        `json:"method"`
	Path      string        `json:"path"`
	IP        string        `json:"ip"`
	UserID    string        `json:"user_id,omitempty"`
	RequestID string        `json:"request_id,omitempty"`
	Details   interface{}   `json:"details,omitempty"`
	Severity  ErrorSeverity `json:"severity"`
}

// ErrorSeverity 错误严重程度
type ErrorSeverity string

const (
	SeverityLow      ErrorSeverity = "low"
	SeverityMedium   ErrorSeverity = "medium"
	SeverityHigh     ErrorSeverity = "high"
	SeverityCritical ErrorSeverity = "critical"
)

// ErrorAlertConfig 告警配置
type ErrorAlertConfig struct {
	// 错误率阈值（每分钟）
	ErrorRateThreshold float64
	// 连续错误阈值
	ConsecutiveErrorThreshold int
	// 严重错误阈值
	CriticalErrorThreshold int
	// 告警间隔
	ErrorAlertInterval time.Duration
	// 是否启用告警
	Enabled bool
}

// ErrorMonitor 错误监控器
type ErrorMonitor struct {
	stats          *ErrorStats
	config         ErrorAlertConfig
	alerts         []ErrorAlert
	lastErrorAlert time.Time
	mu             sync.RWMutex
	logger         *zap.Logger
}

// ErrorAlert 错误告警
type ErrorAlert struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Message    string                 `json:"message"`
	Severity   ErrorSeverity          `json:"severity"`
	Timestamp  time.Time              `json:"timestamp"`
	Details    map[string]interface{} `json:"details"`
	Resolved   bool                   `json:"resolved"`
	ResolvedAt *time.Time             `json:"resolved_at,omitempty"`
}

// NewErrorMonitor 创建错误监控器
func NewErrorMonitor(config ErrorAlertConfig) *ErrorMonitor {
	return &ErrorMonitor{
		stats: &ErrorStats{
			ErrorCounts:   make(map[string]int64),
			RecentErrors:  make([]ErrorRecord, 0),
			ErrorRates:    make(map[string]float64),
			LastResetTime: time.Now(),
		},
		config: config,
		alerts: make([]ErrorAlert, 0),
		logger: zap.L(),
	}
}

// RecordError 记录错误
func (m *ErrorMonitor) RecordError(err error, c *gin.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// 创建错误记录
	record := ErrorRecord{
		Timestamp: now,
		Error:     err.Error(),
		Method:    c.Request.Method,
		Path:      c.Request.URL.Path,
		IP:        c.ClientIP(),
		Severity:  m.determineSeverity(err),
	}

	// 获取请求ID和用户ID
	if requestID, exists := c.Get("request_id"); exists {
		record.RequestID = fmt.Sprintf("%v", requestID)
	}
	if userID, exists := c.Get("user_id"); exists {
		record.UserID = fmt.Sprintf("%v", userID)
	}

	// 获取错误代码
	if appErr, ok := pkgErrors.IsAppError(err); ok {
		record.Code = appErr.Code
		record.Details = appErr.Details
	} else {
		record.Code = "INTERNAL_ERROR"
	}

	// 更新统计信息
	m.stats.TotalErrors++
	m.stats.ErrorCounts[record.Code]++

	// 添加最近错误记录（保留最近100条）
	m.stats.RecentErrors = append(m.stats.RecentErrors, record)
	if len(m.stats.RecentErrors) > 100 {
		m.stats.RecentErrors = m.stats.RecentErrors[1:]
	}

	// 检查是否需要告警
	if m.config.Enabled {
		m.checkAndSendErrorAlert(record)
	}

	// 记录错误日志
	m.logError(record)
}

// GetStats 获取错误统计信息
func (m *ErrorMonitor) GetStats() *ErrorStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 创建副本以避免并发问题
	stats := &ErrorStats{
		TotalErrors:   m.stats.TotalErrors,
		ErrorCounts:   make(map[string]int64),
		RecentErrors:  make([]ErrorRecord, len(m.stats.RecentErrors)),
		ErrorRates:    make(map[string]float64),
		LastResetTime: m.stats.LastResetTime,
	}

	// 复制错误计数
	for code, count := range m.stats.ErrorCounts {
		stats.ErrorCounts[code] = count
	}

	// 复制最近错误
	copy(stats.RecentErrors, m.stats.RecentErrors)

	// 计算错误率
	m.calculateErrorRates(stats)

	return stats
}

// GetErrorAlerts 获取告警列表
func (m *ErrorMonitor) GetErrorAlerts() []ErrorAlert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	alerts := make([]ErrorAlert, len(m.alerts))
	copy(alerts, m.alerts)
	return alerts
}

// ResetStats 重置统计信息
func (m *ErrorMonitor) ResetStats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stats = &ErrorStats{
		ErrorCounts:   make(map[string]int64),
		RecentErrors:  make([]ErrorRecord, 0),
		ErrorRates:    make(map[string]float64),
		LastResetTime: time.Now(),
	}
}

// determineSeverity 确定错误严重程度
func (m *ErrorMonitor) determineSeverity(err error) ErrorSeverity {
	if appErr, ok := pkgErrors.IsAppError(err); ok {
		switch {
		case appErr.HTTPStatus >= 500:
			return SeverityCritical
		case appErr.HTTPStatus >= 400:
			return SeverityHigh
		case appErr.HTTPStatus >= 300:
			return SeverityMedium
		default:
			return SeverityLow
		}
	}
	return SeverityCritical
}

// checkAndSendErrorAlert 检查并发送告警
func (m *ErrorMonitor) checkAndSendErrorAlert(record ErrorRecord) {
	now := time.Now()

	// 检查告警间隔
	if now.Sub(m.lastErrorAlert) < m.config.ErrorAlertInterval {
		return
	}

	shouldErrorAlert := false
	alertType := ""
	alertMessage := ""

	// 检查连续错误
	if m.checkConsecutiveErrors() {
		shouldErrorAlert = true
		alertType = "consecutive_errors"
		alertMessage = fmt.Sprintf("检测到连续错误，错误代码: %s", record.Code)
	}

	// 检查严重错误
	if record.Severity == SeverityCritical {
		shouldErrorAlert = true
		alertType = "critical_error"
		alertMessage = fmt.Sprintf("检测到严重错误: %s", record.Error)
	}

	// 检查错误率
	if m.checkErrorRate() {
		shouldErrorAlert = true
		alertType = "high_error_rate"
		alertMessage = "错误率超过阈值"
	}

	if shouldErrorAlert {
		alert := ErrorAlert{
			ID:        fmt.Sprintf("%d", now.Unix()),
			Type:      alertType,
			Message:   alertMessage,
			Severity:  record.Severity,
			Timestamp: now,
			Details: map[string]interface{}{
				"error_code": record.Code,
				"method":     record.Method,
				"path":       record.Path,
				"ip":         record.IP,
				"user_id":    record.UserID,
			},
		}

		m.alerts = append(m.alerts, alert)
		m.lastErrorAlert = now

		// 发送告警通知
		m.sendErrorAlert(alert)
	}
}

// checkConsecutiveErrors 检查连续错误
func (m *ErrorMonitor) checkConsecutiveErrors() bool {
	if len(m.stats.RecentErrors) < m.config.ConsecutiveErrorThreshold {
		return false
	}

	// 检查最近的错误是否都是相同类型的
	recent := m.stats.RecentErrors
	lastCount := int64(0)

	for i := len(recent) - m.config.ConsecutiveErrorThreshold; i < len(recent); i++ {
		if recent[i].Code == recent[len(recent)-1].Code {
			lastCount++
		}
	}

	return lastCount >= int64(m.config.ConsecutiveErrorThreshold)
}

// checkErrorRate 检查错误率
func (m *ErrorMonitor) checkErrorRate() bool {
	// 计算最近一分钟的错误率
	now := time.Now()
	oneMinuteAgo := now.Add(-time.Minute)

	recentErrorCount := int64(0)
	for _, record := range m.stats.RecentErrors {
		if record.Timestamp.After(oneMinuteAgo) {
			recentErrorCount++
		}
	}

	// 假设每分钟有100个请求（这个值应该从实际的请求统计中获取）
	errorRate := float64(recentErrorCount) / 100.0
	return errorRate > m.config.ErrorRateThreshold
}

// calculateErrorRates 计算错误率
func (m *ErrorMonitor) calculateErrorRates(stats *ErrorStats) {
	now := time.Now()
	oneMinuteAgo := now.Add(-time.Minute)

	// 计算最近一分钟各类型错误的错误率
	for code := range stats.ErrorCounts {
		recentCount := int64(0)
		for _, record := range stats.RecentErrors {
			if record.Code == code && record.Timestamp.After(oneMinuteAgo) {
				recentCount++
			}
		}
		stats.ErrorRates[code] = float64(recentCount) / 100.0 // 假设每分钟100个请求
	}
}

// logError 记录错误日志
func (m *ErrorMonitor) logError(record ErrorRecord) {
	fields := []zap.Field{
		zap.String("error_code", record.Code),
		zap.String("method", record.Method),
		zap.String("path", record.Path),
		zap.String("ip", record.IP),
		zap.String("severity", string(record.Severity)),
	}

	if record.UserID != "" {
		fields = append(fields, zap.String("user_id", record.UserID))
	}
	if record.RequestID != "" {
		fields = append(fields, zap.String("request_id", record.RequestID))
	}
	if record.Details != nil {
		fields = append(fields, zap.Any("details", record.Details))
	}

	switch record.Severity {
	case SeverityCritical:
		m.logger.Error("Error recorded", fields...)
	case SeverityHigh:
		m.logger.Warn("Error recorded", fields...)
	default:
		m.logger.Info("Error recorded", fields...)
	}
}

// sendErrorAlert 发送告警通知
func (m *ErrorMonitor) sendErrorAlert(alert ErrorAlert) {
	// 这里可以集成各种告警通知方式，如：
	// - 发送邮件
	// - 发送短信
	// - 发送到Slack/钉钉等
	// - 发送到监控系统

	fields := []zap.Field{
		zap.String("alert_id", alert.ID),
		zap.String("alert_type", alert.Type),
		zap.String("severity", string(alert.Severity)),
		zap.Any("details", alert.Details),
	}

	m.logger.Error("ErrorAlert triggered", fields...)
}

// ResolveErrorAlert 解决告警
func (m *ErrorMonitor) ResolveErrorAlert(alertID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, alert := range m.alerts {
		if alert.ID == alertID {
			m.alerts[i].Resolved = true
			now := time.Now()
			m.alerts[i].ResolvedAt = &now
			return nil
		}
	}

	return fmt.Errorf("alert not found: %s", alertID)
}

// GetErrorMetrics 获取错误指标（用于监控系统）
func (m *ErrorMonitor) GetErrorMetrics() map[string]interface{} {
	stats := m.GetStats()

	return map[string]interface{}{
		"total_errors":  stats.TotalErrors,
		"error_counts":  stats.ErrorCounts,
		"error_rates":   stats.ErrorRates,
		"active_alerts": len(m.GetErrorAlerts()),
		"last_reset":    stats.LastResetTime,
	}
}
