package monitoring

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// AlertingSystem 告警系统
type AlertingSystem struct {
	rules     map[string]*AlertRule
	notifiers map[string]AlertNotifier
	logger    *zap.Logger
	mu        sync.RWMutex
	interval  time.Duration
	stopChan  chan struct{}
	alerts    map[string]*Alert
}

// AlertRule 告警规则
type AlertRule struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Condition   AlertCondition         `json:"condition"`
	Severity    AlertingSeverity       `json:"severity"`
	Enabled     bool                   `json:"enabled"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Labels      map[string]string      `json:"labels"`
	Annotations map[string]interface{} `json:"annotations"`
}

// AlertCondition 告警条件
type AlertCondition struct {
	MetricName string             `json:"metric_name"`
	Operator   ComparisonOperator `json:"operator"`
	Threshold  float64            `json:"threshold"`
	Duration   time.Duration      `json:"duration"`
	Labels     map[string]string  `json:"labels"`
}

// ComparisonOperator 比较操作符
type ComparisonOperator string

const (
	OperatorGT  ComparisonOperator = "gt"  // 大于
	OperatorGTE ComparisonOperator = "gte" // 大于等于
	OperatorLT  ComparisonOperator = "lt"  // 小于
	OperatorLTE ComparisonOperator = "lte" // 小于等于
	OperatorEQ  ComparisonOperator = "eq"  // 等于
	OperatorNEQ ComparisonOperator = "neq" // 不等于
)

// AlertingSeverity 告警严重程度
type AlertingSeverity string

const (
	AlertingSeverityInfo     AlertingSeverity = "info"
	AlertingSeverityWarning  AlertingSeverity = "warning"
	AlertingSeverityCritical AlertingSeverity = "critical"
)

// Alert 告警
type Alert struct {
	ID          string                 `json:"id"`
	RuleID      string                 `json:"rule_id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Severity    AlertingSeverity       `json:"severity"`
	Status      AlertStatus            `json:"status"`
	StartedAt   time.Time              `json:"started_at"`
	EndedAt     *time.Time             `json:"ended_at,omitempty"`
	Labels      map[string]string      `json:"labels"`
	Annotations map[string]interface{} `json:"annotations"`
	Value       float64                `json:"value"`
	Threshold   float64                `json:"threshold"`
	FiredAt     time.Time              `json:"fired_at"`
	ResolvedAt  *time.Time             `json:"resolved_at,omitempty"`
}

// AlertStatus 告警状态
type AlertStatus string

const (
	AlertStatusPending  AlertStatus = "pending"
	AlertStatusFiring   AlertStatus = "firing"
	AlertStatusResolved AlertStatus = "resolved"
)

// AlertNotifier 告警通知器接口
type AlertNotifier interface {
	Notify(ctx context.Context, alert *Alert) error
	Name() string
	Type() string
}

// NewAlertingSystem 创建告警系统
func NewAlertingSystem(interval time.Duration) *AlertingSystem {
	return &AlertingSystem{
		rules:     make(map[string]*AlertRule),
		notifiers: make(map[string]AlertNotifier),
		logger:    zap.L(),
		interval:  interval,
		stopChan:  make(chan struct{}),
		alerts:    make(map[string]*Alert),
	}
}

// AddRule 添加告警规则
func (as *AlertingSystem) AddRule(rule *AlertRule) {
	as.mu.Lock()
	defer as.mu.Unlock()

	rule.CreatedAt = time.Now()
	rule.UpdatedAt = time.Now()
	as.rules[rule.ID] = rule
}

// UpdateRule 更新告警规则
func (as *AlertingSystem) UpdateRule(rule *AlertRule) {
	as.mu.Lock()
	defer as.mu.Unlock()

	if existing, exists := as.rules[rule.ID]; exists {
		rule.CreatedAt = existing.CreatedAt
		rule.UpdatedAt = time.Now()
		as.rules[rule.ID] = rule
	}
}

// RemoveRule 移除告警规则
func (as *AlertingSystem) RemoveRule(ruleID string) {
	as.mu.Lock()
	defer as.mu.Unlock()

	delete(as.rules, ruleID)
}

// AddNotifier 添加告警通知器
func (as *AlertingSystem) AddNotifier(notifier AlertNotifier) {
	as.mu.Lock()
	defer as.mu.Unlock()

	as.notifiers[notifier.Name()] = notifier
}

// RemoveNotifier 移除告警通知器
func (as *AlertingSystem) RemoveNotifier(name string) {
	as.mu.Lock()
	defer as.mu.Unlock()

	delete(as.notifiers, name)
}

// Start 启动告警系统
func (as *AlertingSystem) Start(ctx context.Context, storage MetricsStorage) {
	ticker := time.NewTicker(as.interval)
	defer ticker.Stop()

	as.logger.Info("Starting alerting system",
		zap.Duration("interval", as.interval),
		zap.Int("rules", len(as.rules)),
		zap.Int("notifiers", len(as.notifiers)),
	)

	for {
		select {
		case <-ticker.C:
			as.evaluateRules(ctx, storage)
		case <-as.stopChan:
			as.logger.Info("Alerting system stopped")
			return
		case <-ctx.Done():
			as.logger.Info("Alerting system context cancelled")
			return
		}
	}
}

// Stop 停止告警系统
func (as *AlertingSystem) Stop() {
	close(as.stopChan)
}

// evaluateRules 评估告警规则
func (as *AlertingSystem) evaluateRules(ctx context.Context, storage MetricsStorage) {
	as.mu.RLock()
	rules := make([]*AlertRule, 0, len(as.rules))
	for _, rule := range as.rules {
		if rule.Enabled {
			rules = append(rules, rule)
		}
	}
	as.mu.RUnlock()

	for _, rule := range rules {
		go as.evaluateRule(ctx, storage, rule)
	}
}

// evaluateRule 评估单个告警规则
func (as *AlertingSystem) evaluateRule(ctx context.Context, storage MetricsStorage, rule *AlertRule) {
	// 查询指标数据
	query := MetricsQuery{
		Name:      rule.Condition.MetricName,
		Labels:    rule.Condition.Labels,
		EndTime:   time.Now(),
		StartTime: time.Now().Add(-rule.Condition.Duration),
		Limit:     1,
	}

	data, err := storage.Query(ctx, query)
	if err != nil {
		as.logger.Error("Failed to query metrics for alert rule",
			zap.String("rule_id", rule.ID),
			zap.Error(err),
		)
		return
	}

	if len(data) == 0 {
		// 没有数据，可能是指标不存在
		return
	}

	metric := data[0]
	value := as.extractNumericValue(metric.Value)
	if value == nil {
		as.logger.Warn("Cannot extract numeric value from metric",
			zap.String("rule_id", rule.ID),
			zap.String("metric_name", rule.Condition.MetricName),
			zap.Any("value", metric.Value),
		)
		return
	}

	// 检查条件
	if as.evaluateCondition(*value, rule.Condition) {
		as.handleAlertFired(ctx, rule, *value)
	} else {
		as.handleAlertResolved(ctx, rule)
	}
}

// extractNumericValue 提取数值
func (as *AlertingSystem) extractNumericValue(value interface{}) *float64 {
	switch v := value.(type) {
	case float64:
		return &v
	case float32:
		f := float64(v)
		return &f
	case int:
		f := float64(v)
		return &f
	case int64:
		f := float64(v)
		return &f
	case int32:
		f := float64(v)
		return &f
	default:
		return nil
	}
}

// evaluateCondition 评估条件
func (as *AlertingSystem) evaluateCondition(value float64, condition AlertCondition) bool {
	switch condition.Operator {
	case OperatorGT:
		return value > condition.Threshold
	case OperatorGTE:
		return value >= condition.Threshold
	case OperatorLT:
		return value < condition.Threshold
	case OperatorLTE:
		return value <= condition.Threshold
	case OperatorEQ:
		return value == condition.Threshold
	case OperatorNEQ:
		return value != condition.Threshold
	default:
		return false
	}
}

// handleAlertFired 处理告警触发
func (as *AlertingSystem) handleAlertFired(ctx context.Context, rule *AlertRule, value float64) {
	alertID := fmt.Sprintf("%s_%d", rule.ID, time.Now().Unix())

	as.mu.Lock()
	existing, exists := as.alerts[rule.ID]
	if exists && existing.Status == AlertStatusFiring {
		// 告警已经存在且正在触发，更新最后触发时间
		existing.FiredAt = time.Now()
		existing.Value = value
		as.mu.Unlock()
		return
	}

	alert := &Alert{
		ID:          alertID,
		RuleID:      rule.ID,
		Name:        rule.Name,
		Description: rule.Description,
		Severity:    rule.Severity,
		Status:      AlertStatusFiring,
		StartedAt:   time.Now(),
		Labels:      rule.Labels,
		Annotations: rule.Annotations,
		Value:       value,
		Threshold:   rule.Condition.Threshold,
		FiredAt:     time.Now(),
	}

	as.alerts[rule.ID] = alert
	as.mu.Unlock()

	// 发送通知
	as.sendNotifications(ctx, alert)
}

// handleAlertResolved 处理告警解决
func (as *AlertingSystem) handleAlertResolved(ctx context.Context, rule *AlertRule) {
	as.mu.Lock()
	alert, exists := as.alerts[rule.ID]
	if !exists || alert.Status != AlertStatusFiring {
		as.mu.Unlock()
		return
	}

	now := time.Now()
	alert.Status = AlertStatusResolved
	alert.EndedAt = &now
	alert.ResolvedAt = &now
	as.mu.Unlock()

	// 发送解决通知
	as.sendNotifications(ctx, alert)
}

// sendNotifications 发送通知
func (as *AlertingSystem) sendNotifications(ctx context.Context, alert *Alert) {
	as.mu.RLock()
	notifiers := make([]AlertNotifier, 0, len(as.notifiers))
	for _, notifier := range as.notifiers {
		notifiers = append(notifiers, notifier)
	}
	as.mu.RUnlock()

	for _, notifier := range notifiers {
		go func(n AlertNotifier) {
			if err := n.Notify(ctx, alert); err != nil {
				as.logger.Error("Failed to send alert notification",
					zap.String("alert_id", alert.ID),
					zap.String("notifier", n.Name()),
					zap.Error(err),
				)
			}
		}(notifier)
	}
}

// GetAlerts 获取告警列表
func (as *AlertingSystem) GetAlerts() []*Alert {
	as.mu.RLock()
	defer as.mu.RUnlock()

	alerts := make([]*Alert, 0, len(as.alerts))
	for _, alert := range as.alerts {
		alerts = append(alerts, alert)
	}
	return alerts
}

// GetRules 获取告警规则列表
func (as *AlertingSystem) GetRules() []*AlertRule {
	as.mu.RLock()
	defer as.mu.RUnlock()

	rules := make([]*AlertRule, 0, len(as.rules))
	for _, rule := range as.rules {
		rules = append(rules, rule)
	}
	return rules
}

// LogNotifier 日志通知器
type LogNotifier struct {
	logger *zap.Logger
}

// NewLogNotifier 创建日志通知器
func NewLogNotifier() *LogNotifier {
	return &LogNotifier{
		logger: zap.L(),
	}
}

// Notify 发送通知
func (ln *LogNotifier) Notify(ctx context.Context, alert *Alert) error {
	fields := []zap.Field{
		zap.String("alert_id", alert.ID),
		zap.String("rule_id", alert.RuleID),
		zap.String("name", alert.Name),
		zap.String("severity", string(alert.Severity)),
		zap.String("status", string(alert.Status)),
		zap.Float64("value", alert.Value),
		zap.Float64("threshold", alert.Threshold),
		zap.Time("fired_at", alert.FiredAt),
	}

	if alert.ResolvedAt != nil {
		fields = append(fields, zap.Time("resolved_at", *alert.ResolvedAt))
	}

	switch alert.Severity {
	case AlertingSeverityCritical:
		ln.logger.Error("Alert notification", fields...)
	case AlertingSeverityWarning:
		ln.logger.Warn("Alert notification", fields...)
	default:
		ln.logger.Info("Alert notification", fields...)
	}

	return nil
}

// Name 返回通知器名称
func (ln *LogNotifier) Name() string {
	return "log"
}

// Type 返回通知器类型
func (ln *LogNotifier) Type() string {
	return "log"
}

// EmailNotifier 邮件通知器
type EmailNotifier struct {
	config EmailConfig
	logger *zap.Logger
}

// EmailConfig 邮件配置
type EmailConfig struct {
	SMTPHost  string   `json:"smtp_host"`
	SMTPPort  int      `json:"smtp_port"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	From      string   `json:"from"`
	To        []string `json:"to"`
	EnableTLS bool     `json:"enable_tls"`
}

// NewEmailNotifier 创建邮件通知器
func NewEmailNotifier(config EmailConfig) *EmailNotifier {
	return &EmailNotifier{
		config: config,
		logger: zap.L(),
	}
}

// Notify 发送通知
func (en *EmailNotifier) Notify(ctx context.Context, alert *Alert) error {
	// 这里应该实现邮件发送逻辑
	// 为了简化，这里只记录日志

	en.logger.Info("Sending email alert notification",
		zap.String("alert_id", alert.ID),
		zap.String("to", fmt.Sprintf("%v", en.config.To)),
	)

	return nil
}

// Name 返回通知器名称
func (en *EmailNotifier) Name() string {
	return "email"
}

// Type 返回通知器类型
func (en *EmailNotifier) Type() string {
	return "email"
}

// WebhookNotifier Webhook通知器
type WebhookNotifier struct {
	config WebhookConfig
	logger *zap.Logger
}

// WebhookConfig Webhook配置
type WebhookConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Timeout time.Duration     `json:"timeout"`
}

// NewWebhookNotifier 创建Webhook通知器
func NewWebhookNotifier(config WebhookConfig) *WebhookNotifier {
	return &WebhookNotifier{
		config: config,
		logger: zap.L(),
	}
}

// Notify 发送通知
func (wn *WebhookNotifier) Notify(ctx context.Context, alert *Alert) error {
	// 这里应该实现Webhook发送逻辑
	// 为了简化，这里只记录日志

	wn.logger.Info("Sending webhook alert notification",
		zap.String("alert_id", alert.ID),
		zap.String("url", wn.config.URL),
	)

	return nil
}

// Name 返回通知器名称
func (wn *WebhookNotifier) Name() string {
	return "webhook"
}

// Type 返回通知器类型
func (wn *WebhookNotifier) Type() string {
	return "webhook"
}
