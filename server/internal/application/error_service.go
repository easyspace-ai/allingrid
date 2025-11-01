package application

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// ErrorService 统一错误处理服务
type ErrorService struct {
	// 可以添加错误监控、告警等依赖
}

// NewErrorService 创建错误处理服务
func NewErrorService() *ErrorService {
	return &ErrorService{}
}

// HandleError 统一错误处理
func (s *ErrorService) HandleError(ctx context.Context, err error, metadata map[string]interface{}) *errors.AppError {
	// 记录错误日志
	s.logError(ctx, err, metadata)

	// 检查是否已经是AppError
	if appErr, ok := errors.IsAppError(err); ok {
		return appErr
	}

	// 转换为AppError
	return s.convertToAppError(err, metadata)
}

// HandleValidationError 处理验证错误
func (s *ErrorService) HandleValidationError(ctx context.Context, field string, value interface{}, reason string) *errors.AppError {
	metadata := map[string]interface{}{
		"field":  field,
		"value":  value,
		"reason": reason,
	}

	err := fmt.Errorf("validation failed for field %s: %s", field, reason)
	return s.HandleError(ctx, err, metadata)
}

// HandleDatabaseError 处理数据库错误
func (s *ErrorService) HandleDatabaseError(ctx context.Context, operation string, err error) *errors.AppError {
	metadata := map[string]interface{}{
		"operation":  operation,
		"error_type": "database",
	}

	return s.HandleError(ctx, err, metadata)
}

// HandleBusinessLogicError 处理业务逻辑错误
func (s *ErrorService) HandleBusinessLogicError(ctx context.Context, operation string, reason string) *errors.AppError {
	metadata := map[string]interface{}{
		"operation":  operation,
		"error_type": "business_logic",
		"reason":     reason,
	}

	err := fmt.Errorf("business logic error in %s: %s", operation, reason)
	return s.HandleError(ctx, err, metadata)
}

// logError 记录错误日志
func (s *ErrorService) logError(ctx context.Context, err error, metadata map[string]interface{}) {
	fields := []zap.Field{
		logger.ErrorField(err),
	}

	// 添加元数据字段
	for key, value := range metadata {
		fields = append(fields, logger.Any(key, value))
	}

	// 添加上下文信息
	if userID := ctx.Value("user_id"); userID != nil {
		fields = append(fields, logger.String("user_id", fmt.Sprintf("%v", userID)))
	}

	if requestID := ctx.Value("request_id"); requestID != nil {
		fields = append(fields, logger.String("request_id", fmt.Sprintf("%v", requestID)))
	}

	logger.Error("Application error occurred", fields...)
}

// convertToAppError 将普通错误转换为AppError
func (s *ErrorService) convertToAppError(err error, metadata map[string]interface{}) *errors.AppError {
	// 根据错误类型和元数据决定错误代码和HTTP状态码
	errorType, ok := metadata["error_type"].(string)
	if !ok {
		errorType = "unknown"
	}

	switch errorType {
	case "database":
		return errors.ErrDatabaseQuery.WithDetails(metadata)
	case "business_logic":
		return errors.ErrValidationFailed.WithDetails(metadata)
	case "validation":
		return errors.ErrValidationFailed.WithDetails(metadata)
	default:
		return errors.ErrInternalServer.WithDetails(metadata)
	}
}

// WrapError 包装错误，添加上下文信息
func (s *ErrorService) WrapError(ctx context.Context, err error, message string) error {
	// 添加上下文信息
	contextInfo := s.getContextInfo(ctx)

	return fmt.Errorf("%s: %w (context: %v)", message, err, contextInfo)
}

// getContextInfo 获取上下文信息
func (s *ErrorService) getContextInfo(ctx context.Context) map[string]interface{} {
	info := make(map[string]interface{})

	if userID := ctx.Value("user_id"); userID != nil {
		info["user_id"] = userID
	}

	if requestID := ctx.Value("request_id"); requestID != nil {
		info["request_id"] = requestID
	}

	if tableID := ctx.Value("table_id"); tableID != nil {
		info["table_id"] = tableID
	}

	if fieldID := ctx.Value("field_id"); fieldID != nil {
		info["field_id"] = fieldID
	}

	return info
}
