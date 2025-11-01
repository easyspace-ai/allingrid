package http

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/pkg/authctx"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"
)

// JWTAuthMiddleware JWT认证中间件
func JWTAuthMiddleware(authService *application.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var token string
		var tokenSource string

		// 检测是否是 WebSocket 请求
		isWebSocket := c.Request.Header.Get("Connection") == "Upgrade" &&
			c.Request.Header.Get("Upgrade") == "websocket"

		// 优先从 Authorization header 获取 token
		authHeader := c.GetHeader("Authorization")
		if authHeader != "" {
			// 提取 Bearer token
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) == 2 && parts[0] == "Bearer" {
				token = parts[1]
				tokenSource = "header"
			}
		}

		// 如果 header 中没有，尝试从查询参数获取（用于 WebSocket 连接）
		if token == "" {
			token = c.Query("token")
			if token != "" {
				tokenSource = "query"
			}
		}

		// 记录认证调试信息
		if isWebSocket {
			fmt.Printf("[JWT Debug] WebSocket auth attempt - Path: %s, Token source: %s, Has token: %v, Connection: %s, Upgrade: %s\n",
				c.Request.URL.Path, tokenSource, token != "",
				c.Request.Header.Get("Connection"), c.Request.Header.Get("Upgrade"))
		}

		if token == "" {
			response.Error(c, errors.ErrUnauthorized.WithDetails("缺少认证信息"))
			c.Abort()
			return
		}

		// 验证 token
		claims, err := authService.ValidateToken(c.Request.Context(), token)
		if err != nil {
			if isWebSocket {
				fmt.Printf("[JWT Debug] WebSocket token validation failed: %v\n", err)
			}
			response.Error(c, err)
			c.Abort()
			return
		}

		if isWebSocket {
			fmt.Printf("[JWT Debug] WebSocket auth successful - User: %s\n", claims.UserID)
		}

		// 将用户信息设置到 request context 中（供 authctx.UserFrom 使用）
		ctx := authctx.WithUser(c.Request.Context(), claims.UserID)
		c.Request = c.Request.WithContext(ctx)

		// 同时也设置到 gin context 中（供其他需要的地方使用）
		c.Set("user_id", claims.UserID)
		c.Set("email", claims.Email)
		c.Set("is_admin", claims.IsAdmin)

		c.Next()
	}
}

// ValidateBindJSON 统一的JSON绑定和验证辅助函数
// 用于替代直接调用 ShouldBindJSON，提供更详细的错误信息
func ValidateBindJSON(c *gin.Context, obj interface{}) error {
	if err := c.ShouldBindJSON(obj); err != nil {
		return convertBindError(err)
	}
	return nil
}

// convertBindError 将 Gin 的绑定错误转换为 AppError
func convertBindError(err error) error {
	// 检查是否为验证错误
	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		// 收集所有字段的验证错误
		fieldErrors := make([]map[string]string, 0, len(validationErrors))
		var firstErrorMsg string

		for _, fieldErr := range validationErrors {
			fieldName := fieldErr.Field()
			// ✅ 转换为小写JSON字段名（Name → name）
			jsonFieldName := strings.ToLower(string(fieldName[0])) + fieldName[1:]
			tag := fieldErr.Tag()
			param := fieldErr.Param()

			// 根据验证标签生成错误信息（使用JSON字段名）
			message := getValidationErrorMessage(jsonFieldName, tag, param)

			if firstErrorMsg == "" {
				firstErrorMsg = message // ✅ 保存第一个错误消息
			}

			fieldErrors = append(fieldErrors, map[string]string{
				"field":   fieldName,
				"tag":     tag,
				"message": message,
			})
		}

		// ✅ 使用第一个字段的错误作为主消息，更友好
		return errors.ErrValidationFailed.WithMessage(firstErrorMsg).WithDetails(map[string]interface{}{
			"errors": fieldErrors,
			"count":  len(fieldErrors),
		})
	}

	// JSON 解析错误
	if strings.Contains(err.Error(), "json") || strings.Contains(err.Error(), "unmarshal") {
		return errors.ErrInvalidFormat.WithDetails(map[string]interface{}{
			"error":   err.Error(),
			"message": "JSON 格式不正确",
		})
	}

	// 其他绑定错误
	return errors.ErrInvalidRequest.WithDetails(map[string]interface{}{
		"error": err.Error(),
	})
}

// getValidationErrorMessage 根据验证标签生成友好的错误信息
func getValidationErrorMessage(field, tag, param string) string {
	switch tag {
	case "required":
		return fmt.Sprintf("字段 %s 是必填的", field)
	case "email":
		return fmt.Sprintf("字段 %s 必须是有效的邮箱地址", field)
	case "url":
		return fmt.Sprintf("字段 %s 必须是有效的URL", field)
	case "min":
		return fmt.Sprintf("字段 %s 的最小值为 %s", field, param)
	case "max":
		return fmt.Sprintf("字段 %s 的最大值为 %s", field, param)
	case "len":
		return fmt.Sprintf("字段 %s 的长度必须为 %s", field, param)
	case "gte":
		return fmt.Sprintf("字段 %s 必须大于或等于 %s", field, param)
	case "lte":
		return fmt.Sprintf("字段 %s 必须小于或等于 %s", field, param)
	case "gt":
		return fmt.Sprintf("字段 %s 必须大于 %s", field, param)
	case "lt":
		return fmt.Sprintf("字段 %s 必须小于 %s", field, param)
	case "oneof":
		return fmt.Sprintf("字段 %s 必须是以下值之一: %s", field, param)
	case "uuid":
		return fmt.Sprintf("字段 %s 必须是有效的UUID", field)
	case "alphanum":
		return fmt.Sprintf("字段 %s 只能包含字母和数字", field)
	case "numeric":
		return fmt.Sprintf("字段 %s 必须是数字", field)
	default:
		return fmt.Sprintf("字段 %s 验证失败 (%s)", field, tag)
	}
}
