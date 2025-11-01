package authctx

import "context"

type ctxKey string

const userKey ctxKey = "auth_user_id"

// WithUser stores the authenticated user ID into context
func WithUser(ctx context.Context, userID string) context.Context {
	if ctx == nil {
		return context.WithValue(context.Background(), userKey, userID)
	}
	return context.WithValue(ctx, userKey, userID)
}

// UserFrom extracts the authenticated user ID from context
func UserFrom(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	v := ctx.Value(userKey)
	if v == nil {
		return "", false
	}
	if s, ok := v.(string); ok && s != "" {
		return s, true
	}
	return "", false
}
