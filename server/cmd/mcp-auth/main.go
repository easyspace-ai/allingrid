package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/mcp/auth"
)

// MockSessionRepository 模拟的 Session 仓储
type MockSessionRepository struct{}

func (m *MockSessionRepository) Create(ctx context.Context, session *auth.Session) error {
	return nil
}

func (m *MockSessionRepository) GetByID(ctx context.Context, id string) (*auth.Session, error) {
	// 返回模拟的会话数据
	return &auth.Session{
		ID:        id,
		UserID:    "test-user",
		IPAddress: "127.0.0.1",
		UserAgent: "Mozilla/5.0",
		CreatedAt: time.Now().Add(-time.Hour),
		ExpiresAt: time.Now().Add(time.Hour),
		IsActive:  true,
	}, nil
}

func (m *MockSessionRepository) Update(ctx context.Context, session *auth.Session) error {
	return nil
}

func (m *MockSessionRepository) Delete(ctx context.Context, id string) error {
	return nil
}

func (m *MockSessionRepository) DeleteByUserID(ctx context.Context, userID string) error {
	return nil
}

func (m *MockSessionRepository) CleanupExpired(ctx context.Context) error {
	return nil
}

func main() {
	var (
		action      = flag.String("action", "", "Action to perform: create-api-key, list-api-keys, revoke-api-key, create-token, validate-token, create-session, validate-session")
		userID      = flag.String("user-id", "", "User ID")
		scopes      = flag.String("scopes", "", "Comma-separated list of scopes")
		spaces      = flag.String("spaces", "", "Comma-separated list of spaces")
		description = flag.String("description", "", "Description for API key")
		ttl         = flag.Duration("ttl", 0, "Time to live (e.g., 1h, 24h, 7d)")
		keyID       = flag.String("key-id", "", "API Key ID")
		token       = flag.String("token", "", "JWT Token")
		sessionID   = flag.String("session-id", "", "Session ID")
		ipAddress   = flag.String("ip", "127.0.0.1", "IP Address")
		userAgent   = flag.String("user-agent", "MCP-Auth-Tool/1.0", "User Agent")
	)
	flag.Parse()

	if *action == "" {
		fmt.Println("Usage: mcp-auth -action=<action> [options]")
		fmt.Println("Actions:")
		fmt.Println("  create-api-key  - Create a new API key")
		fmt.Println("  list-api-keys   - List API keys for a user")
		fmt.Println("  revoke-api-key  - Revoke an API key")
		fmt.Println("  create-token    - Create a JWT token")
		fmt.Println("  validate-token  - Validate a JWT token")
		fmt.Println("  create-session  - Create a session")
		fmt.Println("  validate-session - Validate a session")
		os.Exit(1)
	}

	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 检查 MCP 是否启用
	if !cfg.MCP.Enabled {
		log.Fatal("MCP service is disabled in configuration")
	}

	ctx := context.Background()

	switch *action {
	case "create-api-key":
		createAPIKey(ctx, cfg, *userID, *scopes, *description, *ttl)
	case "list-api-keys":
		listAPIKeys(ctx, cfg, *userID)
	case "revoke-api-key":
		revokeAPIKey(ctx, cfg, *keyID)
	case "create-token":
		createToken(ctx, cfg, *userID, *scopes, *spaces)
	case "validate-token":
		validateToken(ctx, cfg, *token)
	case "create-session":
		createSession(ctx, cfg, *userID, *ipAddress, *userAgent)
	case "validate-session":
		validateSession(ctx, cfg, *sessionID)
	default:
		log.Fatalf("Unknown action: %s", *action)
	}
}

func createAPIKey(ctx context.Context, cfg *config.Config, userID, scopes, description string, ttl time.Duration) {
	if userID == "" {
		log.Fatal("User ID is required")
	}

	scopeList := strings.Split(scopes, ",")
	if scopes == "" {
		scopeList = []string{"mcp:read", "mcp:write"}
	}

	var ttlPtr *time.Duration
	if ttl > 0 {
		ttlPtr = &ttl
	}

	// 创建 API Key 服务
	apiKeyConfig := &auth.APIKeyConfig{
		Enabled:      cfg.MCP.Auth.APIKey.Enabled,
		KeyLength:    cfg.MCP.Auth.APIKey.KeyLength,
		SecretLength: cfg.MCP.Auth.APIKey.SecretLength,
		DefaultTTL:   cfg.MCP.Auth.APIKey.DefaultTTL,
		MaxTTL:       cfg.MCP.Auth.APIKey.MaxTTL,
		Header:       cfg.MCP.Auth.APIKey.Header,
		Format:       cfg.MCP.Auth.APIKey.Format,
	}
	apiKeyService := auth.NewAPIKeyService(nil, apiKeyConfig) // TODO: 实现仓储

	apiKey, err := apiKeyService.GenerateAPIKey(userID, scopeList, description, ttlPtr)
	if err != nil {
		log.Fatalf("Failed to generate API key: %v", err)
	}

	fmt.Printf("API Key created successfully:\n")
	fmt.Printf("  ID: %s\n", apiKey.ID)
	fmt.Printf("  Key ID: %s\n", apiKey.KeyID)
	fmt.Printf("  Secret: %s\n", apiKey.Secret)
	fmt.Printf("  User ID: %s\n", apiKey.UserID)
	fmt.Printf("  Scopes: %s\n", strings.Join(apiKey.Scopes, ", "))
	fmt.Printf("  Description: %s\n", apiKey.Description)
	fmt.Printf("  Expires At: %s\n", apiKey.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("  Created At: %s\n", apiKey.CreatedAt.Format(time.RFC3339))

	fmt.Printf("\nUsage:\n")
	fmt.Printf("  Header: %s\n", cfg.MCP.Auth.APIKey.Header)
	fmt.Printf("  Value: %s:%s\n", apiKey.KeyID, apiKey.Secret)
}

func listAPIKeys(ctx context.Context, cfg *config.Config, userID string) {
	if userID == "" {
		log.Fatal("User ID is required")
	}

	// 创建 API Key 服务
	apiKeyConfig := &auth.APIKeyConfig{
		Enabled:      cfg.MCP.Auth.APIKey.Enabled,
		KeyLength:    cfg.MCP.Auth.APIKey.KeyLength,
		SecretLength: cfg.MCP.Auth.APIKey.SecretLength,
		DefaultTTL:   cfg.MCP.Auth.APIKey.DefaultTTL,
		MaxTTL:       cfg.MCP.Auth.APIKey.MaxTTL,
		Header:       cfg.MCP.Auth.APIKey.Header,
		Format:       cfg.MCP.Auth.APIKey.Format,
	}
	apiKeyService := auth.NewAPIKeyService(nil, apiKeyConfig) // TODO: 实现仓储

	apiKeys, err := apiKeyService.ListUserAPIKeys(ctx, userID)
	if err != nil {
		log.Fatalf("Failed to list API keys: %v", err)
	}

	if len(apiKeys) == 0 {
		fmt.Printf("No API keys found for user: %s\n", userID)
		return
	}

	fmt.Printf("API Keys for user %s:\n", userID)
	for _, apiKey := range apiKeys {
		fmt.Printf("  ID: %s\n", apiKey.ID)
		fmt.Printf("  Key ID: %s\n", apiKey.KeyID)
		fmt.Printf("  Description: %s\n", apiKey.Description)
		fmt.Printf("  Scopes: %s\n", strings.Join(apiKey.Scopes, ", "))
		fmt.Printf("  Active: %t\n", apiKey.IsActive)
		fmt.Printf("  Expires At: %s\n", apiKey.ExpiresAt.Format(time.RFC3339))
		fmt.Printf("  Last Used: %s\n", apiKey.LastUsedAt.Format(time.RFC3339))
		fmt.Println()
	}
}

func revokeAPIKey(ctx context.Context, cfg *config.Config, keyID string) {
	if keyID == "" {
		log.Fatal("API Key ID is required")
	}

	// 创建 API Key 服务
	apiKeyConfig := &auth.APIKeyConfig{
		Enabled:      cfg.MCP.Auth.APIKey.Enabled,
		KeyLength:    cfg.MCP.Auth.APIKey.KeyLength,
		SecretLength: cfg.MCP.Auth.APIKey.SecretLength,
		DefaultTTL:   cfg.MCP.Auth.APIKey.DefaultTTL,
		MaxTTL:       cfg.MCP.Auth.APIKey.MaxTTL,
		Header:       cfg.MCP.Auth.APIKey.Header,
		Format:       cfg.MCP.Auth.APIKey.Format,
	}
	apiKeyService := auth.NewAPIKeyService(nil, apiKeyConfig) // TODO: 实现仓储

	err := apiKeyService.RevokeAPIKey(ctx, keyID)
	if err != nil {
		log.Fatalf("Failed to revoke API key: %v", err)
	}

	fmt.Printf("API Key %s revoked successfully\n", keyID)
}

func createToken(ctx context.Context, cfg *config.Config, userID, scopes, spaces string) {
	if userID == "" {
		log.Fatal("User ID is required")
	}

	scopeList := strings.Split(scopes, ",")
	if scopes == "" {
		scopeList = []string{"mcp:read", "mcp:write"}
	}

	spaceList := strings.Split(spaces, ",")
	if spaces == "" {
		spaceList = []string{}
	}

	// 创建 JWT 服务
	jwtConfig := &auth.JWTConfig{
		Enabled:         cfg.MCP.Auth.JWT.Enabled,
		Header:          cfg.MCP.Auth.JWT.Header,
		Prefix:          cfg.MCP.Auth.JWT.Prefix,
		Secret:          cfg.MCP.Auth.JWT.Secret,
		Issuer:          cfg.MCP.Auth.JWT.Issuer,
		Audience:        cfg.MCP.Auth.JWT.Audience,
		AccessTokenTTL:  cfg.MCP.Auth.JWT.AccessTokenTTL,
		RefreshTokenTTL: cfg.MCP.Auth.JWT.RefreshTokenTTL,
	}
	jwtService := auth.NewJWTService(jwtConfig)

	token, err := jwtService.GenerateToken(userID, scopeList, spaceList)
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}

	fmt.Printf("JWT Token created successfully:\n")
	fmt.Printf("  User ID: %s\n", userID)
	fmt.Printf("  Scopes: %s\n", strings.Join(scopeList, ", "))
	fmt.Printf("  Spaces: %s\n", strings.Join(spaceList, ", "))
	fmt.Printf("  Token: %s\n", token)

	fmt.Printf("\nUsage:\n")
	fmt.Printf("  Header: %s\n", cfg.MCP.Auth.JWT.Header)
	fmt.Printf("  Value: %s%s\n", cfg.MCP.Auth.JWT.Prefix, token)
}

func validateToken(ctx context.Context, cfg *config.Config, token string) {
	if token == "" {
		log.Fatal("Token is required")
	}

	// 创建 JWT 服务
	jwtConfig := &auth.JWTConfig{
		Enabled:         cfg.MCP.Auth.JWT.Enabled,
		Header:          cfg.MCP.Auth.JWT.Header,
		Prefix:          cfg.MCP.Auth.JWT.Prefix,
		Secret:          cfg.MCP.Auth.JWT.Secret,
		Issuer:          cfg.MCP.Auth.JWT.Issuer,
		Audience:        cfg.MCP.Auth.JWT.Audience,
		AccessTokenTTL:  cfg.MCP.Auth.JWT.AccessTokenTTL,
		RefreshTokenTTL: cfg.MCP.Auth.JWT.RefreshTokenTTL,
	}
	jwtService := auth.NewJWTService(jwtConfig)

	claims, err := jwtService.ValidateToken(token)
	if err != nil {
		log.Fatalf("Token validation failed: %v", err)
	}

	fmt.Printf("Token is valid:\n")
	fmt.Printf("  User ID: %s\n", claims.UserID)
	fmt.Printf("  Scopes: %s\n", strings.Join(claims.MCPScopes, ", "))
	fmt.Printf("  Spaces: %s\n", strings.Join(claims.MCPSpaces, ", "))
	fmt.Printf("  Issuer: %s\n", claims.Issuer)
	fmt.Printf("  Audience: %s\n", strings.Join(claims.Audience, ", "))
	fmt.Printf("  Issued At: %s\n", claims.IssuedAt.Time.Format(time.RFC3339))
	fmt.Printf("  Expires At: %s\n", claims.ExpiresAt.Time.Format(time.RFC3339))
}

func createSession(ctx context.Context, cfg *config.Config, userID, ipAddress, userAgent string) {
	if userID == "" {
		log.Fatal("User ID is required")
	}

	// 创建 Session 服务
	sessionConfig := &auth.SessionConfig{
		Enabled:    cfg.MCP.Auth.Session.Enabled,
		CookieName: cfg.MCP.Auth.Session.CookieName,
		Secure:     cfg.MCP.Auth.Session.Secure,
		HTTPOnly:   cfg.MCP.Auth.Session.HTTPOnly,
		SameSite:   cfg.MCP.Auth.Session.SameSite,
		MaxAge:     cfg.MCP.Auth.Session.MaxAge,
	}
	mockSessionRepo := &MockSessionRepository{}
	sessionService := auth.NewSessionService(mockSessionRepo, sessionConfig)

	session, err := sessionService.CreateSession(ctx, userID, ipAddress, userAgent)
	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}

	fmt.Printf("Session created successfully:\n")
	fmt.Printf("  ID: %s\n", session.ID)
	fmt.Printf("  User ID: %s\n", session.UserID)
	fmt.Printf("  IP Address: %s\n", session.IPAddress)
	fmt.Printf("  User Agent: %s\n", session.UserAgent)
	fmt.Printf("  Created At: %s\n", session.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Expires At: %s\n", session.ExpiresAt.Format(time.RFC3339))

	fmt.Printf("\nUsage:\n")
	fmt.Printf("  Cookie: %s=%s\n", cfg.MCP.Auth.Session.CookieName, session.ID)
}

func validateSession(ctx context.Context, cfg *config.Config, sessionID string) {
	if sessionID == "" {
		log.Fatal("Session ID is required")
	}

	// 创建 Session 服务
	sessionConfig := &auth.SessionConfig{
		Enabled:    cfg.MCP.Auth.Session.Enabled,
		CookieName: cfg.MCP.Auth.Session.CookieName,
		Secure:     cfg.MCP.Auth.Session.Secure,
		HTTPOnly:   cfg.MCP.Auth.Session.HTTPOnly,
		SameSite:   cfg.MCP.Auth.Session.SameSite,
		MaxAge:     cfg.MCP.Auth.Session.MaxAge,
	}
	mockSessionRepo := &MockSessionRepository{}
	sessionService := auth.NewSessionService(mockSessionRepo, sessionConfig)

	session, err := sessionService.ValidateSession(ctx, sessionID)
	if err != nil {
		log.Fatalf("Session validation failed: %v", err)
	}

	fmt.Printf("Session is valid:\n")
	fmt.Printf("  ID: %s\n", session.ID)
	fmt.Printf("  User ID: %s\n", session.UserID)
	fmt.Printf("  IP Address: %s\n", session.IPAddress)
	fmt.Printf("  User Agent: %s\n", session.UserAgent)
	fmt.Printf("  Created At: %s\n", session.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Expires At: %s\n", session.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("  Active: %t\n", session.IsActive)
}
