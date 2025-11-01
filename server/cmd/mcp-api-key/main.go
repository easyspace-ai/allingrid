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
	"github.com/easyspace-ai/luckdb/server/internal/mcp/auth/repository"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	var (
		action      = flag.String("action", "", "Action: create, list, revoke")
		userID      = flag.String("user-id", "", "User ID")
		scopes      = flag.String("scopes", "", "Comma-separated scopes")
		description = flag.String("description", "", "API Key description")
		ttl         = flag.String("ttl", "", "Time to live (e.g., 1h, 24h, 7d)")
		keyID       = flag.String("key-id", "", "API Key ID (for revoke)")
	)
	flag.Parse()

	if *action == "" {
		fmt.Println("Usage: mcp-api-key -action=<create|list|revoke> [options]")
		fmt.Println("")
		fmt.Println("Actions:")
		fmt.Println("  create    Create a new API key")
		fmt.Println("  list      List API keys for a user")
		fmt.Println("  revoke    Revoke an API key")
		fmt.Println("")
		fmt.Println("Options:")
		fmt.Println("  -user-id      User ID (required for create, list)")
		fmt.Println("  -scopes       Comma-separated scopes (optional for create)")
		fmt.Println("  -description  API Key description (optional for create)")
		fmt.Println("  -ttl          Time to live (optional for create)")
		fmt.Println("  -key-id       API Key ID (required for revoke)")
		os.Exit(1)
	}

	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 连接数据库
	db, err := gorm.Open(postgres.Open(cfg.Database.GetDSN()), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 创建仓储
	apiKeyRepo := repository.NewAPIKeyRepository(db)

	// 创建服务
	apiKeyConfig := &auth.APIKeyConfig{
		KeyLength:    32,
		SecretLength: 64,
		DefaultTTL:   8760 * time.Hour,  // 1年
		MaxTTL:       87600 * time.Hour, // 10年
		Header:       "X-MCP-API-Key",
		Format:       "key_id:key_secret",
	}
	apiKeyService := auth.NewAPIKeyService(apiKeyRepo, apiKeyConfig)

	ctx := context.Background()

	switch *action {
	case "create":
		if *userID == "" {
			log.Fatal("User ID is required for create action")
		}

		// 解析权限范围
		var scopeList []string
		if *scopes != "" {
			scopeList = strings.Split(*scopes, ",")
			for i, scope := range scopeList {
				scopeList[i] = strings.TrimSpace(scope)
			}
		} else {
			// 默认权限范围
			scopeList = []string{"read:records", "write:records", "read:schema"}
		}

		// 解析 TTL
		var ttlDuration *time.Duration
		if *ttl != "" {
			duration, err := time.ParseDuration(*ttl)
			if err != nil {
				log.Fatalf("Invalid TTL format: %v", err)
			}
			ttlDuration = &duration
		}

		// 创建 API Key
		apiKey, err := apiKeyService.CreateAPIKey(ctx, *userID, scopeList, *description, ttlDuration)
		if err != nil {
			log.Fatalf("Failed to create API key: %v", err)
		}

		fmt.Printf("✅ API Key created successfully!\n")
		fmt.Printf("ID: %s\n", apiKey.ID)
		fmt.Printf("Key ID: %s\n", apiKey.KeyID)
		fmt.Printf("Secret: %s\n", apiKey.Secret)
		fmt.Printf("Full Key: %s:%s\n", apiKey.KeyID, apiKey.Secret)
		fmt.Printf("User ID: %s\n", apiKey.UserID)
		fmt.Printf("Scopes: %s\n", strings.Join(apiKey.Scopes, ", "))
		fmt.Printf("Description: %s\n", apiKey.Description)
		fmt.Printf("Expires At: %s\n", apiKey.ExpiresAt.Format(time.RFC3339))
		fmt.Printf("Created At: %s\n", apiKey.CreatedAt.Format(time.RFC3339))

	case "list":
		if *userID == "" {
			log.Fatal("User ID is required for list action")
		}

		// 列出用户的 API Keys
		apiKeys, err := apiKeyService.ListUserAPIKeys(ctx, *userID)
		if err != nil {
			log.Fatalf("Failed to list API keys: %v", err)
		}

		if len(apiKeys) == 0 {
			fmt.Printf("No API keys found for user: %s\n", *userID)
			return
		}

		fmt.Printf("API Keys for user: %s\n", *userID)
		fmt.Printf("Total: %d\n\n", len(apiKeys))

		for i, apiKey := range apiKeys {
			fmt.Printf("%d. ID: %s\n", i+1, apiKey.ID)
			fmt.Printf("   Key ID: %s\n", apiKey.KeyID)
			fmt.Printf("   Description: %s\n", apiKey.Description)
			fmt.Printf("   Scopes: %s\n", strings.Join(apiKey.Scopes, ", "))
			fmt.Printf("   Status: %s\n", getStatus(apiKey))
			fmt.Printf("   Created: %s\n", apiKey.CreatedAt.Format(time.RFC3339))
			if apiKey.ExpiresAt != nil {
				fmt.Printf("   Expires: %s\n", apiKey.ExpiresAt.Format(time.RFC3339))
			}
			if apiKey.LastUsedAt != nil {
				fmt.Printf("   Last Used: %s\n", apiKey.LastUsedAt.Format(time.RFC3339))
			}
			fmt.Println()
		}

	case "revoke":
		if *keyID == "" {
			log.Fatal("API Key ID is required for revoke action")
		}

		// 撤销 API Key
		err := apiKeyService.RevokeAPIKey(ctx, *keyID)
		if err != nil {
			log.Fatalf("Failed to revoke API key: %v", err)
		}

		fmt.Printf("✅ API Key revoked successfully: %s\n", *keyID)

	default:
		log.Fatalf("Unknown action: %s", *action)
	}
}

func getStatus(apiKey *auth.APIKey) string {
	if !apiKey.IsActive {
		return "INACTIVE"
	}
	if apiKey.IsExpired() {
		return "EXPIRED"
	}
	return "ACTIVE"
}
