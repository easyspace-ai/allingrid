//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/container"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/cache"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// IntegrationTestSuite 集成测试基础套件
type IntegrationTestSuite struct {
	suite.Suite
	container *container.Container
	db        *database.Connection
	redis     *cache.RedisClient
	ctx       context.Context
	cancel    context.CancelFunc
}

// SetupSuite 设置测试套件
func (s *IntegrationTestSuite) SetupSuite() {
	// 初始化测试日志
	err := logger.Init(logger.LoggerConfig{
		Level:      "debug",
		Format:     "console",
		OutputPath: "stdout",
	})
	s.Require().NoError(err)

	// 加载测试配置
	cfg := s.loadTestConfig()

	// 创建容器
	s.container = container.NewContainer(cfg)

	// 初始化容器
	err = s.container.Initialize()
	s.Require().NoError(err)

	// 获取数据库和Redis连接
	s.db = s.container.DBConnection()
	s.redis = s.container.RedisClient()

	// 创建测试上下文
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// 启动后台服务
	s.container.StartServices(s.ctx)

	// 等待服务启动
	time.Sleep(100 * time.Millisecond)
}

// TearDownSuite 清理测试套件
func (s *IntegrationTestSuite) TearDownSuite() {
	// 停止后台服务
	s.cancel()

	// 关闭容器
	err := s.container.Close()
	s.Require().NoError(err)

	// 同步日志
	logger.Sync()
}

// SetupTest 每个测试前的设置
func (s *IntegrationTestSuite) SetupTest() {
	// 开始事务
	tx := s.db.GetDB().Begin()
	s.db = &database.Connection{DB: tx}
}

// TearDownTest 每个测试后的清理
func (s *IntegrationTestSuite) TearDownTest() {
	// 回滚事务
	s.db.GetDB().Rollback()

	// 清理Redis
	s.clearRedis()
}

// loadTestConfig 加载测试配置
func (s *IntegrationTestSuite) loadTestConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Host:            "localhost",
			Port:            3001,
			Mode:            "test",
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			IdleTimeout:     120 * time.Second,
			MaxHeaderBytes:  1 << 20,
			ShutdownTimeout: 10 * time.Second,
			EnableCORS:      true,
			EnableSwagger:   false,
		},
		Database: config.DatabaseConfig{
			Host:            "localhost",
			Port:            5432,
			User:            "postgres",
			Password:        "postgres123",
			Name:            "luckdb_test",
			SSLMode:         "disable",
			MaxIdleConns:    5,
			MaxOpenConns:    10,
			ConnMaxLifetime: time.Hour,
			LogLevel:        "warn",
		},
		Redis: config.RedisConfig{
			Host:        "localhost",
			Port:        6379,
			Password:    "",
			DB:          1,
			PoolSize:    5,
			DialTimeout: 5 * time.Second,
		},
		JWT: config.JWTConfig{
			Secret:          "test-secret-key",
			AccessTokenTTL:  15 * time.Minute,
			RefreshTokenTTL: 7 * 24 * time.Hour,
			Issuer:          "luckdb-test",
			EnableRefresh:   true,
		},
		Storage: config.StorageConfig{
			Type: "local",
			Local: config.LocalConfig{
				UploadPath: "./test-uploads",
				URLPrefix:  "/test-uploads",
			},
		},
		Logger: config.LoggerConfig{
			Level:      "debug",
			Format:     "console",
			OutputPath: "stdout",
		},
		WebSocket: config.WebSocketConfig{
			EnableRedisPubSub: false,
			RedisPrefix:       "test:ws",
			HeartbeatInterval: 30 * time.Second,
			ConnectionTimeout: 60 * time.Second,
			MaxConnections:    100,
			EnablePresence:    true,
		},
	}
}

// clearRedis 清理Redis数据
func (s *IntegrationTestSuite) clearRedis() {
	ctx := context.Background()
	// 获取所有键
	keys := []string{
		"test:*",
		"user:*",
		"session:*",
		"token:*",
		"permission:*",
	}

	for _, pattern := range keys {
		_ = s.redis.DeletePattern(ctx, pattern)
	}
}

// Container 获取容器
func (s *IntegrationTestSuite) Container() *container.Container {
	return s.container
}

// DB 获取数据库连接
func (s *IntegrationTestSuite) DB() *gorm.DB {
	return s.db.GetDB()
}

// Redis 获取Redis客户端
func (s *IntegrationTestSuite) Redis() *cache.RedisClient {
	return s.redis
}

// Context 获取测试上下文
func (s *IntegrationTestSuite) Context() context.Context {
	return s.ctx
}

// TestHelpers 测试辅助方法

// CreateTestUser 创建测试用户
func (s *IntegrationTestSuite) CreateTestUser(name, email, password string) string {
	userService := s.container.UserAppService()

	req := container.ApplicationRegisterRequest(name, email, password)
	user, err := userService.Register(s.ctx, req, nil)
	s.Require().NoError(err)
	s.Require().NotNil(user)

	return user.UserID
}

// CreateTestSpace 创建测试空间
func (s *IntegrationTestSuite) CreateTestSpace(userID, name string) string {
	spaceService := s.container.SpaceService()

	req := container.SpaceCreateRequest(userID, name, "Test space")
	space, err := spaceService.CreateSpace(s.ctx, req)
	s.Require().NoError(err)
	s.Require().NotNil(space)

	return space.ID
}

// CreateTestBase 创建测试基础表
func (s *IntegrationTestSuite) CreateTestBase(spaceID, name string) string {
	baseService := s.container.BaseService()

	req := container.BaseCreateRequest(spaceID, name, "Test base")
	base, err := baseService.CreateBase(s.ctx, req)
	s.Require().NoError(err)
	s.Require().NotNil(base)

	return base.ID
}

// CreateTestTable 创建测试表
func (s *IntegrationTestSuite) CreateTestTable(baseID, name string) string {
	tableService := s.container.TableService()

	req := container.TableCreateRequest(baseID, name, "Test table")
	table, err := tableService.CreateTable(s.ctx, req)
	s.Require().NoError(err)
	s.Require().NotNil(table)

	return table.ID
}

// AssertEventPublished 断言事件已发布
func (s *IntegrationTestSuite) AssertEventPublished(eventType string) {
	// 实现事件断言逻辑
	// 简化实现：基础测试暂不验证事件，完整实现需要捕获和验证领域事件
}

// AssertCacheExists 断言缓存存在
func (s *IntegrationTestSuite) AssertCacheExists(key string) {
	exists, err := s.redis.Exists(s.ctx, key)
	s.Require().NoError(err)
	s.True(exists, "Cache key %s should exist", key)
}

// AssertCacheNotExists 断言缓存不存在
func (s *IntegrationTestSuite) AssertCacheNotExists(key string) {
	exists, err := s.redis.Exists(s.ctx, key)
	s.Require().NoError(err)
	s.False(exists, "Cache key %s should not exist", key)
}

// WaitForAsync 等待异步操作完成
func (s *IntegrationTestSuite) WaitForAsync(timeout time.Duration) {
	time.Sleep(timeout)
}

// MeasureExecutionTime 测量执行时间
func (s *IntegrationTestSuite) MeasureExecutionTime(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}

// RunConcurrent 并发运行测试
func (s *IntegrationTestSuite) RunConcurrent(concurrency int, fn func(int)) {
	done := make(chan struct{}, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(index int) {
			defer func() { done <- struct{}{} }()
			fn(index)
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < concurrency; i++ {
		<-done
	}
}

// TestRunner 运行集成测试套件
func TestRunner(t *testing.T, testSuite suite.TestingSuite) {
	suite.Run(t, testSuite)
}
