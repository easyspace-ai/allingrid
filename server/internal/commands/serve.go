package commands

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/container"
	httpHandlers "github.com/easyspace-ai/luckdb/server/internal/interfaces/http"
	"github.com/easyspace-ai/luckdb/server/pkg/assets"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// NewServeCmd 创建API服务器命令
func NewServeCmd(configPath *string, version string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "启动 LuckDB API 服务器",
		Long: `启动 LuckDB RESTful API 服务器
		
服务器提供完整的多维表格 API 功能：
  - 空间(Space)、基础(Base)管理
  - 表格(Table)、字段(Field)操作
  - 记录(Record) CRUD
  - 视图(View)、计算引擎
  - 用户认证与权限控制
`,
		Example: `  # 使用默认配置启动
  luckdb serve
  
  
  # 指定配置文件启动
  luckdb serve --config production.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(version)
		},
	}

	return cmd
}

func runServe(version string) error {
	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return err
	}

	// 初始化日志
	loggerConfig := logger.LoggerConfig{
		Level:      cfg.Logger.Level,
		Format:     cfg.Logger.Format,
		OutputPath: cfg.Logger.OutputPath,
	}
	if err := logger.Init(loggerConfig); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		return err
	}

	// 初始化SQL日志
	sqlLoggerConfig := logger.SQLLoggerConfig{
		Enabled:    cfg.SQLLogger.Enabled,
		OutputPath: cfg.SQLLogger.OutputPath,
		MaxSize:    cfg.SQLLogger.MaxSize,
		MaxBackups: cfg.SQLLogger.MaxBackups,
		MaxAge:     cfg.SQLLogger.MaxAge,
		Compress:   cfg.SQLLogger.Compress,
	}
	if err := logger.InitSQLLogger(sqlLoggerConfig); err != nil {
		fmt.Printf("Failed to initialize SQL logger: %v\n", err)
		return err
	}

	logger.Info("Starting LuckDB API Server",
		logger.String("version", version),
		logger.String("mode", cfg.Server.Mode),
	)

	if cfg.SQLLogger.Enabled {
		logger.Info("SQL Logger enabled",
			logger.String("output", cfg.SQLLogger.OutputPath),
		)
	}

	// 创建依赖注入容器
	cont := container.NewContainer(cfg)

	// 初始化容器
	if err := cont.Initialize(); err != nil {
		logger.Fatal("Failed to initialize container", logger.ErrorField(err))
	}
	defer cont.Close()

	// 启动后台服务
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	cont.StartServices(srvCtx)

	// 创建Gin引擎
	router := setupRouter(cfg, cont, version)

	// 创建HTTP服务器
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 启动服务器
	go func() {
		logger.Info("API Server starting",
			logger.Int("port", cfg.Server.Port),
			logger.String("mode", cfg.Server.Mode),
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server failed to start", logger.ErrorField(err))
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("API Server shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("Server forced to shutdown", logger.ErrorField(err))
	}

	// 关闭SQL日志记录器
	if logger.SQLLogger != nil {
		if err := logger.SQLLogger.Close(); err != nil {
			logger.Error("Failed to close SQL logger", logger.ErrorField(err))
		}
	}

	logger.Info("API Server exited")
	return nil
}

// setupRouter 设置路由
func setupRouter(cfg *config.Config, cont *container.Container, version string) *gin.Engine {
	// 设置Gin模式
	if cfg.Server.Mode == "release" || cfg.Server.Mode == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	// 基础中间件 - 使用自定义 panic 恢复中间件，记录详细错误
	router.Use(customRecovery())
	router.Use(corsMiddleware())
	router.Use(loggerMiddleware())

	// 健康检查
	router.GET("/health", healthCheckHandler(cont, version))
	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "LuckDB API",
			"version": version,
			"status":  "running",
			"message": "多维表格数据库系统",
		})
	})

	// 设置API路由
	httpHandlers.SetupRoutes(router, cont, assets.StaticFiles)

	return router
}

// customRecovery 自定义 panic 恢复中间件，记录详细错误日志
func customRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// 记录 panic 详细信息
				logger.Error("Panic recovered",
					logger.Any("panic", err),
					logger.String("method", c.Request.Method),
					logger.String("path", c.Request.URL.Path),
					logger.String("ip", c.ClientIP()),
				)
				
				// 确保响应头未写入
				if !c.Writer.Written() {
					// 返回 500 错误响应
					c.JSON(http.StatusInternalServerError, gin.H{
						"code":    http.StatusInternalServerError,
						"message": "服务器内部错误",
						"data":    nil,
					})
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

// healthCheckHandler 健康检查处理器
func healthCheckHandler(cont *container.Container, version string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()

		status := gin.H{
			"status":    "ok",
			"timestamp": time.Now().Unix(),
			"version":   version,
		}

		// 使用容器的健康检查
		if err := cont.Health(ctx); err != nil {
			status["status"] = "degraded"
			status["error"] = err.Error()
			c.JSON(http.StatusServiceUnavailable, status)
			return
		}

		status["database"] = "healthy"
		status["services"] = "healthy"

		c.JSON(http.StatusOK, status)
	}
}

// corsMiddleware CORS中间件
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		fmt.Printf("🔥 CORS Middleware called for path: %s\n", c.Request.URL.Path)

		// 开发环境：允许所有来源
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		// 注意：当使用 * 时，不能设置 Access-Control-Allow-Credentials: true
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE, PATCH")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// loggerMiddleware 日志中间件
func loggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		duration := time.Since(start)

		logger.Info("HTTP Request",
			logger.String("method", c.Request.Method),
			logger.String("path", path),
			logger.String("query", query),
			logger.Int("status", c.Writer.Status()),
			logger.String("ip", c.ClientIP()),
			logger.String("user_agent", c.Request.UserAgent()),
			logger.Duration("duration", duration),
		)
	}
}
