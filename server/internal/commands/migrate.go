package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/easyspace-ai/luckdb/server/internal/application"
	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

const (
	bannerTop = "╔══════════════════════════════════════════════════════════════════╗"
	bannerBot = "╚══════════════════════════════════════════════════════════════════╝"
)

// NewMigrateCmd 创建数据库迁移命令
func NewMigrateCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "数据库迁移管理",
		Long: `数据库迁移管理工具（golang-migrate + GORM）

支持两种迁移方式：
  - golang-migrate: SQL 迁移文件管理
  - GORM AutoMigrate: Go 模型自动同步
  - hybrid: 混合模式（推荐，先执行SQL再同步模型）`,
		Example: `  # 混合迁移（推荐）
  luckdb migrate up
  
  # 只执行SQL迁移
  luckdb migrate up --mode=migrate-only
  
  # 只执行GORM同步
  luckdb migrate up --mode=gorm-only
  
  # 回滚迁移
  luckdb migrate down
  
  # 查看当前版本
  luckdb migrate version
  
  # 强制设置版本
  luckdb migrate force 000010`,
	}

	cmd.AddCommand(newMigrateUpCmd(configPath))
	cmd.AddCommand(newMigrateDownCmd(configPath))
	cmd.AddCommand(newMigrateVersionCmd(configPath))
	cmd.AddCommand(newMigrateForceCmd(configPath))
	cmd.AddCommand(newMigrateDropCmd(configPath))

	return cmd
}

// newMigrateUpCmd 创建up命令
func newMigrateUpCmd(configPath *string) *cobra.Command {
	var mode string

	cmd := &cobra.Command{
		Use:   "up",
		Short: "执行数据库迁移（升级）",
		Long:  "执行所有待应用的数据库迁移文件",
		Example: `  # 混合迁移
  luckdb migrate up
  
  # 只执行SQL迁移
  luckdb migrate up --mode=migrate-only`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigration(mode, "")
		},
	}

	cmd.Flags().StringVar(&mode, "mode", "hybrid", "迁移模式: hybrid, migrate-only, gorm-only")

	return cmd
}

// newMigrateDownCmd 创建down命令
func newMigrateDownCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "down",
		Short: "回滚数据库迁移",
		Long:  "回滚最后一次应用的迁移",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGolangMigrateOnly("down", "")
		},
	}

	return cmd
}

// newMigrateVersionCmd 创建version命令
func newMigrateVersionCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "显示当前迁移版本",
		Long:  "显示当前数据库的迁移版本号和状态",
		RunE: func(cmd *cobra.Command, args []string) error {
			return showMigrationVersion()
		},
	}

	return cmd
}

// newMigrateForceCmd 创建force命令
func newMigrateForceCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "force [version]",
		Short: "强制设置迁移版本",
		Long:  "强制将迁移版本设置为指定值（用于修复错误状态）",
		Args:  cobra.ExactArgs(1),
		Example: `  # 强制设置版本为 000010
  luckdb migrate force 000010`,
		RunE: func(cmd *cobra.Command, args []string) error {
			version := args[0]
			return runGolangMigrateOnly("force", version)
		},
	}

	return cmd
}

// newMigrateDropCmd 创建drop命令
func newMigrateDropCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "drop",
		Short: "删除所有表（危险操作）",
		Long:  "删除数据库中的所有表和数据（此操作不可恢复）",
		RunE: func(cmd *cobra.Command, args []string) error {
			return confirmAndRun(func() error {
				return runGolangMigrateOnly("drop", "")
			}, "此操作将删除所有表和数据")
		},
	}

	return cmd
}

// runMigration 执行迁移
func runMigration(mode, version string) error {
	printBanner()

	// 加载配置
	cfg, err := loadMigrateConfig()
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 初始化日志
	if err := initMigrateLogger(cfg); err != nil {
		return fmt.Errorf("初始化日志失败: %w", err)
	}

	log := logger.Logger.Named("migrate")

	// 打印配置
	printConfig(cfg)

	// 创建迁移服务
	migrationsDir := getMigrationsDir()
	migrateService := application.NewMigrateService(cfg, migrationsDir)

	ctx := context.Background()

	switch mode {
	case "gorm-only":
		return runGORMAutoMigrate(ctx, migrateService, log)
	case "migrate-only":
		return runGolangMigrate(ctx, migrateService, "up", "", log)
	case "hybrid":
		fallthrough
	default:
		return runHybridMigration(ctx, migrateService, log)
	}
}

// runGolangMigrateOnly 只执行golang-migrate
func runGolangMigrateOnly(command, version string) error {
	cfg, err := loadMigrateConfig()
	if err != nil {
		return err
	}

	if err := initMigrateLogger(cfg); err != nil {
		return err
	}

	log := logger.Logger.Named("migrate")

	migrationsDir := getMigrationsDir()
	migrateService := application.NewMigrateService(cfg, migrationsDir)

	return runGolangMigrate(context.Background(), migrateService, command, version, log)
}

// showMigrationVersion 显示迁移版本
func showMigrationVersion() error {
	cfg, err := loadMigrateConfig()
	if err != nil {
		return err
	}

	if err := initMigrateLogger(cfg); err != nil {
		return err
	}

	log := logger.Logger.Named("migrate")

	migrationsDir := getMigrationsDir()
	migrateService := application.NewMigrateService(cfg, migrationsDir)

	version, dirty, err := migrateService.GetMigrationVersion(context.Background())
	if err != nil {
		return fmt.Errorf("获取版本失败: %w", err)
	}

	fmt.Println("📌 当前迁移版本信息:")
	fmt.Printf("   版本号: %d\n", version)

	status := "clean"
	if dirty {
		status = "dirty (需要修复)"
	}
	fmt.Printf("   状态: %s\n", status)
	fmt.Println()

	log.Info("迁移版本信息",
		zap.Uint("version", version),
		zap.Bool("dirty", dirty),
	)

	return nil
}

// runHybridMigration 执行混合迁移
func runHybridMigration(ctx context.Context, service *application.MigrateService, log *zap.Logger) error {
	fmt.Println("📦 混合迁移模式：")
	fmt.Println("   步骤1: 运行 golang-migrate（SQL迁移）")
	fmt.Println("   步骤2: 运行 GORM AutoMigrate（模型同步）")
	fmt.Println()

	log.Info("开始混合迁移")

	result, err := service.RunHybridMigration(ctx)
	if err != nil {
		log.Error("混合迁移失败", zap.Error(err))
		return fmt.Errorf("混合迁移失败: %w", err)
	}

	printMigrationResult(result)

	log.Info("混合迁移完成",
		zap.Duration("duration", result.Duration),
		zap.Int("table_count", result.TableCount),
	)

	fmt.Println()
	fmt.Println("💡 提示:")
	fmt.Println("   - golang-migrate 管理SQL迁移文件")
	fmt.Println("   - GORM AutoMigrate 同步Go模型")
	fmt.Println("   - 现在可以启动服务: luckdb serve")
	fmt.Println()

	return nil
}

// runGolangMigrate 执行 golang-migrate
func runGolangMigrate(ctx context.Context, service *application.MigrateService, command, version string, log *zap.Logger) error {
	fmt.Printf("⚡ 执行 golang-migrate (%s)...\n", command)

	log.Info("开始执行 golang-migrate",
		zap.String("command", command),
		zap.String("version", version),
	)

	if err := service.RunGolangMigrate(ctx, command, version); err != nil {
		log.Error("golang-migrate 执行失败",
			zap.String("command", command),
			zap.Error(err),
		)
		return fmt.Errorf("golang-migrate 失败: %w", err)
	}

	fmt.Println("✅ golang-migrate 完成")
	log.Info("golang-migrate 执行成功", zap.String("command", command))

	return nil
}

// runGORMAutoMigrate 执行 GORM AutoMigrate
func runGORMAutoMigrate(ctx context.Context, service *application.MigrateService, log *zap.Logger) error {
	fmt.Println("📦 GORM AutoMigrate 模式")
	fmt.Println()

	log.Info("开始执行 GORM AutoMigrate")

	if err := service.RunGORMAutoMigrate(ctx); err != nil {
		log.Error("GORM AutoMigrate 执行失败", zap.Error(err))
		return fmt.Errorf("GORM AutoMigrate 失败: %w", err)
	}

	fmt.Println("✅ GORM AutoMigrate 完成")
	log.Info("GORM AutoMigrate 执行成功")

	return nil
}

// confirmAndRun 确认后执行危险操作
func confirmAndRun(fn func() error, warning string) error {
	fmt.Printf("⚠️  警告: %s！\n", warning)
	fmt.Print("确认执行? (yes/no): ")

	var confirm string
	fmt.Scanln(&confirm)

	if confirm != "yes" {
		fmt.Println("操作已取消")
		return nil
	}

	return fn()
}

// loadMigrateConfig 加载配置
func loadMigrateConfig() (*config.Config, error) {
	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		// 如果配置文件不存在，创建默认配置
		cfg = &config.Config{}

		// 设置默认值
		cfg.Database.Host = "localhost"
		cfg.Database.Port = 5432
		cfg.Database.User = "postgres"
		cfg.Database.Password = "postgres"
		cfg.Database.Name = "easytable"
		cfg.Database.SSLMode = "disable"

		cfg.Logger.Level = "info"
		cfg.Logger.Format = "console"
		cfg.Logger.OutputPath = "stdout"
	}

	// 从环境变量覆盖配置
	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		cfg.Database.Host = host
	}
	if port := os.Getenv("POSTGRES_PORT"); port != "" {
		fmt.Sscanf(port, "%d", &cfg.Database.Port)
	}
	if user := os.Getenv("POSTGRES_USER"); user != "" {
		cfg.Database.User = user
	}
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		cfg.Database.Password = password
	}
	if dbname := os.Getenv("POSTGRES_DB"); dbname != "" {
		cfg.Database.Name = dbname
	}
	if sslmode := os.Getenv("POSTGRES_SSL_MODE"); sslmode != "" {
		cfg.Database.SSLMode = sslmode
	}

	return cfg, nil
}

// initMigrateLogger 初始化日志
func initMigrateLogger(cfg *config.Config) error {
	logConfig := logger.LoggerConfig{
		Level:      cfg.Logger.Level,
		Format:     cfg.Logger.Format,
		OutputPath: cfg.Logger.OutputPath,
	}
	return logger.Init(logConfig)
}

// getMigrationsDir 获取迁移文件目录
func getMigrationsDir() string {
	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		dir = "migrations"
	}

	// 转换为绝对路径
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return dir
	}

	return absDir
}

// printBanner 打印 banner
func printBanner() {
	fmt.Println(bannerTop)
	fmt.Println("║                                                                  ║")
	fmt.Println("║      🚀 LuckDB 数据库迁移工具 (golang-migrate + GORM)           ║")
	fmt.Println("║                                                                  ║")
	fmt.Println(bannerBot)
	fmt.Println()
}

// printConfig 打印配置信息
func printConfig(cfg *config.Config) {
	fmt.Printf("📋 数据库配置:\n")
	fmt.Printf("   主机: %s:%d\n", cfg.Database.Host, cfg.Database.Port)
	fmt.Printf("   数据库: %s\n", cfg.Database.Name)
	fmt.Printf("   用户: %s\n", cfg.Database.User)
	fmt.Println()
}

// printMigrationResult 打印迁移结果
func printMigrationResult(result *application.MigrationResult) {
	fmt.Println()
	fmt.Println("📊 数据库统计:")
	fmt.Printf("   表数量: %d\n", result.TableCount)
	fmt.Printf("   索引数量: %d\n", result.IndexCount)
	fmt.Printf("   外键约束: %d\n", result.ForeignKeyCount)
	fmt.Println()

	fmt.Println(bannerTop)
	fmt.Println("║                                                                  ║")
	fmt.Printf("║         🎉 混合迁移完成！耗时: %-30v ║\n", result.Duration)
	fmt.Println("║                                                                  ║")
	fmt.Println(bannerBot)
}
