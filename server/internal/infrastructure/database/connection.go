package database

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	appLogger "github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// Connection 数据库连接结构
type Connection struct {
	DB *gorm.DB
}

// NewConnection 创建新的数据库连接
func NewConnection(cfg config.DatabaseConfig) (*Connection, error) {
	// 构建DSN
	dsn := cfg.GetDSN()

	// 设置GORM日志级别
	var logLevel logger.LogLevel
	switch cfg.LogLevel {
	case "silent":
		logLevel = logger.Silent
	case "error":
		logLevel = logger.Error
	case "warn":
		logLevel = logger.Warn
	case "info":
		logLevel = logger.Info
	default:
		logLevel = logger.Info
	}

	// 创建自定义SQL日志记录器
	sqlLogger := NewSQLLogger(
		appLogger.Logger,
		logger.Config{
			SlowThreshold:             200 * time.Millisecond, // 降低慢查询阈值
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true, // 启用彩色输出
		},
	)

	// GORM配置
	gormConfig := &gorm.Config{
		Logger: sqlLogger,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: false,
			NoLowerCase:   false,
		},
		PrepareStmt:     true, // 启用预编译语句缓存
		CreateBatchSize: 1000, // 全局批量创建大小
	}

	// 连接数据库
	db, err := gorm.Open(postgres.Open(dsn), gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 获取底层sql.DB实例
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB instance: %w", err)
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute) // 空闲连接最大存活时间

	// 测试连接
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	appLogger.Info("Database connected successfully",
		appLogger.String("host", cfg.Host),
		appLogger.Int("port", cfg.Port),
		appLogger.String("database", cfg.Name),
	)

	return &Connection{DB: db}, nil
}

// Close 关闭数据库连接
func (c *Connection) Close() error {
	sqlDB, err := c.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// GetDB 获取GORM数据库实例
func (c *Connection) GetDB() *gorm.DB {
	return c.DB
}

// Health 检查数据库健康状态
func (c *Connection) Health() error {
	sqlDB, err := c.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

// Migrate 执行数据库迁移
func (c *Connection) Migrate(models ...interface{}) error {
	return c.DB.AutoMigrate(models...)
}

// logWriter 自定义日志写入器
type logWriter struct{}

func (w *logWriter) Printf(format string, args ...interface{}) {
	// 格式化SQL日志，使其更易读
	message := fmt.Sprintf(format, args...)

	// 如果包含SQL查询，使用特殊的格式
	if strings.Contains(message, "[rows:") {
		// 这是SQL查询日志，使用更清晰的格式
		appLogger.Info("🔍 SQL Query",
			appLogger.String("query", message),
		)
	} else {
		// 其他数据库日志
		appLogger.Info("📊 Database",
			appLogger.String("message", message),
		)
	}
}

// Transaction 执行事务
func (c *Connection) Transaction(fn func(*gorm.DB) error) error {
	return c.DB.Transaction(fn)
}
