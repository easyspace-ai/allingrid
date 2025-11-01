package application

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/easyspace-ai/luckdb/server/internal/config"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	appLogger "github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// MigrateService 数据库迁移服务
type MigrateService struct {
	config        *config.Config
	logger        *zap.Logger
	migrationsDir string
}

// MigrationResult 迁移结果
type MigrationResult struct {
	Success         bool          `json:"success"`
	Mode            string        `json:"mode"`
	Duration        time.Duration `json:"duration"`
	TableCount      int           `json:"table_count"`
	IndexCount      int           `json:"index_count"`
	ForeignKeyCount int           `json:"foreign_key_count"`
	Error           string        `json:"error,omitempty"`
}

// DatabaseStats 数据库统计信息
type DatabaseStats struct {
	TableCount      int
	IndexCount      int
	ForeignKeyCount int
}

// NewMigrateService 创建迁移服务实例
func NewMigrateService(cfg *config.Config, migrationsDir string) *MigrateService {
	return &MigrateService{
		config:        cfg,
		logger:        appLogger.Logger.Named("migrate"),
		migrationsDir: migrationsDir,
	}
}

// RunHybridMigration 混合迁移模式：先 golang-migrate，后 GORM AutoMigrate
func (s *MigrateService) RunHybridMigration(ctx context.Context) (*MigrationResult, error) {
	startTime := time.Now()
	result := &MigrationResult{
		Success: false,
		Mode:    "hybrid",
	}

	s.logger.Info("开始混合迁移",
		zap.String("mode", "hybrid"),
		zap.String("migrations_dir", s.migrationsDir),
	)

	// 步骤1: golang-migrate
	s.logger.Info("执行 golang-migrate SQL 迁移...")
	if err := s.executeGolangMigrate(ctx, "up", ""); err != nil {
		s.logger.Warn("golang-migrate 执行警告",
			zap.Error(err),
			zap.String("action", "继续执行 GORM AutoMigrate"),
		)
	} else {
		s.logger.Info("golang-migrate 执行成功")
	}

	// 步骤2: GORM AutoMigrate
	s.logger.Info("执行 GORM AutoMigrate 模型同步...")
	if err := s.executeGORMAutoMigrate(ctx); err != nil {
		s.logger.Error("GORM AutoMigrate 执行失败", zap.Error(err))
		result.Error = err.Error()
		result.Duration = time.Since(startTime)
		return result, err
	}
	s.logger.Info("GORM AutoMigrate 执行成功")

	// 获取统计信息
	stats, err := s.getDatabaseStats(ctx)
	if err != nil {
		s.logger.Warn("获取数据库统计信息失败", zap.Error(err))
	} else {
		result.TableCount = stats.TableCount
		result.IndexCount = stats.IndexCount
		result.ForeignKeyCount = stats.ForeignKeyCount
	}

	result.Success = true
	result.Duration = time.Since(startTime)

	s.logger.Info("混合迁移完成",
		zap.Duration("duration", result.Duration),
		zap.Int("table_count", result.TableCount),
		zap.Int("index_count", result.IndexCount),
		zap.Int("foreign_key_count", result.ForeignKeyCount),
	)

	return result, nil
}

// RunGolangMigrate 运行 golang-migrate
func (s *MigrateService) RunGolangMigrate(ctx context.Context, command, version string) error {
	s.logger.Info("执行 golang-migrate",
		zap.String("command", command),
		zap.String("version", version),
	)

	if err := s.executeGolangMigrate(ctx, command, version); err != nil {
		s.logger.Error("golang-migrate 执行失败",
			zap.String("command", command),
			zap.Error(err),
		)
		return errors.ErrDatabaseOperation.WithDetails(map[string]interface{}{
			"command": command,
			"error":   err.Error(),
		})
	}

	s.logger.Info("golang-migrate 执行成功", zap.String("command", command))
	return nil
}

// RunGORMAutoMigrate 仅运行 GORM AutoMigrate
func (s *MigrateService) RunGORMAutoMigrate(ctx context.Context) error {
	s.logger.Info("执行 GORM AutoMigrate")

	if err := s.executeGORMAutoMigrate(ctx); err != nil {
		s.logger.Error("GORM AutoMigrate 执行失败", zap.Error(err))
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	s.logger.Info("GORM AutoMigrate 执行成功")
	return nil
}

// GetMigrationVersion 获取当前迁移版本
func (s *MigrateService) GetMigrationVersion(ctx context.Context) (version uint, dirty bool, err error) {
	s.logger.Info("获取迁移版本信息")

	dsn := s.buildPostgresDSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return 0, false, errors.ErrDatabaseConnection.WithDetails(err.Error())
	}
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return 0, false, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", s.migrationsDir),
		"postgres",
		driver,
	)
	if err != nil {
		return 0, false, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	defer m.Close()

	version, dirty, err = m.Version()
	if err != nil {
		return 0, false, errors.ErrDatabaseQuery.WithDetails(err.Error())
	}

	s.logger.Info("获取迁移版本成功",
		zap.Uint("version", version),
		zap.Bool("dirty", dirty),
	)

	return version, dirty, nil
}

// executeGolangMigrate 执行 golang-migrate
func (s *MigrateService) executeGolangMigrate(ctx context.Context, command, version string) error {
	dsn := s.buildPostgresDSN()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return errors.ErrDatabaseConnection.WithDetails(err.Error())
	}
	defer db.Close()

	// 测试连接
	if err := db.Ping(); err != nil {
		return errors.ErrDatabaseConnection.WithDetails(err.Error())
	}

	// 创建 postgres driver
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	// 创建 migrate 实例
	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", s.migrationsDir),
		"postgres",
		driver,
	)
	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}
	defer m.Close()

	// 执行命令
	switch command {
	case "up":
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			return errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
		if err == migrate.ErrNoChange {
			s.logger.Info("没有需要执行的迁移")
		}
	case "down":
		if err := m.Down(); err != nil && err != migrate.ErrNoChange {
			return errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	case "force":
		if version == "" {
			return errors.ErrBadRequest.WithDetails("force 命令需要版本号")
		}
		var v int
		fmt.Sscanf(version, "%d", &v)
		if err := m.Force(v); err != nil {
			return errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	case "drop":
		// 先清理 UUID schema，再执行标准 drop
		if err := s.cleanupUUIDSchemas(db); err != nil {
			s.logger.Warn("清理 UUID schema 失败", zap.Error(err))
		}
		if err := m.Drop(); err != nil {
			return errors.ErrDatabaseOperation.WithDetails(err.Error())
		}
	default:
		return errors.ErrBadRequest.WithDetails(fmt.Sprintf("未知命令: %s", command))
	}

	return nil
}

// cleanupUUIDSchemas 清理 UUID 格式的 schema
func (s *MigrateService) cleanupUUIDSchemas(db *sql.DB) error {
	s.logger.Info("开始清理 UUID schema...")

	// 查询所有 UUID 格式的 schema
	query := `
		SELECT schema_name 
		FROM information_schema.schemata 
		WHERE schema_name ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
		AND schema_name != 'public'
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("查询 UUID schema 失败: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return fmt.Errorf("扫描 schema 名称失败: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历 schema 结果失败: %w", err)
	}

	s.logger.Info("发现 UUID schema", zap.Strings("schemas", schemas))

	// 删除每个 UUID schema
	for _, schemaName := range schemas {
		dropQuery := fmt.Sprintf("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schemaName)
		s.logger.Info("删除 schema", zap.String("schema", schemaName))

		if _, err := db.Exec(dropQuery); err != nil {
			s.logger.Warn("删除 schema 失败",
				zap.String("schema", schemaName),
				zap.Error(err))
		} else {
			s.logger.Info("成功删除 schema", zap.String("schema", schemaName))
		}
	}

	if len(schemas) > 0 {
		s.logger.Info("UUID schema 清理完成", zap.Int("count", len(schemas)))
	} else {
		s.logger.Info("没有发现需要清理的 UUID schema")
	}

	return nil
}

// executeGORMAutoMigrate 执行 GORM AutoMigrate
func (s *MigrateService) executeGORMAutoMigrate(ctx context.Context) error {
	db, err := s.connectGORM()
	if err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	s.logger.Info("数据库连接成功")

	// 执行 AutoMigrate
	startTime := time.Now()
	if err := s.runAutoMigrate(db); err != nil {
		return errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	duration := time.Since(startTime)
	s.logger.Info("AutoMigrate 完成", zap.Duration("duration", duration))

	// 添加补充索引
	s.logger.Info("添加补充索引和约束...")
	if err := s.addSupplementaryIndexes(db); err != nil {
		s.logger.Warn("部分索引添加失败", zap.Error(err))
	} else {
		s.logger.Info("补充索引添加完成")
	}

	return nil
}

// connectGORM 连接 GORM 数据库
func (s *MigrateService) connectGORM() (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		s.config.Database.Host,
		s.config.Database.Port,
		s.config.Database.User,
		s.config.Database.Password,
		s.config.Database.Name,
		s.config.Database.SSLMode,
	)

	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	}

	db, err := gorm.Open(postgresDriver.Open(dsn), gormConfig)
	if err != nil {
		return nil, errors.ErrDatabaseConnection.WithDetails(err.Error())
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, errors.ErrDatabaseOperation.WithDetails(err.Error())
	}

	if err := sqlDB.Ping(); err != nil {
		return nil, errors.ErrDatabaseConnection.WithDetails(err.Error())
	}

	return db, nil
}

// runAutoMigrate 执行 AutoMigrate
func (s *MigrateService) runAutoMigrate(db *gorm.DB) error {
	// 所有模型列表
	allModels := []interface{}{
		// 核心表
		&models.User{},
		// &models.Account{}, // TODO: Account模型未实现
		&models.Space{},
		&models.SpaceCollaborator{},
		&models.Base{},
		&models.Table{},
		&models.Field{},
		&models.Record{},
		&models.View{}, // ✅ View模型已实现
		&models.Permission{},
		&models.Attachment{},
		&models.UploadToken{}, // ✅ 上传令牌模型
		&models.Collaborator{},
		// &models.Invitation{},        // TODO: Invitation模型待实现
		// &models.InvitationRecord{},  // TODO: InvitationRecord模型待实现
		&models.RecordChange{},
		&models.RecordVersion{},
		&models.Ops{},
		&models.Reference{},
		&models.AccessToken{},
		&models.OAuthApp{},
		&models.OAuthAppAuthorized{},
		&models.OAuthAppSecret{},
		&models.OAuthAppToken{},
		&models.RecordHistory{},
		&models.Trash{},
		&models.TableTrash{},
		// &models.RecordTrash{},  // TODO: RecordTrash模型待实现
		&models.Plugin{},
		&models.PluginInstall{},
		&models.Dashboard{},
		&models.PluginPanel{},
		&models.PluginContextMenu{},
		// Note: PluginPanel defined in plugin.go ✅
		&models.Comment{},
		&models.CommentSubscription{},
		&models.Integration{},
		&models.UserLastVisit{},
		// &models.Template{},          // TODO: Template模型待实现
		// &models.TemplateCategory{},  // TODO: TemplateCategory模型待实现
		// &models.Task{},              // TODO: Task模型待实现
		// &models.TaskRun{},           // TODO: TaskRun模型待实现
		// &models.TaskReference{},     // TODO: TaskReference模型待实现
		&models.PinResource{},
		&models.Setting{},
		&models.Waitlist{},
		&models.Attachments{},
		&models.AttachmentsTable{},

		// 组织架构
		&models.Organization{},
		&models.OrganizationDepartment{},
		&models.OrganizationSetting{},
		&models.OrganizationSpace{},
		&models.OrganizationUser{},
		&models.OrganizationUserDepartment{},
		// Note: Organization models defined in organization.go ✅

		// 许可证管理
		&models.License{},
		&models.LicenseCustomer{},
		&models.LicenseSubscription{},
		&models.EnterpriseLicense{},

		// 通知系统
		&models.Notification{},
		&models.NotificationTemplate{},
		&models.NotificationSubscription{},
		&models.NotificationDelivery{},

		// 审计日志
		&models.AuditLog{},
		&models.AuditLogConfig{},
		&models.AuditLogSummary{},

		// 工作流
		&models.Workflow{},
		&models.WorkflowNode{},
		&models.WorkflowNodeSecret{},
		&models.WorkflowRun{},
		&models.WorkflowRunStep{},
		&models.WorkflowSnapshot{},

		// 聊天和客户
		&models.Chat{},
		&models.ChatMessage{},
		&models.Customer{},
		&models.CreditHistory{},

		// 系统杂项
		&models.VisitLog{},
		&models.Subscription{},
		&models.SpaceAdvancedSetting{},

		// 系统管理
		&models.App{},
		&models.AppVersion{},
		&models.Authentication{},
		&models.DomainVerification{},

		// API监控
		&models.ApiKey{},
		&models.ApiUsage{},
		&models.ApiRateLimit{},
		&models.ApiWebhookLog{},

		// 系统
		&models.SystemSetting{},
		&models.SystemLog{},
		&models.SystemMetrics{},
		&models.SystemHealth{},
		&models.SystemBackup{},

		// 虚拟字段支持
		&models.FieldDependency{},
		&models.VirtualFieldCache{},
	}

	s.logger.Info("开始迁移模型", zap.Int("model_count", len(allModels)))

	// 逐个迁移模型，遇到错误时记录但继续执行
	successCount := 0
	failedModels := []string{}

	for i, model := range allModels {
		modelName := fmt.Sprintf("%T", model)
		if err := db.AutoMigrate(model); err != nil {
			s.logger.Warn("模型迁移失败，跳过",
				zap.Int("index", i),
				zap.String("model", modelName),
				zap.Error(err))
			failedModels = append(failedModels, modelName)
		} else {
			successCount++
		}
	}

	if len(failedModels) > 0 {
		s.logger.Warn("部分模型迁移失败",
			zap.Int("failed_count", len(failedModels)),
			zap.Strings("failed_models", failedModels))
	}

	s.logger.Info("模型迁移完成",
		zap.Int("success_count", successCount),
		zap.Int("total_count", len(allModels)))

	return nil
}

// addSupplementaryIndexes 添加补充索引
func (s *MigrateService) addSupplementaryIndexes(db *gorm.DB) error {
	// GORM AutoMigrate 不会自动创建的复合索引和特殊约束
	indexes := []string{
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_oauth_authorized_client_user ON oauth_app_authorized(client_id, user_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_reference_to_from ON reference(to_field_id, from_field_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_task_reference_to_from ON task_reference(to_field_id, from_field_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_collab_rt_rid_pid_pt ON collaborator(principal_id, principal_type, resource_id, resource_type)",
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_ops_collection_docid_version ON ops(collection, doc_id, version)",
		"CREATE INDEX IF NOT EXISTS idx_ops_collection_created_time ON ops(collection, created_time)",
		"CREATE INDEX IF NOT EXISTS idx_comment_record_table ON comment(record_id, table_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_comment_subscription ON comment_subscription(table_id, record_id)",
		"CREATE INDEX IF NOT EXISTS idx_record_history_table_record_created ON record_history(table_id, record_id, created_time)",
		"CREATE INDEX IF NOT EXISTS idx_record_history_table_created ON record_history(table_id, created_time)",
		"CREATE INDEX IF NOT EXISTS idx_record_trash_table_record ON record_trash(table_id, record_id)",
		"CREATE INDEX IF NOT EXISTS idx_attachments_table_field ON attachments_table(table_id, field_id)",
		"CREATE INDEX IF NOT EXISTS idx_attachments_table_record_field ON attachments_table(record_id, table_id, field_id)",
	}

	s.logger.Info("创建补充索引", zap.Int("index_count", len(indexes)))

	for i, sql := range indexes {
		if err := db.Exec(sql).Error; err != nil {
			s.logger.Warn("索引创建失败（可能已存在）",
				zap.Int("index", i+1),
				zap.Error(err),
			)
		}
	}

	return nil
}

// getDatabaseStats 获取数据库统计信息
func (s *MigrateService) getDatabaseStats(ctx context.Context) (*DatabaseStats, error) {
	db, err := s.connectGORM()
	if err != nil {
		return nil, err
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	var stats DatabaseStats

	db.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'").Scan(&stats.TableCount)
	db.Raw("SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public'").Scan(&stats.IndexCount)
	db.Raw("SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = 'public' AND constraint_type = 'FOREIGN KEY'").Scan(&stats.ForeignKeyCount)

	return &stats, nil
}

// buildPostgresDSN 构建 PostgreSQL DSN
func (s *MigrateService) buildPostgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		s.config.Database.User,
		s.config.Database.Password,
		s.config.Database.Host,
		s.config.Database.Port,
		s.config.Database.Name,
		s.config.Database.SSLMode,
	)
}
