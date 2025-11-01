package database

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// PostgresProvider PostgreSQL数据库提供者
// 实现完全的Schema隔离和动态表管理
// 严格按照旧系统（原 Teable 项目 NestJS）实现
type PostgresProvider struct {
	db *gorm.DB
}

// NewPostgresProvider 创建PostgreSQL提供者
func NewPostgresProvider(db *gorm.DB) *PostgresProvider {
	return &PostgresProvider{
		db: db,
	}
}

// GetDB 获取底层的 GORM DB 实例
func (p *PostgresProvider) GetDB() *gorm.DB {
	return p.db
}

// ==================== Schema管理 ====================

// CreateSchema 创建独立的PostgreSQL Schema
// 参考旧系统：teable-develop/apps/nestjs-backend/src/db-provider/postgres.provider.ts
func (p *PostgresProvider) CreateSchema(ctx context.Context, schemaName string) error {
	// 1. 创建Schema
	createSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", p.quoteIdentifier(schemaName))
	if err := p.db.WithContext(ctx).Exec(createSQL).Error; err != nil {
		return fmt.Errorf("创建Schema失败: %w", err)
	}

	// 2. 撤销public用户的权限（安全隔离）
	revokeSQL := fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM public", p.quoteIdentifier(schemaName))
	if err := p.db.WithContext(ctx).Exec(revokeSQL).Error; err != nil {
		return fmt.Errorf("撤销Schema权限失败: %w", err)
	}

	return nil
}

// DropSchema 删除Schema及其所有内容
func (p *PostgresProvider) DropSchema(ctx context.Context, schemaName string) error {
	sql := fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", p.quoteIdentifier(schemaName))
	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("删除Schema失败: %w", err)
	}
	return nil
}

// ==================== 动态表管理 ====================

// CreatePhysicalTable 创建物理表（包含系统字段）
// 参考旧系统：teable-develop/apps/nestjs-backend/src/features/table/table.service.ts
// 系统字段与旧系统完全一致
func (p *PostgresProvider) CreatePhysicalTable(ctx context.Context, schemaName, tableName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))

	// 创建表SQL（系统字段与旧系统完全一致）
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			__id VARCHAR(50) NOT NULL,
			__auto_number SERIAL PRIMARY KEY,
			__created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
			__last_modified_time TIMESTAMP,
			__created_by VARCHAR(50) NOT NULL,
			__last_modified_by VARCHAR(50),
			__version INTEGER NOT NULL DEFAULT 1
		)
	`, fullTableName)

	if err := p.db.WithContext(ctx).Exec(createTableSQL).Error; err != nil {
		return fmt.Errorf("创建物理表失败: %w", err)
	}

	// 创建系统字段索引
	systemIndexes := []struct {
		suffix  string
		columns string
		unique  bool
	}{
		{"__id_unique", "__id", true},
		{"__created_time", "__created_time", false},
		{"__last_modified_time", "__last_modified_time", false},
		{"__created_by", "__created_by", false},
		{"__version", "__version", false},
	}

	for _, idx := range systemIndexes {
		indexType := "INDEX"
		if idx.unique {
			indexType = "UNIQUE INDEX"
		}

		indexName := fmt.Sprintf("%s_%s_%s", schemaName, tableName, idx.suffix)
		createIndexSQL := fmt.Sprintf(
			"CREATE %s IF NOT EXISTS %s ON %s (%s)",
			indexType,
			p.quoteIdentifier(indexName),
			fullTableName,
			idx.columns,
		)

		if err := p.db.WithContext(ctx).Exec(createIndexSQL).Error; err != nil {
			logger.Warn("创建系统字段索引失败或已存在",
				logger.String("index", indexName),
				logger.ErrorField(err))
		}
	}

	return nil
}

// DropPhysicalTable 删除物理表
func (p *PostgresProvider) DropPhysicalTable(ctx context.Context, schemaName, tableName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", fullTableName)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("删除物理表失败: %w", err)
	}

	return nil
}

// ==================== 列管理 ====================

// AddColumn 添加列到物理表
func (p *PostgresProvider) AddColumn(ctx context.Context, schemaName, tableName string, columnDef ColumnDefinition) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))

	// 构建ALTER TABLE SQL
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		fullTableName,
		p.quoteIdentifier(columnDef.Name),
		columnDef.Type,
	)

	if columnDef.NotNull {
		sql += " NOT NULL"
	}

	if columnDef.DefaultValue != nil {
		sql += fmt.Sprintf(" DEFAULT %s", *columnDef.DefaultValue)
	}

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("添加列失败: %w", err)
	}

	// 如果需要唯一约束，单独添加
	if columnDef.Unique {
		constraintName := fmt.Sprintf("%s_%s_%s_unique", schemaName, tableName, columnDef.Name)
		if err := p.AddUniqueConstraint(ctx, schemaName, tableName, columnDef.Name, constraintName); err != nil {
			return err
		}
	}

	return nil
}

// AlterColumn 修改列类型和约束
func (p *PostgresProvider) AlterColumn(ctx context.Context, schemaName, tableName, columnName string, newDef ColumnDefinition) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	quotedColumn := p.quoteIdentifier(columnName)

	// 1. 修改列类型
	if newDef.Type != "" {
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
			fullTableName,
			quotedColumn,
			newDef.Type,
		)
		if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
			return fmt.Errorf("修改列类型失败: %w", err)
		}
	}

	// 2. 修改NOT NULL约束
	if newDef.NotNull {
		if err := p.SetNotNull(ctx, schemaName, tableName, columnName); err != nil {
			return err
		}
	} else {
		if err := p.DropNotNull(ctx, schemaName, tableName, columnName); err != nil {
			return err
		}
	}

	// 3. 修改默认值
	if newDef.DefaultValue != nil {
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
			fullTableName,
			quotedColumn,
			*newDef.DefaultValue,
		)
		if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
			return fmt.Errorf("设置默认值失败: %w", err)
		}
	}

	return nil
}

// DropColumn 删除列
func (p *PostgresProvider) DropColumn(ctx context.Context, schemaName, tableName, columnName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s CASCADE",
		fullTableName,
		p.quoteIdentifier(columnName),
	)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("删除列失败: %w", err)
	}

	return nil
}

// ==================== 约束管理 ====================

// AddUniqueConstraint 添加唯一性约束
func (p *PostgresProvider) AddUniqueConstraint(ctx context.Context, schemaName, tableName, columnName, constraintName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
		fullTableName,
		p.quoteIdentifier(constraintName),
		p.quoteIdentifier(columnName),
	)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("添加唯一约束失败: %w", err)
	}

	return nil
}

// DropConstraint 删除约束
func (p *PostgresProvider) DropConstraint(ctx context.Context, schemaName, tableName, constraintName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s",
		fullTableName,
		p.quoteIdentifier(constraintName),
	)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("删除约束失败: %w", err)
	}

	return nil
}

// SetNotNull 设置字段为NOT NULL
func (p *PostgresProvider) SetNotNull(ctx context.Context, schemaName, tableName, columnName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
		fullTableName,
		p.quoteIdentifier(columnName),
	)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("设置NOT NULL失败: %w", err)
	}

	return nil
}

// DropNotNull 移除NOT NULL约束
func (p *PostgresProvider) DropNotNull(ctx context.Context, schemaName, tableName, columnName string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
		fullTableName,
		p.quoteIdentifier(columnName),
	)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("移除NOT NULL失败: %w", err)
	}

	return nil
}

// AddCheckConstraint 添加CHECK约束
func (p *PostgresProvider) AddCheckConstraint(ctx context.Context, schemaName, tableName, constraintName, checkExpression string) error {
	fullTableName := fmt.Sprintf("%s.%s", p.quoteIdentifier(schemaName), p.quoteIdentifier(tableName))
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
		fullTableName,
		p.quoteIdentifier(constraintName),
		checkExpression,
	)

	if err := p.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("添加CHECK约束失败: %w", err)
	}

	return nil
}

// ==================== 工具方法 ====================

// GenerateTableName 生成完整的表名
// 格式：schemaName.tableName
func (p *PostgresProvider) GenerateTableName(baseID, tableID string) string {
	return fmt.Sprintf("%s.%s", baseID, tableID)
}

// MapFieldTypeToDBType 将字段类型映射到数据库类型
func (p *PostgresProvider) MapFieldTypeToDBType(fieldType string) string {
	if dbType, ok := FieldTypeMapping[fieldType]; ok {
		return dbType
	}
	// 默认类型
	return "TEXT"
}

// DriverName 返回驱动名称
func (p *PostgresProvider) DriverName() string {
	return "postgres"
}

// SupportsSchema 是否支持Schema
func (p *PostgresProvider) SupportsSchema() bool {
	return true
}

// ==================== 私有辅助方法 ====================

// quoteIdentifier 为标识符添加引号（防止SQL注入和关键字冲突）
func (p *PostgresProvider) quoteIdentifier(identifier string) string {
	// 简单处理：移除可能的恶意字符，添加双引号
	// PostgreSQL使用双引号
	cleaned := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, cleaned)
}
