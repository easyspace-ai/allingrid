package database

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// SQLiteProvider SQLite数据库提供者
// SQLite不支持Schema，使用表名前缀作为降级方案
// 例如：bse_xxx_tbl_yyy
type SQLiteProvider struct {
	db *gorm.DB
}

// NewSQLiteProvider 创建SQLite提供者
func NewSQLiteProvider(db *gorm.DB) *SQLiteProvider {
	return &SQLiteProvider{
		db: db,
	}
}

// ==================== Schema管理（降级实现）====================

// CreateSchema SQLite不支持Schema，什么也不做
func (s *SQLiteProvider) CreateSchema(ctx context.Context, schemaName string) error {
	// SQLite不支持Schema，跳过
	return nil
}

// DropSchema SQLite不支持Schema，删除所有带前缀的表
func (s *SQLiteProvider) DropSchema(ctx context.Context, schemaName string) error {
	// 查询所有以schemaName开头的表
	var tables []string
	query := `
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name LIKE ?
	`
	if err := s.db.WithContext(ctx).Raw(query, schemaName+"_%").Scan(&tables).Error; err != nil {
		return fmt.Errorf("查询Schema表失败: %w", err)
	}

	// 逐个删除
	for _, table := range tables {
		sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", s.quoteIdentifier(table))
		if err := s.db.WithContext(ctx).Exec(sql).Error; err != nil {
			return fmt.Errorf("删除表%s失败: %w", table, err)
		}
	}

	return nil
}

// ==================== 动态表管理 ====================

// CreatePhysicalTable 创建物理表（带前缀）
// 表名格式：schemaName_tableName
func (s *SQLiteProvider) CreatePhysicalTable(ctx context.Context, schemaName, tableName string) error {
	fullTableName := s.GenerateTableName(schemaName, tableName)

	// 创建表SQL（系统字段与PostgreSQL版本一致）
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			__id TEXT NOT NULL,
			__auto_number INTEGER PRIMARY KEY AUTOINCREMENT,
			__created_time DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
			__last_modified_time DATETIME,
			__created_by TEXT NOT NULL,
			__last_modified_by TEXT,
			__version INTEGER NOT NULL DEFAULT 1
		)
	`, s.quoteIdentifier(fullTableName))

	if err := s.db.WithContext(ctx).Exec(createTableSQL).Error; err != nil {
		return fmt.Errorf("创建物理表失败: %w", err)
	}

	// 创建__id唯一索引
	indexName := fmt.Sprintf("%s__%s__id_unique", schemaName, tableName)
	createIndexSQL := fmt.Sprintf(
		"CREATE UNIQUE INDEX %s ON %s (__id)",
		s.quoteIdentifier(indexName),
		s.quoteIdentifier(fullTableName),
	)

	if err := s.db.WithContext(ctx).Exec(createIndexSQL).Error; err != nil {
		return fmt.Errorf("创建唯一索引失败: %w", err)
	}

	return nil
}

// DropPhysicalTable 删除物理表
func (s *SQLiteProvider) DropPhysicalTable(ctx context.Context, schemaName, tableName string) error {
	fullTableName := s.GenerateTableName(schemaName, tableName)
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", s.quoteIdentifier(fullTableName))

	if err := s.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("删除物理表失败: %w", err)
	}

	return nil
}

// ==================== 列管理 ====================

// AddColumn 添加列到物理表
func (s *SQLiteProvider) AddColumn(ctx context.Context, schemaName, tableName string, columnDef ColumnDefinition) error {
	fullTableName := s.GenerateTableName(schemaName, tableName)

	// SQLite的ALTER TABLE ADD COLUMN限制较多
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		s.quoteIdentifier(fullTableName),
		s.quoteIdentifier(columnDef.Name),
		s.mapTypeToSQLite(columnDef.Type),
	)

	if columnDef.NotNull {
		// SQLite添加NOT NULL列时必须提供默认值
		if columnDef.DefaultValue != nil {
			sql += fmt.Sprintf(" NOT NULL DEFAULT %s", *columnDef.DefaultValue)
		} else {
			// 提供一个合理的默认值
			sql += fmt.Sprintf(" NOT NULL DEFAULT %s", s.getDefaultValueForType(columnDef.Type))
		}
	} else if columnDef.DefaultValue != nil {
		sql += fmt.Sprintf(" DEFAULT %s", *columnDef.DefaultValue)
	}

	if err := s.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("添加列失败: %w", err)
	}

	// SQLite不支持直接添加UNIQUE约束到现有表，需要创建索引
	if columnDef.Unique {
		indexName := fmt.Sprintf("%s_%s_unique", fullTableName, columnDef.Name)
		createIndexSQL := fmt.Sprintf(
			"CREATE UNIQUE INDEX %s ON %s (%s)",
			s.quoteIdentifier(indexName),
			s.quoteIdentifier(fullTableName),
			s.quoteIdentifier(columnDef.Name),
		)
		if err := s.db.WithContext(ctx).Exec(createIndexSQL).Error; err != nil {
			return fmt.Errorf("创建唯一索引失败: %w", err)
		}
	}

	return nil
}

// AlterColumn 修改列（SQLite限制：需要重建表）
func (s *SQLiteProvider) AlterColumn(ctx context.Context, schemaName, tableName, columnName string, newDef ColumnDefinition) error {
	// SQLite不支持直接ALTER COLUMN，需要重建表
	// 这是一个复杂的操作，暂时返回不支持错误
	// 如果需要完整实现，需要：
	// 1. 创建临时表
	// 2. 复制数据
	// 3. 删除原表
	// 4. 重命名临时表
	return fmt.Errorf("SQLite不支持直接修改列类型，需要重建表（暂未实现）")
}

// DropColumn 删除列（SQLite限制：需要重建表）
func (s *SQLiteProvider) DropColumn(ctx context.Context, schemaName, tableName, columnName string) error {
	// SQLite不支持直接DROP COLUMN（SQLite 3.35.0之前的版本）
	// 新版本支持，但为了兼容性，返回不支持错误
	return fmt.Errorf("SQLite不支持直接删除列，需要重建表（暂未实现）")
}

// ==================== 约束管理 ====================

// AddUniqueConstraint 添加唯一性约束（通过创建索引）
func (s *SQLiteProvider) AddUniqueConstraint(ctx context.Context, schemaName, tableName, columnName, constraintName string) error {
	fullTableName := s.GenerateTableName(schemaName, tableName)
	sql := fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s)",
		s.quoteIdentifier(constraintName),
		s.quoteIdentifier(fullTableName),
		s.quoteIdentifier(columnName),
	)

	if err := s.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("添加唯一约束失败: %w", err)
	}

	return nil
}

// DropConstraint 删除约束（通过删除索引）
func (s *SQLiteProvider) DropConstraint(ctx context.Context, schemaName, tableName, constraintName string) error {
	sql := fmt.Sprintf("DROP INDEX IF EXISTS %s", s.quoteIdentifier(constraintName))

	if err := s.db.WithContext(ctx).Exec(sql).Error; err != nil {
		return fmt.Errorf("删除约束失败: %w", err)
	}

	return nil
}

// SetNotNull SQLite不支持修改列约束
func (s *SQLiteProvider) SetNotNull(ctx context.Context, schemaName, tableName, columnName string) error {
	return fmt.Errorf("SQLite不支持修改列的NOT NULL约束")
}

// DropNotNull SQLite不支持修改列约束
func (s *SQLiteProvider) DropNotNull(ctx context.Context, schemaName, tableName, columnName string) error {
	return fmt.Errorf("SQLite不支持修改列的NOT NULL约束")
}

// AddCheckConstraint SQLite不支持添加CHECK约束到现有表
func (s *SQLiteProvider) AddCheckConstraint(ctx context.Context, schemaName, tableName, constraintName, checkExpression string) error {
	return fmt.Errorf("SQLite不支持添加CHECK约束到现有表")
}

// ==================== 工具方法 ====================

// GenerateTableName 生成完整的表名（带前缀）
// 格式：schemaName_tableName
func (s *SQLiteProvider) GenerateTableName(baseID, tableID string) string {
	return fmt.Sprintf("%s_%s", baseID, tableID)
}

// MapFieldTypeToDBType 将字段类型映射到SQLite类型
func (s *SQLiteProvider) MapFieldTypeToDBType(fieldType string) string {
	// SQLite类型系统较简单，映射到5种存储类
	sqliteMapping := map[string]string{
		"singleLineText": "TEXT",
		"longText":       "TEXT",
		"number":         "REAL",
		"rating":         "INTEGER",
		"percent":        "REAL",
		"currency":       "REAL",
		"date":           "DATETIME",
		"checkbox":       "INTEGER", // 0 or 1
		"singleSelect":   "TEXT",
		"multipleSelect": "TEXT", // JSON string
		"user":           "TEXT", // JSON string
		"attachment":     "TEXT", // JSON string
		"link":           "TEXT", // JSON string
		"formula":        "TEXT",
		"rollup":         "REAL",
		"lookup":         "TEXT", // JSON string
		"count":          "INTEGER",
		"autoNumber":     "INTEGER",
		"url":            "TEXT",
		"email":          "TEXT",
		"phone":          "TEXT",
	}

	if dbType, ok := sqliteMapping[fieldType]; ok {
		return dbType
	}

	return "TEXT"
}

// DriverName 返回驱动名称
func (s *SQLiteProvider) DriverName() string {
	return "sqlite"
}

// SupportsSchema 是否支持Schema
func (s *SQLiteProvider) SupportsSchema() bool {
	return false
}

// ==================== 私有辅助方法 ====================

// quoteIdentifier 为标识符添加引号
// SQLite使用双引号或方括号
func (s *SQLiteProvider) quoteIdentifier(identifier string) string {
	cleaned := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, cleaned)
}

// mapTypeToSQLite 将PostgreSQL类型映射到SQLite类型
func (s *SQLiteProvider) mapTypeToSQLite(pgType string) string {
	upper := strings.ToUpper(pgType)

	if strings.Contains(upper, "VARCHAR") || strings.Contains(upper, "TEXT") {
		return "TEXT"
	}
	if strings.Contains(upper, "INTEGER") || strings.Contains(upper, "SERIAL") {
		return "INTEGER"
	}
	if strings.Contains(upper, "NUMERIC") || strings.Contains(upper, "REAL") {
		return "REAL"
	}
	if strings.Contains(upper, "TIMESTAMP") || strings.Contains(upper, "DATETIME") {
		return "DATETIME"
	}
	if strings.Contains(upper, "BOOLEAN") {
		return "INTEGER"
	}
	if strings.Contains(upper, "JSONB") || strings.Contains(upper, "JSON") {
		return "TEXT"
	}

	return "TEXT"
}

// getDefaultValueForType 根据类型获取默认值
func (s *SQLiteProvider) getDefaultValueForType(dbType string) string {
	upper := strings.ToUpper(dbType)

	if strings.Contains(upper, "VARCHAR") || strings.Contains(upper, "TEXT") {
		return "''"
	}
	if strings.Contains(upper, "INTEGER") || strings.Contains(upper, "SERIAL") {
		return "0"
	}
	if strings.Contains(upper, "NUMERIC") || strings.Contains(upper, "REAL") {
		return "0.0"
	}
	if strings.Contains(upper, "TIMESTAMP") || strings.Contains(upper, "DATETIME") {
		return "CURRENT_TIMESTAMP"
	}
	if strings.Contains(upper, "BOOLEAN") {
		return "0"
	}
	if strings.Contains(upper, "JSONB") || strings.Contains(upper, "JSON") {
		return "'{}'"
	}

	return "''"
}
