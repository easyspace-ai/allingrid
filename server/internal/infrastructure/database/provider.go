package database

import (
	"context"
)

// DBProvider 数据库提供者接口
// 抽象不同数据库的Schema隔离和动态表管理能力
// 严格按照旧系统（原 Teable 项目）实现：每个Base独立Schema + 每个Table独立物理表
type DBProvider interface {
	// ==================== Schema管理 ====================

	// CreateSchema 创建独立的数据库Schema
	// PostgreSQL: CREATE SCHEMA schemaName
	// SQLite: 不支持，使用表名前缀降级方案
	CreateSchema(ctx context.Context, schemaName string) error

	// DropSchema 删除Schema及其所有内容
	// PostgreSQL: DROP SCHEMA schemaName CASCADE
	// SQLite: 删除所有带前缀的表
	DropSchema(ctx context.Context, schemaName string) error

	// ==================== 动态表管理（完全复刻旧系统）====================

	// CreatePhysicalTable 创建物理表（包含系统字段）
	// 系统字段：__id, __auto_number, __created_time, __created_by,
	//          __last_modified_time, __last_modified_by, __version
	CreatePhysicalTable(ctx context.Context, schemaName, tableName string) error

	// DropPhysicalTable 删除物理表
	DropPhysicalTable(ctx context.Context, schemaName, tableName string) error

	// ==================== 列管理（字段作为列）====================

	// AddColumn 添加列到物理表
	// 对应用户创建字段：ALTER TABLE ADD COLUMN
	AddColumn(ctx context.Context, schemaName, tableName string, columnDef ColumnDefinition) error

	// AlterColumn 修改列类型和约束
	// 对应用户修改字段类型：ALTER TABLE ALTER COLUMN
	AlterColumn(ctx context.Context, schemaName, tableName, columnName string, newDef ColumnDefinition) error

	// DropColumn 删除列
	// 对应用户删除字段：ALTER TABLE DROP COLUMN
	DropColumn(ctx context.Context, schemaName, tableName, columnName string) error

	// ==================== 约束管理 ====================

	// AddUniqueConstraint 添加唯一性约束
	AddUniqueConstraint(ctx context.Context, schemaName, tableName, columnName, constraintName string) error

	// DropConstraint 删除约束
	DropConstraint(ctx context.Context, schemaName, tableName, constraintName string) error

	// SetNotNull 设置字段为NOT NULL
	SetNotNull(ctx context.Context, schemaName, tableName, columnName string) error

	// DropNotNull 移除NOT NULL约束
	DropNotNull(ctx context.Context, schemaName, tableName, columnName string) error

	// AddCheckConstraint 添加CHECK约束
	AddCheckConstraint(ctx context.Context, schemaName, tableName, constraintName, checkExpression string) error

	// ==================== 工具方法 ====================

	// GenerateTableName 生成完整的表名
	// PostgreSQL: schemaName.tableName
	// SQLite: schemaName_tableName
	GenerateTableName(baseID, tableID string) string

	// MapFieldTypeToDBType 将字段类型映射到数据库类型
	// 例如：singleLineText -> VARCHAR(255), number -> NUMERIC
	MapFieldTypeToDBType(fieldType string) string

	// DriverName 返回驱动名称
	DriverName() string

	// SupportsSchema 是否支持Schema
	SupportsSchema() bool
}

// ColumnDefinition 列定义
type ColumnDefinition struct {
	// Name 列名（字段的DBFieldName，例如：field_fld_xxx）
	Name string

	// Type 数据库类型（VARCHAR(255), INTEGER, TIMESTAMP, JSONB等）
	Type string

	// NotNull 是否NOT NULL约束
	NotNull bool

	// DefaultValue 默认值（SQL表达式，例如："'default'", "0", "CURRENT_TIMESTAMP"）
	DefaultValue *string

	// Unique 是否唯一约束
	Unique bool

	// Comment 列注释（用于文档和调试）
	Comment string
}

// FieldTypeMapping 字段类型映射表
// 将原 Teable 项目字段类型映射到数据库类型
var FieldTypeMapping = map[string]string{
	// 文本类型
	"singleLineText": "VARCHAR(255)",
	"longText":       "TEXT",

	// 数字类型
	"number":   "NUMERIC",
	"rating":   "INTEGER",
	"percent":  "NUMERIC",
	"currency": "NUMERIC",

	// 日期时间类型
	"date":             "TIMESTAMP",
	"createdTime":      "TIMESTAMP",
	"lastModifiedTime": "TIMESTAMP",

	// 布尔类型
	"checkbox": "BOOLEAN",

	// 选择类型
	"singleSelect":   "VARCHAR(255)",
	"multipleSelect": "JSONB", // 存储为JSON数组

	// 用户类型
	"user":           "JSONB", // 存储用户ID数组
	"createdBy":      "VARCHAR(50)",
	"lastModifiedBy": "VARCHAR(50)",

	// 附件类型
	"attachment": "JSONB", // 存储附件元数据数组

	// 关联类型
	"link": "JSONB", // 存储关联记录ID数组

	// 计算类型（存储计算结果）
	"formula":    "TEXT",    // 公式结果（可能是任意类型，用TEXT存储）
	"rollup":     "NUMERIC", // 聚合结果
	"lookup":     "JSONB",   // 查找结果（可能是多个值）
	"count":      "INTEGER", // 计数结果
	"autoNumber": "SERIAL",  // 自动编号

	// 其他类型
	"url":     "VARCHAR(2048)",
	"email":   "VARCHAR(255)",
	"phone":   "VARCHAR(50)",
	"barcode": "VARCHAR(255)",
	"button":  "TEXT", // 按钮配置
}
