package database

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// ProviderFactory DB Provider 工厂
type ProviderFactory struct{}

// NewProviderFactory 创建工厂实例
func NewProviderFactory() *ProviderFactory {
	return &ProviderFactory{}
}

// CreateProvider 根据数据库驱动创建对应的Provider
// 支持的驱动：postgres, postgresql, sqlite, sqlite3
func (f *ProviderFactory) CreateProvider(db *gorm.DB) (DBProvider, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库连接不能为空")
	}

	// 获取数据库驱动名称
	driverName := f.getDriverName(db)

	switch driverName {
	case "postgres", "postgresql":
		return NewPostgresProvider(db), nil
	case "sqlite", "sqlite3":
		return NewSQLiteProvider(db), nil
	default:
		return nil, fmt.Errorf("不支持的数据库驱动: %s", driverName)
	}
}

// getDriverName 获取数据库驱动名称
func (f *ProviderFactory) getDriverName(db *gorm.DB) string {
	// 通过GORM的Dialector获取驱动名称
	dialector := db.Dialector.Name()
	return strings.ToLower(dialector)
}

// MustCreateProvider 创建Provider，失败则panic
// 用于启动阶段
func (f *ProviderFactory) MustCreateProvider(db *gorm.DB) DBProvider {
	provider, err := f.CreateProvider(db)
	if err != nil {
		panic(fmt.Sprintf("创建DB Provider失败: %v", err))
	}
	return provider
}
