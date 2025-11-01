-- =====================================================
-- Migration: 000001_init_schema
-- Description: 初始化数据库schema（由GORM AutoMigrate管理）
-- Author: System
-- Date: 2025-10-08
-- =====================================================

-- 本迁移为占位迁移，实际的表创建由 GORM AutoMigrate 处理
-- 这样可以保持迁移版本追踪，同时利用 GORM 的自动迁移功能

-- 创建迁移版本追踪表（如果不存在）
-- golang-migrate 会自动创建 schema_migrations 表
-- 这里只是一个标记，表示初始化完成

SELECT 'Initial schema created by GORM AutoMigrate' as migration_status;

