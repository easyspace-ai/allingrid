-- =====================================================
-- Migration: 000006_create_user_configs
-- Description: 创建用户配置表
-- Author: System
-- Date: 2025-10-29
-- =====================================================

CREATE TABLE IF NOT EXISTS user_configs (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL UNIQUE,
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    language VARCHAR(16) NOT NULL DEFAULT 'en-US',
    date_format VARCHAR(32) NOT NULL DEFAULT 'YYYY-MM-DD',
    time_format VARCHAR(8) NOT NULL DEFAULT '24h',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    deleted_at BIGINT
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_user_configs_user_id ON user_configs(user_id);
CREATE INDEX IF NOT EXISTS idx_user_configs_deleted_at ON user_configs(deleted_at);

-- 注释
COMMENT ON TABLE user_configs IS '用户配置表';
COMMENT ON COLUMN user_configs.timezone IS '时区，如：UTC, Asia/Shanghai';
COMMENT ON COLUMN user_configs.language IS '语言，如：en-US, zh-CN';
COMMENT ON COLUMN user_configs.date_format IS '日期格式，如：YYYY-MM-DD';
COMMENT ON COLUMN user_configs.time_format IS '时间格式：12h 或 24h';







