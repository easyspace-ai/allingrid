-- =====================================================
-- Migration: 000005_create_mcp_api_keys
-- Description: 创建MCP API Keys表
-- Author: System
-- Date: 2025-10-29
-- =====================================================

-- 创建 MCP API Keys 表
CREATE TABLE IF NOT EXISTS mcp_api_keys (
    id VARCHAR(255) PRIMARY KEY,
    key_id VARCHAR(255) UNIQUE NOT NULL,
    secret VARCHAR(255) NOT NULL,
    user_id VARCHAR(255),
    scopes JSON,
    description TEXT,
    expires_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSON
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_mcp_api_keys_user_id ON mcp_api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_api_keys_key_id ON mcp_api_keys(key_id);
CREATE INDEX IF NOT EXISTS idx_mcp_api_keys_is_active ON mcp_api_keys(is_active);
CREATE INDEX IF NOT EXISTS idx_mcp_api_keys_expires_at ON mcp_api_keys(expires_at);

-- 添加注释
COMMENT ON TABLE mcp_api_keys IS 'MCP API Keys 表';
COMMENT ON COLUMN mcp_api_keys.id IS 'API Key 唯一标识';
COMMENT ON COLUMN mcp_api_keys.key_id IS 'API Key ID';
COMMENT ON COLUMN mcp_api_keys.secret IS 'API Key 密钥';
COMMENT ON COLUMN mcp_api_keys.user_id IS '关联的用户 ID';
COMMENT ON COLUMN mcp_api_keys.scopes IS '权限范围列表';
COMMENT ON COLUMN mcp_api_keys.description IS 'API Key 描述';
COMMENT ON COLUMN mcp_api_keys.expires_at IS '过期时间';
COMMENT ON COLUMN mcp_api_keys.created_at IS '创建时间';
COMMENT ON COLUMN mcp_api_keys.updated_at IS '更新时间';
COMMENT ON COLUMN mcp_api_keys.last_used_at IS '最后使用时间';
COMMENT ON COLUMN mcp_api_keys.is_active IS '是否激活';
COMMENT ON COLUMN mcp_api_keys.metadata IS '元数据';







