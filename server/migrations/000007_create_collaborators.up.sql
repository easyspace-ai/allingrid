-- =====================================================
-- Migration: 000007_create_collaborators
-- Description: 创建协作者表
-- Author: System
-- Date: 2025-10-29
-- =====================================================

CREATE TABLE IF NOT EXISTS collaborators (
    id VARCHAR(36) PRIMARY KEY,
    resource_id VARCHAR(36) NOT NULL,
    resource_type VARCHAR(32) NOT NULL,
    principal_id VARCHAR(36) NOT NULL,
    principal_type VARCHAR(32) NOT NULL,
    role_name VARCHAR(32) NOT NULL,
    created_by VARCHAR(36) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    deleted_at BIGINT
);

-- 创建唯一索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_collaborators_resource_principal 
    ON collaborators(resource_id, principal_id) WHERE deleted_at IS NULL;

-- 创建其他索引
CREATE INDEX IF NOT EXISTS idx_collaborators_resource ON collaborators(resource_id, resource_type);
CREATE INDEX IF NOT EXISTS idx_collaborators_principal ON collaborators(principal_id, principal_type);
CREATE INDEX IF NOT EXISTS idx_collaborators_deleted_at ON collaborators(deleted_at);

-- 注释
COMMENT ON TABLE collaborators IS '协作者表';
COMMENT ON COLUMN collaborators.resource_id IS '资源ID（Space ID 或 Base ID）';
COMMENT ON COLUMN collaborators.resource_type IS '资源类型：space, base';
COMMENT ON COLUMN collaborators.principal_id IS '主体ID（User ID 或 Department ID）';
COMMENT ON COLUMN collaborators.principal_type IS '主体类型：user, department';
COMMENT ON COLUMN collaborators.role_name IS '角色：owner, creator, editor, viewer, commenter';
COMMENT ON COLUMN collaborators.created_by IS '创建者ID';







