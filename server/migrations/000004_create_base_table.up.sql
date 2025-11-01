-- 创建base表（对齐原版）
CREATE TABLE IF NOT EXISTS base (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    icon VARCHAR(200),
    space_id VARCHAR(64) NOT NULL,
    created_by VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    
    CONSTRAINT fk_base_space FOREIGN KEY (space_id) 
        REFERENCES space(id) ON DELETE CASCADE,
    
    CONSTRAINT fk_base_creator FOREIGN KEY (created_by) 
        REFERENCES "user"(id) ON DELETE RESTRICT
);

-- 创建索引（优化查询性能）
CREATE INDEX idx_base_space_id ON base(space_id);
CREATE INDEX idx_base_deleted_at ON base(deleted_at);
CREATE INDEX idx_base_created_at ON base(created_at DESC);
CREATE INDEX idx_base_created_by ON base(created_by);

-- 添加表注释
COMMENT ON TABLE base IS 'Base表-数据库基地，一个Space包含多个Base';
COMMENT ON COLUMN base.id IS 'Base唯一标识';
COMMENT ON COLUMN base.name IS 'Base名称';
COMMENT ON COLUMN base.icon IS 'Base图标emoji或URL';
COMMENT ON COLUMN base.space_id IS '所属Space ID';
COMMENT ON COLUMN base.created_by IS '创建者用户ID';
COMMENT ON COLUMN base.created_at IS '创建时间';
COMMENT ON COLUMN base.updated_at IS '更新时间';
COMMENT ON COLUMN base.deleted_at IS '删除时间（软删除）';

