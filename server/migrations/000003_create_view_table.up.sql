-- 创建视图表
CREATE TABLE IF NOT EXISTS view (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    table_id VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    filter TEXT,
    sort TEXT,
    "group" TEXT,
    column_meta TEXT,
    options TEXT,
    "order" DOUBLE PRECISION NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    is_locked BOOLEAN DEFAULT FALSE,
    enable_share BOOLEAN DEFAULT FALSE,
    share_id VARCHAR(255) UNIQUE,
    share_meta TEXT,
    created_by VARCHAR(255) NOT NULL,
    created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_time TIMESTAMP,
    deleted_time TIMESTAMP,
    
    CONSTRAINT fk_view_table FOREIGN KEY (table_id) REFERENCES table_meta(id) ON DELETE CASCADE
);

-- 创建索引
CREATE INDEX idx_view_table_id ON view(table_id);
CREATE INDEX idx_view_order ON view("order");
CREATE INDEX idx_view_deleted_time ON view(deleted_time);
CREATE UNIQUE INDEX idx_view_share_id ON view(share_id) WHERE share_id IS NOT NULL;

-- 添加注释
COMMENT ON TABLE view IS '视图表 - 存储表格的各种视图配置';
COMMENT ON COLUMN view.id IS '视图唯一标识';
COMMENT ON COLUMN view.name IS '视图名称';
COMMENT ON COLUMN view.description IS '视图描述';
COMMENT ON COLUMN view.table_id IS '所属表格ID';
COMMENT ON COLUMN view.type IS '视图类型: grid, kanban, gallery, form, calendar';
COMMENT ON COLUMN view.filter IS '过滤器配置(JSON)';
COMMENT ON COLUMN view.sort IS '排序配置(JSON)';
COMMENT ON COLUMN view."group" IS '分组配置(JSON)';
COMMENT ON COLUMN view.column_meta IS '列配置(JSON)';
COMMENT ON COLUMN view.options IS '视图选项(JSON)';
COMMENT ON COLUMN view."order" IS '视图排序位置';
COMMENT ON COLUMN view.version IS '版本号';
COMMENT ON COLUMN view.is_locked IS '是否锁定';
COMMENT ON COLUMN view.enable_share IS '是否启用分享';
COMMENT ON COLUMN view.share_id IS '分享ID';
COMMENT ON COLUMN view.share_meta IS '分享元数据(JSON)';
COMMENT ON COLUMN view.created_by IS '创建人';
COMMENT ON COLUMN view.created_time IS '创建时间';
COMMENT ON COLUMN view.last_modified_time IS '最后修改时间';
COMMENT ON COLUMN view.deleted_time IS '删除时间(软删除)';

