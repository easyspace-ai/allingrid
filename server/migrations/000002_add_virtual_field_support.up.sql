-- =====================================================
-- Migration: 000002_add_virtual_field_support
-- Description: 添加虚拟字段和AI字段支持
-- Author: System
-- Date: 2025-10-08
-- =====================================================

-- 为 field 表添加虚拟字段相关字段
ALTER TABLE field 
    ADD COLUMN IF NOT EXISTS is_pending BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_error BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS lookup_linked_field_id VARCHAR(30),
    ADD COLUMN IF NOT EXISTS lookup_options TEXT,
    ADD COLUMN IF NOT EXISTS ai_config TEXT;

-- 添加字段注释
COMMENT ON COLUMN field.is_pending IS '虚拟字段是否正在等待计算';
COMMENT ON COLUMN field.has_error IS '虚拟字段计算是否出错';
COMMENT ON COLUMN field.lookup_linked_field_id IS 'Lookup字段关联的link字段ID';
COMMENT ON COLUMN field.lookup_options IS 'Lookup字段配置选项（JSON格式）';
COMMENT ON COLUMN field.ai_config IS 'AI字段配置（JSON格式）';

-- =====================================================
-- 创建字段依赖关系表
-- =====================================================
CREATE TABLE IF NOT EXISTS field_dependency (
    id VARCHAR(50) PRIMARY KEY,
    source_field_id VARCHAR(30) NOT NULL,
    dependent_field_id VARCHAR(30) NOT NULL,
    dependency_type VARCHAR(50) NOT NULL,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_field_dependency_source 
        FOREIGN KEY (source_field_id) REFERENCES field(id) ON DELETE CASCADE,
    CONSTRAINT fk_field_dependency_dependent 
        FOREIGN KEY (dependent_field_id) REFERENCES field(id) ON DELETE CASCADE,
    CONSTRAINT unique_dependency UNIQUE (source_field_id, dependent_field_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_field_dependency_source 
    ON field_dependency(source_field_id);

CREATE INDEX IF NOT EXISTS idx_field_dependency_dependent 
    ON field_dependency(dependent_field_id);

COMMENT ON TABLE field_dependency IS '字段依赖关系表，用于管理虚拟字段的依赖图';
COMMENT ON COLUMN field_dependency.source_field_id IS '源字段ID（被依赖的字段）';
COMMENT ON COLUMN field_dependency.dependent_field_id IS '依赖字段ID（依赖其他字段的虚拟字段）';
COMMENT ON COLUMN field_dependency.dependency_type IS '依赖类型：lookup/formula/rollup/ai';

-- =====================================================
-- 创建虚拟字段计算缓存表
-- =====================================================
CREATE TABLE IF NOT EXISTS virtual_field_cache (
    id VARCHAR(50) PRIMARY KEY,
    record_id VARCHAR(30) NOT NULL,
    field_id VARCHAR(30) NOT NULL,
    cached_value TEXT,
    value_type VARCHAR(50),
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    CONSTRAINT fk_virtual_cache_field 
        FOREIGN KEY (field_id) REFERENCES field(id) ON DELETE CASCADE,
    CONSTRAINT unique_cache_key UNIQUE (record_id, field_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_virtual_cache_record 
    ON virtual_field_cache(record_id);

CREATE INDEX IF NOT EXISTS idx_virtual_cache_field 
    ON virtual_field_cache(field_id);

CREATE INDEX IF NOT EXISTS idx_virtual_cache_expires 
    ON virtual_field_cache(expires_at) 
    WHERE expires_at IS NOT NULL;

COMMENT ON TABLE virtual_field_cache IS '虚拟字段计算结果缓存表';
COMMENT ON COLUMN virtual_field_cache.cached_value IS '缓存的计算结果（JSON格式）';
COMMENT ON COLUMN virtual_field_cache.value_type IS '值类型（text/number/boolean/object等）';
COMMENT ON COLUMN virtual_field_cache.expires_at IS '缓存过期时间';

-- =====================================================
-- 为field表添加虚拟字段相关的外键和索引
-- =====================================================

-- 添加外键约束
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'fk_field_lookup_linked'
    ) THEN
        ALTER TABLE field 
        ADD CONSTRAINT fk_field_lookup_linked 
        FOREIGN KEY (lookup_linked_field_id) 
        REFERENCES field(id) ON DELETE SET NULL;
    END IF;
END $$;

-- 添加部分索引（性能优化）
CREATE INDEX IF NOT EXISTS idx_field_is_computed 
    ON field(is_computed) WHERE is_computed = TRUE;

CREATE INDEX IF NOT EXISTS idx_field_is_lookup 
    ON field(is_lookup) WHERE is_lookup = TRUE;

CREATE INDEX IF NOT EXISTS idx_field_has_error 
    ON field(has_error) WHERE has_error = TRUE;

CREATE INDEX IF NOT EXISTS idx_field_is_pending 
    ON field(is_pending) WHERE is_pending = TRUE;

CREATE INDEX IF NOT EXISTS idx_field_lookup_linked 
    ON field(lookup_linked_field_id) 
    WHERE lookup_linked_field_id IS NOT NULL;

-- =====================================================
-- 数据验证
-- =====================================================

-- 检查是否有无效的 lookup_linked_field_id 引用
DO $$
DECLARE
    invalid_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO invalid_count
    FROM field f1
    WHERE f1.lookup_linked_field_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM field f2 
        WHERE f2.id = f1.lookup_linked_field_id 
        AND f2.deleted_time IS NULL
    );
    
    IF invalid_count > 0 THEN
        RAISE WARNING '发现 % 条字段有无效的 lookup_linked_field_id 引用', invalid_count;
    ELSE
        RAISE NOTICE '✓ 所有 lookup_linked_field_id 引用都有效';
    END IF;
END $$;

-- 迁移完成标记
SELECT 
    'Virtual Field Support Migration Completed' as status,
    NOW() as completed_at;

