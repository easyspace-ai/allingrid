-- =====================================================
-- Rollback: 000002_add_virtual_field_support
-- Description: 回滚虚拟字段和AI字段支持
-- Author: System
-- Date: 2025-10-08
-- =====================================================

-- 警告：此回滚将删除所有虚拟字段配置数据！
-- 请确认是否真的需要回滚

-- =====================================================
-- 删除外键约束
-- =====================================================
ALTER TABLE field DROP CONSTRAINT IF EXISTS fk_field_lookup_linked;

-- =====================================================
-- 删除表
-- =====================================================
DROP TABLE IF EXISTS virtual_field_cache CASCADE;
DROP TABLE IF EXISTS field_dependency CASCADE;

-- =====================================================
-- 删除索引
-- =====================================================
DROP INDEX IF EXISTS idx_field_lookup_linked;
DROP INDEX IF EXISTS idx_field_is_pending;
DROP INDEX IF EXISTS idx_field_has_error;
DROP INDEX IF EXISTS idx_field_is_lookup;
DROP INDEX IF EXISTS idx_field_is_computed;

-- =====================================================
-- 删除字段
-- =====================================================
-- 注意：删除列会丢失数据，请谨慎操作！
ALTER TABLE field DROP COLUMN IF EXISTS ai_config;
ALTER TABLE field DROP COLUMN IF EXISTS lookup_options;
ALTER TABLE field DROP COLUMN IF EXISTS lookup_linked_field_id;
ALTER TABLE field DROP COLUMN IF EXISTS has_error;
ALTER TABLE field DROP COLUMN IF EXISTS is_pending;

-- 回滚完成标记
SELECT 
    'Virtual Field Support Rolled Back' as status,
    NOW() as rolled_back_at;

