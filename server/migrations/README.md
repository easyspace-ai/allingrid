# 数据库迁移文件

本项目使用 [golang-migrate/migrate](https://github.com/golang-migrate/migrate) 进行数据库迁移管理。

## 📁 目录结构

```
migrations/
├── 000001_init_schema.up.sql                # 初始化schema
├── 000001_init_schema.down.sql              # 回滚初始化
├── 000002_add_virtual_field_support.up.sql  # 虚拟字段支持
├── 000002_add_virtual_field_support.down.sql # 回滚虚拟字段
└── README.md                                 # 本文档
```

## 🚀 快速使用

### 执行迁移（推荐）

```bash
cd /Users/leven/space/easy/luckdb/server

# 使用 Makefile（推荐）
make -f Makefile.migrate migrate-hybrid

# 或直接运行
go run cmd/migrate/main.go hybrid
```

### 查看当前版本

```bash
make -f Makefile.migrate migrate-version
```

### 创建新迁移

```bash
make -f Makefile.migrate migrate-create NAME=add_new_feature
```

## 📝 迁移文件命名规范

格式：`{version}_{description}.{up|down}.sql`

示例：
- `000001_init_schema.up.sql` - 初始化up
- `000001_init_schema.down.sql` - 初始化down
- `000002_add_virtual_field_support.up.sql` - 功能up
- `000002_add_virtual_field_support.down.sql` - 功能down

**版本号规则：**
- 使用6位数字（000001, 000002...）
- 或使用Unix时间戳（1728380000）
- 必须递增，不能重复

## 🔄 迁移模式

### 1. 混合模式（推荐）⭐

结合 golang-migrate 和 GORM AutoMigrate 的优势：

```bash
go run cmd/migrate/main.go hybrid
```

**流程：**
1. 先执行 golang-migrate（SQL迁移文件）
2. 再执行 GORM AutoMigrate（模型同步）
3. 添加补充索引和约束

**优点：**
- ✅ SQL迁移版本可追踪
- ✅ 支持回滚
- ✅ GORM模型自动同步
- ✅ 两全其美

### 2. 仅 golang-migrate

只执行SQL迁移文件：

```bash
go run cmd/migrate/main.go up
```

### 3. 仅 GORM AutoMigrate

只执行GORM模型同步：

```bash
go run cmd/migrate/main.go gorm-only
```

## 📌 命令参考

### 基础命令

```bash
# 执行所有待执行迁移
go run cmd/migrate/main.go up

# 回滚最后一次迁移
go run cmd/migrate/main.go down

# 查看当前版本
go run cmd/migrate/main.go version

# 强制设置版本（解决dirty状态）
go run cmd/migrate/main.go force 2

# 删除所有表（危险！）
go run cmd/migrate/main.go drop
```

### Makefile 命令

```bash
# 执行混合迁移
make -f Makefile.migrate migrate-hybrid

# 创建新迁移
make -f Makefile.migrate migrate-create NAME=add_user_avatar

# 查看版本
make -f Makefile.migrate migrate-version

# 回滚
make -f Makefile.migrate migrate-down

# 备份数据库
make -f Makefile.migrate db-backup

# 连接数据库
make -f Makefile.migrate db-console

# 查看所有命令
make -f Makefile.migrate help
```

## 📖 编写迁移文件

### UP 迁移示例

```sql
-- 000003_add_user_avatar.up.sql

-- 添加头像字段
ALTER TABLE users ADD COLUMN avatar_url VARCHAR(500);

-- 创建索引
CREATE INDEX idx_users_avatar ON users(avatar_url) 
    WHERE avatar_url IS NOT NULL;

-- 添加注释
COMMENT ON COLUMN users.avatar_url IS '用户头像URL';
```

### DOWN 迁移示例

```sql
-- 000003_add_user_avatar.down.sql

-- 删除索引
DROP INDEX IF EXISTS idx_users_avatar;

-- 删除字段
ALTER TABLE users DROP COLUMN IF EXISTS avatar_url;
```

## ⚠️ 最佳实践

### 1. 迁移前备份

```bash
make -f Makefile.migrate db-backup
```

### 2. 测试环境验证

先在测试环境执行：
```bash
DB_NAME=luckdb_dev_test make -f Makefile.migrate migrate-hybrid
```

### 3. 版本控制

- ✅ 所有迁移文件提交到 git
- ✅ 不要修改已执行的迁移文件
- ✅ 新功能创建新的迁移文件

### 4. 命名规范

- 使用描述性名称：`add_virtual_field_support`
- 避免特殊字符，使用下划线
- 保持简洁清晰

### 5. 回滚安全

- 每个 up 迁移都应有对应的 down
- down 迁移应该能完全回滚 up 的更改
- 测试回滚功能是否正常

## 🐛 故障排查

### 问题1: dirty 状态

```
Error: Dirty database version 2. Fix and force version.
```

**解决：**
```bash
# 检查是哪个迁移失败了
make -f Makefile.migrate migrate-version

# 手动修复数据库问题后，强制设置版本
make -f Makefile.migrate migrate-force VERSION=2
```

### 问题2: 迁移文件找不到

```
Error: file does not exist
```

**解决：**
- 确认在 server 目录下执行命令
- 检查 migrations/ 目录是否存在
- 检查迁移文件命名是否正确

### 问题3: 连接数据库失败

**解决：**
```bash
# 检查配置
cat config.yaml

# 或设置环境变量
export POSTGRES_HOST=localhost
export POSTGRES_PASSWORD=your_password

# 测试连接
make -f Makefile.migrate db-console
```

## 📊 迁移版本追踪

golang-migrate 自动创建 `schema_migrations` 表：

```sql
-- 查看迁移历史
SELECT * FROM schema_migrations;

-- 输出示例：
-- version | dirty
-- --------+-------
--       2 | f
```

## 🔗 相关资源

- [golang-migrate 文档](https://github.com/golang-migrate/migrate)
- [GORM 文档](https://gorm.io/docs/migration.html)
- 项目迁移指南: `/MIGRATION_GUIDE_VIRTUAL_FIELDS.md`

---

**维护者**: 开发团队  
**最后更新**: 2025-10-08

