# LuckDB SDK 演示项目

这是一个完整的 SDK 功能演示项目，包含了所有主要 API 模块的演示和验证。

## 目录结构

```
examples/
├── .env.example          # 环境变量示例文件
├── .env                  # 实际环境变量（需创建，gitignore）
├── config.ts             # 配置管理
├── runner.ts             # 主入口，运行所有演示
├── utils/
│   ├── logger.ts        # 日志工具
│   ├── helpers.ts       # 辅助函数
│   └── types.ts         # 类型定义
└── demos/
    ├── 01-auth.ts       # 认证 API 演示
    ├── 02-space.ts      # Space API 演示
    ├── 03-base.ts       # Base API 演示
    ├── 04-table.ts      # Table API 演示
    ├── 05-field.ts      # Field API 演示
    ├── 06-record.ts     # Record API 演示
    ├── 07-view.ts       # View API 演示
    └── 08-sharedb.ts    # ShareDB WebSocket 演示
```

## 快速开始

### 1. 安装依赖

```bash
cd luckdb-sdk
npm install
```

### 2. 配置环境变量

复制环境变量示例文件：

```bash
cp examples/.env.example examples/.env
```

编辑 `examples/.env` 文件，设置服务器地址和测试账号：

```env
SERVER_URL=http://localhost:8080
TEST_EMAIL=demo@example.com
TEST_PASSWORD=Test123456!
TEST_NAME=Demo User
```

### 3. 运行演示

#### 运行所有演示

```bash
npm run demo:all
```

#### 运行指定模块

```bash
# 认证演示
npm run demo:auth

# Space 演示
npm run demo:space

# Base 演示
npm run demo:base

# Table 演示
npm run demo:table

# Field 演示
npm run demo:field

# Record 演示
npm run demo:record

# View 演示
npm run demo:view

# ShareDB WebSocket 演示
npm run demo:sharedb
```

#### 直接使用 tsx 运行

```bash
# 运行所有演示
tsx examples/runner.ts

# 运行指定模块
tsx examples/runner.ts --module=space

# 运行指定模块，不清理资源
tsx examples/runner.ts --module=record --no-cleanup

# 运行单个演示文件
tsx examples/demos/01-auth.ts
```

## 演示模块说明

### 01-auth.ts - 认证 API

演示功能：
- 用户注册
- 用户登录
- 获取当前用户信息
- 刷新访问令牌
- 更新用户信息
- 修改密码
- 登出

### 02-space.ts - Space API

演示功能：
- 创建 Space
- 获取 Space 列表（分页）
- 获取单个 Space
- 更新 Space
- 删除 Space
- 获取 Space 的 Base 列表
- 在 Space 中创建 Base
- Space 协作者管理

### 03-base.ts - Base API

演示功能：
- 创建 Base（在 Space 中）
- 获取 Base 列表（Space 下或全局）
- 获取单个 Base
- 更新 Base
- 复制 Base
- 删除 Base
- 获取 Base 权限
- Base 协作者管理

### 04-table.ts - Table API

演示功能：
- 创建 Table（支持字段和视图配置）
- 获取 Table 列表
- 获取单个 Table
- 更新 Table
- 重命名 Table
- 复制 Table（支持数据和视图选项）
- 获取 Table 使用情况
- 获取 Table 管理菜单
- 删除 Table

### 05-field.ts - Field API

演示功能：
- 创建各种类型的字段：
  - 单行文本 (singleLineText)
  - 多行文本 (longText)
  - 数字 (number)
  - 日期 (date)
  - 日期时间 (datetime)
  - 布尔值 (boolean)
  - 单选 (singleSelect)
  - 多选 (multipleSelect)
- 获取字段列表
- 获取所有字段
- 获取单个字段
- 更新字段
- 删除字段

### 06-record.ts - Record API

演示功能：
- 创建记录
- 获取记录列表（分页、过滤、排序）
- 获取所有记录（自动分页）
- 获取单个记录（新 API 和旧 API）
- 更新记录（支持乐观锁）
- 删除记录
- 批量创建记录
- 批量更新记录
- 批量删除记录
- 搜索记录（如果服务端支持）

### 07-view.ts - View API

演示功能：
- 创建视图（grid, kanban, calendar, gallery, form）
- 获取视图列表
- 获取单个视图
- 更新视图
- 更新视图配置：
  - 过滤器 (filter)
  - 排序 (sort)
  - 分组 (group)
  - 列配置 (columnMeta)
  - 选项 (options)
  - 排序位置 (order)
  - 分享元数据 (shareMeta)
- 视图分享功能（启用、禁用、刷新分享 ID）
- 视图锁定/解锁
- 复制视图
- 删除视图

### 08-sharedb.ts - ShareDB WebSocket API

演示功能：
- 初始化 ShareDB 连接
- 连接 WebSocket
- 订阅记录文档
- 监听文档事件（load, op, error）
- 提交操作（更新字段值）
- 多客户端实时同步测试：
  - 创建两个客户端连接
  - 一个更新记录
  - 另一个接收实时更新
- 断开连接和清理

## 环境变量说明

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `SERVER_URL` | 服务器 HTTP 地址 | `http://localhost:8080` |
| `WEBSOCKET_URL` | WebSocket 地址（可选） | 从 `SERVER_URL` 推导 |
| `TEST_EMAIL` | 测试用户邮箱 | `demo@example.com` |
| `TEST_PASSWORD` | 测试用户密码 | `Test123456!` |
| `TEST_NAME` | 测试用户名称 | `Demo User` |
| `DEBUG` | 调试模式 | `false` |
| `CLEANUP` | 是否清理资源 | `true`（设置为 `false` 可保留测试数据） |

## 使用示例

### 运行完整的端到端演示

```bash
# 运行所有模块，自动创建资源链
npm run demo:all

# 输出示例：
# ============================================================
#   LuckDB SDK 完整演示
# ============================================================
# 
# [INFO] 正在登录...
# [SUCCESS] 登录成功
# 
# ============================================================
#   模块: AUTH
# ============================================================
# ...
```

### 运行单个模块

```bash
# 只运行 Record 演示
npm run demo:record

# 只运行 ShareDB 演示
npm run demo:sharedb
```

### 保留测试数据

```bash
# 运行演示但不清理资源
CLEANUP=false npm run demo:all

# 或使用 --no-cleanup 参数
tsx examples/runner.ts --no-cleanup
```

## 注意事项

1. **服务器必须运行**：确保服务器在 `SERVER_URL` 指定的地址运行
2. **认证**：所有演示（除了 auth）都需要先登录
3. **资源依赖**：某些模块需要先创建其他资源（如 record 需要 table 和 field）
4. **资源清理**：默认会清理创建的测试资源，设置 `CLEANUP=false` 可保留
5. **ShareDB 连接**：确保服务器支持 WebSocket 连接（`/socket` 端点）

## 故障排除

### 连接失败

检查：
- 服务器是否运行
- `SERVER_URL` 是否正确
- 防火墙设置

### 认证失败

检查：
- `TEST_EMAIL` 和 `TEST_PASSWORD` 是否正确
- 用户是否已注册
- Token 是否有效

### ShareDB 连接失败

检查：
- WebSocket 端点是否可访问
- Token 是否正确传递
- 服务器 ShareDB 服务是否启用

### 模块运行失败

检查：
- 前置资源是否已创建（如 record 需要 table 和 field）
- 服务端 API 是否实现
- 查看详细错误信息（设置 `DEBUG=true`）

## 开发

### 添加新演示模块

1. 在 `demos/` 目录下创建新文件（如 `09-new-feature.ts`）
2. 导出 `runNewFeatureDemo` 函数
3. 在 `runner.ts` 中导入并添加到 `modules` 数组
4. 在 `package.json` 中添加对应的脚本

### 修改日志格式

编辑 `utils/logger.ts` 中的 `formatMessage` 方法。

### 添加辅助函数

在 `utils/helpers.ts` 中添加新的辅助函数。

## 贡献

欢迎提交 PR 改进演示项目！


