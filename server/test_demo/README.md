# ShareDB 实时同步测试演示

这是一个实战性的测试演示程序，用于验证 ShareDB 的实时同步功能。重点测试字段修改后的实时广播机制。

## 功能特性

- ✅ 自动创建测试账号并完成注册登录
- ✅ 完整的资源创建流程：Space → Base → Table → Field → Record
- ✅ 双客户端协作测试：一个更新记录，另一个监听广播
- ✅ 实时验证 ShareDB 操作广播是否正常工作

## 文件结构

```
test_demo/
├── README.md              # 本文档
├── main.go                # 主测试程序
├── config.go              # 配置管理
├── test_helpers.go        # HTTP API 辅助函数
├── client_listener.go     # WebSocket 监听客户端
└── client_updater.go      # HTTP 更新客户端
```

## 前置要求

1. **服务器运行中**：确保服务器正在运行，默认地址为 `http://localhost:8080`
2. **ShareDB 服务已启用**：确保 ShareDB 服务已正确配置并启用
3. **Go 环境**：需要 Go 1.19 或更高版本

## 使用方法

### 1. 进入测试目录

```bash
cd server/test_demo
```

### 2. 安装依赖

```bash
go mod init test_demo
go get github.com/gorilla/websocket
```

### 3. 运行测试

```bash
go run .
```

### 4. 使用环境变量配置（可选）

```bash
# 自定义服务器地址
export SERVER_URL=http://localhost:8080
export WEBSOCKET_URL=ws://localhost:8080

# 自定义测试账号
export TEST_EMAIL=test@example.com
export TEST_PASSWORD=Test123456!
export TEST_NAME=测试用户

go run .
```

## 测试流程

程序执行以下步骤：

1. **初始化配置**
   - 加载服务器地址和测试账号信息
   - 支持环境变量覆盖默认值

2. **注册/登录**
   - 尝试注册新账号，如果已存在则自动登录
   - 获取 JWT Token 用于后续认证

3. **创建资源链**
   - 创建 Space（测试空间）
   - 创建 Base（测试Base）
   - 创建 Table（测试表）
   - 创建 Field（标题字段，类型：singleLineText）
   - 创建 Record（初始值为"初始值"）

4. **启动监听客户端**
   - 建立 WebSocket 连接到 `/socket`
   - 发送握手消息（`{"a": "hs"}`）
   - 订阅记录文档（`{"a": "s", "c": "rec_{tableId}", "d": "{recordId}"}`）

5. **执行更新并验证**
   - 更新客户端通过 HTTP API 更新记录字段
   - 监听客户端接收 ShareDB 广播的操作消息
   - 验证每条更新是否都触发了广播

6. **输出结果**
   - 显示收到的操作消息详情
   - 打印所有接收到的消息
   - 清理资源并退出

## ShareDB 协议说明

### WebSocket 端点

- URL: `ws://localhost:8080/socket`
- 认证: 通过 `Authorization: Bearer {token}` 头部

### 消息格式

#### 握手消息（客户端 → 服务器）
```json
{"a": "hs"}
```

#### 握手响应（服务器 → 客户端）
```json
{
  "a": "hs",
  "protocol": 1,
  "type": "json0",
  "id": "connection_id"
}
```

#### 订阅消息（客户端 → 服务器）
```json
{
  "a": "s",
  "c": "rec_{tableId}",
  "d": "{recordId}"
}
```

#### 订阅响应（服务器 → 客户端）
```json
{
  "a": "s",
  "c": "rec_{tableId}",
  "d": "{recordId}",
  "v": 1,
  "data": {...}
}
```

#### 操作消息（服务器 → 客户端，广播）
```json
{
  "a": "op",
  "c": "rec_{tableId}",
  "d": "{recordId}",
  "v": 2,
  "op": [
    {
      "p": ["data", "fieldId"],
      "oi": "新值",
      "od": "旧值"
    }
  ]
}
```

## 测试场景

### 场景 1: 基本实时同步

1. 启动监听客户端并订阅记录
2. 更新记录字段值为"第一次更新"
3. 验证监听客户端收到操作消息

### 场景 2: 多次更新

1. 启动监听客户端并订阅记录
2. 连续更新记录字段多次（每次间隔 2 秒）
3. 验证每次更新都触发了广播

### 场景 3: 广播验证

验证接收到的操作消息包含：
- `action` 为 `"op"`
- `collection` 正确（`rec_{tableId}`）
- `docID` 正确（记录ID）
- `op` 数组包含操作详情

## 故障排查

### 问题 1: WebSocket 连接失败

**可能原因：**
- 服务器未运行
- WebSocket 端点配置错误
- JWT Token 无效或过期

**解决方法：**
- 检查服务器是否运行：`curl http://localhost:8080/api/v1/monitoring/db-stats`
- 确认 WebSocket URL 配置正确
- 重新登录获取新的 Token

### 问题 2: 订阅失败

**可能原因：**
- ShareDB 服务未启用
- 记录不存在
- 权限问题

**解决方法：**
- 检查服务器日志确认 ShareDB 服务状态
- 确认记录已正确创建
- 检查用户权限

### 问题 3: 未收到操作消息

**可能原因：**
- ShareDB 广播未触发
- 订阅的 collection/docID 不正确
- 网络延迟

**解决方法：**
- 检查服务器日志确认是否调用了 `BroadcastOperation`
- 确认 collection 格式为 `rec_{tableId}`
- 增加等待时间

### 问题 4: 注册失败

**可能原因：**
- 邮箱已被注册
- 密码不符合要求

**解决方法：**
- 程序会自动尝试登录已存在的账号
- 如果登录也失败，更换测试邮箱

## 预期输出

成功的测试输出应该类似：

```
🚀 开始 ShareDB 实时同步测试演示
============================================================
📋 配置信息:
   - 服务器地址: http://localhost:8080
   - WebSocket 地址: ws://localhost:8080
   - 测试邮箱: test_demo@example.com

📝 步骤 1: 注册/登录测试账号
✅ 登录成功: UserID=xxx, Email=test_demo@example.com

📝 步骤 2: 创建资源链 (Space -> Base -> Table -> Field -> Record)
  创建 Space...
  ✅ Space 创建成功: ID=xxx
  创建 Base...
  ✅ Base 创建成功: ID=xxx
  创建 Table...
  ✅ Table 创建成功: ID=xxx
  创建 Field...
  ✅ Field 创建成功: ID=xxx, Name=标题
  创建 Record...
  ✅ Record 创建成功: ID=xxx

📝 步骤 3: 启动监听客户端
✅ WebSocket 连接成功，等待握手响应...
✅ 收到握手响应: protocol=1, type=json0
📡 订阅记录: collection=rec_xxx, docID=xxx
✅ 订阅成功，收到初始数据: version=1

📝 步骤 4: 执行记录更新并验证广播
🔄 更新记录字段: tableID=xxx, recordID=xxx, fieldID=xxx, value=第一次更新
✅ 记录更新成功
📨 收到操作消息: version=2, opCount=1
✅ 收到操作消息 1/4
...
✅ 测试成功！收到 4 条操作消息

📨 收到的操作消息详情:
消息 1:
  - Action: op
  - Collection: rec_xxx
  - DocID: xxx
  - Version: 2
  - Operations: 1
...
```

## 技术细节

### ShareDB 广播机制

当记录更新时，`RecordBroadcasterImpl.BroadcastRecordUpdate()` 会：

1. 创建 ShareDB 操作（OTOperation）
2. 调用 `ShareDBService.BroadcastOperation()`
3. 广播到所有连接的 WebSocket 客户端
4. 客户端收到操作消息并更新本地状态

### 操作类型

ShareDB 使用 JSON0 操作类型，包含：
- `p`: 路径（path）
- `oi`: 插入的值（old insert）
- `od`: 删除的值（old delete）

## 扩展测试

可以扩展此测试以验证：

- 多个客户端同时订阅同一记录
- 并发更新场景
- 操作冲突处理
- 网络断开重连
- 大量操作的性能

## 许可证

本项目遵循主项目的许可证。

