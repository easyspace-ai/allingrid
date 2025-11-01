# LuckDB TypeScript SDK

[![npm version](https://badge.fury.io/js/luckdb-sdk.svg)](https://badge.fury.io/js/luckdb-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

LuckDB 的官方 TypeScript SDK，提供完整的 API 客户端和 ShareDB 实时协作功能。

## 特性

- 🚀 **完整的 API 支持** - 支持所有 LuckDB API 端点
- 🔄 **ShareDB 集成** - 内置实时协作功能，支持 OT 操作转换
- 🔌 **自动重连** - WebSocket 连接自动重连和心跳检测
- 📱 **TypeScript 支持** - 完整的类型定义和智能提示
- 🎯 **开箱即用** - 无需额外安装 ShareDB 相关依赖
- 🔐 **认证管理** - 多种认证存储方式（本地、异步）
- 📊 **批量操作** - 支持批量 CRUD 操作
- 🌐 **多格式构建** - 支持 ES、CommonJS、UMD、IIFE 格式
- 🏢 **组织管理** - 完整的组织、工作流、历史记录和通知系统
- 🔒 **乐观锁支持** - 记录更新支持版本控制和冲突解决
- 🎨 **高级字段配置** - 支持 Teable 风格的字段选项和格式化

## 安装

```bash
npm install luckdb-sdk
# 或
yarn add luckdb-sdk
# 或
pnpm add luckdb-sdk
```

## 快速开始

### 基础使用

```typescript
import LuckDBClient from 'luckdb-sdk'

// 初始化客户端
const luckdb = new LuckDBClient('http://localhost:8080')

// 用户登录
const { user, accessToken } = await luckdb.auth.login('user@example.com', 'password')

// 创建空间
const space = await luckdb.spaces.create({ name: 'My Space' })

// 创建 Base
const base = await luckdb.bases.create(space.id, { name: 'My Base' })

// 创建表格
const table = await luckdb.tables.create(base.id, { name: 'Tasks' })

// 创建字段
const nameField = await luckdb.fields.create(table.id, {
  name: 'Name',
  type: 'singleLineText',
  required: true
})

// 创建选择字段
const statusField = await luckdb.fields.create(table.id, {
  name: 'Status',
  type: 'singleSelect',
  options: {
    choices: [
      { id: 'todo', name: '待办', color: '#ff0000' },
      { id: 'doing', name: '进行中', color: '#00ff00' },
      { id: 'done', name: '已完成', color: '#0000ff' }
    ]
  }
})

// 创建数字字段
const priorityField = await luckdb.fields.create(table.id, {
  name: 'Priority',
  type: 'number',
  options: {
    precision: 2,
    format: 'decimal',
    showCommas: true,
    min: 1,
    max: 10
  }
})

// 创建记录
const record = await luckdb.records.create(table.id, {
  fields: {
    [nameField.id]: 'Task 1',
    [statusField.id]: 'todo',
    [priorityField.id]: 5
  }
})

// 更新记录（支持乐观锁）
const updatedRecord = await luckdb.records.update(record.id, {
  fields: {
    [statusField.id]: 'doing'
  },
  version: record.version // 乐观锁版本号
})
```

### ShareDB 实时协作

> 注意：当前版本暂不支持 Presence 与 YJS；仅提供 ShareDB 文档的订阅与操作。

```typescript
// 连接 ShareDB（自动重连、心跳）
await luckdb.sharedb.connect()

// 获取文档
const doc = luckdb.sharedb.getDocument('records', 'record-123')

// 订阅文档
await doc.subscribe()

console.log('Document data:', doc.data)
console.log('Document version:', doc.version)

// 监听操作
doc.on('op', ({ op, source, data }) => {
  console.log('Operation applied:', op)
  console.log('New data:', data)
  // 自动更新 UI
})

doc.on('error', (err) => {
  console.error('Document error:', err)
})

// 提交操作（JSON0 格式）
// 更新字段
doc.submitOp([{ p: ['name'], oi: 'New Name' }])

// 删除字段
doc.submitOp([{ p: ['description'], od: 'Old description' }])

// 数组操作
doc.submitOp([{ p: ['tags', 0], li: 'urgent' }])

// 清理
doc.destroy()
await luckdb.sharedb.disconnect()
```

## API 参考

### 客户端初始化

```typescript
const luckdb = new LuckDBClient(baseURL, authStore?, lang?)
```

**参数：**
- `baseURL` (string): LuckDB 服务器地址
- `authStore` (BaseAuthStore?, 可选): 认证存储实例
- `lang` (string?, 可选): 语言设置，默认为 'zh-CN'

### 认证服务

```typescript
// 登录
await luckdb.auth.login(email, password)

// 注册
await luckdb.auth.register(email, password, passwordConfirm, name?)

// 登出
await luckdb.auth.logout()

// 刷新令牌
await luckdb.auth.refreshToken()

// 获取当前用户
await luckdb.auth.getCurrentUser()

// 更新用户信息
await luckdb.auth.updateUser({ name: 'New Name' })

// 更新密码
await luckdb.auth.updatePassword(oldPassword, newPassword, newPasswordConfirm)
```

### 空间和 Base 管理

```typescript
// 空间操作
const space = await luckdb.spaces.create({ name: 'My Space' })
const spaces = await luckdb.spaces.getList(page, perPage, filter)
const space = await luckdb.spaces.getOne(id)
await luckdb.spaces.update(id, data)
await luckdb.spaces.delete(id)

// Base 操作
const base = await luckdb.bases.create(spaceId, { name: 'My Base' })
const bases = await luckdb.bases.getList(spaceId?, page, perPage, filter)
const base = await luckdb.bases.getOne(id)
await luckdb.bases.update(id, data)
await luckdb.bases.delete(id)
```

### 表格和字段管理

```typescript
// 表格操作
const table = await luckdb.tables.create(baseId, { name: 'My Table' })
const tables = await luckdb.tables.getList(baseId, page, perPage, filter)
const table = await luckdb.tables.getOne(id)
await luckdb.tables.update(id, data)
await luckdb.tables.delete(id)
await luckdb.tables.rename(id, { name: 'New Name' })
const duplicated = await luckdb.tables.duplicate(id, { name: 'Copy' })

// 字段操作
const field = await luckdb.fields.create(tableId, {
  name: 'Name',
  type: 'text',
  required: true
})
const fields = await luckdb.fields.getList(tableId, page, perPage, filter)
const field = await luckdb.fields.getOne(id)
await luckdb.fields.update(id, data)
await luckdb.fields.delete(id)
```

### 记录管理

```typescript
// 记录操作
const record = await luckdb.records.create(tableId, { fields: { name: 'Value' } })
const records = await luckdb.records.getList(tableId, page, perPage, filter)
const record = await luckdb.records.getOne(id)
await luckdb.records.update(id, { fields: { name: 'New Value' } })
await luckdb.records.delete(id)

// 批量操作
const batchResult = await luckdb.records.batchCreate(tableId, {
  records: [{ fields: { name: 'Item 1' } }, { fields: { name: 'Item 2' } }]
})

// 搜索
const results = await luckdb.records.search(tableId, 'search term', ['field1', 'field2'])

// 导入导出
const csvData = await luckdb.records.export(tableId, 'csv')
await luckdb.records.import(tableId, file)
```

### 视图管理

```typescript
// 视图操作
const view = await luckdb.views.create(tableId, {
  name: 'My View',
  type: 'grid',
  config: { fields: ['field1', 'field2'] }
})
const views = await luckdb.views.getList(tableId, page, perPage, filter)
const view = await luckdb.views.getOne(id)
await luckdb.views.update(id, data)
await luckdb.views.delete(id)

// 视图数据
const data = await luckdb.views.getData(viewId, page, perPage)
const allData = await luckdb.views.getAllData(viewId)

// 分享
const shareResult = await luckdb.views.share(viewId, { isShared: true })
const sharedView = await luckdb.views.getByShareId(shareId)
```

### ShareDB 实时协作（API 摘要）

```typescript
// 连接管理
await luckdb.sharedb.connect()
luckdb.sharedb.disconnect()
const isConnected = luckdb.sharedb.isConnected

// 文档操作
const doc = luckdb.sharedb.getDocument(collection, id)
await doc.subscribe()
await doc.unsubscribe()
doc.submitOp([{ p: ['field'], oi: 'value' }])
const data = doc.data
const version = doc.version
doc.destroy()
```

### 用户和协作者管理

```typescript
// 用户操作
const user = await luckdb.users.create({ email: 'user@example.com', name: 'User' })
const users = await luckdb.users.getList(page, perPage, filter)
const user = await luckdb.users.getOne(id)
await luckdb.users.update(id, data)
await luckdb.users.delete(id)

// 协作者操作
const collaborators = await luckdb.collaborators.getList('space', spaceId, page, perPage)
await luckdb.collaborators.add('space', spaceId, { userId: 'user-id', role: 'editor' })
await luckdb.collaborators.update('space', spaceId, collaboratorId, { role: 'viewer' })
await luckdb.collaborators.remove('space', spaceId, collaboratorId)

// 邀请
const invite = await luckdb.collaborators.invite('space', spaceId, {
  email: 'user@example.com',
  role: 'editor'
})
await luckdb.collaborators.acceptInvite({ token: 'invite-token' })
```

### 组织管理

```typescript
// 组织操作
const organization = await luckdb.organizations.create({
  name: 'My Organization',
  description: 'Organization description'
})
const organizations = await luckdb.organizations.getList(page, perPage, filter)
const organization = await luckdb.organizations.getOne(id)
await luckdb.organizations.update(id, data)
await luckdb.organizations.delete(id)

// 成员管理
const members = await luckdb.organizations.getMembers(orgId, page, perPage)
await luckdb.organizations.addMember(orgId, userId, 'admin')
await luckdb.organizations.removeMember(orgId, userId)
await luckdb.organizations.updateMemberRole(orgId, userId, 'member')

// 设置管理
const settings = await luckdb.organizations.getSettings(orgId)
await luckdb.organizations.updateSettings(orgId, { theme: 'dark' })
```

### 工作流管理

```typescript
// 工作流操作
const workflow = await luckdb.workflows.create({
  name: 'Auto Notify',
  tableId: 'table-id',
  trigger: {
    type: 'record_created',
    conditions: []
  },
  actions: [
    {
      type: 'send_notification',
      config: { message: 'New record created' }
    }
  ]
})
const workflows = await luckdb.workflows.getList(page, perPage, filter)
const workflow = await luckdb.workflows.getOne(id)
await luckdb.workflows.update(id, data)
await luckdb.workflows.delete(id)

// 工作流控制
await luckdb.workflows.activate(id)
await luckdb.workflows.deactivate(id)
const execution = await luckdb.workflows.trigger(id, recordId)

// 执行历史
const executions = await luckdb.workflows.getExecutions(id, page, perPage)
const execution = await luckdb.workflows.getExecution(id, executionId)
await luckdb.workflows.retryExecution(id, executionId)
```

### 记录历史管理

```typescript
// 历史记录查询
const history = await luckdb.recordHistory.getList(page, perPage, filter)
const recordHistory = await luckdb.recordHistory.getByRecord(recordId, page, perPage)
const tableHistory = await luckdb.recordHistory.getByTable(tableId, page, perPage)
const fieldHistory = await luckdb.recordHistory.getByField(tableId, fieldId, page, perPage)

// 历史操作
await luckdb.recordHistory.restoreToVersion(recordId, historyId)
const comparison = await luckdb.recordHistory.compareVersions(historyId1, historyId2)

// 统计信息
const stats = await luckdb.recordHistory.getStats(filter)
const recordStats = await luckdb.recordHistory.getRecordStats(recordId)
const tableStats = await luckdb.recordHistory.getTableStats(tableId)

// 数据管理
const exportData = await luckdb.recordHistory.export('csv', filter)
await luckdb.recordHistory.cleanup('2023-01-01', filter)
```

### 通知管理

```typescript
// 通知操作
const notification = await luckdb.notifications.create({
  title: 'System Alert',
  message: 'Database maintenance scheduled',
  type: 'warning',
  category: 'system'
})
const notifications = await luckdb.notifications.getList(page, perPage, filter)
const notification = await luckdb.notifications.getOne(id)
await luckdb.notifications.update(id, data)
await luckdb.notifications.delete(id)

// 通知状态
await luckdb.notifications.markAsRead(id)
await luckdb.notifications.markAsUnread(id)
await luckdb.notifications.markAllAsRead(filter)
await luckdb.notifications.deleteAll(filter)

// 统计和设置
const unreadCount = await luckdb.notifications.getUnreadCount()
const stats = await luckdb.notifications.getStats()
const preferences = await luckdb.notifications.getPreferences()
await luckdb.notifications.updatePreferences({
  email: true,
  push: false,
  inApp: true,
  categories: { system: true, record: false }
})

// 模板和测试
const templates = await luckdb.notifications.getTemplates()
await luckdb.notifications.sendWithTemplate(templateId, variables, userId)
await luckdb.notifications.sendTest('email')
```

## 认证存储

### LocalAuthStore（默认）

```typescript
import { LocalAuthStore } from 'luckdb-sdk'

const authStore = new LocalAuthStore()
const luckdb = new LuckDBClient('http://localhost:8080', authStore)
```

### AsyncAuthStore

```typescript
import { AsyncAuthStore } from 'luckdb-sdk'

const authStore = new AsyncAuthStore({
  save: async (token, record) => {
    // 自定义保存逻辑
    await myStorage.save('auth', { token, record })
  },
  load: async () => {
    // 自定义加载逻辑
    return await myStorage.load('auth')
  },
  clear: async () => {
    // 自定义清除逻辑
    await myStorage.remove('auth')
  }
})

const luckdb = new LuckDBClient('http://localhost:8080', authStore)
```

## 错误处理

```typescript
import { ClientResponseError } from 'luckdb-sdk'

try {
  await luckdb.auth.login('invalid@email.com', 'wrong-password')
} catch (error) {
  if (error instanceof ClientResponseError) {
    console.log('Status:', error.status)
    console.log('Code:', error.code)
    console.log('Message:', error.message)
    console.log('Details:', error.details)
    
    // 错误类型检查
    if (error.isAuthError) {
      console.log('Authentication error')
    }
    
    if (error.isPermissionError) {
      console.log('Permission denied')
    }
    
    if (error.isValidationError) {
      console.log('Validation failed:', error.details)
    }
    
    if (error.isFieldError) {
      console.log('Field validation error')
    }
    
    if (error.isNetworkError) {
      console.log('Network error occurred')
    }
    
    if (error.isTimeoutError) {
      console.log('Request timeout')
    }
    
    if (error.isConflictError) {
      console.log('Resource conflict')
    }
  }
}
```

## 请求钩子

```typescript
// 请求前钩子
luckdb.beforeSend = async (url, options) => {
  console.log('Sending request to:', url)
  return options
}

// 响应后钩子
luckdb.afterSend = async (response, data) => {
  console.log('Response received:', response.status)
  return data
}
```

## 构建配置

SDK 支持多种构建格式：

- **ES Module**: `dist/luckdb.es.mjs`
- **CommonJS**: `dist/luckdb.cjs.js`
- **UMD**: `dist/luckdb.umd.js`
- **IIFE**: `dist/luckdb.iife.js`

### Rollup 配置示例

```javascript
// rollup.config.mjs
export default [
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/luckdb.es.mjs',
      format: 'es'
    },
    external: ['sharedb', 'reconnecting-websocket']
  },
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/luckdb.cjs.js',
      format: 'cjs'
    },
    external: ['sharedb', 'reconnecting-websocket']
  }
]
```

## 开发

```bash
# 安装依赖
npm install

# 开发模式
npm run dev

# 构建
npm run build

# 测试
npm test

# 格式化代码
npm run format

# 类型检查
npm run type-check
```

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 更新日志

### 2.0.0

- 🎉 **重大更新** - 完全重构以匹配服务端 API
- 🔄 **字段类型统一** - 更新字段类型命名以匹配服务端（select → singleSelect）
- 🔒 **乐观锁支持** - 记录更新支持版本控制和冲突解决
- 🏢 **新增服务** - 添加组织、工作流、历史记录和通知管理服务
- 🎨 **高级字段配置** - 支持 Teable 风格的字段选项和格式化
- 📊 **批量创建支持** - 表格创建时支持批量创建字段和视图
- 🛠️ **完善错误处理** - 增强错误类型定义和错误检查方法
- 🧪 **测试覆盖** - 添加 ShareDB 和新服务的单元测试
- 📚 **文档更新** - 更新 README 以反映最新的 API 变更

### 1.0.0

- 初始版本发布
- 完整的 API 支持
- ShareDB 实时协作集成
- TypeScript 类型定义
- 多种构建格式支持
