# SDK 重构对齐总结

## 概述

本次重构完全对齐了 `luckdb-sdk` 与服务端 API 的设计与实现，确保 SDK 能够正确调用服务端的所有功能。

## 主要变更

### 1. 类型定义更新

#### 分页响应格式
- **服务端格式**: `{ list: T[], pagination: { page, limit, total, total_pages } }`
- **SDK 格式**: `{ items: T[], pagination: PaginationResponse }`
- **兼容性**: SDK 自动转换服务端格式为标准格式

#### 记录相关类型
- `RecordResponse`: 添加 `data` 字段（服务端使用），保留 `fields` 字段（兼容）
- `RecordCreateRequest`: 支持 `data` 和 `fields` 两种格式
- `RecordUpdateRequest`: 支持服务端格式（`data`, `version`）和 Teable 格式（`record.fields`）

#### 批量操作响应
- `BatchCreateRecordResponse`: `{ records, successCount, failedCount, errors }`
- `BatchUpdateRecordResponse`: `{ records, successCount, failedCount, errors }`
- `BatchDeleteRecordResponse`: `{ successCount, failedCount, errors }`

### 2. 服务重构

#### AuthService
- ✅ 修复 `refreshToken`: 使用 `refresh_token` 字段名（snake_case）
- ✅ 修复 `getCurrentUser`: 正确处理服务端返回的 `TokenClaims` 格式

#### RecordService
- ✅ 修复 `create`: 请求格式 `{ tableId, data: { ... } }`
- ✅ 修复 `getList`: 正确处理服务端分页响应格式 `{ list, pagination }`
- ✅ 修复 `getOne`: 支持新 API（需要 `tableId`）和旧 API（仅 `recordId`）兼容
- ✅ 修复 `update`: 支持服务端格式（`data`, `version`）和 Teable 格式
- ✅ 修复批量操作: 使用服务端标准响应格式

#### SpaceService
- ✅ 修复 `getList`: 正确处理服务端响应格式 `{ spaces, total, limit, offset }`
- ✅ 添加兼容字段: `ownerId` 映射自 `createdBy`
- ✅ 修复分页格式转换

#### BaseService
- ✅ 修复 `getList`: 正确处理服务端响应格式
- ✅ 添加兼容字段: `ownerId` 映射自 `createdBy`
- ✅ 修复 `duplicate`: 支持可选的 `name` 参数

#### TableService
- ✅ 修复 `create`: 确保 `baseId` 在请求体中
- ✅ 修复 `getList`: 处理服务端响应格式（数组或对象）

#### FieldService
- ✅ 修复 `create`: 确保 `tableId` 在请求体中
- ✅ 修复 `getList`: 处理服务端响应格式（数组或对象）

#### ViewService
- ✅ 修复 `getList`: 正确处理服务端数组响应
- ✅ 修复配置更新方法: 使用正确的请求体格式

### 3. API 路由对齐

#### 记录操作
- ✅ `POST /api/v1/tables/:tableId/records` - 创建记录
- ✅ `GET /api/v1/tables/:tableId/records` - 获取记录列表（支持分页）
- ✅ `GET /api/v1/tables/:tableId/records/:recordId` - 获取单个记录（新API）
- ✅ `GET /api/v1/records/:recordId` - 获取单个记录（旧API，已废弃但保留兼容）
- ✅ `PATCH /api/v1/tables/:tableId/records/:recordId` - 更新记录
- ✅ `DELETE /api/v1/tables/:tableId/records/:recordId` - 删除记录

#### 批量操作
- ✅ `POST /api/v1/tables/:tableId/records/batch` - 批量创建
- ✅ `PATCH /api/v1/tables/:tableId/records/batch` - 批量更新
- ✅ `DELETE /api/v1/tables/:tableId/records/batch` - 批量删除

#### 视图操作
- ✅ `POST /api/v1/tables/:tableId/views` - 创建视图
- ✅ `GET /api/v1/tables/:tableId/views` - 获取视图列表
- ✅ `PATCH /api/v1/views/:viewId/filter` - 更新过滤器
- ✅ `PATCH /api/v1/views/:viewId/sort` - 更新排序
- ✅ `PATCH /api/v1/views/:viewId/group` - 更新分组
- ✅ `PATCH /api/v1/views/:viewId/column-meta` - 更新列配置
- ✅ `PATCH /api/v1/views/:viewId/options` - 更新选项
- ✅ `PATCH /api/v1/views/:viewId/order` - 更新排序位置
- ✅ `PATCH /api/v1/views/:viewId/share-meta` - 更新分享元数据

### 4. 响应格式处理

#### 标准响应格式
服务端统一使用：
```json
{
  "code": 200,
  "message": "操作成功",
  "data": { ... },
  "timestamp": "...",
  "request_id": "..."
}
```

SDK 的 `Client.parseResponse` 自动解包 `data` 字段，各服务直接使用解包后的数据。

#### 分页响应格式
记录列表使用：
```json
{
  "code": 200,
  "message": "获取记录列表成功",
  "data": {
    "list": [...],
    "pagination": {
      "page": 1,
      "limit": 20,
      "total": 100,
      "total_pages": 5
    }
  }
}
```

SDK 自动转换为：
```typescript
{
  items: [...],
  pagination: {
    total: 100,
    page: 1,
    pageSize: 20,
    totalPages: 5,
    hasNext: true,
    hasPrevious: false
  }
}
```

### 5. 兼容性处理

#### 字段映射
- `RecordResponse.fields` ← `RecordResponse.data`（自动映射）
- `SpaceResponse.ownerId` ← `SpaceResponse.createdBy`（自动映射）
- `BaseResponse.ownerId` ← `BaseResponse.createdBy`（自动映射）

#### API 兼容
- `RecordService.getOne`: 支持新旧两种调用方式
  - 新: `getOne(tableId, recordId)` - 推荐使用
  - 旧: `getOne(recordId)` - 已废弃，但保留兼容

#### 未实现端点
部分端点服务端可能未实现，SDK 会：
- 尝试调用
- 如果返回 404，抛出明确的错误信息
- 例如: `ViewService.getData`, `ViewService.getStats`, `RecordService.export`

## 验证清单

✅ **类型定义对齐**
- [x] PaginationResponse 格式
- [x] RecordResponse 格式
- [x] BatchOperationResponse 格式
- [x] SpaceResponse 格式
- [x] BaseResponse 格式

✅ **服务实现对齐**
- [x] AuthService
- [x] RecordService
- [x] SpaceService
- [x] BaseService
- [x] TableService
- [x] FieldService
- [x] ViewService

✅ **API 路由对齐**
- [x] 记录 CRUD 操作
- [x] 批量操作
- [x] 视图操作
- [x] 协作者管理

✅ **响应格式处理**
- [x] 标准响应解包
- [x] 分页响应转换
- [x] 错误处理

## 使用示例

### 创建记录
```typescript
const record = await client.records.create(tableId, {
  data: {
    [fieldId]: 'value'
  }
})
// 或使用兼容格式
const record = await client.records.create(tableId, {
  fields: {
    [fieldId]: 'value'
  }
})
```

### 更新记录（支持乐观锁）
```typescript
const updated = await client.records.update(tableId, recordId, {
  data: {
    [fieldId]: 'new value'
  },
  version: record.version // 乐观锁版本号
})
```

### 获取记录列表
```typescript
const response = await client.records.getList(tableId, 1, 20)
console.log(response.items) // 记录列表
console.log(response.pagination) // 分页信息
```

## 注意事项

1. **记录操作需要 tableId**: 所有记录操作（除了旧版 `getOne`）都需要提供 `tableId`
2. **分页参数**: 使用 `perPage` 而不是 `pageSize`
3. **响应格式**: SDK 会自动处理服务端的响应格式，但建议使用 SDK 提供的类型定义
4. **兼容字段**: SDK 会自动添加兼容字段（如 `fields` 映射自 `data`），但建议使用服务端标准字段

## 后续工作

1. 添加单元测试验证所有 API 调用
2. 添加集成测试验证端到端流程
3. 更新文档和示例代码
4. 考虑移除已废弃的 API（如旧版 `getOne`）

