# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2023-01-01

### Added

- 初始版本发布
- 完整的 LuckDB API 支持
- ShareDB 实时协作集成
- TypeScript 类型定义
- 多种构建格式支持（ES、CommonJS、UMD、IIFE）
- 认证存储系统（本地、异步）
- 自动重连和心跳检测
- 批量操作支持
- 完整的错误处理
- 请求钩子系统
- 单元测试覆盖

### Features

- **核心功能**
  - LuckDBClient 主客户端类
  - ClientResponseError 错误处理
  - BaseService 服务基类

- **认证系统**
  - AuthService 认证服务
  - LocalAuthStore 本地存储
  - AsyncAuthStore 异步存储
  - JWT 工具函数

- **CRUD 服务**
  - SpaceService 空间管理
  - LuckDBBaseService Base 管理
  - TableService 表格管理
  - FieldService 字段管理
  - RecordService 记录管理
  - ViewService 视图管理

- **用户管理**
  - UserService 用户管理
  - CollaboratorService 协作者管理

- **实时通信**
  - RealtimeService SSE 订阅
  - ShareDBService ShareDB 集成
  - ShareDBConnection 连接管理
  - ShareDBDoc 文档封装
  - ShareDBPresence 在线状态

- **工具函数**
  - Cookie 工具
  - FormData 工具
  - 请求选项工具

### Technical Details

- 使用 TypeScript 5.1.6
- 支持 Node.js 16+
- 使用 Rollup 构建
- 使用 Vitest 测试
- 使用 ESLint 和 Prettier 代码规范
