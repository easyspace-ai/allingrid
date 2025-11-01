# 优化重构实施清单

## 📋 快速开始

### 优先级说明
- 🔴 **P0 (紧急)**: 影响系统稳定性和安全性，必须立即修复
- 🟡 **P1 (高)**: 影响性能和用户体验，1-2周内修复
- 🟢 **P2 (中)**: 代码质量和可维护性，1个月内修复
- 🔵 **P3 (低)**: 优化和增强，有时间时修复

---

## 🔴 P0 - 紧急修复项

### 1. 服务初始化顺序问题 ⚠️
**文件**: `internal/container/container.go`
**问题**: 
- `initInfrastructureServices()` 被调用两次（第228行和第298行）
- `ViewService` 被初始化两次（第259行和第301行）

**修复步骤**:
- [ ] 移除第228行的 `initInfrastructureServices()` 调用
- [ ] 移除第259行的 `ViewService` 初始化（传nil的那个）
- [ ] 确保 `businessEventManager` 在 `ViewService` 初始化之前创建
- [ ] 测试服务启动流程

**预计时间**: 2小时

---

### 2. 权限检查不完整 🔒
**文件**: 
- `internal/application/base_service.go` (4个TODO)
- `internal/application/permission_service.go` (Field/View权限返回false)
- `config.yaml` (权限检查被禁用)

**修复步骤**:
- [ ] 实现 `CanReadField` 方法
- [ ] 实现 `CanReadView` 方法
- [ ] 在 `BaseService` 中集成权限检查
- [ ] 在 `TableService` 中集成权限检查
- [ ] 在 `RecordService` 中集成权限检查
- [ ] 移除配置中的 `permissions_disabled: true`
- [ ] 添加权限检查单元测试

**预计时间**: 1天

---

### 3. 缺少关键依赖 🔧
**文件**: `internal/container/container.go:261-268`

**问题**: `FieldService` 初始化时传入了多个 `nil` 参数

**修复步骤**:
- [ ] 创建 `DependencyGraphRepository` 接口和实现
- [ ] 创建基于事件总线的 `FieldBroadcaster` 实现
- [ ] 移除 `FieldService` 初始化中的 `nil` 参数
- [ ] 添加依赖注入测试

**预计时间**: 1天

---

## 🟡 P1 - 高优先级优化项

### 4. N+1 查询问题 🚀
**文件**: `internal/application/record_service.go`

**问题**: 循环查询导致性能问题

**修复步骤**:
- [ ] 分析所有存在N+1查询的地方
- [ ] 实现批量预加载方法
- [ ] 使用 GORM 的 `Preload` 功能
- [ ] 添加查询性能测试
- [ ] 监控查询性能指标

**预计时间**: 2天

---

### 5. 缺少查询缓存 💾
**文件**: 多个 Repository 文件

**修复步骤**:
- [ ] 创建 `CachedRepository` 包装器
- [ ] 实现缓存键生成策略
- [ ] 实现缓存失效策略
- [ ] 在 Container 中集成缓存层
- [ ] 添加缓存命中率监控

**预计时间**: 2天

---

### 6. 计算引擎性能优化 ⚡
**文件**: `internal/application/calculation_service.go`

**问题**: 
- 每次更新都重新构建依赖图
- 缺少批量计算优化

**修复步骤**:
- [ ] 实现依赖图缓存
- [ ] 实现批量计算方法
- [ ] 添加异步计算队列
- [ ] 优化依赖传播算法
- [ ] 添加性能基准测试

**预计时间**: 3天

---

### 7. Base复制功能实现 📋
**文件**: `internal/interfaces/http/base_handler.go:156`

**修复步骤**:
- [ ] 实现 `DuplicateBase` 方法
- [ ] 实现表结构复制逻辑
- [ ] 实现字段复制逻辑
- [ ] 实现视图复制逻辑
- [ ] 实现记录复制逻辑（可选）
- [ ] 添加权限检查
- [ ] 添加单元测试

**预计时间**: 2天

---

### 8. 协作者管理功能实现 👥
**文件**: `internal/interfaces/http/base_handler.go:195-237`

**修复步骤**:
- [ ] 实现 `GetCollaborators` 接口
- [ ] 实现 `AddCollaborator` 接口
- [ ] 实现 `UpdateCollaborator` 接口
- [ ] 实现 `RemoveCollaborator` 接口
- [ ] 添加权限检查
- [ ] 添加单元测试

**预计时间**: 2天

---

## 🟢 P2 - 中优先级优化项

### 9. 字段依赖解析完善 🔗
**文件**: `internal/domain/fields/service/dependency_graph.go:120-147`

**修复步骤**:
- [ ] 实现 Formula 字段依赖提取
- [ ] 实现 Lookup 字段依赖提取
- [ ] 实现 Rollup 字段依赖提取
- [ ] 实现 Count 字段依赖提取
- [ ] 添加依赖解析测试
- [ ] 优化依赖图构建性能

**预计时间**: 3天

---

### 10. 字段验证器完善 ✅
**文件**: `internal/domain/fields/validation/validators.go:480,539`

**修复步骤**:
- [ ] 完善 SingleSelectValidator
- [ ] 完善 MultipleSelectValidator
- [ ] 完善 AttachmentValidator
- [ ] 完善 UserValidator
- [ ] 添加验证器测试
- [ ] 优化验证性能

**预计时间**: 2天

---

### 11. MCP工具实现 🔌
**文件**: 
- `internal/mcp/tools/record_tools.go`
- `internal/mcp/resources/table_resources.go`

**修复步骤**:
- [ ] 实现 `CreateRecordTool`
- [ ] 实现 `UpdateRecordTool`
- [ ] 实现 `DeleteRecordTool`
- [ ] 实现 `TableSchemaResource`
- [ ] 实现 `TableDataResource`
- [ ] 实现 `TableMetadataResource`
- [ ] 添加集成测试

**预计时间**: 3天

---

### 12. 测试覆盖率提升 📊
**目标**: 单元测试覆盖率 ≥ 80%

**修复步骤**:
- [ ] 分析当前测试覆盖率
- [ ] 为 Application 层添加测试
- [ ] 为 Domain 层添加测试
- [ ] 为 Infrastructure 层添加测试
- [ ] 为 Interfaces 层添加测试
- [ ] 设置 CI/CD 中的覆盖率检查
- [ ] 目标：覆盖率 ≥ 80%

**预计时间**: 1周

---

## 🔵 P3 - 低优先级优化项

### 13. S3/Minio存储支持 ☁️
**文件**: `internal/infrastructure/storage/factory.go:90,97`

**修复步骤**:
- [ ] 实现 S3 存储适配器
- [ ] 实现 Minio 存储适配器
- [ ] 添加存储配置选项
- [ ] 添加存储测试

**预计时间**: 2天

---

### 14. 结构化日志集成 📝
**文件**: `internal/mcp/protocol/handler.go:68,267`

**修复步骤**:
- [ ] 集成 zap logger 到 MCP handler
- [ ] 替换所有 `log.Printf` 调用
- [ ] 添加结构化日志字段
- [ ] 统一日志格式

**预计时间**: 1天

---

### 15. 监控和可观测性 📈
**修复步骤**:
- [ ] 集成 Prometheus 指标
- [ ] 添加 OpenTelemetry 链路追踪
- [ ] 增强健康检查端点
- [ ] 设置告警规则
- [ ] 添加性能监控面板

**预计时间**: 3天

---

### 16. API文档完善 📚
**修复步骤**:
- [ ] 集成 Swagger/OpenAPI
- [ ] 为所有API端点添加文档
- [ ] 添加请求/响应示例
- [ ] 生成交互式API文档

**预计时间**: 2天

---

## 📊 进度跟踪

### Week 1
- [ ] P0-1: 服务初始化顺序问题
- [ ] P0-2: 权限检查不完整
- [ ] P0-3: 缺少关键依赖

### Week 2-3
- [ ] P1-4: N+1 查询问题
- [ ] P1-5: 缺少查询缓存
- [ ] P1-6: 计算引擎性能优化

### Week 4-5
- [ ] P1-7: Base复制功能
- [ ] P1-8: 协作者管理功能
- [ ] P2-9: 字段依赖解析完善

### Week 6-7
- [ ] P2-10: 字段验证器完善
- [ ] P2-11: MCP工具实现
- [ ] P2-12: 测试覆盖率提升

### Week 8+
- [ ] P3-13: S3/Minio存储支持
- [ ] P3-14: 结构化日志集成
- [ ] P3-15: 监控和可观测性
- [ ] P3-16: API文档完善

---

## 🎯 完成标准

每个任务完成后，需要满足以下标准：

1. ✅ **代码审查通过**: 通过 golangci-lint 检查
2. ✅ **单元测试**: 新增代码有对应的单元测试
3. ✅ **集成测试**: 关键功能有集成测试
4. ✅ **文档更新**: 相关文档已更新
5. ✅ **性能验证**: 性能优化需要基准测试验证
6. ✅ **安全审查**: 安全相关修复需要安全审查

---

## 📝 注意事项

1. **不要一次性修改太多**: 每次只修改一个模块，确保可回滚
2. **保持向后兼容**: 除非必要，不要破坏现有API
3. **测试先行**: 先写测试，再实现功能
4. **代码审查**: 所有修改都需要代码审查
5. **文档更新**: 及时更新相关文档

---

## 🔗 相关文档

- [优化重构方案](./OPTIMIZATION_RECOMMENDATIONS.md)
- [源码清理报告](./SOURCE_CLEANUP_REPORT.md)
- [最终清理报告](./CLEANUP_REPORT_FINAL.md)

