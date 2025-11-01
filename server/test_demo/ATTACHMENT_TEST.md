# 附件上传功能测试

## 测试文件

- `attachment_main.go` - 主测试程序
- `attachment_helpers.go` - 附件 API 辅助函数
- `test_helpers.go` - HTTP API 辅助函数（已存在）
- `config.go` - 配置管理（已存在）

## 测试功能

### 1. 生成上传签名
- 端点: `POST /api/v1/attachments/signature`
- 功能: 为文件上传生成签名令牌

### 2. 上传文件
- 端点: `POST /api/v1/attachments/upload/:token`
- 功能: 使用令牌上传文件

### 3. 通知上传完成
- 端点: `POST /api/v1/attachments/notify/:token`
- 功能: 通知服务器上传完成，创建附件记录

### 4. 获取附件信息
- 端点: `GET /api/v1/attachments/:id`
- 功能: 获取单个附件信息

### 5. 列出附件
- 端点: `GET /api/v1/attachments?table_id=xxx&field_id=xxx&record_id=xxx`
- 功能: 列出符合条件的附件

### 6. 获取附件统计
- 端点: `GET /api/v1/tables/:tableId/attachments/stats`
- 功能: 获取表格的附件统计信息

### 7. 读取文件
- 端点: `GET /api/v1/attachments/read/:path`
- 功能: 读取文件内容

### 8. 删除附件
- 端点: `DELETE /api/v1/attachments/:id`
- 功能: 删除附件

## 运行测试

### 前置条件

1. **服务器必须运行**：确保服务器正在运行，默认地址为 `http://localhost:8080`
2. **附件服务已集成**：确保服务器已重新编译并启动，包含新的附件路由

### 运行步骤

```bash
cd server/test_demo
go run attachment_main.go attachment_helpers.go test_helpers.go config.go
```

### 环境变量配置（可选）

```bash
export SERVER_URL=http://localhost:8080
export TEST_EMAIL=test_demo@example.com
export TEST_PASSWORD=Test123456!
export TEST_NAME=测试用户

go run attachment_main.go attachment_helpers.go test_helpers.go config.go
```

## 测试流程

测试程序会依次执行以下步骤：

1. **注册/登录** - 创建或登录测试账号
2. **创建资源链** - 创建 Space → Base → Table → Field（附件类型）→ Record
3. **生成上传签名** - 为附件上传生成签名令牌
4. **创建测试文件** - 在临时目录创建测试文件
5. **上传文件** - 使用令牌上传文件到服务器
6. **通知上传完成** - 通知服务器上传完成，创建附件记录
7. **获取附件信息** - 验证附件信息是否正确
8. **列出附件** - 验证附件列表功能
9. **获取附件统计** - 验证附件统计功能
10. **读取文件** - 验证文件读取功能
11. **删除附件** - 验证附件删除功能
12. **验证删除** - 确认附件已删除

## 预期结果

所有步骤都应该成功执行，测试结束时应该看到：

```
✅ 所有附件功能测试通过！

📋 测试覆盖的功能:
   ✅ 生成上传签名
   ✅ 上传文件
   ✅ 通知上传完成
   ✅ 获取附件信息
   ✅ 列出附件
   ✅ 获取附件统计
   ✅ 读取文件
   ✅ 删除附件

🎉 附件上传功能测试完成！
```

## 注意事项

1. **服务器必须重启**：如果服务器在添加附件路由之前就已经运行，需要重启服务器以加载新的路由
2. **文件存储路径**：确保服务器配置的 `storage.local.upload_path` 目录存在且有写权限
3. **数据库表**：确保数据库中存在 `attachments` 和 `upload_tokens` 表（通过迁移创建）

## 故障排查

### 404 错误（路由未找到）

如果测试时遇到 404 错误，说明服务器没有加载新的附件路由。解决方法：

1. 重启服务器：
   ```bash
   cd server
   go run cmd/mcp-server/main.go serve --config config.yaml
   ```

2. 检查路由是否正确注册：
   ```bash
   curl http://localhost:8080/api/v1/attachments/signature
   ```

### 500 错误（服务器内部错误）

检查服务器日志，可能是：
- 附件服务初始化失败
- 数据库连接问题
- 文件存储路径权限问题

### 文件上传失败

检查：
- 文件大小是否超过限制（默认 100MB）
- 文件类型是否在允许列表中
- 上传令牌是否过期


