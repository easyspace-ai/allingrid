# Application Layer DTO

## 设计原则

按照 `.cursor/rules/application-layer.mdx` 规范：

1. **DTO职责**
   - 数据传输对象，用于API请求和响应
   - 不包含业务逻辑
   - 与Domain实体分离

2. **命名规范**
   - Request：`Create*Request`, `Update*Request`, `Delete*Request`
   - Response：`*Response`, `*DetailResponse`, `*ListResponse`
   - Filter：`List*Filter`, `*SearchFilter`

3. **包结构**
   ```
   application/dto/
   ├── record_dto.go      # 记录相关DTO
   ├── user_dto.go        # 用户相关DTO
   ├── space_dto.go       # 空间相关DTO
   ├── table_dto.go       # 表相关DTO
   ├── field_dto.go       # 字段相关DTO
   └── common_dto.go      # 通用DTO
   ```

4. **转换器**
   - `ToEntity()` - DTO转Domain实体
   - `FromEntity()` - Domain实体转DTO
   - 转换器放在对应的Service中

## 与Domain层的关系

- Application层DTO ≠ Domain层Entity
- Application Service负责DTO与Entity之间的转换
- Domain层保持纯粹，不依赖Application层

## 示例

```go
// DTO定义
type CreateRecordRequest struct {
    TableID string                 `json:"tableId"`  // ✅ 统一使用 camelCase
    Data    map[string]interface{} `json:"data"`
}

// Service中转换
func (s *RecordService) CreateRecord(ctx context.Context, req CreateRecordRequest, userID string) (*RecordResponse, error) {
    // 1. DTO验证
    if err := req.Validate(); err != nil {
        return nil, err
    }
    
    // 2. 转换为Domain实体
    record := &recordEntity.Record{
        TableID: req.TableID,
        Data: req.Data,
    }
    
    // 3. 调用Domain层
    createdRecord, err := s.recordRepo.Save(ctx, record)
    
    // 4. 转换为Response DTO
    return FromRecordEntity(createdRecord), nil
}
```

