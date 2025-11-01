# 字段类型支持情况总结

## 当前已支持的字段类型

### 1. **short-text** (单行文本)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `ShortTextCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 可编辑的单行文本输入框，支持内容编辑

### 2. **long-text** (长文本)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `LongTextCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 多行文本编辑器，使用 Popover + Textarea

### 3. **number** (数字)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `NumberCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 数字输入框，支持 min/max/step 配置

### 4. **select** (单选)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `SelectCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 下拉选择框，需要配置 options

### 5. **multi-select** (多选)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `MultiSelectCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 多选下拉框，使用 Command 组件，支持搜索和标签显示

### 6. **checkbox** (复选框)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `CheckboxCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 布尔值复选框，点击切换 true/false

### 7. **date** (日期)
- **实现位置**: `src/components/data-grid/data-grid-cell-variants.tsx` - `DateCell`
- **类型定义**: `src/types/data-grid.ts`
- **功能**: 日期选择器，使用 Calendar 组件

## AddColumnMenu 中定义但未支持的字段类型

### 1. **link** (链接)
- **状态**: ❌ 未实现
- **建议实现**: URL 输入框，支持链接验证和点击跳转

### 2. **email** (邮箱)
- **状态**: ❌ 未实现
- **建议实现**: 邮箱输入框，支持邮箱格式验证和邮件链接

### 3. **phone** (电话)
- **状态**: ❌ 未实现
- **建议实现**: 电话号码输入框，支持电话链接（tel:）

### 4. **rating** (评分)
- **状态**: ❌ 未实现
- **建议实现**: 星级评分组件，支持 1-5 星选择

### 5. **user** (成员)
- **状态**: ❌ 未实现
- **建议实现**: 用户选择器，可能需要用户列表和头像显示

### 6. **attachment** (附件)
- **状态**: ❌ 未实现
- **建议实现**: 文件上传组件，支持文件预览和下载

## 扩展新字段类型的步骤

### 步骤 1: 更新类型定义

在 `src/types/data-grid.ts` 中添加新的 Cell 类型：

```typescript
export type Cell =
  | // ... 现有类型
  | {
      variant: "link";
    }
  | {
      variant: "email";
    }
  | {
      variant: "phone";
    }
  | {
      variant: "rating";
      max?: number; // 默认 5
    }
  | {
      variant: "user";
      options?: Array<{ id: string; name: string; avatar?: string }>;
    }
  | {
      variant: "attachment";
    };
```

### 步骤 2: 创建 Cell 组件

在 `src/components/data-grid/data-grid-cell-variants.tsx` 中添加新的 Cell 组件：

```typescript
export function LinkCell<TData>({...}: CellVariantProps<TData>) {
  // 实现链接单元格
}

export function EmailCell<TData>({...}: CellVariantProps<TData>) {
  // 实现邮箱单元格
}

// ... 其他类型
```

### 步骤 3: 在 DataGridCell 中注册

在 `src/components/data-grid/data-grid-cell.tsx` 的 switch 语句中添加新的 case：

```typescript
case "link":
  return <LinkCell ... />;
case "email":
  return <EmailCell ... />;
// ... 其他类型
```

### 步骤 4: 更新列头图标映射

在 `src/components/data-grid/data-grid-column-header.tsx` 的 `getColumnVariant` 函数中添加图标映射：

```typescript
case "link":
  return { icon: LinkIcon, label: "Link" };
case "email":
  return { icon: MailIcon, label: "Email" };
// ... 其他类型
```

### 步骤 5: 更新 AddColumnMenu（可选）

确保 `src/components/data-grid/add-column-menu.tsx` 中的字段类型 ID 与实际的 variant 匹配。目前已经匹配，无需修改。

## 实现建议

### Link 类型
- 使用 `ShortTextCell` 作为基础
- 添加 URL 验证（可选）
- 显示时添加链接图标和点击跳转功能

### Email 类型
- 使用 `ShortTextCell` 作为基础
- 添加邮箱格式验证
- 显示时添加邮件图标和 mailto: 链接

### Phone 类型
- 使用 `ShortTextCell` 作为基础
- 添加电话号码格式验证（可选）
- 显示时添加电话图标和 tel: 链接

### Rating 类型
- 创建新的 RatingCell 组件
- 使用 Star 图标或自定义评分组件
- 支持 1-5 星或自定义最大值

### User 类型
- 创建新的 UserCell 组件
- 使用 Select 或 Command 组件选择用户
- 显示用户头像和名称

### Attachment 类型
- 创建新的 AttachmentCell 组件
- 使用文件上传组件
- 显示文件列表和预览功能

## 参考实现

可以参考现有的 `ShortTextCell` 或 `SelectCell` 的实现模式，它们都遵循相同的接口和生命周期管理。

