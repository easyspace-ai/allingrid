# Dice UI Data Grid 完整演示项目

这是一个基于 [Dice UI Data Grid](https://www.diceui.com/docs/components/data-grid) 组件的完整演示项目,展示了类似 Airtable 的可编辑数据表格功能。

## ✨ 功能特性

- ✅ **可编辑单元格** - 点击即可编辑,支持多种数据类型
- ✅ **列宽调整** - 拖动列边界调整宽度
- ✅ **添加/删除行** - 动态管理数据行
- ✅ **虚拟化滚动** - 高性能处理大数据量
- ✅ **键盘导航** - 完整的键盘快捷键支持
- ✅ **搜索功能** - 快速查找数据
- ✅ **多列排序** - 灵活的排序选项
- ✅ **右键菜单** - 便捷的操作菜单
- ✅ **多种单元格类型** - 7 种不同的输入类型

## 🚀 快速开始

### 安装依赖

```bash
pnpm install
```

### 启动开发服务器

```bash
pnpm dev
```

访问 [http://localhost:5173](http://localhost:5173) 查看演示。

## 📋 演示内容

### 1. 滑板技巧演示 (完整功能)
- **数据结构**: 技巧名称、滑手、难度、类别、是否成功、尝试次数、得分、地点、日期
- **单元格类型**: 展示所有 7 种单元格类型
- **功能**: 完整工具栏、搜索、排序、键盘快捷键、右键菜单

### 2. 基础演示 (快速开始)
- **数据结构**: 姓名、邮箱、年龄、状态
- **目的**: 展示最小可用示例,便于快速理解

### 3. 产品管理演示 (实际应用)
- **数据结构**: 产品名称、SKU、价格、库存、类别、状态、最后更新
- **功能**: 批量编辑、库存管理、价格调整

### 4. 用户管理演示 (企业场景)
- **数据结构**: 姓名、邮箱、角色、部门、入职日期、状态、备注
- **功能**: 角色管理、批量操作、多选编辑

## 🎯 单元格类型

Data Grid 支持 7 种不同的单元格类型:

1. **ShortTextCell** - 单行文本输入
2. **LongTextCell** - 多行文本(弹出框)
3. **NumberCell** - 数字输入(支持 min/max/step)
4. **SelectCell** - 单选下拉
5. **MultiSelectCell** - 多选(徽章显示)
6. **CheckboxCell** - 布尔值复选框
7. **DateCell** - 日期选择器

## ⌨️ 键盘快捷键

### 导航
- `↑↓←→` - 单元格导航
- `Tab` - 下一个单元格
- `Shift+Tab` - 上一个单元格
- `Home` - 移动到第一列
- `End` - 移动到最后一列
- `Ctrl+Home` - 移动到第一个单元格
- `Ctrl+End` - 移动到最后一个单元格
- `PgUp/PgDn` - 翻页

### 选择
- `Shift+箭头` - 扩展选择
- `Ctrl+A` - 全选
- `Ctrl+Click` - 切换选择
- `Shift+Click` - 范围选择

### 编辑
- `Enter` - 开始编辑单元格
- `Escape` - 退出编辑模式
- `Delete/Backspace` - 清除选中内容

### 功能
- `Ctrl+F` - 打开搜索
- `Ctrl+Shift+S` - 打开排序菜单
- `Ctrl+/` - 显示快捷键帮助

## 🛠️ 技术栈

- **React 18** - 前端框架
- **TypeScript** - 类型安全
- **Vite** - 构建工具
- **Tailwind CSS** - 样式框架
- **shadcn/ui** - UI 组件库
- **TanStack Table** - 表格状态管理
- **TanStack Virtual** - 虚拟化滚动
- **Faker.js** - 测试数据生成

## 📁 项目结构

```
src/
├── components/
│   ├── ui/                    # shadcn/ui 基础组件
│   ├── data-grid/             # Data Grid 核心组件
│   └── demos/                 # 演示示例
│       ├── skate-trick-demo.tsx
│       ├── basic-demo.tsx
│       ├── product-demo.tsx
│       └── user-management-demo.tsx
├── hooks/                     # Data Grid Hooks
├── lib/                       # 工具函数
└── types/                     # TypeScript 类型定义
```

## 🔧 自定义配置

### 列定义示例

```typescript
const columns: ColumnDef<DataType>[] = [
  {
    id: "fieldName",
    accessorKey: "fieldName",
    header: "显示名称",
    meta: {
      cell: {
        variant: "short-text" | "long-text" | "number" | "select" | "multi-select" | "checkbox" | "date",
        options: [...],  // 用于 select/multi-select
        min: 0,          // 用于 number
        max: 100,        // 用于 number
      },
    },
    minSize: 150,
  },
];
```

### useDataGrid Hook 使用

```typescript
const { table, dataGridRef, ... } = useDataGrid({
  data,
  columns,
  onDataChange: (newData) => setData(newData),
  onRowAdd: () => { /* 添加新行 */ },
  onRowsDelete: (rows) => { /* 删除行 */ },
  enableSearch: true,
  autoFocus: true,
});
```

## 📚 相关链接

- [Dice UI Data Grid 官方文档](https://www.diceui.com/docs/components/data-grid)
- [TanStack Table 文档](https://tanstack.com/table/latest)
- [shadcn/ui 组件库](https://ui.shadcn.com/)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这个演示项目。

## 📄 许可证

MIT License