/**
 * DataGridFilterMenu - 筛选菜单组件
 * 
 * 功能：
 * - 直接在 Popover 中编辑筛选条件（一步操作）
 * - 管理筛选条件
 * - 自动应用筛选到数据
 */

"use client";

import * as React from "react";
import type { Table } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Calendar } from "@/components/ui/calendar";
import { Filter, X, Plus, Trash2, CircleCheck, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { format } from "date-fns";
import { zhCN } from "date-fns/locale";

export interface FilterCondition {
  id: string;
  fieldId: string;
  operator: FilterOperator;
  value: unknown;
}

export type FilterOperator =
  | "equals"
  | "not_equals"
  | "contains"
  | "not_contains"
  | "starts_with"
  | "ends_with"
  | "greater_than"
  | "less_than"
  | "greater_equal"
  | "less_equal"
  | "is_empty"
  | "is_not_empty"
  | "in"
  | "not_in";

interface DataGridFilterMenuProps<TData> {
  table: Table<TData>;
  conditions?: FilterCondition[];
  onConditionsChange?: (conditions: FilterCondition[]) => void;
}

const OPERATOR_LABELS: Record<FilterOperator, string> = {
  equals: "等于",
  not_equals: "不等于",
  contains: "包含",
  not_contains: "不包含",
  starts_with: "开头是",
  ends_with: "结尾是",
  greater_than: "大于",
  less_than: "小于",
  greater_equal: "大于等于",
  less_equal: "小于等于",
  is_empty: "为空",
  is_not_empty: "不为空",
  in: "在列表中",
  not_in: "不在列表中",
};

// 根据字段类型获取支持的操作符
function getOperatorsForFieldType(
  fieldType: string | undefined,
): FilterOperator[] {
  if (!fieldType) return ["equals"];

  const fieldTypeMap: Record<string, FilterOperator[]> = {
    "short-text": [
      "equals",
      "not_equals",
      "contains",
      "not_contains",
      "starts_with",
      "ends_with",
      "is_empty",
      "is_not_empty",
    ],
    "long-text": [
      "equals",
      "not_equals",
      "contains",
      "not_contains",
      "starts_with",
      "ends_with",
      "is_empty",
      "is_not_empty",
    ],
    number: [
      "equals",
      "not_equals",
      "greater_than",
      "less_than",
      "greater_equal",
      "less_equal",
      "is_empty",
      "is_not_empty",
    ],
    date: [
      "equals",
      "not_equals",
      "greater_than",
      "less_than",
      "greater_equal",
      "less_equal",
      "is_empty",
      "is_not_empty",
    ],
    select: [
      "equals",
      "not_equals",
      "in",
      "not_in",
      "is_empty",
      "is_not_empty",
    ],
    "multi-select": [
      "in",
      "not_in",
      "is_empty",
      "is_not_empty",
    ],
    checkbox: ["equals", "not_equals"],
  };

  return fieldTypeMap[fieldType] || ["equals"];
}

// 获取字段类型
function getFieldType(column: any): string | undefined {
  return column?.columnDef?.meta?.cell?.variant;
}

// 创建新条件
function createNewCondition(
  columns: ReturnType<Table<any>["getAllColumns"]>,
): FilterCondition {
  const firstFilterableColumn = columns.find(
    (col) => col.getCanFilter() && col.id !== "select",
  );
  const fieldType = getFieldType(firstFilterableColumn);
  const operators = getOperatorsForFieldType(fieldType);

  return {
    id: `condition_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    fieldId: firstFilterableColumn?.id || "",
    operator: operators[0] || "equals",
    value: "",
  };
}

// 值输入组件
interface ValueInputProps {
  fieldType?: string;
  operator: FilterOperator;
  value: unknown;
  onChange: (value: unknown) => void;
  cellMeta?: any;
}

function ValueInput({
  fieldType,
  operator,
  value,
  onChange,
  cellMeta,
}: ValueInputProps) {
  if (fieldType === "select" || fieldType === "multi-select") {
    const options = cellMeta?.options || [];
    return (
      <Select
        value={String(value || "")}
        onValueChange={onChange}
      >
        <SelectTrigger className="flex-1">
          <SelectValue placeholder="选择值" />
        </SelectTrigger>
        <SelectContent>
          {options.map((opt: { label: string; value: string }) => (
            <SelectItem key={opt.value} value={opt.value}>
              {opt.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }

  if (fieldType === "checkbox") {
    return (
      <Select
        value={String(value ?? "")}
        onValueChange={(val) => onChange(val === "true")}
      >
        <SelectTrigger className="flex-1">
          <SelectValue placeholder="选择" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="true">是</SelectItem>
          <SelectItem value="false">否</SelectItem>
        </SelectContent>
      </Select>
    );
  }

  if (fieldType === "date") {
    const dateValue = value ? new Date(value as string) : undefined;
    return (
      <Popover>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className={cn(
              "w-full justify-start text-left font-normal",
              !dateValue && "text-muted-foreground",
            )}
          >
            {dateValue ? (
              format(dateValue, "yyyy-MM-dd", { locale: zhCN })
            ) : (
              <span>选择日期</span>
            )}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar
            mode="single"
            selected={dateValue}
            onSelect={(date) => onChange(date ? format(date, "yyyy-MM-dd") : "")}
            initialFocus
          />
        </PopoverContent>
      </Popover>
    );
  }

  if (fieldType === "number") {
    return (
      <Input
        type="number"
        value={String(value ?? "")}
        onChange={(e) =>
          onChange(e.target.value ? Number(e.target.value) : "")
        }
        placeholder="请输入数字"
        className="flex-1"
      />
    );
  }

  // 默认文本输入
  return (
    <Input
      type="text"
      value={String(value ?? "")}
      onChange={(e) => onChange(e.target.value)}
      placeholder="请输入"
      className="flex-1"
    />
  );
}

// 筛选条件编辑器
interface FilterConditionEditorProps {
  condition: FilterCondition;
  index: number;
  columns: ReturnType<Table<any>["getAllColumns"]>;
  onUpdate: (index: number, updates: Partial<FilterCondition>) => void;
  onDelete: (index: number) => void;
}

function FilterConditionEditor({
  condition,
  index,
  columns,
  onUpdate,
  onDelete,
}: FilterConditionEditorProps) {
  const column = columns.find((col) => col.id === condition.fieldId);
  const fieldType = getFieldType(column);
  const operators = getOperatorsForFieldType(fieldType);
  const cellMeta = column?.columnDef?.meta?.cell;

  const needsValue = !["is_empty", "is_not_empty"].includes(condition.operator);

  const handleFieldChange = React.useCallback(
    (fieldId: string) => {
      const newColumn = columns.find((col) => col.id === fieldId);
      const newFieldType = getFieldType(newColumn);
      const newOperators = getOperatorsForFieldType(newFieldType);
      const newOperator = newOperators.includes(condition.operator)
        ? condition.operator
        : newOperators[0] || "equals";

      onUpdate(index, {
        fieldId,
        operator: newOperator,
        value: "",
      });
    },
    [columns, condition.operator, index, onUpdate],
  );

  const handleOperatorChange = React.useCallback(
    (operator: FilterOperator) => {
      onUpdate(index, {
        operator,
        value: "",
      });
    },
    [index, onUpdate],
  );

  const handleValueChange = React.useCallback(
    (value: unknown) => {
      onUpdate(index, { value });
    },
    [index, onUpdate],
  );

  const headerLabel =
    typeof column?.columnDef.header === "string"
      ? column.columnDef.header
      : condition.fieldId || "选择字段";

  return (
    <div className="flex items-center gap-2">
      {index > 0 && (
        <Select defaultValue="and">
          <SelectTrigger className="w-16">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="and">且</SelectItem>
            <SelectItem value="or">或</SelectItem>
          </SelectContent>
        </Select>
      )}
      <span className="text-sm text-muted-foreground whitespace-nowrap">
        {index === 0 ? "当" : ""}
      </span>
      <Select value={condition.fieldId} onValueChange={handleFieldChange}>
        <SelectTrigger className="min-w-[120px]">
          <SelectValue placeholder="选择字段" />
        </SelectTrigger>
        <SelectContent>
          {columns.map((col) => {
            const colHeader =
              typeof col.columnDef.header === "string"
                ? col.columnDef.header
                : col.id;
            return (
              <SelectItem key={col.id} value={col.id}>
                {colHeader}
              </SelectItem>
            );
          })}
        </SelectContent>
      </Select>

      {column && (
        <>
          <Select
            value={condition.operator}
            onValueChange={handleOperatorChange}
          >
            <SelectTrigger className="min-w-[100px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {operators.map((op) => (
                <SelectItem key={op} value={op}>
                  {OPERATOR_LABELS[op]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {needsValue && (
            <div className="flex-1 min-w-[150px]">
              <ValueInput
                fieldType={fieldType}
                operator={condition.operator}
                value={condition.value}
                onChange={handleValueChange}
                cellMeta={cellMeta}
              />
            </div>
          )}
        </>
      )}

      <Button
        variant="ghost"
        size="icon"
        onClick={() => onDelete(index)}
        className="shrink-0 h-8 w-8 text-muted-foreground hover:text-destructive"
      >
        <Trash2 className="size-4" />
      </Button>
    </div>
  );
}

export function DataGridFilterMenu<TData>({
  table,
  conditions = [],
  onConditionsChange,
}: DataGridFilterMenuProps<TData>) {
  const [open, setOpen] = React.useState(false);
  const [localConditions, setLocalConditions] = React.useState<FilterCondition[]>(conditions);
  const [applyToCurrentView, setApplyToCurrentView] = React.useState(true);

  React.useEffect(() => {
    setLocalConditions(conditions);
  }, [conditions]);

  const columns = table.getAllColumns().filter(
    (col) => col.getCanFilter() && col.id !== "select",
  );

  // 条件变化时自动应用
  React.useEffect(() => {
    onConditionsChange?.(localConditions);
  }, [localConditions, onConditionsChange]);

  const handleAddCondition = React.useCallback(() => {
    const newCondition = createNewCondition(columns);
    setLocalConditions((prev) => [...prev, newCondition]);
  }, [columns]);

  const handleUpdateCondition = React.useCallback(
    (index: number, updates: Partial<FilterCondition>) => {
      setLocalConditions((prev) =>
        prev.map((condition, i) =>
          i === index ? { ...condition, ...updates } : condition,
        ),
      );
    },
    [],
  );

  const handleDeleteCondition = React.useCallback((index: number) => {
    setLocalConditions((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const handleClearAll = React.useCallback(() => {
    setLocalConditions([]);
  }, []);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className={cn(
            "h-8 gap-2",
            conditions.length > 0 && "bg-accent",
          )}
        >
          <Filter className="size-4" />
          <span>筛选</span>
          {conditions.length > 0 && (
            <span className="ml-1 rounded-full bg-primary px-1.5 py-0.5 text-xs text-primary-foreground">
              {conditions.length}
            </span>
          )}
          <ChevronDown className="size-3 ml-1 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="w-[600px] p-0"
        align="start"
        sideOffset={4}
      >
        <div className="p-4 space-y-4">
          {/* 仅作用于当前视图的选项 */}
          <div className="flex items-center gap-2 text-sm">
            <CircleCheck className="size-4 text-primary" />
            <span className="text-muted-foreground">仅作用于当前视图的记录</span>
          </div>

          {/* 筛选条件列表 */}
          <div className="space-y-3">
            {localConditions.length === 0 ? (
              <div className="text-sm text-muted-foreground py-2">
                暂无筛选条件
              </div>
            ) : (
              localConditions.map((condition, index) => (
                <FilterConditionEditor
                  key={condition.id}
                  condition={condition}
                  index={index}
                  columns={columns}
                  onUpdate={handleUpdateCondition}
                  onDelete={handleDeleteCondition}
                />
              ))
            )}
          </div>

          {/* 操作按钮 */}
          <div className="flex items-center gap-2 pt-2 border-t">
            <Button
              variant="ghost"
              size="sm"
              onClick={handleAddCondition}
              className="gap-2"
            >
              <Plus className="size-4" />
              添加条件
            </Button>
            {localConditions.length > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={handleClearAll}
                className="gap-2 text-destructive hover:text-destructive"
              >
                <X className="size-4" />
                清除全部
              </Button>
            )}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
