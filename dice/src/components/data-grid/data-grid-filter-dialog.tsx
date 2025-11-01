/**
 * DataGridFilterDialog - 筛选对话框组件
 * 
 * 功能：
 * - 编辑筛选条件
 * - 添加/删除条件
 * - 应用筛选
 */

"use client";

import * as React from "react";
import type { Table } from "@tanstack/react-table";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Filter, Plus, X, Trash2 } from "lucide-react";
import { cn } from "@/lib/utils";
import { format } from "date-fns";
import { zhCN } from "date-fns/locale";
import type { FilterCondition, FilterOperator } from "./data-grid-filter-menu";

interface DataGridFilterDialogProps<TData> {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  table: Table<TData>;
  conditions: FilterCondition[];
  onConditionsChange: (conditions: FilterCondition[]) => void;
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

export function DataGridFilterDialog<TData>({
  open,
  onOpenChange,
  table,
  conditions,
  onConditionsChange,
}: DataGridFilterDialogProps<TData>) {
  const [localConditions, setLocalConditions] = React.useState<FilterCondition[]>(conditions);

  React.useEffect(() => {
    setLocalConditions(conditions);
  }, [conditions]);

  const columns = table.getAllColumns().filter((col) => col.getCanFilter() && col.id !== "select");

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

  const handleApply = React.useCallback(() => {
    onConditionsChange(localConditions);
    onOpenChange(false);
  }, [localConditions, onConditionsChange, onOpenChange]);

  const handleClear = React.useCallback(() => {
    setLocalConditions([]);
  }, []);

  const handleReset = React.useCallback(() => {
    setLocalConditions(conditions);
  }, [conditions]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px] max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Filter className="size-5" />
            设置筛选条件
          </DialogTitle>
          <DialogDescription>
            定义数据筛选规则，支持多条件组合
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 px-1">
          {localConditions.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Filter className="size-12 text-muted-foreground mb-4 opacity-50" />
              <p className="text-sm text-muted-foreground">暂无筛选条件</p>
              <p className="text-xs text-muted-foreground mt-1">
                点击下方按钮添加筛选条件
              </p>
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

        <div className="flex flex-col gap-2 border-t pt-4">
          <Button
            variant="outline"
            onClick={handleAddCondition}
            className="w-full"
          >
            <Plus className="size-4 mr-2" />
            添加条件
          </Button>

          <DialogFooter>
            <div className="flex items-center justify-between w-full">
              <div className="flex items-center gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleClear}
                  disabled={localConditions.length === 0}
                >
                  清除全部
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleReset}
                  disabled={
                    JSON.stringify(localConditions) === JSON.stringify(conditions)
                  }
                >
                  重置
                </Button>
              </div>
              <div className="flex items-center gap-2">
                <Button variant="outline" onClick={() => onOpenChange(false)}>
                  取消
                </Button>
                <Button onClick={handleApply}>确定</Button>
              </div>
            </div>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
}

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
    <div className="flex items-start gap-2 rounded-lg border bg-muted/30 p-4">
      <div className="flex-1 space-y-3">
        {/* 字段选择 */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground whitespace-nowrap">当</span>
          <Select value={condition.fieldId} onValueChange={handleFieldChange}>
            <SelectTrigger className="flex-1">
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
        </div>

        {/* 操作符选择 */}
        {column && (
          <div className="flex items-center gap-2">
            <Select
              value={condition.operator}
              onValueChange={handleOperatorChange}
            >
              <SelectTrigger className="flex-1">
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
          </div>
        )}

        {/* 值输入 */}
        {needsValue && column && (
          <div className="flex items-center gap-2">
            <ValueInput
              fieldType={fieldType}
              operator={condition.operator}
              value={condition.value}
              onChange={handleValueChange}
              cellMeta={cellMeta}
            />
          </div>
        )}
      </div>

      {/* 删除按钮 */}
      <Button
        variant="ghost"
        size="icon"
        onClick={() => onDelete(index)}
        className="shrink-0 text-destructive hover:text-destructive"
      >
        <Trash2 className="size-4" />
      </Button>
    </div>
  );
}

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

