/**
 * DataGridToolbar - 数据网格工具栏
 * 
 * 整合了排序、行高、视图等菜单组件
 * 参考文档：https://www.diceui.com/docs/components/data-grid
 */

"use client";

import * as React from "react";
import type { Table } from "@tanstack/react-table";
import { Plus } from "lucide-react";
import { DataGridSortMenu } from "@/components/data-grid/data-grid-sort-menu";
import { DataGridRowHeightMenu } from "@/components/data-grid/data-grid-row-height-menu";
import { DataGridViewMenu } from "@/components/data-grid/data-grid-view-menu";
import { DataGridFilterMenu } from "@/components/data-grid/data-grid-filter-menu";
import type { FilterCondition } from "@/components/data-grid/data-grid-filter-menu";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface DataGridToolbarProps<TData> {
  table: Table<TData>;
  filterConditions?: FilterCondition[];
  onFilterConditionsChange?: (conditions: FilterCondition[]) => void;
  onAddRecord?: () => void;
  className?: string;
}

export function DataGridToolbar<TData>({
  table,
  filterConditions,
  onFilterConditionsChange,
  onAddRecord,
  className,
}: DataGridToolbarProps<TData>) {
  return (
    <div
      className={cn(
        "flex items-center gap-2",
        className,
      )}
      role="toolbar"
      aria-label="数据网格工具栏"
    >
      <DataGridFilterMenu
        table={table}
        conditions={filterConditions}
        onConditionsChange={onFilterConditionsChange}
      />
      <DataGridSortMenu table={table} />
      <DataGridRowHeightMenu table={table} />
      <DataGridViewMenu table={table} />
      {onAddRecord && (
        <Button
          onClick={onAddRecord}
          size="sm"
          className="gap-2"
        >
          <Plus className="h-4 w-4" />
          添加新记录
        </Button>
      )}
    </div>
  );
}

