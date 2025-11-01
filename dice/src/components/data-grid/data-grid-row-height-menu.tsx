/**
 * DataGridRowHeightMenu - 行高菜单组件
 * 
 * 功能：
 * - 切换行高（short, medium, tall, extra-tall）
 * - 显示当前行高
 */

"use client";

import * as React from "react";
import type { Table } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ArrowUpDown } from "lucide-react";
import { cn } from "@/lib/utils";
import type { RowHeightValue } from "@/types/data-grid";

interface DataGridRowHeightMenuProps<TData> {
  table: Table<TData>;
}

const rowHeightOptions: Array<{
  value: RowHeightValue;
  label: string;
  description: string;
}> = [
  { value: "short", label: "短", description: "紧凑显示" },
  { value: "medium", label: "中", description: "标准显示" },
  { value: "tall", label: "高", description: "宽松显示" },
  { value: "extra-tall", label: "超高", description: "最大显示" },
];

export function DataGridRowHeightMenu<TData>({
  table,
}: DataGridRowHeightMenuProps<TData>) {
  const meta = table.options.meta;
  const rowHeight = meta?.rowHeight ?? "short";
  const onRowHeightChange = meta?.onRowHeightChange;

  const handleChange = (value: string) => {
    onRowHeightChange?.(value as RowHeightValue);
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 gap-2">
          <ArrowUpDown className="size-4" />
          <span>行高</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-48">
        <DropdownMenuLabel>行高设置</DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuRadioGroup value={rowHeight} onValueChange={handleChange}>
          {rowHeightOptions.map((option) => (
            <DropdownMenuRadioItem
              key={option.value}
              value={option.value}
              className="flex items-center justify-between"
            >
              <div className="flex flex-col">
                <span>{option.label}</span>
                <span className="text-xs text-muted-foreground">
                  {option.description}
                </span>
              </div>
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}


