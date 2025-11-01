/**
 * DataGridViewMenu - 视图菜单组件（列可见性）
 * 
 * 功能：
 * - 显示/隐藏列
 * - 支持搜索列
 * - 支持全选/全不选
 */

"use client";

import * as React from "react";
import type { Table } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Eye, Search } from "lucide-react";
import { cn } from "@/lib/utils";

interface DataGridViewMenuProps<TData> {
  table: Table<TData>;
}

export function DataGridViewMenu<TData>({
  table,
}: DataGridViewMenuProps<TData>) {
  const [open, setOpen] = React.useState(false);
  const [searchQuery, setSearchQuery] = React.useState("");

  const columnVisibility = table.getState().columnVisibility;
  const columns = table.getAllColumns().filter((column) => {
    // 过滤掉不能被隐藏的列（如选择列）
    return column.getCanHide();
  });

  const visibleColumns = columns.filter(
    (column) => columnVisibility[column.id] !== false,
  );

  const filteredColumns = React.useMemo(() => {
    if (!searchQuery.trim()) return columns;
    const query = searchQuery.toLowerCase();
    return columns.filter((column) =>
      column.id.toLowerCase().includes(query),
    );
  }, [columns, searchQuery]);

  const handleToggleColumn = (columnId: string) => {
    const currentVisibility = columnVisibility[columnId];
    table.setColumnVisibility({
      ...columnVisibility,
      [columnId]: currentVisibility === false,
    });
  };

  const handleToggleAll = () => {
    const allVisible = visibleColumns.length === columns.length;
    const newVisibility: Record<string, boolean> = {};
    columns.forEach((column) => {
      newVisibility[column.id] = !allVisible;
    });
    table.setColumnVisibility(newVisibility);
  };

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 gap-2">
          <Eye className="size-4" />
          <span>视图</span>
          {visibleColumns.length < columns.length && (
            <span className="ml-1 rounded-full bg-primary px-1.5 py-0.5 text-xs text-primary-foreground">
              {visibleColumns.length}/{columns.length}
            </span>
          )}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-64">
        <DropdownMenuLabel>列可见性</DropdownMenuLabel>
        <DropdownMenuSeparator />

        {/* 搜索框 */}
        <div className="px-2 py-2">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="搜索列..."
              className="h-8 pl-8"
            />
          </div>
        </div>

        <DropdownMenuSeparator />

        {/* 全选/全不选 */}
        <div className="px-2 py-2">
          <button
            onClick={handleToggleAll}
            className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent"
          >
            <Checkbox
              checked={visibleColumns.length === columns.length}
              onCheckedChange={handleToggleAll}
            />
            <span className="text-sm">
              {visibleColumns.length === columns.length
                ? "全部隐藏"
                : "全部显示"}
            </span>
          </button>
        </div>

        <DropdownMenuSeparator />

        {/* 列列表 */}
        <div className="max-h-[300px] overflow-y-auto p-2">
          {filteredColumns.length === 0 ? (
            <div className="py-4 text-center text-sm text-muted-foreground">
              没有找到匹配的列
            </div>
          ) : (
            <div className="space-y-1">
              {filteredColumns.map((column) => {
                const isVisible =
                  columnVisibility[column.id] !== false;
                const headerLabel =
                  typeof column.columnDef.header === "string"
                    ? column.columnDef.header
                    : column.id;

                return (
                  <button
                    key={column.id}
                    onClick={() => handleToggleColumn(column.id)}
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm hover:bg-accent"
                  >
                    <Checkbox
                      checked={isVisible}
                      onCheckedChange={() => handleToggleColumn(column.id)}
                    />
                    <span className="flex-1 truncate">{headerLabel}</span>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

