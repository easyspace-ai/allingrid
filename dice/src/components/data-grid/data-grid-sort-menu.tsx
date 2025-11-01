/**
 * DataGridSortMenu - 排序菜单组件
 * 
 * 功能：
 * - 显示当前排序规则
 * - 支持添加多个排序规则
 * - 支持拖拽排序
 * - 支持升序/降序切换
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
import {
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  X,
  GripVertical,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";

interface DataGridSortMenuProps<TData> {
  table: Table<TData>;
}

export function DataGridSortMenu<TData>({
  table,
}: DataGridSortMenuProps<TData>) {
  const sorting = table.getState().sorting;
  const [open, setOpen] = React.useState(false);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  );

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const oldIndex = sorting.findIndex((item) => item.id === active.id);
    const newIndex = sorting.findIndex((item) => item.id === over.id);

    if (oldIndex !== -1 && newIndex !== -1) {
      const newSorting = arrayMove(sorting, oldIndex, newIndex);
      table.setSorting(newSorting);
    }
  };

  const handleSortToggle = (columnId: string) => {
    const currentSort = sorting.find((s) => s.id === columnId);
    const newSorting = [...sorting];

    if (!currentSort) {
      // 添加新排序（升序）
      newSorting.push({ id: columnId, desc: false });
    } else if (currentSort.desc === false) {
      // 切换到降序
      const index = newSorting.findIndex((s) => s.id === columnId);
      newSorting[index] = { id: columnId, desc: true };
    } else {
      // 移除排序
      const index = newSorting.findIndex((s) => s.id === columnId);
      newSorting.splice(index, 1);
    }

    table.setSorting(newSorting);
  };

  const handleRemoveSort = (columnId: string) => {
    const newSorting = sorting.filter((s) => s.id !== columnId);
    table.setSorting(newSorting);
  };

  const handleClearAll = () => {
    table.setSorting([]);
  };

  const sortableColumns = table
    .getAllColumns()
    .filter((column) => column.getCanSort());

  const activeSorts = sorting.filter((sort) =>
    sortableColumns.some((col) => col.id === sort.id),
  );

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className={cn(
            "h-8 gap-2",
            activeSorts.length > 0 && "bg-accent",
          )}
        >
          <ArrowUpDown className="size-4" />
          <span>排序</span>
          {activeSorts.length > 0 && (
            <span className="ml-1 rounded-full bg-primary px-1.5 py-0.5 text-xs text-primary-foreground">
              {activeSorts.length}
            </span>
          )}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-64">
        <DropdownMenuLabel>排序规则</DropdownMenuLabel>
        <DropdownMenuSeparator />

        {activeSorts.length === 0 ? (
          <div className="px-2 py-4 text-center text-sm text-muted-foreground">
            暂无排序规则
          </div>
        ) : (
          <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={handleDragEnd}
          >
            <SortableContext
              items={activeSorts.map((s) => s.id)}
              strategy={verticalListSortingStrategy}
            >
              <div className="space-y-1 p-2">
                {activeSorts.map((sort) => {
                  const column = table.getColumn(sort.id);
                  if (!column) return null;

                  const headerLabel =
                    typeof column.columnDef.header === "string"
                      ? column.columnDef.header
                      : column.id;

                  return (
                    <SortableSortItem
                      key={sort.id}
                      id={sort.id}
                      label={headerLabel}
                      desc={sort.desc}
                      onToggle={() => handleSortToggle(sort.id)}
                      onRemove={() => handleRemoveSort(sort.id)}
                    />
                  );
                })}
              </div>
            </SortableContext>
          </DndContext>
        )}

        <DropdownMenuSeparator />
        <div className="p-2">
          <div className="mb-2 text-xs font-medium text-muted-foreground">
            添加排序：
          </div>
          <div className="space-y-1">
            {sortableColumns
              .filter(
                (column) =>
                  !activeSorts.some((sort) => sort.id === column.id),
              )
              .map((column) => {
                const headerLabel =
                  typeof column.columnDef.header === "string"
                    ? column.columnDef.header
                    : column.id;

                return (
                  <button
                    key={column.id}
                    onClick={() => handleSortToggle(column.id)}
                    className="flex w-full items-center justify-between rounded-md px-2 py-1.5 text-sm hover:bg-accent"
                  >
                    <span>{headerLabel}</span>
                    <ArrowUpDown className="size-4 text-muted-foreground" />
                  </button>
                );
              })}
          </div>
        </div>

        {activeSorts.length > 0 && (
          <>
            <DropdownMenuSeparator />
            <div className="p-2">
              <Button
                variant="ghost"
                size="sm"
                onClick={handleClearAll}
                className="w-full"
              >
                清除所有排序
              </Button>
            </div>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

interface SortableSortItemProps {
  id: string;
  label: string;
  desc: boolean;
  onToggle: () => void;
  onRemove: () => void;
}

function SortableSortItem({
  id,
  label,
  desc,
  onToggle,
  onRemove,
}: SortableSortItemProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      className="flex items-center gap-2 rounded-md border bg-background p-2"
    >
      <button
        {...attributes}
        {...listeners}
        className="cursor-grab touch-none text-muted-foreground hover:text-foreground"
      >
        <GripVertical className="size-4" />
      </button>
      <button
        onClick={onToggle}
        className="flex flex-1 items-center justify-between"
      >
        <span className="text-sm">{label}</span>
        {desc ? (
          <ArrowDown className="size-4 text-muted-foreground" />
        ) : (
          <ArrowUp className="size-4 text-muted-foreground" />
        )}
      </button>
      <button
        onClick={onRemove}
        className="rounded-md p-1 text-muted-foreground hover:text-foreground"
      >
        <X className="size-4" />
      </button>
    </div>
  );
}

