"use client";

import { Plus } from "lucide-react";
import * as React from "react";
import { DraggableColumnHeaders } from "@/components/data-grid/data-grid-column-header";
import { DataGridContextMenu } from "@/components/data-grid/data-grid-context-menu";
import { DataGridRow } from "@/components/data-grid/data-grid-row";
import { DataGridSearch } from "@/components/data-grid/data-grid-search";
import { AddColumnMenu } from "@/components/data-grid/add-column-menu";
import type { useDataGrid } from "@/hooks/use-data-grid";
import { cn } from "@/lib/utils";

interface DataGridProps<TData>
  extends ReturnType<typeof useDataGrid<TData>>,
    React.ComponentProps<"div"> {
  height?: number;
}

export function DataGrid<TData>({
  dataGridRef,
  headerRef,
  rowMapRef,
  footerRef,
  table,
  rowVirtualizer,
  height = 600,
  searchState,
  columnSizeVars,
  onRowAdd,
  onAddColumn,
  className,
  ...props
}: DataGridProps<TData>) {
  const rows = table.getRowModel().rows;
  const columns = table.getAllColumns();

  const meta = table.options.meta;
  const rowHeight = meta?.rowHeight ?? "short";
  const focusedCell = meta?.focusedCell ?? null;

  // 拖动状态管理
  const [dragState, setDragState] = React.useState<{ 
    activeId: string | null; 
    overId: string | null;
    dragX: number | null;
  }>({
    activeId: null,
    overId: null,
    dragX: null,
  });

  // 添加列菜单状态
  const [isAddColumnMenuOpen, setIsAddColumnMenuOpen] = React.useState(false);
  const addColumnTriggerRef = React.useRef<HTMLDivElement>(null);

  const onGridContextMenu = React.useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      event.preventDefault();
    },
    [],
  );

  const onAddRowKeyDown = React.useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (!onRowAdd) return;

      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        onRowAdd();
      }
    },
    [onRowAdd],
  );

  const handleAddColumnConfirm = React.useCallback(
    (payload: { type: string; name?: string; options?: any }) => {
      onAddColumn?.(payload);
      setIsAddColumnMenuOpen(false);
    },
    [onAddColumn],
  );

  const handleAddColumnClick = React.useCallback(() => {
    if (onAddColumn) {
      setIsAddColumnMenuOpen(true);
    }
  }, [onAddColumn]);

  return (
    <div
      data-slot="grid-wrapper"
      className={cn("relative flex w-full flex-col", className)}
      {...props}
    >
      {searchState && <DataGridSearch {...searchState} />}
      <DataGridContextMenu table={table} />
      <div
        role="grid"
        aria-label="Data grid"
        aria-rowcount={rows.length + (onRowAdd ? 1 : 0)}
        aria-colcount={columns.length + (onAddColumn ? 1 : 0)}
        data-slot="grid"
        tabIndex={0}
        ref={dataGridRef}
        className="relative grid select-none overflow-auto rounded-b-md border-t focus:outline-none"
        style={{
          ...columnSizeVars,
          height: `${height}px`,
        }}
        onContextMenu={onGridContextMenu}
      >
        <div
          role="rowgroup"
          data-slot="grid-header"
          ref={headerRef}
          className="sticky top-0 z-10 grid border-b bg-background"
        >
          {table.getHeaderGroups().map((headerGroup, rowIndex) => (
            <div
              key={headerGroup.id}
              role="row"
              aria-rowindex={rowIndex + 1}
              data-slot="grid-header-row"
              tabIndex={-1}
              className="flex w-full"
            >
              <DraggableColumnHeaders
                headers={headerGroup.headers}
                table={table}
                gridHeight={height}
                headerElementRef={headerRef as React.RefObject<HTMLElement>}
                gridContainerRef={dataGridRef as React.RefObject<HTMLElement>}
                onDragStateChange={setDragState}
                onAddColumnClick={onAddColumn ? handleAddColumnClick : undefined}
                addColumnTriggerRef={addColumnTriggerRef as React.RefObject<HTMLDivElement>}
              />
            </div>
          ))}
        </div>
        <div
          role="rowgroup"
          data-slot="grid-body"
          className="relative grid"
          style={{
            height: `${rowVirtualizer.getTotalSize()}px`,
          }}
        >
          {rowVirtualizer.getVirtualIndexes().map((virtualRowIndex) => {
            const row = rows[virtualRowIndex];
            if (!row) return null;

            return (
              <DataGridRow
                key={row.id}
                row={row}
                rowMapRef={rowMapRef}
                virtualRowIndex={virtualRowIndex}
                rowVirtualizer={rowVirtualizer}
                rowHeight={rowHeight}
                focusedCell={focusedCell}
              />
            );
          })}
        </div>
        {/* 列高亮指示器 - 拖动时显示整个列的边线 */}
        {dragState.activeId && (() => {
          const headers = table.getHeaderGroups()[0]?.headers || [];
          const activeHeader = headers.find(h => h.column.id === dragState.activeId);
          if (!activeHeader) return null;

          // 基于拖动阴影的位置计算目标列和插入位置
          let targetColumnId: string | null = null;
          let highlightLeft = false;
          
          if (dragState.dragX !== null && dataGridRef?.current) {
            // 基于鼠标位置计算目标列
            const containerRect = dataGridRef.current.getBoundingClientRect();
            const scrollLeft = dataGridRef.current.scrollLeft;
            const relativeX = dragState.dragX - containerRect.left + scrollLeft;
            
            // 计算累计宽度，找到鼠标位置对应的列
            let cumulativeWidth = 0;
            for (let i = 0; i < headers.length; i++) {
              const header = headers[i];
              const columnWidth = header.column.getSize();
              const columnStart = cumulativeWidth;
              const columnEnd = cumulativeWidth + columnWidth;
              const columnCenter = cumulativeWidth + columnWidth / 2;
              
              if (relativeX >= columnStart && relativeX < columnEnd) {
                // 判断鼠标在列的前半部分还是后半部分
                if (relativeX <= columnCenter) {
                  // 鼠标在列的前半部分，插入到当前列之前，高亮当前列的左边线
                  targetColumnId = header.column.id;
                  highlightLeft = true;
                } else {
                  // 鼠标在列的后半部分，插入到当前列之后，高亮当前列的右边线
                  targetColumnId = header.column.id;
                  highlightLeft = false;
                }
                break;
              }
              cumulativeWidth += columnWidth;
            }
            
            // 如果没有找到目标列（可能在边界外），使用 overId 作为后备
            if (!targetColumnId && dragState.overId) {
              targetColumnId = dragState.overId;
              const activeIndex = headers.findIndex(h => h.column.id === dragState.activeId);
              const targetIndex = headers.findIndex(h => h.column.id === targetColumnId);
              highlightLeft = activeIndex !== -1 && activeIndex < targetIndex;
            }
          } else if (dragState.overId) {
            // 如果没有 dragX，回退到使用 overId
            targetColumnId = dragState.overId;
            const activeIndex = headers.findIndex(h => h.column.id === dragState.activeId);
            const targetIndex = headers.findIndex(h => h.column.id === dragState.overId);
            highlightLeft = activeIndex !== -1 && activeIndex < targetIndex;
          }

          // 如果没有目标列，不显示高亮
          if (!targetColumnId || targetColumnId === dragState.activeId) return null;

          const targetHeader = headers.find(h => h.column.id === targetColumnId);
          if (!targetHeader) return null;

          // 计算列的位置 - 使用 CSS 变量计算累计宽度
          let left = 0;
          for (let i = 0; i < headers.length; i++) {
            if (headers[i].column.id === targetColumnId) {
              if (highlightLeft) {
                // 高亮左边线，位置在列的开始位置
                break;
              } else {
                // 高亮右边线，位置在列的结束位置
                const columnSize = headers[i].column.getSize();
                left += columnSize;
                break;
              }
            }
            const columnSize = headers[i].column.getSize();
            left += columnSize;
          }

          return (
            <div
              key={`drag-highlight-${targetColumnId}-${highlightLeft}`}
              className="pointer-events-none absolute z-9998"
              style={{
                left: highlightLeft ? `${left - 0.5}px` : `${left - 0.5}px`,
                width: '1px',
                backgroundColor: '#8b5cf6', // 参考 aitable: interactionLineColorHighlight (violet-500)
                top: 0,
                height: `${height}px`,
              }}
            />
          );
        })()}
        {onRowAdd && (
          <div
            role="rowgroup"
            data-slot="grid-footer"
            ref={footerRef}
            className="sticky bottom-0 z-5 grid border-t bg-background"
          >
            <div
              role="row"
              aria-rowindex={rows.length + 2}
              data-slot="grid-add-row"
              tabIndex={-1}
              className="flex w-full"
            >
              <div
                role="gridcell"
                tabIndex={0}
                className="relative flex h-9 grow items-center bg-muted/30 transition-colors hover:bg-muted/50 focus:bg-muted/50 focus:outline-none"
                style={{
                  width: table.getTotalSize(),
                  minWidth: table.getTotalSize(),
                }}
                onClick={onRowAdd}
                onKeyDown={onAddRowKeyDown}
              >
                <div className="sticky left-0 flex items-center gap-2 px-3 text-muted-foreground">
                  <Plus className="size-3.5" />
                  <span className="text-sm">Add row</span>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
      {onAddColumn && (
        <AddColumnMenu
          isOpen={isAddColumnMenuOpen}
          onClose={() => setIsAddColumnMenuOpen(false)}
          onConfirm={handleAddColumnConfirm}
          triggerRef={addColumnTriggerRef}
        />
      )}
    </div>
  );
}
