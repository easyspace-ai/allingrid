"use client";

import type {
  ColumnSort,
  Header,
  SortDirection,
  SortingState,
  Table,
} from "@tanstack/react-table";
import {
  BaselineIcon,
  CalendarIcon,
  CheckSquareIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  EyeOffIcon,
  GripVerticalIcon,
  HashIcon,
  ListChecksIcon,
  ListIcon,
  PinIcon,
  PinOffIcon,
  TextInitialIcon,
  XIcon,
  Plus,
  Link as LinkIcon,
  Mail as MailIcon,
  Phone as PhoneIcon,
  Star as StarIcon,
  User as UserIcon,
  Image as ImageIcon,
  Code as CodeIcon,
  Bot as BotIcon,
} from "lucide-react";
import * as React from "react";
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragOverlay,
  type DragEndEvent,
  type DragStartEvent,
} from "@dnd-kit/core";
import { restrictToHorizontalAxis } from "@dnd-kit/modifiers";
import {
  SortableContext,
  sortableKeyboardCoordinates,
  horizontalListSortingStrategy,
} from "@dnd-kit/sortable";
import {
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";

import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { getCommonPinningStyles } from "@/lib/data-table";
import type { Cell } from "@/types/data-grid";

function getColumnVariant(variant?: Cell["variant"]): {
  icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
  label: string;
} | null {
  switch (variant) {
    case "short-text":
      return { icon: BaselineIcon, label: "Short text" };
    case "long-text":
      return { icon: TextInitialIcon, label: "Long text" };
    case "number":
      return { icon: HashIcon, label: "Number" };
    case "select":
      return { icon: ListIcon, label: "Select" };
    case "multi-select":
      return { icon: ListChecksIcon, label: "Multi-select" };
    case "checkbox":
      return { icon: CheckSquareIcon, label: "Checkbox" };
    case "date":
      return { icon: CalendarIcon, label: "Date" };
    case "link":
      return { icon: LinkIcon, label: "Link" };
    case "email":
      return { icon: MailIcon, label: "Email" };
    case "phone":
      return { icon: PhoneIcon, label: "Phone" };
    case "rating":
      return { icon: StarIcon, label: "Rating" };
    case "user":
      return { icon: UserIcon, label: "User" };
    case "attachment":
      return { icon: ImageIcon, label: "Attachment" };
    case "formula":
      return { icon: CodeIcon, label: "Formula" };
    case "ai":
      return { icon: BotIcon, label: "AI" };
    default:
      return null;
  }
}

interface DataGridColumnHeaderProps<TData, TValue>
  extends React.ComponentProps<"div"> {
  header: Header<TData, TValue>;
  table: Table<TData>;
  dragListeners?: ReturnType<typeof useSortable>['listeners'];
}

interface SortableColumnHeaderProps<TData, TValue>
  extends DataGridColumnHeaderProps<TData, TValue> {
  isAnyDragging?: boolean;
}

function SortableColumnHeader<TData, TValue>({
  header,
  table,
  isAnyDragging = false,
  ...props
}: SortableColumnHeaderProps<TData, TValue>) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id: header.column.id,
  });

  // 不在拖动过程中应用 transform，保持列头位置不变
  // 只在拖动结束时才更新列顺序
  const style = {
    // transform: CSS.Transform.toString(transform), // 禁用自动 transform
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      className="flex items-center size-full"
      {...attributes}
    >
      <div className="flex-1 min-w-0 size-full">
        <DataGridColumnHeader
          header={header}
          table={table}
          isDragging={isDragging || isAnyDragging}
          dragListeners={listeners}
          {...props}
        />
      </div>
    </div>
  );
}

export function DataGridColumnHeader<TData, TValue>({
  header,
  table,
  className,
  onPointerDown,
  isDragging,
  dragListeners,
  ...props
}: DataGridColumnHeaderProps<TData, TValue>) {
  const column = header.column;
  const label = column.columnDef.meta?.label
    ? column.columnDef.meta.label
    : typeof column.columnDef.header === "string"
      ? column.columnDef.header
      : column.id;

  const isAnyColumnResizing = table.getState().columnSizingInfo?.isResizingColumn || false;
  const isAnyDragging = globalIsDragging || isDragging;

  const cellVariant = column.columnDef.meta?.cell;
  const columnVariant = getColumnVariant(cellVariant?.variant);

  // 启用列固定功能
  const pinnedPosition = column.getIsPinned?.() || false;
  const isPinnedLeft = pinnedPosition === "left";
  const isPinnedRight = pinnedPosition === "right";

  const onSortingChange = React.useCallback(
    (direction: SortDirection) => {
      table.setSorting((prev: SortingState) => {
        const existingSortIndex = prev.findIndex(
          (sort) => sort.id === column.id,
        );
        const newSort: ColumnSort = {
          id: column.id,
          desc: direction === "desc",
        };

        if (existingSortIndex >= 0) {
          const updated = [...prev];
          updated[existingSortIndex] = newSort;
          return updated;
        } else {
          return [...prev, newSort];
        }
      });
    },
    [column.id, table],
  );

  const onSortRemove = React.useCallback(() => {
    table.setSorting((prev: SortingState) =>
      prev.filter((sort) => sort.id !== column.id),
    );
  }, [column.id, table]);

  const onLeftPin = React.useCallback(() => {
    if (column.pin) {
    column.pin("left");
    }
  }, [column]);

  const onRightPin = React.useCallback(() => {
    if (column.pin) {
    column.pin("right");
    }
  }, [column]);

  const onUnpin = React.useCallback(() => {
    if (column.pin) {
    column.pin(false);
    }
  }, [column]);

  const [menuOpen, setMenuOpen] = React.useState(false);
  const [menuPosition, setMenuPosition] = React.useState({ x: 0, y: 0 });

  const onTriggerContextMenu = React.useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      event.preventDefault();
      event.stopPropagation();
      
      // 设置菜单位置
      setMenuPosition({ x: event.clientX, y: event.clientY });
      setMenuOpen(true);
    },
    [],
  );

  const onMenuOpenChange = React.useCallback(
    (open: boolean) => {
      setMenuOpen(open);
    },
    [],
  );

  // 创建隐藏的 Trigger 样式，用于定位菜单
  const triggerStyle = React.useMemo<React.CSSProperties>(
    () => ({
      position: "fixed",
      left: `${menuPosition.x}px`,
      top: `${menuPosition.y}px`,
      width: "1px",
      height: "1px",
      padding: 0,
      margin: 0,
      border: "none",
      background: "transparent",
      pointerEvents: "none",
      opacity: 0,
    }),
    [menuPosition.x, menuPosition.y],
  );

  const onTriggerPointerDown = React.useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      const target = event.target as HTMLElement;
      
      // 检查是否是点击文本区域（文本区域有拖拽监听器）
      const isClickingText = target.closest('span.truncate') !== null;
      
      // 如果点击的是文本区域，不阻止事件，让文本元素的拖拽监听器处理
      if (isClickingText) {
        return;
      }
      
      // 其他区域也不阻止事件，避免干扰拖拽
    },
    [],
  );

  const onTriggerClick = React.useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      // 不阻止点击事件，让拖拽可以正常工作
      // 下拉菜单现在通过右键触发，所以不需要阻止点击
    },
    [],
  );

  return (
    <>
      <DropdownMenu open={menuOpen} onOpenChange={onMenuOpenChange}>
        {/* 隐藏的 Trigger，用于定位菜单 */}
        <DropdownMenuTrigger style={triggerStyle} />
        {/* 实际的列头容器 */}
        <div
          className={cn(
            "flex size-full items-center justify-between gap-2 p-2 text-sm hover:bg-accent/40 [&_svg]:size-4 cursor-grab active:cursor-grabbing",
            (isAnyColumnResizing || isAnyDragging) && "pointer-events-none",
            className,
          )}
          onContextMenu={onTriggerContextMenu}
          onPointerDown={(e) => {
            // 如果点击的是resize handle或其父元素，不触发拖动
            const target = e.target as HTMLElement;
            if (target.closest('[role="separator"]')) {
              e.stopPropagation();
              return;
            }
            // 调用原有的 onPointerDown 处理
            onTriggerPointerDown(e);
            // 调用原始的拖动监听器（在原有处理之后）
            if (dragListeners) {
              dragListeners.onPointerDown?.(e);
            }
          }}
          onClick={onTriggerClick}
          {...(dragListeners ? Object.fromEntries(
            Object.entries(dragListeners).filter(([key]) => key !== 'onPointerDown')
          ) : {})}
          {...props}
        >
          <div className="flex min-w-0 flex-1 items-center gap-1.5">
            {columnVariant && (
              <Tooltip delayDuration={100}>
                <TooltipTrigger asChild>
                  <columnVariant.icon className="size-3.5 shrink-0 text-muted-foreground" />
                </TooltipTrigger>
                <TooltipContent side="top">
                  <p>{columnVariant.label}</p>
                </TooltipContent>
              </Tooltip>
            )}
            <span 
              className="truncate select-none"
            >
              {label}
            </span>
          </div>
        </div>
        <DropdownMenuContent 
          align="start" 
          sideOffset={4} 
          className="w-60 bg-white/95 backdrop-blur-sm border shadow-lg"
        >
          {column.getCanSort?.() && ( // 启用排序功能
            <>
              <DropdownMenuCheckboxItem
                className="relative pr-8 pl-2 [&>span:first-child]:right-2 [&>span:first-child]:left-auto [&_svg]:text-muted-foreground"
                checked={column.getIsSorted?.() === "asc"}
                onClick={() => onSortingChange("asc")}
              >
                <ChevronUpIcon />
                Sort asc
              </DropdownMenuCheckboxItem>
              <DropdownMenuCheckboxItem
                className="relative pr-8 pl-2 [&>span:first-child]:right-2 [&>span:first-child]:left-auto [&_svg]:text-muted-foreground"
                checked={column.getIsSorted?.() === "desc"}
                onClick={() => onSortingChange("desc")}
              >
                <ChevronDownIcon />
                Sort desc
              </DropdownMenuCheckboxItem>
              {column.getIsSorted?.() && (
                <DropdownMenuItem onClick={onSortRemove}>
                  <XIcon />
                  Remove sort
                </DropdownMenuItem>
              )}
            </>
          )}
          {column.getCanPin?.() && ( // 启用列固定功能
            <>
              {column.getCanSort?.() && <DropdownMenuSeparator />}

              {isPinnedLeft ? (
                <DropdownMenuItem
                  className="[&_svg]:text-muted-foreground"
                  onClick={onUnpin}
                >
                  <PinOffIcon />
                  Unpin from left
                </DropdownMenuItem>
              ) : (
                <DropdownMenuItem
                  className="[&_svg]:text-muted-foreground"
                  onClick={onLeftPin}
                >
                  <PinIcon />
                  Pin to left
                </DropdownMenuItem>
              )}
              {isPinnedRight ? (
                <DropdownMenuItem
                  className="[&_svg]:text-muted-foreground"
                  onClick={onUnpin}
                >
                  <PinOffIcon />
                  Unpin from right
                </DropdownMenuItem>
              ) : (
                <DropdownMenuItem
                  className="[&_svg]:text-muted-foreground"
                  onClick={onRightPin}
                >
                  <PinIcon />
                  Pin to right
                </DropdownMenuItem>
              )}
            </>
          )}
          {column.getCanHide?.() && ( // 启用隐藏列功能
            <>
              <DropdownMenuSeparator />
              <DropdownMenuCheckboxItem
                className="relative pr-8 pl-2 [&>span:first-child]:right-2 [&>span:first-child]:left-auto [&_svg]:text-muted-foreground"
                checked={!column.getIsVisible?.()}
                onClick={() => column.toggleVisibility?.(false)}
              >
                <EyeOffIcon />
                Hide column
              </DropdownMenuCheckboxItem>
            </>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
      {true && ( // 启用列调整大小功能
        <DataGridColumnResizer header={header} table={table} label={label} isDragging={isDragging} isAnyDragging={isAnyDragging} />
      )}
    </>
  );
}

const DataGridColumnResizer = React.memo(
  DataGridColumnResizerImpl,
  (prev, next) => {
    const prevColumn = prev.header.column;
    const nextColumn = next.header.column;

    // 检查拖动状态变化
    if (prev.isDragging !== next.isDragging || prev.isAnyDragging !== next.isAnyDragging) {
      return false;
    }

    // 检查列调整大小状态和大小是否变化
    if (
      prevColumn.getIsResizing?.() !== nextColumn.getIsResizing?.() ||
      prevColumn.getSize?.() !== nextColumn.getSize?.()
    ) {
      return false;
    }

    if (prev.label !== next.label) return false;

    return true;
  },
) as typeof DataGridColumnResizerImpl;

interface DataGridColumnResizerProps<TData, TValue> {
  header: Header<TData, TValue>;
  table: Table<TData>;
  label: string;
  isDragging?: boolean;
  isAnyDragging?: boolean;
}

function DataGridColumnResizerImpl<TData, TValue>({
  header,
  table,
  label,
  isDragging = false,
  isAnyDragging = false,
}: DataGridColumnResizerProps<TData, TValue>) {
  const defaultColumnDef = table._getDefaultColumnDef();

  const onDoubleClick = React.useCallback(() => {
    // 如果正在拖动，阻止双击重置列大小
    if (globalIsDragging) return;
    // 双击重置列大小
    if (header.column.resetSize) {
      header.column.resetSize();
    }
  }, [header.column]);

  const getSize = () => {
    return header.column.getSize?.() || 150;
  };

  const isResizing = () => {
    return header.column.getIsResizing?.() || false;
  };

  const getResizeHandler = () => {
    // 如果正在拖动，返回一个空函数，阻止列宽调整
    if (globalIsDragging || isDragging) {
      return () => {};
    }
    return header.getResizeHandler?.() || (() => {});
  };

  const handleMouseDown = React.useCallback((e: React.MouseEvent) => {
    // 如果正在拖动，阻止所有鼠标事件
    if (globalIsDragging || isDragging || isAnyDragging) {
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
    // 调用原始的 resize handler（先调用，确保resize handler能正常工作）
    const handler = header.getResizeHandler?.();
    if (handler) {
      handler(e.nativeEvent);
    }
    // 阻止事件冒泡到拖动监听器（在调用resize handler之后）
    e.stopPropagation();
  }, [isDragging, isAnyDragging, header]);

  const handleTouchStart = React.useCallback((e: React.TouchEvent) => {
    // 如果正在拖动，阻止所有触摸事件
    if (globalIsDragging || isDragging || isAnyDragging) {
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
    // 调用原始的 resize handler（先调用，确保resize handler能正常工作）
    const handler = header.getResizeHandler?.();
    if (handler) {
      handler(e.nativeEvent);
    }
    // 阻止事件冒泡到拖动监听器（在调用resize handler之后）
    e.stopPropagation();
  }, [isDragging, isAnyDragging, header]);

  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label={`Resize ${label} column`}
      aria-valuenow={getSize()}
      aria-valuemin={defaultColumnDef.minSize || 60}
      aria-valuemax={defaultColumnDef.maxSize || 800}
      tabIndex={isDragging ? -1 : 0}
      data-dragging={isDragging || isAnyDragging || globalIsDragging ? 'true' : 'false'}
      className={cn(
        "after:-translate-x-1/2 -right-px absolute top-0 z-50 h-full w-0.5 cursor-ew-resize touch-none select-none bg-border transition-opacity after:absolute after:inset-y-0 after:left-1/2 after:h-full after:w-[18px] after:content-[''] focus:bg-primary focus:outline-none",
        isResizing()
          ? "bg-primary opacity-100 pointer-events-auto"
          : isDragging || isAnyDragging || globalIsDragging
            ? "opacity-0 pointer-events-none cursor-default" // 拖动时完全隐藏并禁用交互
            : "opacity-0 hover:bg-primary hover:opacity-100 pointer-events-auto", // 只有在非拖动状态下才显示 hover 效果
      )}
      style={isDragging || isAnyDragging || globalIsDragging ? { display: 'none', pointerEvents: 'none' } : undefined}
      onDoubleClick={isDragging || isAnyDragging || globalIsDragging ? undefined : onDoubleClick}
      onMouseDown={handleMouseDown}
      onTouchStart={handleTouchStart}
    />
  );
}
export { SortableColumnHeader };

// 列拖拽排序的包装器组件
interface DraggableColumnHeadersProps<TData> {
  headers: Header<TData, unknown>[];
  table: Table<TData>;
  gridHeight?: number;
  headerElementRef?: React.RefObject<HTMLElement>;
  gridContainerRef?: React.RefObject<HTMLElement>;
  onDragStateChange?: (state: { 
    activeId: string | null; 
    overId: string | null;
    dragX: number | null; // 拖动时的鼠标 X 坐标
  }) => void;
  onAddColumnClick?: () => void;
  addColumnTriggerRef?: React.RefObject<HTMLDivElement>;
}

// 全局拖动状态，用于禁用列宽调整
let globalIsDragging = false;

export function DraggableColumnHeaders<TData>({
  headers,
  table,
  gridHeight = 600,
  headerElementRef,
  gridContainerRef,
  onDragStateChange,
  onAddColumnClick,
  addColumnTriggerRef,
}: DraggableColumnHeadersProps<TData>) {
  const [activeId, setActiveId] = React.useState<string | null>(null);
  const [overId, setOverId] = React.useState<string | null>(null);
  
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8, // 需要移动8px才开始拖拽，避免与点击冲突
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const handleDragStart = (event: DragStartEvent) => {
    setActiveId(event.active.id as string);
    globalIsDragging = true; // 设置全局拖动状态
    onDragStateChange?.({ activeId: event.active.id as string, overId: null, dragX: null });
  };

  const handleDragOver = (event: any) => {
    const { over } = event;
    const newOverId = over?.id || null;
    setOverId(newOverId);
    // 获取拖动时的鼠标位置 - 使用 delta 和 active 的位置来计算
    let dragX: number | null = null;
    if (event.activatorEvent) {
      dragX = event.activatorEvent.clientX;
    } else if (over && gridContainerRef?.current) {
      // 如果没有 activatorEvent，尝试从 over 元素的位置计算
      const containerRect = gridContainerRef.current.getBoundingClientRect();
      const overElement = document.querySelector(`[data-column-id="${over.id}"]`);
      if (overElement) {
        const overRect = overElement.getBoundingClientRect();
        dragX = overRect.left + overRect.width / 2 - containerRect.left;
      }
    }
    onDragStateChange?.({ activeId, overId: newOverId, dragX });
  };

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveId(null);
    setOverId(null);
    globalIsDragging = false; // 清除全局拖动状态
    onDragStateChange?.({ activeId: null, overId: null, dragX: null });

    if (over && active.id !== over.id) {
      const oldIndex = headers.findIndex((header) => header.column.id === active.id);
      const newIndex = headers.findIndex((header) => header.column.id === over.id);

      if (oldIndex !== -1 && newIndex !== -1) {
        // 使用 TanStack Table 的 setColumnOrder 方法
        // 获取当前列顺序（如果未设置，则使用所有可见列的ID）
        const currentOrder = table.getState().columnOrder.length > 0
          ? table.getState().columnOrder
          : headers.map((h) => h.column.id);
        
        const newOrder = [...currentOrder];
        const [movedColumn] = newOrder.splice(oldIndex, 1);
        newOrder.splice(newIndex, 0, movedColumn);
        table.setColumnOrder(newOrder);
      }
    }
  };

  const activeHeader = activeId ? headers.find(header => header.column.id === activeId) : null;
  const activeColumnWidth = activeHeader 
    ? `${activeHeader.column.getSize()}px`
    : 'auto';

  // 获取表格容器的位置，用于对齐拖动阴影的顶部
  const [gridContainerTop, setGridContainerTop] = React.useState<number | null>(null);
  
  React.useEffect(() => {
    if (activeId) {
      // 优先使用 gridContainerRef，如果没有则使用 headerElementRef
      const containerElement = gridContainerRef?.current || headerElementRef?.current;
      if (containerElement) {
        const rect = containerElement.getBoundingClientRect();
        setGridContainerTop(rect.top);
      }
    } else {
      setGridContainerTop(null);
    }
  }, [activeId, headerElementRef, gridContainerRef]);

  // 监听鼠标移动以获取实时位置
  React.useEffect(() => {
    if (!activeId) return;

    const handleMouseMove = (e: MouseEvent) => {
      const dragX = e.clientX;
      onDragStateChange?.({ activeId, overId, dragX });
    };

    document.addEventListener('mousemove', handleMouseMove);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
    };
  }, [activeId, overId, onDragStateChange]);

  return (
    <DndContext
      sensors={sensors}
      collisionDetection={closestCenter}
      modifiers={[restrictToHorizontalAxis]}
      onDragStart={handleDragStart}
      onDragOver={handleDragOver}
      onDragEnd={handleDragEnd}
    >
      <SortableContext
        items={headers.map((header) => header.column.id)}
        strategy={horizontalListSortingStrategy}
      >
        {headers.map((header, colIndex) => {
          const sorting = table.getState().sorting;
          const currentSort = sorting.find(
            (sort) => sort.id === header.column.id,
          );
          const isSortable = header.column.getCanSort();
          const isOver = overId === header.column.id && activeId !== header.column.id;
          const isDragging = activeId === header.column.id;
          
          // 判断拖动方向：如果 activeId 的索引小于 overId，说明向右拖动，高亮左边；否则高亮右边
          const activeIndex = activeId ? headers.findIndex(h => h.column.id === activeId) : -1;
          const overIndex = colIndex;
          const highlightLeft = activeIndex !== -1 && activeIndex < overIndex;

          return (
            <div
              key={header.id}
              role="columnheader"
              aria-colindex={colIndex + 1}
              data-column-id={header.column.id}
              aria-sort={
                currentSort?.desc === false
                  ? "ascending"
                  : currentSort?.desc === true
                    ? "descending"
                    : isSortable
                      ? "none"
                      : undefined
              }
              data-slot="grid-header-cell"
              tabIndex={-1}
              className={cn("relative", {
                // 只有在没有高亮时才显示默认边框
                "border-r": header.column.id !== "select" && !isOver,
                // 根据拖动方向高亮目标列的左边或右边线
                "border-l-2 border-l-primary": isOver && highlightLeft,
                "border-r-2 border-r-primary": isOver && !highlightLeft,
                // 拖动时降低透明度
                "opacity-50": isDragging,
                // 拖动时禁用所有子元素的 hover 效果
                "[&_[role='separator']]:pointer-events-none [&_[role='separator']]:opacity-0": activeId !== null,
              })}
              style={{
                ...getCommonPinningStyles({ column: header.column }),
                width: `calc(var(--header-${header.id}-size) * 1px)`,
              }}
            >
              {header.isPlaceholder ? null : (
                <SortableColumnHeader
                  header={header}
                  table={table}
                  isAnyDragging={activeId !== null}
                />
              )}
            </div>
          );
        })}
      </SortableContext>
      
      {/* 添加列按钮 */}
      {onAddColumnClick && (
        <div
          ref={addColumnTriggerRef}
          role="columnheader"
          aria-colindex={headers.length + 1}
          data-slot="grid-header-cell-add-column"
          tabIndex={-1}
          className="relative border-r"
          style={{
            width: '70px',
            minWidth: '70px',
          }}
        >
          <div
            className="flex size-full items-center justify-center p-2 text-sm hover:bg-accent/40 cursor-pointer transition-colors"
            onClick={onAddColumnClick}
            onKeyDown={(e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                onAddColumnClick();
              }
            }}
            tabIndex={0}
            role="button"
            aria-label="Add column"
          >
            <Plus className="size-4 text-muted-foreground" />
          </div>
        </div>
      )}
      
      <DragOverlay
        style={{
          top: gridContainerTop !== null ? `${gridContainerTop}px` : undefined,
          zIndex: 9999, // 确保在 add row 之上
        }}
        className="!translate-y-0"
      >
        {activeHeader ? (
          <div 
            style={{
              width: activeColumnWidth,
              height: `${gridHeight}px`,
              backgroundColor: 'rgba(0, 0, 0, 0.2)', // 参考 aitable: columnDraggingPlaceholderBg
              boxSizing: 'border-box',
              padding: 0,
              margin: 0,
              border: 'none',
            }}
          />
        ) : null}
      </DragOverlay>
    </DndContext>
  );
}

