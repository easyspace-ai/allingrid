"use client";

import { faker } from "@faker-js/faker";
import type { ColumnDef } from "@tanstack/react-table";
import * as React from "react";
import { DataGrid } from "@/components/data-grid/data-grid";
import { DataGridKeyboardShortcuts } from "@/components/data-grid/data-grid-keyboard-shortcuts";
import { DataGridToolbar } from "@/components/data-grid/data-grid-toolbar";
import { useDataGrid } from "@/hooks/use-data-grid";
import type { FilterCondition } from "@/components/data-grid/data-grid-filter-menu";
import { applyFilters } from "@/utils/data-grid-filter";

interface SkateTrick {
  id: string;
  trickName?: string;
  skaterName?: string;
  difficulty?: "beginner" | "intermediate" | "advanced" | "expert";
  variant?: "flip" | "grind" | "grab" | "transition" | "manual" | "slide";
  landed?: boolean;
  attempts?: number;
  bestScore?: number;
  location?: string;
  dateAttempted?: string;
  attachments?: Array<{ name: string; url: string }>;
}

const skateSpots = [
  "Venice Beach Skate Park",
  "Burnside Skate Park",
  "Love Park (Philadelphia)",
  "MACBA (Barcelona)",
  "Southbank (London)",
  "FDR Skate Park",
  "Brooklyn Banks",
  "El Toro High School",
  "Hubba Hideout",
  "Wallenberg High School",
  "EMB (Embarcadero)",
  "Pier 7 (San Francisco)",
] as const;

const skateTricks = {
  flip: [
    "Kickflip",
    "Heelflip",
    "Tre Flip",
    "Hardflip",
    "Inward Heelflip",
    "Frontside Flip",
    "Backside Flip",
    "Varial Flip",
    "Varial Heelflip",
    "Double Flip",
    "Laser Flip",
    "Anti-Casper Flip",
    "Casper Flip",
    "Impossible",
    "360 Flip",
    "Big Spin",
    "Bigspin Flip",
  ],
  grind: [
    "50-50 Grind",
    "5-0 Grind",
    "Nosegrind",
    "Crooked Grind",
    "Feeble Grind",
    "Smith Grind",
    "Lipslide",
    "Boardslide",
    "Tailslide",
    "Noseslide",
    "Bluntslide",
    "Nollie Backside Lipslide",
    "Switch Frontside Boardslide",
  ],
  grab: [
    "Indy Grab",
    "Melon Grab",
    "Stalefish",
    "Tail Grab",
    "Nose Grab",
    "Method",
    "Mute Grab",
    "Crail Grab",
    "Seatbelt Grab",
    "Roast Beef",
    "Chicken Wing",
    "Tweaked Indy",
    "Japan Air",
  ],
  transition: [
    "Frontside Air",
    "Backside Air",
    "McTwist",
    "540",
    "720",
    "900",
    "Frontside 180",
    "Backside 180",
    "Frontside 360",
    "Backside 360",
    "Alley-Oop",
    "Fakie",
    "Revert",
    "Carve",
    "Pump",
    "Drop In",
  ],
  manual: [
    "Manual",
    "Nose Manual",
    "Casper",
    "Rail Stand",
    "Pogo",
    "Handstand",
    "One Foot Manual",
    "Spacewalk",
    "Truckstand",
    "Primo",
  ],
  slide: [
    "Powerslide",
    "Bert Slide",
    "Coleman Slide",
    "Pendulum Slide",
    "Stand-up Slide",
    "Toeside Slide",
    "Heelside Slide",
  ],
} as const;

function generateTrickData(): SkateTrick[] {
  return Array.from({ length: 30 }, (_, index) => {
    const variant = faker.helpers.arrayElement(
      Object.keys(skateTricks) as Array<keyof typeof skateTricks>,
    );
    const trickName = faker.helpers.arrayElement(skateTricks[variant]);
    const skaterName = faker.person.fullName();
    const attempts = faker.number.int({ min: 1, max: 50 });
    const landed = faker.datatype.boolean(0.6);

    const getDifficulty = (trick: string): SkateTrick["difficulty"] => {
      const expertTricks = [
        "Tre Flip",
        "900",
        "McTwist",
        "Laser Flip",
        "Impossible",
      ];
      const advancedTricks = [
        "Hardflip",
        "720",
        "540",
        "Crooked Grind",
        "Switch Frontside Boardslide",
      ];
      const intermediateTricks = [
        "Kickflip",
        "Heelflip",
        "Frontside 180",
        "50-50 Grind",
        "Boardslide",
      ];

      if (expertTricks.some((t) => trick.includes(t))) return "expert";
      if (advancedTricks.some((t) => trick.includes(t))) return "advanced";
      if (intermediateTricks.some((t) => trick.includes(t)))
        return "intermediate";
      return "beginner";
    };

    const difficulty = getDifficulty(trickName);

    // 为前几行添加示例附件数据
    const attachments: Array<{ name: string; url: string }> = [];
    if (index < 5) {
      // 前5行添加一些示例附件
      const fileNames = [
        "trick-analysis.pdf",
        "performance-review.jpg",
        "competition-video.mp4",
        "training-notes.pdf",
      ];
      const count = faker.number.int({ min: 0, max: 2 });
      for (let i = 0; i < count; i++) {
        const fileName = faker.helpers.arrayElement(fileNames);
        attachments.push({
          name: fileName,
          url: `https://example.com/files/${fileName}`, // 示例URL
        });
      }
    }

    return {
      id: faker.string.nanoid(),
      trickName,
      skaterName,
      difficulty,
      variant,
      landed,
      attempts,
      bestScore: landed
        ? faker.number.int({ min: 6, max: 10 })
        : faker.number.int({ min: 1, max: 5 }),
      location: faker.helpers.arrayElement(skateSpots),
      dateAttempted:
        faker.date
          .between({
            from: new Date(2023, 0, 1),
            to: new Date(),
          })
          .toISOString()
          .split("T")[0] ?? "",
      attachments: attachments.length > 0 ? attachments : undefined,
    };
  });
}

const initialData: SkateTrick[] = generateTrickData();

export default function DataGridDemo() {
  const [data, setData] = React.useState<SkateTrick[]>(initialData);
  const [filterConditions, setFilterConditions] = React.useState<FilterCondition[]>([]);
  const [columns, setColumns] = React.useState<ColumnDef<SkateTrick>[]>(
    () => [
      {
        id: "trickName",
        accessorKey: "trickName",
        header: "Trick name",
        meta: {
          cell: {
            variant: "short-text",
          },
        },
        minSize: 180,
      },
      {
        id: "skaterName",
        accessorKey: "skaterName",
        header: "Skater",
        meta: {
          cell: {
            variant: "short-text",
          },
        },
        minSize: 150,
      },
      {
        id: "difficulty",
        accessorKey: "difficulty",
        header: "Difficulty",
        meta: {
          cell: {
            variant: "select",
            options: [
              { label: "Beginner", value: "beginner" },
              { label: "Intermediate", value: "intermediate" },
              { label: "Advanced", value: "advanced" },
              { label: "Expert", value: "expert" },
            ],
          },
        },
        minSize: 120,
      },
      {
        id: "variant",
        accessorKey: "variant",
        header: "Category",
        meta: {
          cell: {
            variant: "select",
            options: [
              { label: "Flip", value: "flip" },
              { label: "Grind", value: "grind" },
              { label: "Grab", value: "grab" },
              { label: "Transition", value: "transition" },
              { label: "Manual", value: "manual" },
              { label: "Slide", value: "slide" },
            ],
          },
        },
        minSize: 120,
      },
      {
        id: "landed",
        accessorKey: "landed",
        header: "Landed",
        meta: {
          cell: {
            variant: "checkbox",
          },
        },
        minSize: 100,
      },
      {
        id: "attempts",
        accessorKey: "attempts",
        header: "Attempts",
        meta: {
          cell: {
            variant: "number",
            min: 1,
            max: 100,
          },
        },
        minSize: 100,
      },
      {
        id: "bestScore",
        accessorKey: "bestScore",
        header: "Score",
        meta: {
          cell: {
            variant: "number",
            min: 1,
            max: 10,
          },
        },
        minSize: 110,
      },
      {
        id: "location",
        accessorKey: "location",
        header: "Location",
        meta: {
          cell: {
            variant: "select",
            options: skateSpots.map((spot) => ({ label: spot, value: spot })),
          },
        },
        minSize: 180,
      },
      {
        id: "dateAttempted",
        accessorKey: "dateAttempted",
        header: "Attempted at",
        meta: {
          cell: {
            variant: "date",
          },
        },
        minSize: 130,
      },
      {
        id: "attachments",
        accessorKey: "attachments",
        header: "附件",
        meta: {
          cell: {
            variant: "attachment",
          },
        },
        minSize: 200,
        size: 200,
      },
    ],
  );

  const onRowAdd = React.useCallback(() => {
    setData((prev) => [...prev, { id: faker.string.nanoid() }]);

    return {
      rowIndex: data.length,
      columnId: "trickName",
    };
  }, [data.length]);

  const onAddColumn = React.useCallback(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (columnConfig: { type: string; name?: string; options?: any }) => {
      const columnId = `column_${Date.now()}`;
      const columnName = columnConfig.name || `New Column ${columns.length + 1}`;
      
      // 映射字段类型到列定义
      const newColumn: ColumnDef<SkateTrick> = {
        id: columnId,
        accessorFn: (row) => {
          // 使用动态访问，因为新列的 key 可能不在类型定义中
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return (row as any)[columnId];
        },
        header: columnName,
        meta: {
          cell: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            variant: columnConfig.type as any,
            ...(columnConfig.options?.options && {
              options: columnConfig.options.options.map((opt: { name: string }) => ({
                label: opt.name,
                value: opt.name,
              })),
            }),
            ...(columnConfig.options?.expression && {
              expression: columnConfig.options.expression,
            }),
            ...(columnConfig.options?.task && {
              task: columnConfig.options.task,
              prompt: columnConfig.options.prompt,
              dependencies: columnConfig.options.dependencies,
              trigger: columnConfig.options.trigger,
              cache: columnConfig.options.cache,
            }),
          },
        },
        minSize: 150,
        size: 150, // 设置初始宽度
      };

      setColumns((prev) => [...prev, newColumn]);
    },
    [columns.length],
  );

  // 应用筛选条件
  const filteredData = React.useMemo(() => {
    return applyFilters({
      data,
      conditions: filterConditions,
      getFieldValue: (row, fieldId) => {
        // 处理动态列和静态列
        if (fieldId in row) {
          return (row as unknown as Record<string, unknown>)[fieldId];
        }
        // 对于动态添加的列，尝试从 accessorFn 获取
        const column = columns.find((col) => col.id === fieldId);
        if (column && "accessorFn" in column && column.accessorFn) {
          return column.accessorFn(row, 0);
        }
        return undefined;
      },
    });
  }, [data, filterConditions, columns]);

  const { table, ...dataGridProps } = useDataGrid({
    columns,
    data: filteredData,
    onDataChange: setData,
    onRowAdd,
    onAddColumn,
    enableSearch: true,
  });

  return (
    <>
      <DataGridKeyboardShortcuts enableSearch={!!dataGridProps.searchState} />
      <div className="flex flex-col rounded-md border">
        <DataGridToolbar
          table={table}
          filterConditions={filterConditions}
          onFilterConditionsChange={setFilterConditions}
        />
        <DataGrid {...dataGridProps} table={table} height={340} />
      </div>
    </>
  );
}
