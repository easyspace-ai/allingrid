/**
 * AI 字段 Hook
 * 
 * 功能：
 * - 管理 AI 字段的状态（idle, generating, success, error）
 * - 处理依赖字段变化
 * - 调用 AI Service 生成内容
 * - 缓存管理
 */

import * as React from "react";
import type { Cell, Table } from "@tanstack/react-table";
import { defaultAIService, type AIRequest, type AIResponse } from "@/services/ai-service";

export type AIFieldState = "idle" | "generating" | "success" | "error" | "cached";

export interface UseAIFieldOptions<TData> {
  cell: Cell<TData, unknown>;
  table: Table<TData>;
  rowIndex: number;
  columnId: string;
}

export interface UseAIFieldReturn {
  state: AIFieldState;
  result: string | null;
  error: Error | null;
  generate: () => Promise<void>;
  regenerate: () => Promise<void>;
  clear: () => void;
}

/**
 * AI 字段 Hook
 */
export function useAIField<TData>({
  cell,
  table,
  rowIndex,
  columnId,
}: UseAIFieldOptions<TData>): UseAIFieldReturn {
  const [state, setState] = React.useState<AIFieldState>("idle");
  const [result, setResult] = React.useState<string | null>(null);
  const [error, setError] = React.useState<Error | null>(null);

  const cellOpts = cell.column.columnDef.meta?.cell;
  const aiConfig = cellOpts?.variant === "ai" ? cellOpts : null;

  // 获取当前行的数据
  const row = table.getRowModel().rows[rowIndex];
  const rowData = row?.original;

  // 获取依赖字段的值
  const getDependencyValues = React.useCallback(() => {
    if (!aiConfig?.dependencies || aiConfig.dependencies.length === 0) {
      return {};
    }

    const columns = table.getAllColumns();
    const values: Record<string, any> = {};

    for (const dep of aiConfig.dependencies) {
      // 查找对应的列（支持列头名称、列ID或meta.label）
      const column = columns.find((col) => {
        const colLabel = col.columnDef.meta?.label;
        const colHeader = typeof col.columnDef.header === "string" ? col.columnDef.header : null;
        return colLabel === dep || colHeader === dep || col.id === dep;
      });

      if (column) {
        const cellValue = row?.getValue(column.id);
        values[dep] = cellValue;
      }
    }

    return values;
  }, [aiConfig, table, row]);

  // 构建 Prompt
  const buildPrompt = React.useCallback(() => {
    if (!aiConfig) return "";

    const dependencies = getDependencyValues();
    const context = Object.entries(dependencies)
      .map(([key, value]) => {
        if (value === null || value === undefined) return "";
        return `${key}: ${String(value)}`;
      })
      .filter(Boolean)
      .join("\n");

    const task = aiConfig.task || "custom";
    const customPrompt = aiConfig.prompt || "";

    // 根据任务类型构建不同的 prompt
    switch (task) {
      case "generate":
        return customPrompt || `基于以下信息生成内容：\n${context}`;
      case "summarize":
        return `总结以下内容：\n${context}`;
      case "extract":
        return `从以下文本中提取关键信息：\n${context}`;
      case "translate":
        return `将以下内容翻译成中文：\n${context}`;
      case "classify":
        return `将以下内容分类：\n${context}`;
      case "custom":
        // 替换自定义 prompt 中的 {字段名} 占位符
        let prompt = customPrompt;
        for (const [key, value] of Object.entries(dependencies)) {
          prompt = prompt.replace(new RegExp(`\\{${key}\\}`, "g"), String(value || ""));
        }
        return prompt || `处理以下内容：\n${context}`;
      default:
        return customPrompt || `处理以下内容：\n${context}`;
    }
  }, [aiConfig, getDependencyValues]);

  // 生成缓存键
  const getCacheKey = React.useCallback(() => {
    const dependencies = getDependencyValues();
    const prompt = buildPrompt();
    return `${columnId}:${rowIndex}:${JSON.stringify(dependencies)}:${prompt}`;
  }, [columnId, rowIndex, getDependencyValues, buildPrompt]);

  // 检查缓存
  const checkCache = React.useCallback(() => {
    if (!aiConfig?.cache) return null;

    const cacheKey = getCacheKey();
    const cached = sessionStorage.getItem(cacheKey);
    if (cached) {
      try {
        const parsed = JSON.parse(cached);
        return parsed as AIResponse;
      } catch {
        return null;
      }
    }
    return null;
  }, [aiConfig, getCacheKey]);

  // 保存到缓存
  const saveCache = React.useCallback(
    (response: AIResponse) => {
      if (!aiConfig?.cache) return;

      const cacheKey = getCacheKey();
      sessionStorage.setItem(cacheKey, JSON.stringify(response));
    },
    [aiConfig, getCacheKey],
  );

  // 生成内容
  const generate = React.useCallback(async () => {
    if (!aiConfig) return;

    // 检查缓存
    const cached = checkCache();
    if (cached) {
      setResult(cached.content);
      setState("cached");
      setError(null);
      return;
    }

    setState("generating");
    setError(null);

    try {
      const prompt = buildPrompt();
      const dependencies = getDependencyValues();

      const request: AIRequest = {
        prompt,
        context: dependencies,
      };

      const aiService = defaultAIService; // 使用默认的 Mock Service
      const response = await aiService.generate(request);

      setResult(response.content);
      setState("success");
      setError(null);

      // 保存到缓存
      saveCache(response);

      // 更新单元格值
      const meta = table.options.meta;
      meta?.onDataUpdate?.({
        rowIndex,
        columnId,
        value: response.content,
      });
    } catch (err) {
      const error = err instanceof Error ? err : new Error("生成失败");
      setError(error);
      setState("error");
      setResult(null);
    }
  }, [aiConfig, buildPrompt, getDependencyValues, checkCache, saveCache, table, rowIndex, columnId]);

  // 重新生成
  const regenerate = React.useCallback(async () => {
    // 清除缓存
    const cacheKey = getCacheKey();
    sessionStorage.removeItem(cacheKey);
    await generate();
  }, [generate, getCacheKey]);

  // 清除结果
  const clear = React.useCallback(() => {
    setState("idle");
    setResult(null);
    setError(null);
  }, []);

  // 初始化：根据触发模式自动生成
  React.useEffect(() => {
    if (!aiConfig) return;

    const trigger = aiConfig.trigger || "manual";
    const currentValue = cell.getValue();

    // 如果已经有值且是缓存状态，不自动生成
    if (currentValue && state === "cached") {
      setResult(String(currentValue));
      return;
    }

    // 如果是自动触发且有依赖字段，监听依赖字段变化
    if (trigger === "auto" && aiConfig.dependencies && aiConfig.dependencies.length > 0) {
      generate();
    } else if (trigger === "on-create" && !currentValue) {
      // 仅在创建时生成
      generate();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // 只在组件挂载时执行一次

  // 监听依赖字段变化（自动触发模式）
  React.useEffect(() => {
    if (!aiConfig || aiConfig.trigger !== "auto") return;

    const dependencies = getDependencyValues();
    const hasDependencies = Object.keys(dependencies).length > 0;

    if (hasDependencies) {
      generate();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rowData, aiConfig?.trigger]); // 依赖 rowData 变化

  return {
    state,
    result,
    error,
    generate,
    regenerate,
    clear,
  };
}

