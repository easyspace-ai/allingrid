"use client";

import type { ColumnDef } from "@tanstack/react-table";
import * as React from "react";
import { DataGrid } from "@/components/data-grid/data-grid";
import { DataGridKeyboardShortcuts } from "@/components/data-grid/data-grid-keyboard-shortcuts";
import { DataGridToolbar } from "@/components/data-grid/data-grid-toolbar";
import { AddRecordDialog } from "@/components/data-grid/add-record-dialog";
import { useDataGrid } from "@/hooks/use-data-grid";
import type { FilterCondition } from "@/components/data-grid/data-grid-filter-menu";
import { applyFilters } from "@/utils/data-grid-filter";
import { Login } from "@/components/auth/login";
import { luckdbClient } from "@/config/client";
import { recordService, transformRecordToTableData } from "@/services/recordService";
import { mapFieldTypeToCellVariant, mapFieldOptionsToCellOptions } from "@/services/fieldMapper";
import { buildCellType } from "@/services/cellTypeHelper";
import { useShareDBSync } from "@/hooks/use-sharedb-sync";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { testTableConfig } from "@/config/testTable";
import type { Field } from "luckdb-sdk";

interface TableRecord {
  id: string;
  [key: string]: unknown;
}

export default function DataGridDemo() {
  const [isAuthenticated, setIsAuthenticated] = React.useState(false);
  const [isLoading, setIsLoading] = React.useState(true);
  const [data, setData] = React.useState<TableRecord[]>([]);
  const [columns, setColumns] = React.useState<ColumnDef<TableRecord>[]>([]);
  const [filterConditions, setFilterConditions] = React.useState<FilterCondition[]>([]);
  const [tableId, setTableId] = React.useState<string | null>(testTableConfig.tableId);
  const [fieldMapping, setFieldMapping] = React.useState<Map<string, string>>(new Map()); // fieldId -> columnId
  const [columnToFieldMapping, setColumnToFieldMapping] = React.useState<Map<string, string>>(new Map()); // columnId -> fieldId
  const [, setRecordVersions] = React.useState<Map<string, number>>(new Map()); // recordId -> version
  const [fields, setFields] = React.useState<Field[]>([]); // 保存字段列表
  const [isAddRecordDialogOpen, setIsAddRecordDialogOpen] = React.useState(false);

  // 检查认证状态
  React.useEffect(() => {
    const checkAuth = async () => {
      console.log("[checkAuth] 开始检查认证状态");
      
      // 检查 token 是否存在
      const token = luckdbClient.authStore.token;
      console.log("[checkAuth] Token 状态:", {
        hasToken: !!token,
        tokenLength: token?.length || 0,
        isValid: luckdbClient.authStore.isValid,
      });
      
      try {
        const authResponse = await luckdbClient.auth.getCurrentUser();
        console.log("[checkAuth] 获取当前用户响应:", authResponse);
        const user = authResponse?.record;
        console.log("[checkAuth] 获取当前用户:", user ? "已登录" : "未登录");
        if (user) {
          console.log("[checkAuth] 用户信息:", { id: user.id, email: user.email });
          setIsAuthenticated(true);
          // 使用测试表格配置
          const testTableId = testTableConfig.tableId;
          console.log("[checkAuth] 设置 tableId:", testTableId);
          setTableId(testTableId);
        } else {
          setIsAuthenticated(false);
        }
      } catch (error) {
        console.error("[checkAuth] 认证检查失败:", error);
        if (error instanceof Error) {
          console.error("[checkAuth] 错误详情:", {
            message: error.message,
            name: error.name,
          });
        }
        setIsAuthenticated(false);
      } finally {
        setIsLoading(false);
      }
    };
    checkAuth();
  }, []);

  // 加载表格和字段
  const loadTableAndFields = React.useCallback(async () => {
    if (!tableId) {
      console.warn("[loadTableAndFields] tableId is empty");
      return;
    }

    console.log("[loadTableAndFields] 开始加载字段，tableId:", tableId);

    try {
      // 使用 getFullList 获取所有字段（与 10-test-table.ts 保持一致）
      const fields = await luckdbClient.fields.getFullList(tableId);
      
      console.log("[loadTableAndFields] 获取到字段数量:", fields.length);
      console.log("[loadTableAndFields] 字段列表:", fields.map((f: { id: string; name: string; type: string }) => ({ id: f.id, name: f.name, type: f.type })));

      if (fields.length === 0) {
        console.warn("[loadTableAndFields] 字段列表为空");
        toast.warning("表格中没有字段");
        return;
      }

      // 保存字段列表
      setFields(fields as Field[]);

      // 创建字段映射
      const newFieldMapping = new Map<string, string>();
      const newColumnToFieldMapping = new Map<string, string>();
      
      // 将字段转换为列定义
      const newColumns: ColumnDef<TableRecord>[] = fields.map((field: { id: string; name: string; type: string; options?: unknown }) => {
        const columnId = field.id; // 使用字段 ID 作为列 ID
        newFieldMapping.set(field.id, columnId);
        newColumnToFieldMapping.set(columnId, field.id);

        const cellVariant = mapFieldTypeToCellVariant(field.type as string, field.options);
        const cellOptions = mapFieldOptionsToCellOptions(field.options as { choices?: Array<{ id: string; name: string }>; min?: number; max?: number; expression?: string });

        // 构建单元格配置
        const cellConfig = buildCellType(cellVariant, cellOptions);

        return {
          id: columnId,
          accessorKey: columnId,
          header: field.name,
          meta: {
            cell: cellConfig,
          },
          minSize: 150,
        };
      });

      console.log("[loadTableAndFields] 创建了", newColumns.length, "列");

      setColumns(newColumns);
      setFieldMapping(newFieldMapping);
      setColumnToFieldMapping(newColumnToFieldMapping);
      
      console.log("[loadTableAndFields] 字段加载完成");
    } catch (error: unknown) {
      console.error("[loadTableAndFields] 加载字段失败:", error);
      if (error instanceof Error) {
        console.error("[loadTableAndFields] 错误详情:", {
          message: error.message,
          stack: error.stack,
        });
      }
      const message = error instanceof Error ? error.message : "未知错误";
      toast.error(`加载表格失败: ${message}`);
    }
  }, [tableId]);

  // 使用 useMemo 计算字段映射的哈希，避免直接依赖 Map 对象
  const fieldMappingKey = React.useMemo(() => {
    const keys = Array.from(fieldMapping.keys()).sort();
    return keys.join(',');
  }, [fieldMapping]);

  // 加载记录数据（使用稳定的 fieldMappingKey 依赖）
  const loadRecords = React.useCallback(async () => {
    if (!tableId) {
      console.warn("[loadRecords] tableId is empty");
      return;
    }

    // 检查字段映射是否为空
    if (fieldMapping.size === 0) {
      console.warn("[loadRecords] fieldMapping is empty, skipping record load");
      return;
    }

    try {
      setIsLoading(true);
      console.log("[loadRecords] 开始加载记录, tableId:", tableId);
      console.log("[loadRecords] 字段映射状态:", {
        mappingSize: fieldMapping.size,
        fieldIds: Array.from(fieldMapping.keys()),
        columnIds: Array.from(fieldMapping.values()),
      });
      
      const records = await recordService.getAll(tableId);
      console.log("[loadRecords] 获取到原始记录数:", records.length);
      console.log("[loadRecords] 原始记录 IDs:", records.map(r => r.id));
      
      // 转换记录格式为表格数据格式
      const tableData = records.map((record) => {
        const transformedData = transformRecordToTableData<TableRecord>(record, fieldMapping);
        // 保存版本号用于乐观锁
        setRecordVersions((prev) => {
          const next = new Map(prev);
          next.set(record.id, record.version || 0);
          return next;
        });
        return transformedData;
      });

      console.log("[loadRecords] 转换后数据数:", tableData.length);
      console.log("[loadRecords] 转换后数据 IDs:", tableData.map(d => d.id));
      
      // 验证数据完整性和唯一性
      const uniqueIds = new Set(tableData.map(d => d.id));
      if (uniqueIds.size !== tableData.length) {
        console.error("[loadRecords] 警告：存在重复的 ID！", {
          total: tableData.length,
          unique: uniqueIds.size,
          duplicates: tableData.length - uniqueIds.size,
        });
      }
      
      if (tableData.length !== records.length) {
        console.error("[loadRecords] 错误：数据转换后数量不匹配！", {
          original: records.length,
          transformed: tableData.length,
        });
      }

      setData(tableData);
      console.log("[loadRecords] 数据已设置到状态，最终数据数:", tableData.length);
    } catch (error: unknown) {
      console.error("[loadRecords] 加载记录失败:", error);
      const message = error instanceof Error ? error.message : "未知错误";
      toast.error(`加载记录失败: ${message}`);
    } finally {
      setIsLoading(false);
    }
  }, [tableId, fieldMapping]);

  // 当表格 ID 或字段映射变化时，加载数据
  React.useEffect(() => {
    console.log("[useEffect] 触发字段加载检查:", { tableId, isAuthenticated });
    if (tableId && isAuthenticated) {
      console.log("[useEffect] 条件满足，调用 loadTableAndFields");
      loadTableAndFields();
    } else {
      console.log("[useEffect] 条件不满足，跳过加载:", { tableId: !!tableId, isAuthenticated });
    }
  }, [tableId, isAuthenticated, loadTableAndFields]);

  React.useEffect(() => {
    if (tableId && isAuthenticated && fieldMappingKey) {
      loadRecords();
    }
  }, [tableId, isAuthenticated, fieldMappingKey, loadRecords]);

  // ShareDB 同步：处理记录更新（添加防重复更新检查）
  const lastUpdateRef = React.useRef<Map<string, string>>(new Map()); // 记录上次更新的数据哈希
  const handleRecordUpdate = React.useCallback(
    (recordId: string, shareDBData: { data?: Record<string, unknown> }) => {
      // ShareDB 数据格式：{ data: { [fieldId]: value, ... } }
      
      console.log(`[handleRecordUpdate] 收到更新，recordId: ${recordId}`, {
        shareDBData,
        fieldMappingSize: fieldMapping.size,
        fieldMappingEntries: Array.from(fieldMapping.entries()),
      });
      
      // 计算当前数据的哈希，用于检查是否真正有变化
      const shareDBFields = shareDBData.data || {};
      const dataHash = JSON.stringify(shareDBFields);
      const lastHash = lastUpdateRef.current.get(recordId);
      const hashMatch = lastHash === dataHash;
      
      // 注意：即使哈希匹配，我们也应该检查字段值是否有实际变化
      // 因为哈希可能因为对象键的顺序而不同，但实际值可能相同
      // 但我们仍然继续处理，让 setData 内部的比较来决定是否更新
      
      console.log(`[handleRecordUpdate] 应用更新，recordId: ${recordId}`, {
        shareDBFields,
        fieldMappingSize: fieldMapping.size,
        lastHash,
        dataHash,
        hashMatch,
      });
      
      // 保存当前哈希状态，供 setData 内部使用
      const currentHashRef = { hashMatch, dataHash, lastHash };
      
      // 更新哈希（在 setData 调用之前，但允许 setData 决定是否实际更新）
      lastUpdateRef.current.set(recordId, dataHash);
      
      // 使用函数式更新，确保拿到最新的 state
      setData((prev) => {
        const index = prev.findIndex((row) => row.id === recordId);
        
        console.log(`[handleRecordUpdate] setData 开始`, {
          recordId,
          index,
          shareDBFields,
          fieldMappingSize: fieldMapping.size,
          prevDataLength: prev.length,
          prevData: prev.find((r) => r.id === recordId),
        });
        
        if (index === -1) {
          // 新记录，添加到列表
          console.log(`[handleRecordUpdate] 新记录，添加到列表`);
          const newRow: TableRecord = { id: recordId };
          if (fieldMapping.size > 0) {
            fieldMapping.forEach((columnId, fieldId) => {
              if (shareDBFields[fieldId] !== undefined) {
                newRow[columnId] = shareDBFields[fieldId];
                console.log(`[handleRecordUpdate] 新记录字段映射: fieldId=${fieldId}, columnId=${columnId}, value=`, shareDBFields[fieldId]);
              }
            });
          } else {
            // 如果 fieldMapping 为空，直接使用 fieldId 作为 columnId
            Object.keys(shareDBFields).forEach((fieldId) => {
              newRow[fieldId] = shareDBFields[fieldId];
              console.log(`[handleRecordUpdate] 新记录直接字段: fieldId=${fieldId}, value=`, shareDBFields[fieldId]);
            });
          }
          console.log(`[handleRecordUpdate] 新记录完整数据:`, newRow);
          const newData = [...prev, newRow];
          console.log(`[handleRecordUpdate] 返回新数据数组，长度:`, newData.length);
          return newData;
        }

        // 更新现有记录
        const existingRow = prev[index];
        
        console.log(`[handleRecordUpdate] 现有记录，准备更新`, {
          existingRow,
          shareDBFields,
          existingRowKeys: Object.keys(existingRow),
          shareDBFieldsKeys: Object.keys(shareDBFields),
        });
        
        // 检查数据是否真正有变化
        let hasChanges = false;
        const updatedRow: TableRecord = { ...existingRow };
        const changes: Array<{ fieldId: string; columnId: string; oldValue: unknown; newValue: unknown }> = [];

        // 应用 ShareDB 数据更新
        if (fieldMapping.size === 0) {
          console.warn(`[handleRecordUpdate] fieldMapping 为空，尝试直接更新字段`);
          // 如果 fieldMapping 为空，假设 fieldId === columnId（在代码中确实是这样设计的）
          Object.keys(shareDBFields).forEach((fieldId) => {
            const newValue = shareDBFields[fieldId];
            const oldValue = updatedRow[fieldId];
            // 使用严格比较，但允许 null/undefined 相等
            const isEqual = oldValue === newValue || (oldValue == null && newValue == null);
            if (!isEqual) {
              hasChanges = true;
              updatedRow[fieldId] = newValue;
              changes.push({ fieldId, columnId: fieldId, oldValue, newValue });
              console.log(`[handleRecordUpdate] 字段变化（直接更新）: fieldId=${fieldId}, oldValue=`, oldValue, `newValue=`, newValue);
            } else {
              console.log(`[handleRecordUpdate] 字段无变化（直接更新）: fieldId=${fieldId}, oldValue=`, oldValue, `newValue=`, newValue);
            }
          });
        } else {
          // 使用 fieldMapping 进行更新
          fieldMapping.forEach((columnId, fieldId) => {
            if (shareDBFields[fieldId] !== undefined) {
              const newValue = shareDBFields[fieldId];
              const oldValue = updatedRow[columnId];
              // 使用严格比较，但允许 null/undefined 相等
              const isEqual = oldValue === newValue || (oldValue == null && newValue == null);
              if (!isEqual) {
                hasChanges = true;
                updatedRow[columnId] = newValue;
                changes.push({ fieldId, columnId, oldValue, newValue });
                console.log(`[handleRecordUpdate] 字段变化: fieldId=${fieldId}, columnId=${columnId}, oldValue=`, oldValue, `newValue=`, newValue);
              } else {
                console.log(`[handleRecordUpdate] 字段无变化: fieldId=${fieldId}, columnId=${columnId}, oldValue=`, oldValue, `newValue=`, newValue);
              }
            }
          });
        }

        console.log(`[handleRecordUpdate] 更新结果`, {
          hasChanges,
          changesCount: changes.length,
          changes,
          updatedRow,
          fieldMappingSize: fieldMapping.size,
          updatedRowKeys: Object.keys(updatedRow),
        });

        // 如果没有实际变化，我们仍然需要检查是否应该触发更新
        // 如果哈希不匹配（意味着 ShareDB 认为数据变化了），即使字段值看起来相同，也要强制更新
        if (!hasChanges) {
          // 如果哈希不匹配，说明 ShareDB 认为数据变化了，即使我们的比较没有发现变化
          // 也要强制更新以确保 UI 同步
          // 注意：我们在 setData 外部保存了哈希状态，这里需要通过闭包访问
          if (!currentHashRef.hashMatch) {
            console.log(`[handleRecordUpdate] 哈希不匹配但字段值相同，强制返回新数组以触发重新渲染`, {
              hashMatch: currentHashRef.hashMatch,
              lastHash: currentHashRef.lastHash,
              dataHash: currentHashRef.dataHash,
            });
            return prev.map((row, i) => {
              if (i === index) {
                // 创建新对象，确保 React 检测到更新
                return { ...updatedRow }
              }
              return row
            });
          } else {
            console.log(`[handleRecordUpdate] 确实没有变化（哈希和字段值都相同），返回原数组`);
            return prev;
          }
        }

        // 创建新数组并更新对应索引
        const updated = [...prev];
        updated[index] = updatedRow;
        console.log(`[handleRecordUpdate] 返回更新后的数组`, {
          updatedLength: updated.length,
          updatedRow,
          updatedRowKeys: Object.keys(updatedRow),
        });
        return updated;
      });
    },
    [fieldMapping]
  );

  // 使用 useMemo 稳定化 recordIds，避免因数组引用变化导致频繁重新订阅
  const recordIds = React.useMemo(() => {
    return data.map((row) => row.id);
  }, [data]);

  // ShareDB 同步 Hook
  const shareDBSync = useShareDBSync({
    tableId,
    recordIds,
    onRecordUpdate: handleRecordUpdate,
    enabled: isAuthenticated && tableId !== null,
  });

  // 调试 ShareDB 状态
  React.useEffect(() => {
    console.log('[product-demo] ShareDB 状态:', {
      hasShareDBSync: !!shareDBSync,
      isConnected: shareDBSync?.isConnected,
      subscribedCount: shareDBSync?.subscribedCount,
      isAuthenticated,
      tableId,
      enabled: isAuthenticated && tableId !== null,
    });
  }, [shareDBSync, isAuthenticated, tableId]);

  // 处理数据变更（用户编辑）
  const handleDataChange = React.useCallback(
    async (newData: TableRecord[]) => {
      if (!tableId) return;

      // 找出变更的记录
      const oldDataMap = new Map(data.map((row) => [row.id, row]));
      const updatedRecords: Array<{ id: string; fields: Record<string, unknown> }> = [];

      for (const newRow of newData) {
        const oldRow = oldDataMap.get(newRow.id);
        if (!oldRow) continue;

        // 检查是否有字段变更
        let hasChanges = false;
        const fields: Record<string, unknown> = {};

        columnToFieldMapping.forEach((fieldId, columnId) => {
          const newValue = newRow[columnId];
          const oldValue = oldRow[columnId];

          if (newValue !== oldValue) {
            hasChanges = true;
            fields[fieldId] = newValue;
          }
        });

        if (hasChanges) {
          updatedRecords.push({ id: newRow.id, fields });
        }
      }

      // 批量更新记录
      if (updatedRecords.length > 0) {
        // 保存原始数据以便失败时回滚
        const originalData = data;
        
        try {
          // 先立即更新本地状态（乐观更新）
          console.log('[handleDataChange] 乐观更新本地状态，记录数:', updatedRecords.length);
          setData(newData);
          
          // 然后通过 API 保存
          await recordService.batchUpdate(
            tableId,
            updatedRecords.map((record) => ({
              id: record.id,
              fields: record.fields,
            }))
          );

          console.log('[handleDataChange] API 保存成功，通过 ShareDB 同步更新');
          
          // API 保存成功后，通过 ShareDB 提交更新以确保实时同步
          // 即使服务器端也会自动广播，但我们在客户端也提交可以确保实时性
          if (shareDBSync && shareDBSync.isConnected) {
            try {
              // 为每个更新的记录提交 ShareDB 更新
              for (const record of updatedRecords) {
                const oldRow = oldDataMap.get(record.id);
                for (const [fieldId, newValue] of Object.entries(record.fields)) {
                  // 从旧数据中获取旧值（使用 fieldMapping 反向查找）
                  const columnId = Array.from(columnToFieldMapping.entries()).find(([, fid]) => fid === fieldId)?.[0];
                  const oldValue = oldRow && columnId ? oldRow[columnId] : undefined;
                  await shareDBSync.submitFieldUpdate(record.id, fieldId, newValue, oldValue);
                }
              }
              console.log('[handleDataChange] ShareDB 更新已提交');
            } catch (shareDBError) {
              console.warn('[handleDataChange] ShareDB 更新失败，但 API 已保存:', shareDBError);
              // ShareDB 更新失败不影响整体流程，因为 API 已保存
            }
          }
          
          // 更新版本号
          const updatedRecordsResponse = await recordService.getAll(tableId);
          setRecordVersions((prev) => {
            const next = new Map(prev);
            updatedRecordsResponse.forEach((record) => {
              next.set(record.id, record.version || 0);
            });
            return next;
          });

          toast.success(`已保存 ${updatedRecords.length} 条记录的更改`);
        } catch (error: unknown) {
          console.error("[handleDataChange] Failed to update records:", error);
          const message = error instanceof Error ? error.message : "未知错误";
          
          // 检查是否是冲突错误（版本号不匹配）
          const isConflictError = 
            error instanceof Error && 
            (error.message.includes('version') || 
             error.message.includes('conflict') || 
             error.message.includes('409'));
          
          if (isConflictError) {
            toast.error(`保存失败：数据冲突。请刷新页面获取最新数据。`, {
              duration: 5000,
            });
            
            // 恢复原数据
            console.log('[handleDataChange] 数据冲突，恢复原数据');
            setData(originalData);
            
            // 重新加载数据以获取最新版本
            loadRecords().catch((err) => {
              console.error("Failed to reload records after conflict:", err);
            });
          } else {
            toast.error(`保存失败: ${message}`);
            // 恢复原数据
            console.log('[handleDataChange] API 保存失败，恢复原数据');
            setData(originalData);
          }
        }
      }
    },
    [tableId, data, columnToFieldMapping, loadRecords, shareDBSync]
  );

  // 处理添加新行
  const onRowAdd = React.useCallback(async () => {
    if (!tableId) {
      toast.error("请先设置表格 ID");
      return {
        rowIndex: data.length,
        columnId: columns[0]?.id || "",
      };
    }

    try {
      // 创建空记录
      const newRecord = await recordService.create(tableId, { data: {} });
      
      // 添加到本地数据
      const newRow: TableRecord = { id: newRecord.id };
      setData((prev) => [...prev, newRow]);
      
      // 保存版本号
      setRecordVersions((prev) => {
        const next = new Map(prev);
        next.set(newRecord.id, newRecord.version || 0);
        return next;
      });

      // ShareDB Hook 会自动订阅新记录
      toast.success("已添加新记录");
      
      return {
        rowIndex: data.length,
        columnId: columns[0]?.id || "",
      };
    } catch (error: unknown) {
      console.error("Failed to create record:", error);
      const message = error instanceof Error ? error.message : "未知错误";
      toast.error(`创建记录失败: ${message}`);
      // 即使失败也返回有效值，避免类型错误
      return {
        rowIndex: data.length,
        columnId: columns[0]?.id || "",
      };
    }
  }, [tableId, data.length, columns]);

  // 处理删除行
  const onRowsDelete = React.useCallback(
    async (rows: TableRecord[]) => {
      if (!tableId) return;

      try {
        const recordIds = rows.map((row) => row.id);
        await recordService.batchDelete(tableId, recordIds);

        // 从本地数据中移除
        setData((prev) => prev.filter((row) => !recordIds.includes(row.id)));

        // 更新版本号映射
        setRecordVersions((prev) => {
          const next = new Map(prev);
          recordIds.forEach((id) => next.delete(id));
          return next;
        });

        toast.success(`已删除 ${recordIds.length} 条记录`);
      } catch (error: unknown) {
        console.error("Failed to delete records:", error);
        const message = error instanceof Error ? error.message : "未知错误";
        toast.error(`删除失败: ${message}`);
      }
    },
    [tableId]
  );

  // 处理添加列（创建新字段）
  const onAddColumn = React.useCallback(
    async (columnConfig: { type: string; name?: string; options?: Record<string, unknown> }) => {
      if (!tableId) {
        toast.error("请先设置表格 ID");
        return;
      }

      try {
        const fieldName = columnConfig.name || `新字段 ${columns.length + 1}`;
        
        // 映射单元格类型到字段类型
        let fieldType: string = "singleLineText";
        if (columnConfig.type === "number") fieldType = "number";
        else if (columnConfig.type === "checkbox") fieldType = "checkbox";
        else if (columnConfig.type === "select") fieldType = "singleSelect";
        else if (columnConfig.type === "date") fieldType = "date";

        // 创建字段
        const field = await luckdbClient.fields.create(tableId, {
          name: fieldName,
          type: fieldType as "singleLineText" | "number" | "checkbox" | "singleSelect" | "date",
          options: columnConfig.options,
        });

        // 添加到列定义
        const columnId = field.id;
        const cellVariant = mapFieldTypeToCellVariant(field.type, field.options);
        const cellOptions = mapFieldOptionsToCellOptions(field.options);

        // 构建单元格配置
        const cellConfig = buildCellType(cellVariant, cellOptions);

        const newColumn: ColumnDef<TableRecord> = {
          id: columnId,
          accessorKey: columnId,
          header: field.name,
          meta: {
            cell: cellConfig,
          },
          minSize: 150,
        };

        setColumns((prev) => [...prev, newColumn]);
        setFieldMapping((prev) => {
          const next = new Map(prev);
          next.set(field.id, columnId);
          return next;
        });
        setColumnToFieldMapping((prev) => {
          const next = new Map(prev);
          next.set(columnId, field.id);
          return next;
        });

        toast.success(`已添加字段: ${fieldName}`);
      } catch (error: unknown) {
        console.error("Failed to create field:", error);
        const message = error instanceof Error ? error.message : "未知错误";
        toast.error(`创建字段失败: ${message}`);
      }
    },
    [tableId, columns.length]
  );

  // 处理删除字段（删除列）
  const onDeleteField = React.useCallback(
    async (columnId: string) => {
      if (!tableId) {
        toast.error("请先设置表格 ID");
        return;
      }

      const fieldId = columnToFieldMapping.get(columnId);
      if (!fieldId) {
        toast.error("未找到对应的字段 ID");
        return;
      }

      try {
        // 调用 SDK 删除字段
        await luckdbClient.fields.delete(fieldId);

        // 从列定义中移除
        setColumns((prev) => prev.filter((col) => col.id !== columnId));

        // 更新字段映射
        setFieldMapping((prev) => {
          const next = new Map(prev);
          next.delete(fieldId);
          return next;
        });
        setColumnToFieldMapping((prev) => {
          const next = new Map(prev);
          next.delete(columnId);
          return next;
        });

        // 更新字段列表
        setFields((prev) => prev.filter((f) => f.id !== fieldId));

        // 从数据中移除该列的数据
        setData((prev) =>
          prev.map((row) => {
            const newRow = { ...row };
            delete newRow[columnId];
            return newRow;
          })
        );

        toast.success("已删除字段");
      } catch (error: unknown) {
        console.error("Failed to delete field:", error);
        const message = error instanceof Error ? error.message : "未知错误";
        toast.error(`删除字段失败: ${message}`);
      }
    },
    [tableId, columnToFieldMapping]
  );

  // 处理添加新记录（从表单对话框）
  const handleAddRecord = React.useCallback(
    async (formData: Record<string, unknown>) => {
      if (!tableId) {
        toast.error("请先设置表格 ID");
        throw new Error("表格 ID 未设置");
      }

      try {
        // 创建记录数据，将字段 ID 映射到数据
        const recordData: Record<string, unknown> = {};
        Object.keys(formData).forEach((fieldId) => {
          const value = formData[fieldId];
          // 过滤空值（空字符串、null、undefined）
          if (value !== "" && value !== null && value !== undefined) {
            recordData[fieldId] = value;
          }
        });

        // 创建记录
        const newRecord = await recordService.create(tableId, { data: recordData });

        // 转换记录格式为表格数据格式
        const newRow = transformRecordToTableData<TableRecord>(newRecord, fieldMapping);

        // 添加到本地数据
        setData((prev) => [...prev, newRow]);

        // 保存版本号
        setRecordVersions((prev) => {
          const next = new Map(prev);
          next.set(newRecord.id, newRecord.version || 0);
          return next;
        });

        toast.success("已成功添加新记录");
      } catch (error: unknown) {
        console.error("Failed to add record:", error);
        const message = error instanceof Error ? error.message : "未知错误";
        toast.error(`添加记录失败: ${message}`);
        throw error;
      }
    },
    [tableId, fieldMapping]
  );

  // 监听 data 变化，用于调试
  React.useEffect(() => {
    console.log("[product-demo] data 状态已更新，数据数:", data.length);
    console.log("[product-demo] data IDs:", data.map(d => d.id));
    if (data.length > 0) {
      console.log("[product-demo] 第一条数据示例:", data[0]);
    }
  }, [data]);

  // 应用筛选条件
  const filteredData = React.useMemo(() => {
    console.log("[filteredData] 开始筛选，原始数据数:", data.length);
    console.log("[filteredData] 原始数据 IDs:", data.map(d => d.id));
    console.log("[filteredData] 筛选条件数:", filterConditions.length);
    if (filterConditions.length > 0) {
      console.log("[filteredData] 筛选条件:", filterConditions);
    }
    
    const result = applyFilters({
      data,
      conditions: filterConditions,
      getFieldValue: (row, fieldId) => {
        if (fieldId in row) {
          return (row as unknown as Record<string, unknown>)[fieldId];
        }
        return undefined;
      },
    });
    
    console.log("[filteredData] 筛选后数据数:", result.length);
    console.log("[filteredData] 筛选后数据 IDs:", result.map(d => d.id));
    if (result.length !== data.length && filterConditions.length > 0) {
      console.log("[filteredData] 数据被筛选过滤，原始:", data.length, "筛选后:", result.length);
    } else if (result.length !== data.length && filterConditions.length === 0) {
      console.error("[filteredData] 警告：没有筛选条件但数据数量不匹配！", {
        original: data.length,
        filtered: result.length,
      });
    }
    
    return result;
  }, [data, filterConditions]);

  const dataGridResult = useDataGrid({
    columns,
    data: filteredData,
    onDataChange: handleDataChange,
    onRowAdd,
    onRowsDelete,
    onAddColumn,
    enableSearch: true,
  });

  const { table, ...dataGridProps } = dataGridResult;

  // 如果未认证，显示登录界面
  if (!isAuthenticated && !isLoading) {
    return <Login onLoginSuccess={() => setIsAuthenticated(true)} />;
  }

  // 如果没有表格 ID，显示提示
  if (!tableId) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <div className="text-center space-y-4">
          <h2 className="text-2xl font-bold">未配置表格 ID</h2>
          <p className="text-muted-foreground">
            请在环境变量中设置 VITE_TABLE_ID，或修改代码手动设置表格 ID
          </p>
        </div>
      </div>
    );
  }

  // 如果正在加载，显示加载状态
  if (isLoading && data.length === 0) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <div className="text-center space-y-4">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 mx-auto"></div>
          <p>加载中...</p>
        </div>
      </div>
    );
  }

  return (
    <>
      <DataGridKeyboardShortcuts enableSearch={!!dataGridProps.searchState} />
      <AddRecordDialog
        open={isAddRecordDialogOpen}
        onOpenChange={setIsAddRecordDialogOpen}
        fields={fields}
        onSubmit={handleAddRecord}
      />
      <div className="flex flex-col rounded-md border">
        <div className="flex items-center justify-between border-b bg-background px-4 py-2 gap-4" data-testid="toolbar-container">
          <div className="flex-1">
            <DataGridToolbar
              table={table}
              filterConditions={filterConditions}
              onFilterConditionsChange={setFilterConditions}
              onAddRecord={() => setIsAddRecordDialogOpen(true)}
            />
          </div>
          <div className="flex items-center gap-2 shrink-0" data-testid="shareDB-badge-container">
            {shareDBSync ? (
              <>
                <Badge variant={shareDBSync.isConnected ? "default" : "destructive"} className="cursor-pointer" onClick={() => {
                  if (!shareDBSync.isConnected) {
                    shareDBSync.connect().catch((err) => {
                      console.error("Failed to connect ShareDB:", err);
                      toast.error("连接失败，请检查网络连接");
                    });
                  } else {
                    shareDBSync.disconnect();
                    toast.info("已断开 ShareDB 连接");
                  }
                }}>
                  {shareDBSync.isConnected ? "🟢 已连接" : "🔴 未连接"}
                </Badge>
                {shareDBSync.subscribedCount > 0 && (
                  <Badge variant="outline">
                    已订阅 {shareDBSync.subscribedCount} 条记录
                  </Badge>
                )}
                {!shareDBSync.isConnected && (
                  <button
                    onClick={() => {
                      shareDBSync.connect().catch((err) => {
                        console.error("Failed to connect ShareDB:", err);
                        toast.error("连接失败，请检查网络连接");
                      });
                    }}
                    className="text-xs text-muted-foreground hover:text-foreground"
                  >
                    重连
                  </button>
                )}
              </>
            ) : (
              <Badge variant="outline" className="text-xs">
                ShareDB 未初始化
              </Badge>
            )}
          </div>
        </div>
        <DataGrid {...dataGridProps} table={table} height={600} onDeleteField={onDeleteField} />
      </div>
    </>
  );
}
