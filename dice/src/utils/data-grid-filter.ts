/**
 * DataGridFilter - 筛选功能工具函数
 * 
 * 根据筛选条件过滤数据
 */

import type { FilterCondition, FilterOperator } from "@/components/data-grid/data-grid-filter-menu";

export interface FilteredDataOptions<TData> {
  data: TData[];
  conditions: FilterCondition[];
  getFieldValue: (row: TData, fieldId: string) => unknown;
  getFieldType?: (fieldId: string) => string | undefined;
}

/**
 * 应用筛选条件到数据
 */
export function applyFilters<TData>({
  data,
  conditions,
  getFieldValue,
  getFieldType,
}: FilteredDataOptions<TData>): TData[] {
  if (conditions.length === 0) {
    return data;
  }

  return data.filter((row) => {
    // 所有条件都需要满足（AND 逻辑）
    return conditions.every((condition) => {
      const rowValue = getFieldValue(row, condition.fieldId);
      const filterValue = condition.value;
      const operator = condition.operator;

      return evaluateCondition(rowValue, filterValue, operator);
    });
  });
}

/**
 * 评估单个筛选条件
 */
function evaluateCondition(
  rowValue: unknown,
  filterValue: unknown,
  operator: FilterOperator,
): boolean {
  switch (operator) {
    case "equals":
      return rowValue === filterValue;

    case "not_equals":
      return rowValue !== filterValue;

    case "contains":
      return String(rowValue)
        .toLowerCase()
        .includes(String(filterValue).toLowerCase());

    case "not_contains":
      return !String(rowValue)
        .toLowerCase()
        .includes(String(filterValue).toLowerCase());

    case "starts_with":
      return String(rowValue)
        .toLowerCase()
        .startsWith(String(filterValue).toLowerCase());

    case "ends_with":
      return String(rowValue)
        .toLowerCase()
        .endsWith(String(filterValue).toLowerCase());

    case "greater_than":
      return Number(rowValue) > Number(filterValue);

    case "less_than":
      return Number(rowValue) < Number(filterValue);

    case "greater_equal":
      return Number(rowValue) >= Number(filterValue);

    case "less_equal":
      return Number(rowValue) <= Number(filterValue);

    case "is_empty":
      return (
        rowValue === null ||
        rowValue === undefined ||
        rowValue === "" ||
        (Array.isArray(rowValue) && rowValue.length === 0)
      );

    case "is_not_empty":
      return (
        rowValue !== null &&
        rowValue !== undefined &&
        rowValue !== "" &&
        !(Array.isArray(rowValue) && rowValue.length === 0)
      );

    case "in":
      if (Array.isArray(filterValue)) {
        return filterValue.includes(rowValue);
      }
      return String(rowValue) === String(filterValue);

    case "not_in":
      if (Array.isArray(filterValue)) {
        return !filterValue.includes(rowValue);
      }
      return String(rowValue) !== String(filterValue);

    default:
      return true;
  }
}


