import type {
  RecordResponse,
  RecordCreateRequest,
  RecordUpdateRequest,
  ListResponse,
  PaginationResponse,
} from 'luckdb-sdk'
import { luckdbClient } from '@/config/client'

/**
 * 记录服务封装
 */
export class RecordService {
  /**
   * 创建记录
   */
  async create(tableId: string, data: RecordCreateRequest): Promise<RecordResponse> {
    return luckdbClient.records.create(tableId, data)
  }

  /**
   * 获取记录列表
   */
  async getList(
    tableId: string,
    page = 1,
    perPage = 20
  ): Promise<ListResponse<RecordResponse>> {
    return luckdbClient.records.getList(tableId, page, perPage)
  }

  /**
   * 获取所有记录（分页获取）
   */
  async getAll(tableId: string): Promise<RecordResponse[]> {
    return luckdbClient.records.getFullList(tableId)
  }

  /**
   * 获取单个记录
   */
  async getOne(tableId: string, recordId: string): Promise<RecordResponse> {
    return luckdbClient.records.getOne(tableId, recordId)
  }

  /**
   * 更新记录
   */
  async update(
    tableId: string,
    recordId: string,
    data: RecordUpdateRequest
  ): Promise<RecordResponse> {
    return luckdbClient.records.update(tableId, recordId, data)
  }

  /**
   * 删除记录
   */
  async delete(tableId: string, recordId: string): Promise<void> {
    return luckdbClient.records.delete(tableId, recordId)
  }

  /**
   * 批量创建记录
   */
  async batchCreate(
    tableId: string,
    records: Array<{ fields: Record<string, any> }>
  ): Promise<RecordResponse[]> {
    const response = await luckdbClient.records.batchCreate(tableId, { records })
    return response.records
  }

  /**
   * 批量更新记录
   */
  async batchUpdate(
    tableId: string,
    records: Array<{ id: string; fields: Record<string, any> }>
  ): Promise<RecordResponse[]> {
    const response = await luckdbClient.records.batchUpdate(tableId, { records })
    return response.records
  }

  /**
   * 批量删除记录
   */
  async batchDelete(tableId: string, recordIds: string[]): Promise<void> {
    await luckdbClient.records.batchDelete(tableId, { recordIds })
  }
}

/**
 * 将 API 记录格式转换为表格数据格式
 */
export function transformRecordToTableData<T extends Record<string, any>>(
  record: RecordResponse,
  fieldMapping: Map<string, string> // fieldId -> columnId
): T {
  const data: any = { id: record.id }
  
  // 将字段数据映射到列数据
  const fields = record.data || record.fields || {}
  fieldMapping.forEach((columnId, fieldId) => {
    if (fields[fieldId] !== undefined) {
      data[columnId] = fields[fieldId]
    }
  })

  return data as T
}

/**
 * 将表格数据格式转换为 API 记录格式
 */
export function transformTableDataToRecord(
  tableData: Record<string, any>,
  fieldMapping: Map<string, string>, // columnId -> fieldId
  excludeFields: string[] = ['id']
): Record<string, any> {
  const fields: Record<string, any> = {}

  Object.keys(tableData).forEach((columnId) => {
    if (!excludeFields.includes(columnId)) {
      const fieldId = fieldMapping.get(columnId)
      if (fieldId) {
        fields[fieldId] = tableData[columnId]
      }
    }
  })

  return fields
}

// 导出单例
export const recordService = new RecordService()

