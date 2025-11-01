import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'
import {
  Record,
  RecordCreateRequest,
  RecordUpdateRequest,
  RecordListFilter,
  RecordResponse,
  BatchCreateRecordsRequest,
  BatchUpdateRecordsRequest,
  BatchDeleteRecordsRequest,
  BatchCreateRecordResponse,
  BatchUpdateRecordResponse,
  BatchDeleteRecordResponse,
  BatchOperationResponse
} from '@/types/record'

export class RecordService extends BaseService {
  /**
   * 创建记录
   * 对齐服务端 API: POST /api/v1/tables/:tableId/records
   * 请求格式: { tableId, data: { [fieldId]: value } }
   */
  async create(tableId: string, data: RecordCreateRequest): Promise<RecordResponse> {
    // 构建请求体：优先使用 data，否则使用 fields
    const requestData = {
      tableId,
      data: data.data || data.fields || {}
    }
    
    const response = await this.send<RecordResponse>(`/api/v1/tables/${tableId}/records`, {
      method: 'POST',
      body: requestData
    })

    // 添加兼容字段：fields 映射自 data
    return {
      ...response,
      fields: response.data || response.fields || {}
    }
  }

  /**
   * 获取记录列表
   * 对齐服务端 API: GET /api/v1/tables/:tableId/records
   * 响应格式: { code, message, data: { list: [], pagination: { page, limit, total, total_pages } } }
   */
  async getList(
    tableId: string,
    page = 1,
    perPage = 20,
    filter: RecordListFilter = {}
  ): Promise<ListResponse<RecordResponse>> {
    const response = await this.send<{ list: RecordResponse[], pagination: { page: number, limit: number, total: number, total_pages: number } }>(
      `/api/v1/tables/${tableId}/records`,
      {
        method: 'GET',
        query: {
          page,
          perPage, // 服务端使用 perPage（不是 pageSize）
          ...filter
        }
      }
    )

    // 添加兼容字段：为每个记录添加 fields 字段
    const records = response.list.map(record => ({
      ...record,
      fields: record.data || record.fields || {}
    }))

    // 转换分页格式
    const pagination: PaginationResponse = {
      total: response.pagination.total,
      page: response.pagination.page,
      pageSize: response.pagination.limit,
      totalPages: response.pagination.total_pages,
      hasNext: response.pagination.page < response.pagination.total_pages,
      hasPrevious: response.pagination.page > 1
    }

    return {
      items: records,
      pagination
    }
  }

  /**
   * 获取所有记录
   */
  async getFullList(tableId: string, filter: RecordListFilter = {}): Promise<RecordResponse[]> {
    const result: RecordResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getList(tableId, page, perPage, filter)
      result.push(...response.items)

      if (!response.pagination.hasNext) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 获取单个记录
   * 注意：服务端要求 tableId，但旧API支持仅 recordId（已废弃）
   */
  async getOne(tableId: string, recordId: string): Promise<RecordResponse>
  async getOne(tableIdOrRecordId: string, recordId?: string): Promise<RecordResponse> {
    let url: string
    if (recordId) {
      // 新API：需要 tableId 和 recordId
      url = `/api/v1/tables/${tableIdOrRecordId}/records/${recordId}`
    } else {
      // 旧API：仅 recordId（已废弃，但保留兼容）
      url = `/api/v1/records/${tableIdOrRecordId}`
    }
    
    const response = await this.send<RecordResponse>(url, {
      method: 'GET'
    })

    // 添加兼容字段
    return {
      ...response,
      fields: response.data || response.fields || {}
    }
  }

  /**
   * 更新记录
   * 对齐服务端 API: PATCH /api/v1/tables/:tableId/records/:recordId
   * 请求格式: { data: { [fieldId]: value }, version?: number }
   */
  async update(tableId: string, recordId: string, data: RecordUpdateRequest): Promise<RecordResponse> {
    // 构建请求体：优先使用 data，否则使用 fields
    const requestData: any = {
      data: data.data || data.fields || data.record?.fields || {}
    }

    // 添加版本号（乐观锁）
    if (data.version !== undefined) {
      requestData.version = data.version
    }

    // 添加 fieldKeyType（Teable 格式）
    if (data.fieldKeyType) {
      requestData.fieldKeyType = data.fieldKeyType
    }
    
    const response = await this.send<RecordResponse>(
      `/api/v1/tables/${tableId}/records/${recordId}`,
      {
        method: 'PATCH',
        body: requestData
      }
    )

    // 添加兼容字段
    return {
      ...response,
      fields: response.data || response.fields || {}
    }
  }

  /**
   * 删除记录
   */
  async delete(tableId: string, recordId: string): Promise<void> {
    await this.send(`/api/v1/tables/${tableId}/records/${recordId}`, {
      method: 'DELETE'
    })
  }

  /**
   * 批量创建记录
   * 对齐服务端 API: POST /api/v1/tables/:tableId/records/batch
   * 响应格式: { records: [], successCount, failedCount, errors }
   */
  async batchCreate(tableId: string, data: BatchCreateRecordsRequest): Promise<BatchCreateRecordResponse> {
    const response = await this.send<BatchCreateRecordResponse>(
      `/api/v1/tables/${tableId}/records/batch`,
      {
        method: 'POST',
        body: data
      }
    )

    // 为每个记录添加兼容字段
    const records = response.records.map(record => ({
      ...record,
      fields: record.data || record.fields || {}
    }))

    return {
      ...response,
      records
    }
  }

  /**
   * 批量更新记录
   * 对齐服务端 API: PATCH /api/v1/tables/:tableId/records/batch
   */
  async batchUpdate(tableId: string, data: BatchUpdateRecordsRequest): Promise<BatchUpdateRecordResponse> {
    const response = await this.send<BatchUpdateRecordResponse>(
      `/api/v1/tables/${tableId}/records/batch`,
      {
        method: 'PATCH',
        body: data
      }
    )

    // 为每个记录添加兼容字段
    const records = response.records.map(record => ({
      ...record,
      fields: record.data || record.fields || {}
    }))

    return {
      ...response,
      records
    }
  }

  /**
   * 批量删除记录
   * 对齐服务端 API: DELETE /api/v1/tables/:tableId/records/batch
   */
  async batchDelete(tableId: string, data: BatchDeleteRecordsRequest): Promise<BatchDeleteRecordResponse> {
    return this.send<BatchDeleteRecordResponse>(`/api/v1/tables/${tableId}/records/batch`, {
      method: 'DELETE',
      body: data
    })
  }

  /**
   * 根据条件获取第一个记录
   */
  async getFirstListItem(
    tableId: string,
    filter: RecordListFilter = {}
  ): Promise<RecordResponse> {
    const response = await this.getList(tableId, 1, 1, filter)
    
    if (response.items.length === 0) {
      throw new Error('No records found')
    }

    const firstItem = response.items[0]
    if (!firstItem) {
      throw new Error('No records found')
    }

    return firstItem
  }

  /**
   * 根据条件获取记录（如果不存在则创建）
   */
  async getFirstListItemOrCreate(
    tableId: string,
    filter: RecordListFilter,
    createData: RecordCreateRequest
  ): Promise<RecordResponse> {
    try {
      return await this.getFirstListItem(tableId, filter)
    } catch {
      return await this.create(tableId, createData)
    }
  }

  /**
   * 搜索记录
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async search(
    tableId: string,
    query: string,
    fields?: string[],
    page = 1,
    perPage = 20
  ): Promise<ListResponse<RecordResponse>> {
    const searchQuery: { [key: string]: any } = {
      q: query,
      page,
      pageSize: perPage // 服务端使用 perPage
    }

    if (fields && fields.length > 0) {
      searchQuery.fields = fields.join(',')
    }

    try {
      const response = await this.send<{ records: RecordResponse[], pagination: PaginationResponse }>(
        `/api/v1/tables/${tableId}/records/search`,
        {
          method: 'GET',
          query: searchQuery
        }
      )

      const records = response.records.map(record => ({
        ...record,
        fields: record.data || record.fields || {}
      }))

      return {
        items: records,
        pagination: response.pagination
      }
    } catch (error: any) {
      // 如果搜索端点不存在，降级到普通列表查询
      if (error.status === 404) {
        return this.getList(tableId, page, perPage, { fields: { _search: query } as any })
      }
      throw error
    }
  }

  /**
   * 导出记录
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async export(
    tableId: string,
    format: 'csv' | 'json' | 'xlsx' = 'csv',
    filter: RecordListFilter = {}
  ): Promise<Blob> {
    try {
      const response = await this.send<Blob>(`/api/v1/tables/${tableId}/records/export`, {
        method: 'GET',
        query: {
          format,
          ...filter
        }
      })
      return response
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('Export endpoint not implemented on server')
      }
      throw error
    }
  }

  /**
   * 导入记录
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async import(
    tableId: string,
    file: File,
    options: {
      updateExisting?: boolean
      skipErrors?: boolean
    } = {}
  ): Promise<BatchCreateRecordResponse> {
    const formData = new FormData()
    formData.append('file', file)
    
    if (options.updateExisting !== undefined) {
      formData.append('updateExisting', String(options.updateExisting))
    }
    if (options.skipErrors !== undefined) {
      formData.append('skipErrors', String(options.skipErrors))
    }

    try {
      return await this.send<BatchCreateRecordResponse>(`/api/v1/tables/${tableId}/records/import`, {
        method: 'POST',
        body: formData
      })
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('Import endpoint not implemented on server')
      }
      throw error
    }
  }

  /**
   * 获取记录统计
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async getStats(tableId: string, filter: RecordListFilter = {}): Promise<{
    total: number
    createdToday: number
    updatedToday: number
    createdThisWeek: number
    updatedThisWeek: number
  }> {
    try {
      return await this.send(`/api/v1/tables/${tableId}/records/stats`, {
        method: 'GET',
        query: filter
      })
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('Stats endpoint not implemented on server')
      }
      throw error
    }
  }
}
