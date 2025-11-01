import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse } from '@/types/common'
import {
  Field,
  FieldCreateRequest,
  FieldUpdateRequest,
  FieldListFilter,
  FieldResponse
} from '@/types/table'

export class FieldService extends BaseService {
  /**
   * 创建字段
   */
  async create(tableId: string, data: FieldCreateRequest): Promise<FieldResponse> {
    // 确保 tableId 在请求体中（服务端要求）
    const requestBody = {
      ...data,
      tableId
    }
    return this.send<FieldResponse>(`/api/v1/tables/${tableId}/fields`, {
      method: 'POST',
      body: requestBody
    })
  }

  /**
   * 获取字段列表
   */
  async getList(
    tableId: string,
    page = 1,
    perPage = 20,
    filter: FieldListFilter = {}
  ): Promise<PaginationResponse<FieldResponse>> {
    const response = await this.send<any>(`/api/v1/tables/${tableId}/fields`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })

    // 处理不同的响应格式
    let fields: FieldResponse[] = []
    let total = 0
    let pageSize = perPage
    
    if (Array.isArray(response)) {
      // 格式：{ data: FieldResponse[] }
      fields = response
      total = response.length
      pageSize = perPage
    } else if (response.fields && Array.isArray(response.fields)) {
      // 格式：{ data: { fields: [] } }
      fields = response.fields
      total = response.total || fields.length
      pageSize = response.pageSize || response.limit || perPage
    } else if (response.items && Array.isArray(response.items)) {
      // 格式：{ items: [], pagination: {} }
      fields = response.items
      if (response.pagination) {
        total = response.pagination.total || fields.length
        pageSize = response.pagination.pageSize || perPage
      } else {
        total = fields.length
        pageSize = perPage
      }
    } else {
      fields = []
      total = 0
    }

    const totalPages = Math.ceil(total / pageSize) || 1
    const pagination: PaginationResponse = {
      total,
      page: page,
      pageSize,
      totalPages,
      hasNext: page < totalPages,
      hasPrevious: page > 1
    }

    return {
      items: fields,
      pagination
    }
  }

  /**
   * 获取所有字段
   */
  async getFullList(tableId: string, filter: FieldListFilter = {}): Promise<FieldResponse[]> {
    const result: FieldResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getList(tableId, page, perPage, filter)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 获取单个字段
   */
  async getOne(id: string): Promise<FieldResponse> {
    return this.send<FieldResponse>(`/api/v1/fields/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新字段
   */
  async update(id: string, data: FieldUpdateRequest): Promise<FieldResponse> {
    return this.send<FieldResponse>(`/api/v1/fields/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除字段
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/fields/${id}`, {
      method: 'DELETE'
    })
  }
}
