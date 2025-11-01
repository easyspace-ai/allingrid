import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'
import {
  Table,
  TableCreateRequest,
  TableUpdateRequest,
  TableRenameRequest,
  TableDuplicateRequest,
  TableListFilter,
  TableResponse,
  TableUsage,
  TableManagementMenu,
  Field,
  FieldCreateRequest,
  FieldUpdateRequest,
  FieldListFilter,
  FieldResponse
} from '@/types/table'

export class TableService extends BaseService {
  /**
   * 创建表格
   * 对齐服务端 API: POST /api/v1/bases/:baseId/tables
   * 请求格式: { name, description?, baseId, fields?, views? }
   */
  async create(baseId: string, data: TableCreateRequest): Promise<TableResponse> {
    // 确保请求数据包含baseId
    const requestData = { ...data, baseId }
    
    const response = await this.send<TableResponse>(`/api/v1/bases/${baseId}/tables`, {
      method: 'POST',
      body: requestData
    })

    return response
  }

  /**
   * 获取表格列表
   * 对齐服务端 API: GET /api/v1/bases/:baseId/tables
   */
  async getList(
    baseId: string,
    page = 1,
    perPage = 20,
    filter: TableListFilter = {}
  ): Promise<ListResponse<TableResponse>> {
    const response = await this.send<{ tables: TableResponse[] }>(`/api/v1/bases/${baseId}/tables`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })

    // 服务端可能返回简单的数组或带分页的对象
    const items = Array.isArray(response) ? response : (response.tables || [])
    
    // 如果没有分页信息，创建一个简单的分页对象
    const pagination: PaginationResponse = {
      total: items.length,
      page: 1,
      pageSize: perPage,
      totalPages: 1,
      hasNext: false,
      hasPrevious: false
    }

    return {
      items,
      pagination
    }
  }

  /**
   * 获取所有表格
   */
  async getFullList(baseId: string, filter: TableListFilter = {}): Promise<TableResponse[]> {
    const result: TableResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getList(baseId, page, perPage, filter)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 获取单个表格
   */
  async getOne(id: string): Promise<TableResponse> {
    return this.send<TableResponse>(`/api/v1/tables/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新表格
   */
  async update(id: string, data: TableUpdateRequest): Promise<TableResponse> {
    return this.send<TableResponse>(`/api/v1/tables/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除表格
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/tables/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 重命名表格
   */
  async rename(id: string, data: TableRenameRequest): Promise<TableResponse> {
    return this.send<TableResponse>(`/api/v1/tables/${id}/rename`, {
      method: 'PUT',
      body: data
    })
  }

  /**
   * 复制表格
   */
  async duplicate(id: string, data: TableDuplicateRequest): Promise<TableResponse> {
    return this.send<TableResponse>(`/api/v1/tables/${id}/duplicate`, {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取表格使用情况
   */
  async getUsage(id: string): Promise<TableUsage> {
    return this.send<TableUsage>(`/api/v1/tables/${id}/usage`, {
      method: 'GET'
    })
  }

  /**
   * 获取表格管理菜单
   */
  async getManagementMenu(id: string): Promise<TableManagementMenu> {
    return this.send<TableManagementMenu>(`/api/v1/tables/${id}/menu`, {
      method: 'GET'
    })
  }
}

export class FieldService extends BaseService {
  /**
   * 创建字段
   * 对齐服务端 API: POST /api/v1/tables/:tableId/fields
   * 请求格式: { name, type, tableId, options?, required?, unique? }
   */
  async create(tableId: string, data: FieldCreateRequest): Promise<FieldResponse> {
    // 确保请求数据包含tableId
    const requestData = { ...data, tableId }
    
    const response = await this.send<FieldResponse>(`/api/v1/tables/${tableId}/fields`, {
      method: 'POST',
      body: requestData
    })

    return response
  }

  /**
   * 获取字段列表
   * 对齐服务端 API: GET /api/v1/tables/:tableId/fields
   */
  async getList(
    tableId: string,
    page = 1,
    perPage = 20,
    filter: FieldListFilter = {}
  ): Promise<ListResponse<FieldResponse>> {
    const response = await this.send<{ fields: FieldResponse[] }>(`/api/v1/tables/${tableId}/fields`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })

    // 服务端可能返回简单的数组或带分页的对象
    const items = Array.isArray(response) ? response : (response.fields || [])
    
    // 如果没有分页信息，创建一个简单的分页对象
    const pagination: PaginationResponse = {
      total: items.length,
      page: 1,
      pageSize: perPage,
      totalPages: 1,
      hasNext: false,
      hasPrevious: false
    }

    return {
      items,
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
