import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'
import {
  View,
  ViewCreateRequest,
  ViewUpdateRequest,
  ViewListFilter,
  ViewResponse,
  ViewShareRequest,
  ViewShareResponse
} from '@/types/record'

export class ViewService extends BaseService {
  /**
   * 创建视图
   * 对齐服务端 API: POST /api/v1/tables/:tableId/views
   * 注意：tableId 在 URL 中，请求体中不需要 tableId
   */
  async create(tableId: string, data: ViewCreateRequest): Promise<ViewResponse> {
    // 注意：服务端从 URL 获取 tableId，不需要在 body 中传递
    const response = await this.send<ViewResponse>(`/api/v1/tables/${tableId}/views`, {
      method: 'POST',
      body: data
    })

    return response
  }

  /**
   * 获取视图列表
   * 对齐服务端 API: GET /api/v1/tables/:tableId/views
   * 响应格式: { code, message, data: ViewResponse[] }
   */
  async getList(
    tableId: string,
    page = 1,
    perPage = 20,
    filter: ViewListFilter = {}
  ): Promise<ListResponse<ViewResponse>> {
    const response = await this.send<ViewResponse[]>(`/api/v1/tables/${tableId}/views`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })

    // 服务端返回数组，需要转换为 ListResponse 格式
    const items = Array.isArray(response) ? response : []
    
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
   * 获取所有视图
   */
  async getFullList(tableId: string, filter: ViewListFilter = {}): Promise<ViewResponse[]> {
    const result: ViewResponse[] = []
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
   * 获取单个视图
   */
  async getOne(id: string): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新视图
   */
  async update(id: string, data: ViewUpdateRequest): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除视图
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/views/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 分享视图
   */
  async share(id: string, data: ViewShareRequest): Promise<ViewShareResponse> {
    // 兼容旧接口：改为服务端新接口 enable-share
    return this.send<ViewShareResponse>(`/api/v1/views/${id}/enable-share`, {
      method: 'POST',
      body: data
    })
  }

  /**
   * 取消分享视图
   */
  async unshare(id: string): Promise<void> {
    await this.send(`/api/v1/views/${id}/disable-share`, {
      method: 'POST'
    })
  }

  /**
   * 通过分享 ID 获取视图
   */
  async getByShareId(shareId: string): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/share/views/${shareId}`, {
      method: 'GET'
    })
  }

  /**
   * 获取所有视图数据
   */
  async getAllData(id: string, additionalQuery: Record<string, any> = {}): Promise<any[]> {
    const result: any[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getData(id, page, perPage, additionalQuery)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 复制视图
   */
  async duplicate(id: string, name: string): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/duplicate`, {
      method: 'POST',
      body: { name }
    })
  }

  /**
   * 刷新分享ID
   */
  async refreshShareId(id: string): Promise<{ shareId: string }> {
    return this.send<{ shareId: string }>(`/api/v1/views/${id}/refresh-share-id`, {
      method: 'POST'
    })
  }

  /** 锁定/解锁视图 */
  async lock(id: string): Promise<void> {
    await this.send(`/api/v1/views/${id}/lock`, { method: 'POST' })
  }
  async unlock(id: string): Promise<void> {
    await this.send(`/api/v1/views/${id}/unlock`, { method: 'POST' })
  }

  /**
   * 更新视图配置
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/options
   */
  async updateOptions(id: string, options: Record<string, any>): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/options`, {
      method: 'PATCH',
      body: { options }
    })
  }

  /**
   * 更新视图排序位置
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/order
   */
  async updateOrder(id: string, order: number): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/order`, {
      method: 'PATCH',
      body: { order }
    })
  }

  /**
   * 更新视图过滤器
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/filter
   */
  async updateFilter(id: string, filter: Record<string, any>): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/filter`, {
      method: 'PATCH',
      body: { filter }
    })
  }

  /**
   * 更新视图排序
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/sort
   */
  async updateSort(id: string, sort: any[]): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/sort`, {
      method: 'PATCH',
      body: { sort }
    })
  }

  /**
   * 更新视图分组
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/group
   */
  async updateGroup(id: string, group: any[]): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/group`, {
      method: 'PATCH',
      body: { group }
    })
  }

  /**
   * 更新视图列配置
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/column-meta
   */
  async updateColumnMeta(id: string, columnMeta: any[]): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/column-meta`, {
      method: 'PATCH',
      body: { columnMeta }
    })
  }

  /**
   * 更新视图分享元数据
   * 对齐服务端 API: PATCH /api/v1/views/:viewId/share-meta
   */
  async updateShareMeta(id: string, shareMeta: Record<string, any>): Promise<ViewResponse> {
    return this.send<ViewResponse>(`/api/v1/views/${id}/share-meta`, {
      method: 'PATCH',
      body: { shareMeta }
    })
  }

  /**
   * 获取视图数据
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async getData(
    id: string,
    page = 1,
    perPage = 20,
    additionalQuery: Record<string, any> = {}
  ): Promise<ListResponse<any>> {
    try {
      const response = await this.send<{ records: any[], pagination: PaginationResponse }>(
        `/api/v1/views/${id}/data`,
        {
          method: 'GET',
          query: {
            page,
            perPage,
            ...additionalQuery
          }
        }
      )

      return {
        items: response.records || [],
        pagination: response.pagination
      }
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('View data endpoint not implemented on server')
      }
      throw error
    }
  }

  /**
   * 获取视图统计
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async getStats(id: string): Promise<{
    recordCount: number
    lastAccessedAt?: string
    accessCount: number
  }> {
    try {
      return await this.send(`/api/v1/views/${id}/stats`, {
        method: 'GET'
      })
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('View stats endpoint not implemented on server')
      }
      throw error
    }
  }

  /**
   * 获取视图权限
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async getPermissions(id: string): Promise<{
    canRead: boolean
    canWrite: boolean
    canDelete: boolean
    canShare: boolean
  }> {
    try {
      return await this.send(`/api/v1/views/${id}/permissions`, {
        method: 'GET'
      })
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('View permissions endpoint not implemented on server')
      }
      throw error
    }
  }

  /**
   * 更新视图配置
   * 注意：服务端可能未实现此端点，保留兼容性
   */
  async updateConfig(id: string, config: any): Promise<ViewResponse> {
    try {
      return await this.send<ViewResponse>(`/api/v1/views/${id}/config`, {
        method: 'PATCH',
        body: { config }
      })
    } catch (error: any) {
      if (error.status === 404) {
        throw new Error('View config endpoint not implemented on server')
      }
      throw error
    }
  }
}
