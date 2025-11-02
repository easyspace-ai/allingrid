import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'
import {
  Base as LuckDBBase,
  BaseCreateRequest,
  BaseUpdateRequest,
  BaseListFilter,
  BaseResponse
} from '@/types/space'

export class LuckDBBaseService extends BaseService {
  /**
   * 创建 Base
   * 对齐服务端 API: POST /api/v1/spaces/:spaceId/bases
   */
  async create(spaceId: string, data: BaseCreateRequest): Promise<BaseResponse> {
    const response = await this.send<BaseResponse>(`/api/v1/spaces/${spaceId}/bases`, {
      method: 'POST',
      body: data
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy ?? response.ownerId ?? ''
    }
  }

  /**
   * 获取 Base 列表
   * 对齐服务端 API: GET /api/v1/spaces/:spaceId/bases 或 GET /api/v1/bases
   */
  async getList(
    spaceId?: string,
    page = 1,
    perPage = 20,
    filter: BaseListFilter = {}
  ): Promise<ListResponse<BaseResponse>> {
    // 如果 spaceId 为空，尝试使用全局 API，如果失败则返回空列表
    if (!spaceId) {
      try {
        const response = await this.send<any>('/api/v1/bases', {
          method: 'GET',
          query: {
            page,
            perPage,
            ...filter
          }
        })
        
        // 处理响应格式
        let bases: BaseResponse[] = []
        let total = 0
        let limit = perPage
        
        if (Array.isArray(response)) {
          bases = response
          total = response.length
        } else if (response.bases && Array.isArray(response.bases)) {
          bases = response.bases
          total = response.total || bases.length
          limit = response.limit || perPage
        }
        
        const items = bases.map(base => ({
          ...base,
          ownerId: base.createdBy ?? base.ownerId ?? ''
        }))
        
        const totalPages = Math.ceil(total / limit) || 1
        return {
          items,
          pagination: {
            total,
            page,
            pageSize: limit,
            totalPages,
            hasNext: page < totalPages,
            hasPrevious: page > 1
          }
        }
      } catch (error: any) {
        // 如果全局 API 不存在，返回空列表
        if (error.status === 404) {
          return {
            items: [],
            pagination: {
              total: 0,
              page: 1,
              pageSize: perPage,
              totalPages: 0,
              hasNext: false,
              hasPrevious: false
            }
          }
        }
        throw error
      }
    }

    // 使用 spaceId 获取列表
    const url = `/api/v1/spaces/${spaceId}/bases`
    const response = await this.send<any>(url, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })

    // 处理不同的响应格式
    let bases: BaseResponse[] = []
    let total = 0
    let limit = perPage
    
    if (Array.isArray(response)) {
      bases = response
      total = response.length
    } else if (response.bases && Array.isArray(response.bases)) {
      bases = response.bases
      total = response.total || bases.length
      limit = response.limit || perPage
    } else {
      bases = []
      total = 0
    }

    // 添加兼容字段并转换格式
    const items = bases.map(base => ({
      ...base,
      ownerId: base.createdBy ?? base.ownerId ?? ''
    }))

    const totalPages = Math.ceil(total / limit) || 1
    const pagination: PaginationResponse = {
      total,
      page: page,
      pageSize: limit,
      totalPages,
      hasNext: page < totalPages,
      hasPrevious: page > 1
    }

    return {
      items,
      pagination
    }
  }

  /**
   * 获取所有 Base
   */
  async getFullList(spaceId?: string, filter: BaseListFilter = {}): Promise<BaseResponse[]> {
    const result: BaseResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getList(spaceId, page, perPage, filter)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 获取单个 Base
   */
  async getOne(id: string): Promise<BaseResponse> {
    const response = await this.send<BaseResponse>(`/api/v1/bases/${id}`, {
      method: 'GET'
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy ?? response.ownerId ?? ''
    }
  }

  /**
   * 复制 Base
   * 对齐服务端 API: POST /api/v1/bases/:baseId/duplicate
   */
  async duplicate(id: string, name?: string): Promise<BaseResponse> {
    const response = await this.send<BaseResponse>(`/api/v1/bases/${id}/duplicate`, {
      method: 'POST',
      body: name ? { name } : {}
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy ?? response.ownerId ?? ''
    }
  }

  /**
   * 更新 Base
   */
  async update(id: string, data: BaseUpdateRequest): Promise<BaseResponse> {
    const response = await this.send<BaseResponse>(`/api/v1/bases/${id}`, {
      method: 'PATCH',
      body: data
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy ?? response.ownerId ?? ''
    }
  }

  /**
   * 删除 Base
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/bases/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 获取 Base 权限
   */
  async getPermission(baseId: string): Promise<any> {
    return this.send(`/api/v1/bases/${baseId}/permission`, {
      method: 'GET'
    })
  }

  /**
   * 获取 Base 的协作者
   */
  async getCollaborators(
    baseId: string,
    page = 1,
    perPage = 20
  ): Promise<ListResponse<any>> {
    const response = await this.send<{ collaborators: any[], pagination: PaginationResponse }>(
      `/api/v1/bases/${baseId}/collaborators`,
      {
        method: 'GET',
        query: { page, perPage }
      }
    )

    return {
      items: response.collaborators || [],
      pagination: response.pagination
    }
  }

  /**
   * 添加 Base 协作者
   */
  async addCollaborator(baseId: string, data: any): Promise<any> {
    return this.send(`/api/v1/bases/${baseId}/collaborators`, {
      method: 'POST',
      body: data
    })
  }

  /**
   * 更新 Base 协作者
   */
  async updateCollaborator(baseId: string, collaboratorId: string, data: any): Promise<any> {
    return this.send(`/api/v1/bases/${baseId}/collaborators/${collaboratorId}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除 Base 协作者
   */
  async removeCollaborator(baseId: string, collaboratorId: string): Promise<void> {
    await this.send(`/api/v1/bases/${baseId}/collaborators/${collaboratorId}`, {
      method: 'DELETE'
    })
  }
}
