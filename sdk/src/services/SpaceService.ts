import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'
import {
  Space,
  SpaceCreateRequest,
  SpaceUpdateRequest,
  SpaceListFilter,
  SpaceResponse,
  Base as LuckDBBase,
  BaseCreateRequest,
  BaseUpdateRequest,
  BaseListFilter,
  BaseResponse
} from '@/types/space'

export class SpaceService extends BaseService {
  /**
   * 创建空间
   * 对齐服务端 API: POST /api/v1/spaces
   */
  async create(data: SpaceCreateRequest): Promise<SpaceResponse> {
    const response = await this.send<SpaceResponse>('/api/v1/spaces', {
      method: 'POST',
      body: data
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy || response.ownerId
    }
  }

  /**
   * 获取空间列表
   * 对齐服务端 API: GET /api/v1/spaces
   * 响应格式: { code, message, data: SpaceResponse[] } 或 { code, message, data: { spaces: [], total, limit, offset } }
   */
  async getList(
    page = 1,
    perPage = 20,
    filter: SpaceListFilter = {}
  ): Promise<ListResponse<SpaceResponse>> {
    const response = await this.send<any>('/api/v1/spaces', {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })

    // 处理不同的响应格式
    let spaces: SpaceResponse[] = []
    let total = 0
    let limit = perPage
    
    if (Array.isArray(response)) {
      // 格式：{ data: SpaceResponse[] }
      spaces = response
      total = response.length
      limit = perPage
    } else if (response.spaces && Array.isArray(response.spaces)) {
      // 格式：{ data: { spaces: [], total, limit, offset } }
      spaces = response.spaces
      total = response.total || spaces.length
      limit = response.limit || perPage
    } else {
      spaces = []
      total = 0
    }

    // 添加兼容字段并转换格式
    const items = spaces.map(space => ({
      ...space,
      ownerId: space.createdBy || space.ownerId
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
   * 获取所有空间
   */
  async getFullList(filter: SpaceListFilter = {}): Promise<SpaceResponse[]> {
    const result: SpaceResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getList(page, perPage, filter)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 获取单个空间
   */
  async getOne(id: string): Promise<SpaceResponse> {
    const response = await this.send<SpaceResponse>(`/api/v1/spaces/${id}`, {
      method: 'GET'
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy || response.ownerId
    }
  }

  /**
   * 更新空间
   */
  async update(id: string, data: SpaceUpdateRequest): Promise<SpaceResponse> {
    const response = await this.send<SpaceResponse>(`/api/v1/spaces/${id}`, {
      method: 'PATCH',
      body: data
    })

    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy || response.ownerId
    }
  }

  /**
   * 删除空间
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/spaces/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 获取 Space 协作者列表
   */
  async getCollaborators(
    spaceId: string,
    page = 1,
    perPage = 20
  ): Promise<ListResponse<any>> {
    const response = await this.send<{ collaborators: any[], pagination: PaginationResponse }>(
      `/api/v1/spaces/${spaceId}/collaborators`,
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
   * 添加 Space 协作者
   */
  async addCollaborator(spaceId: string, data: any): Promise<any> {
    return this.send(`/api/v1/spaces/${spaceId}/collaborators`, {
      method: 'POST',
      body: data
    })
  }

  /**
   * 更新 Space 协作者
   */
  async updateCollaborator(spaceId: string, collaboratorId: string, data: any): Promise<any> {
    return this.send(`/api/v1/spaces/${spaceId}/collaborators/${collaboratorId}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除 Space 协作者
   */
  async removeCollaborator(spaceId: string, collaboratorId: string): Promise<void> {
    await this.send(`/api/v1/spaces/${spaceId}/collaborators/${collaboratorId}`, {
      method: 'DELETE'
    })
  }

  /**
   * 获取空间的 Base 列表
   * 对齐服务端 API: GET /api/v1/spaces/:spaceId/bases
   */
  /**
   * 获取空间下的 Base 列表
   * 对齐服务端 API: GET /api/v1/spaces/:spaceId/bases
   */
  async getBases(
    spaceId: string,
    page = 1,
    perPage = 20,
    filter: BaseListFilter = {}
  ): Promise<ListResponse<BaseResponse>> {
    const response = await this.send<any>(
      `/api/v1/spaces/${spaceId}/bases`,
      {
        method: 'GET',
        query: {
          page,
          perPage,
          ...filter
        }
      }
    )

    // 处理不同的响应格式
    let bases: BaseResponse[] = []
    let total = 0
    let limit = perPage
    
    if (Array.isArray(response)) {
      // 格式：{ data: BaseResponse[] }
      bases = response
      total = response.length
      limit = perPage
    } else if (response.bases && Array.isArray(response.bases)) {
      // 格式：{ data: { bases: [], total, limit, offset } }
      bases = response.bases
      total = response.total || bases.length
      limit = response.limit || perPage
    } else {
      bases = []
      total = 0
    }

    // 添加兼容字段
    const items = bases.map(base => ({
      ...base,
      ownerId: base.createdBy || base.ownerId
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
   * 获取空间的所有 Base
   */
  async getAllBases(spaceId: string, filter: BaseListFilter = {}): Promise<BaseResponse[]> {
    const result: BaseResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getBases(spaceId, page, perPage, filter)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 在空间中创建 Base
   * 对齐服务端 API: POST /api/v1/spaces/:spaceId/bases
   */
  async createBase(spaceId: string, data: BaseCreateRequest): Promise<BaseResponse> {
    const response = await this.send<BaseResponse>(`/api/v1/spaces/${spaceId}/bases`, {
      method: 'POST',
      body: data
    })
    
    // 添加兼容字段
    return {
      ...response,
      ownerId: response.createdBy || response.ownerId
    }
  }
}
