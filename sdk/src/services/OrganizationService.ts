import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'

export interface Organization {
  id: string
  name: string
  description?: string
  logo?: string
  settings?: Record<string, any>
  createdAt: string
  updatedAt: string
  ownerId: string
}

export interface OrganizationCreateRequest {
  name: string
  description?: string
  logo?: string
  settings?: Record<string, any>
}

export interface OrganizationUpdateRequest {
  name?: string
  description?: string
  logo?: string
  settings?: Record<string, any>
}

export interface OrganizationListFilter {
  name?: string
  ownerId?: string
  createdAfter?: string
  createdBefore?: string
}

export interface OrganizationResponse {
  id: string
  name: string
  description?: string
  logo?: string
  settings?: Record<string, any>
  createdAt: string
  updatedAt: string
  ownerId: string
}

export class OrganizationService extends BaseService {
  /**
   * 创建组织
   */
  async create(data: OrganizationCreateRequest): Promise<OrganizationResponse> {
    return this.send<OrganizationResponse>('/api/v1/organizations', {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取组织列表
   */
  async getList(
    page = 1,
    perPage = 20,
    filter: OrganizationListFilter = {}
  ): Promise<ListResponse<OrganizationResponse>> {
    return this.send<ListResponse<OrganizationResponse>>('/api/v1/organizations', {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取所有组织
   */
  async getFullList(filter: OrganizationListFilter = {}): Promise<OrganizationResponse[]> {
    const result: OrganizationResponse[] = []
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
   * 获取单个组织
   */
  async getOne(id: string): Promise<OrganizationResponse> {
    return this.send<OrganizationResponse>(`/api/v1/organizations/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新组织
   */
  async update(id: string, data: OrganizationUpdateRequest): Promise<OrganizationResponse> {
    return this.send<OrganizationResponse>(`/api/v1/organizations/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除组织
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/organizations/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 获取组织成员
   */
  async getMembers(
    id: string,
    page = 1,
    perPage = 20
  ): Promise<ListResponse<any>> {
    return this.send<ListResponse<any>>(`/api/v1/organizations/${id}/members`, {
      method: 'GET',
      query: {
        page,
        perPage
      }
    })
  }

  /**
   * 添加组织成员
   */
  async addMember(id: string, userId: string, role: string): Promise<any> {
    return this.send(`/api/v1/organizations/${id}/members`, {
      method: 'POST',
      body: {
        userId,
        role
      }
    })
  }

  /**
   * 移除组织成员
   */
  async removeMember(id: string, userId: string): Promise<void> {
    await this.send(`/api/v1/organizations/${id}/members/${userId}`, {
      method: 'DELETE'
    })
  }

  /**
   * 更新成员角色
   */
  async updateMemberRole(id: string, userId: string, role: string): Promise<any> {
    return this.send(`/api/v1/organizations/${id}/members/${userId}`, {
      method: 'PATCH',
      body: {
        role
      }
    })
  }

  /**
   * 获取组织设置
   */
  async getSettings(id: string): Promise<Record<string, any>> {
    return this.send<Record<string, any>>(`/api/v1/organizations/${id}/settings`, {
      method: 'GET'
    })
  }

  /**
   * 更新组织设置
   */
  async updateSettings(id: string, settings: Record<string, any>): Promise<Record<string, any>> {
    return this.send<Record<string, any>>(`/api/v1/organizations/${id}/settings`, {
      method: 'PATCH',
      body: settings
    })
  }
}
