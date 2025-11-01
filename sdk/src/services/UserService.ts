import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse } from '@/types/common'
import {
  User,
  UserCreateRequest,
  UserUpdateRequest,
  UserListFilter,
  UserResponse
} from '@/types/user'

export class UserService extends BaseService {
  /**
   * 创建用户
   */
  async create(data: UserCreateRequest): Promise<UserResponse> {
    return this.send<UserResponse>('/api/v1/users', {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取用户列表
   */
  async getList(
    page = 1,
    perPage = 20,
    filter: UserListFilter = {}
  ): Promise<PaginationResponse<UserResponse>> {
    return this.send<PaginationResponse<UserResponse>>('/api/v1/users', {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取所有用户
   */
  async getFullList(filter: UserListFilter = {}): Promise<UserResponse[]> {
    const result: UserResponse[] = []
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
   * 获取单个用户
   */
  async getOne(id: string): Promise<UserResponse> {
    return this.send<UserResponse>(`/api/v1/users/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新用户
   */
  async update(id: string, data: UserUpdateRequest): Promise<UserResponse> {
    return this.send<UserResponse>(`/api/v1/users/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除用户
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/users/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 更新用户密码
   */
  async updatePassword(id: string, data: { oldPassword: string; newPassword: string; newPasswordConfirm: string }): Promise<void> {
    await this.send(`/api/v1/users/${id}/password`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 激活用户
   */
  async activate(id: string): Promise<UserResponse> {
    throw new Error('Not supported by server: /api/v1/users/:id/activate')
  }

  /**
   * 停用用户
   */
  async deactivate(id: string): Promise<UserResponse> {
    throw new Error('Not supported by server: /api/v1/users/:id/deactivate')
  }

  /**
   * 获取用户统计
   */
  async getStats(): Promise<{
    total: number
    active: number
    inactive: number
    createdToday: number
    createdThisWeek: number
    createdThisMonth: number
  }> {
    throw new Error('Not supported by server: /api/v1/users/stats')
  }
}
