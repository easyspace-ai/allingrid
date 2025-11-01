import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse } from '@/types/common'
import {
  Collaborator,
  CollaboratorCreateRequest,
  CollaboratorUpdateRequest,
  CollaboratorListFilter,
  CollaboratorResponse,
  InviteRequest,
  InviteResponse,
  InviteAcceptRequest,
  InviteDeclineRequest
} from '@/types/user'

export class CollaboratorService extends BaseService {
  /**
   * 获取协作者列表
   */
  async getList(
    resourceType: string,
    resourceId: string,
    page = 1,
    perPage = 20,
    filter: CollaboratorListFilter = {}
  ): Promise<PaginationResponse<CollaboratorResponse>> {
    return this.send<PaginationResponse<CollaboratorResponse>>(`/api/v1/${resourceType}s/${resourceId}/collaborators`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取所有协作者
   */
  async getFullList(
    resourceType: string,
    resourceId: string,
    filter: CollaboratorListFilter = {}
  ): Promise<CollaboratorResponse[]> {
    const result: CollaboratorResponse[] = []
    let page = 1
    const perPage = 100

    while (true) {
      const response = await this.getList(resourceType, resourceId, page, perPage, filter)
      result.push(...response.items)

      if (response.items.length < perPage) {
        break
      }
      page++
    }

    return result
  }

  /**
   * 添加协作者
   */
  async add(
    resourceType: string,
    resourceId: string,
    data: CollaboratorCreateRequest
  ): Promise<CollaboratorResponse> {
    return this.send<CollaboratorResponse>(`/api/v1/${resourceType}s/${resourceId}/collaborators`, {
      method: 'POST',
      body: data
    })
  }

  /**
   * 更新协作者
   */
  async update(
    resourceType: string,
    resourceId: string,
    collaboratorId: string,
    data: CollaboratorUpdateRequest
  ): Promise<CollaboratorResponse> {
    return this.send<CollaboratorResponse>(`/api/v1/${resourceType}s/${resourceId}/collaborators/${collaboratorId}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除协作者
   */
  async remove(
    resourceType: string,
    resourceId: string,
    collaboratorId: string
  ): Promise<void> {
    await this.send(`/api/v1/${resourceType}s/${resourceId}/collaborators/${collaboratorId}`, {
      method: 'DELETE'
    })
  }

  /**
   * 邀请协作者
   */
  async invite(
    resourceType: string,
    resourceId: string,
    data: InviteRequest
  ): Promise<InviteResponse> {
    return this.send<InviteResponse>(`/api/v1/${resourceType}s/${resourceId}/invites`, {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取邀请列表
   */
  async getInvites(
    resourceType: string,
    resourceId: string,
    page = 1,
    perPage = 20
  ): Promise<PaginationResponse<InviteResponse>> {
    return this.send<PaginationResponse<InviteResponse>>(`/api/v1/${resourceType}s/${resourceId}/invites`, {
      method: 'GET',
      query: { page, perPage }
    })
  }

  /**
   * 取消邀请
   */
  async cancelInvite(
    resourceType: string,
    resourceId: string,
    inviteId: string
  ): Promise<void> {
    await this.send(`/api/v1/${resourceType}s/${resourceId}/invites/${inviteId}`, {
      method: 'DELETE'
    })
  }

  /**
   * 接受邀请
   */
  async acceptInvite(data: InviteAcceptRequest): Promise<CollaboratorResponse> {
    return this.send<CollaboratorResponse>('/api/v1/invites/accept', {
      method: 'POST',
      body: data
    })
  }

  /**
   * 拒绝邀请
   */
  async declineInvite(data: InviteDeclineRequest): Promise<void> {
    await this.send('/api/v1/invites/decline', {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取我的邀请
   */
  async getMyInvites(page = 1, perPage = 20): Promise<PaginationResponse<InviteResponse>> {
    return this.send<PaginationResponse<InviteResponse>>('/api/v1/invites/me', {
      method: 'GET',
      query: { page, perPage }
    })
  }

  /**
   * 获取协作者权限
   */
  async getPermissions(
    resourceType: string,
    resourceId: string,
    userId: string
  ): Promise<{
    canRead: boolean
    canWrite: boolean
    canDelete: boolean
    canShare: boolean
    canManage: boolean
  }> {
    return this.send(`/api/v1/${resourceType}s/${resourceId}/collaborators/${userId}/permissions`, {
      method: 'GET'
    })
  }

  /**
   * 检查用户是否为协作者
   */
  async isCollaborator(
    resourceType: string,
    resourceId: string,
    userId: string
  ): Promise<boolean> {
    try {
      await this.getPermissions(resourceType, resourceId, userId)
      return true
    } catch {
      return false
    }
  }
}
