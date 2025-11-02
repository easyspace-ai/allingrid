import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse, ListResponse } from '@/types/common'

export interface Notification {
  id: string
  title: string
  message: string
  type: 'info' | 'success' | 'warning' | 'error'
  category: 'system' | 'record' | 'workflow' | 'collaboration' | 'security'
  data?: Record<string, any>
  isRead: boolean
  userId: string
  createdAt: string
  readAt?: string
  expiresAt?: string
}

export interface NotificationCreateRequest {
  title: string
  message: string
  type: 'info' | 'success' | 'warning' | 'error'
  category: 'system' | 'record' | 'workflow' | 'collaboration' | 'security'
  data?: Record<string, any>
  userId?: string
  expiresAt?: string
}

export interface NotificationUpdateRequest {
  isRead?: boolean
}

export interface NotificationListFilter {
  type?: 'info' | 'success' | 'warning' | 'error'
  category?: 'system' | 'record' | 'workflow' | 'collaboration' | 'security'
  isRead?: boolean
  userId?: string
  createdAfter?: string
  createdBefore?: string
}

export interface NotificationResponse {
  id: string
  title: string
  message: string
  type: 'info' | 'success' | 'warning' | 'error'
  category: 'system' | 'record' | 'workflow' | 'collaboration' | 'security'
  data?: Record<string, any>
  isRead: boolean
  userId: string
  createdAt: string
  readAt?: string
  expiresAt?: string
}

export interface NotificationStats {
  total: number
  unread: number
  byType: Record<string, number>
  byCategory: Record<string, number>
}

export interface NotificationPreferences {
  email: boolean
  push: boolean
  inApp: boolean
  categories: Record<string, boolean>
}

export class NotificationService extends BaseService {
  /**
   * 创建通知
   */
  async create(data: NotificationCreateRequest): Promise<NotificationResponse> {
    return this.send<NotificationResponse>('/api/v1/notifications', {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取通知列表
   */
  async getList(
    page = 1,
    perPage = 20,
    filter: NotificationListFilter = {}
  ): Promise<ListResponse<NotificationResponse>> {
    return this.send<ListResponse<NotificationResponse>>('/api/v1/notifications', {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取所有通知
   */
  async getFullList(filter: NotificationListFilter = {}): Promise<NotificationResponse[]> {
    const result: NotificationResponse[] = []
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
   * 获取单个通知
   */
  async getOne(id: string): Promise<NotificationResponse> {
    return this.send<NotificationResponse>(`/api/v1/notifications/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新通知
   */
  async update(id: string, data: NotificationUpdateRequest): Promise<NotificationResponse> {
    return this.send<NotificationResponse>(`/api/v1/notifications/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除通知
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/notifications/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 标记通知为已读
   */
  async markAsRead(id: string): Promise<NotificationResponse> {
    return this.send<NotificationResponse>(`/api/v1/notifications/${id}/read`, {
      method: 'POST'
    })
  }

  /**
   * 标记通知为未读
   */
  async markAsUnread(id: string): Promise<NotificationResponse> {
    return this.send<NotificationResponse>(`/api/v1/notifications/${id}/unread`, {
      method: 'POST'
    })
  }

  /**
   * 批量标记为已读
   */
  async markAllAsRead(filter: NotificationListFilter = {}): Promise<{
    updatedCount: number
  }> {
    return this.send(`/api/v1/notifications/mark-all-read`, {
      method: 'POST',
      body: filter
    })
  }

  /**
   * 批量删除通知
   */
  async deleteAll(filter: NotificationListFilter = {}): Promise<{
    deletedCount: number
  }> {
    return this.send(`/api/v1/notifications/delete-all`, {
      method: 'POST',
      body: filter
    })
  }

  /**
   * 获取未读通知数量
   */
  async getUnreadCount(): Promise<{
    count: number
  }> {
    return this.send<{
      count: number
    }>('/api/v1/notifications/unread-count', {
      method: 'GET'
    })
  }

  /**
   * 获取通知统计
   */
  async getStats(): Promise<NotificationStats> {
    return this.send<NotificationStats>('/api/v1/notifications/stats', {
      method: 'GET'
    })
  }

  /**
   * 获取用户通知偏好设置
   */
  async getPreferences(): Promise<NotificationPreferences> {
    return this.send<NotificationPreferences>('/api/v1/notifications/preferences', {
      method: 'GET'
    })
  }

  /**
   * 更新用户通知偏好设置
   */
  async updatePreferences(preferences: NotificationPreferences): Promise<NotificationPreferences> {
    return this.send<NotificationPreferences>('/api/v1/notifications/preferences', {
      method: 'PATCH',
      body: preferences
    })
  }

  /**
   * 发送测试通知
   */
  async sendTest(type: 'email' | 'push' | 'inApp'): Promise<{
    success: boolean
    message: string
  }> {
    return this.send(`/api/v1/notifications/test`, {
      method: 'POST',
      body: {
        type
      }
    })
  }

  /**
   * 订阅实时通知
   */
  subscribeToNotifications(callback: (notification: NotificationResponse) => void): () => void {
    // 这里应该实现 WebSocket 连接
    // 暂时返回一个空的取消函数
    return () => {}
  }

  /**
   * 获取通知模板
   */
  async getTemplates(): Promise<Array<{
    id: string
    name: string
    title: string
    message: string
    variables: string[]
  }>> {
    return this.send('/api/v1/notifications/templates', {
      method: 'GET'
    })
  }

  /**
   * 使用模板发送通知
   */
  async sendWithTemplate(
    templateId: string,
    variables: Record<string, any>,
    userId?: string
  ): Promise<NotificationResponse> {
    return this.send<NotificationResponse>('/api/v1/notifications/send-template', {
      method: 'POST',
      body: {
        templateId,
        variables,
        userId
      }
    })
  }
}
