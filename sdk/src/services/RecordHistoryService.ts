import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse } from '@/types/common'

export interface RecordHistory {
  id: string
  recordId: string
  tableId: string
  action: 'create' | 'update' | 'delete' | 'restore'
  changes?: RecordChange[]
  previousData?: Record<string, any>
  newData?: Record<string, any>
  userId: string
  userName?: string
  userEmail?: string
  timestamp: string
  ipAddress?: string
  userAgent?: string
}

export interface RecordChange {
  fieldId: string
  fieldName: string
  oldValue: any
  newValue: any
  changeType: 'added' | 'modified' | 'removed'
}

export interface RecordHistoryListFilter {
  recordId?: string
  tableId?: string
  action?: 'create' | 'update' | 'delete' | 'restore'
  userId?: string
  startDate?: string
  endDate?: string
  fieldId?: string
}

export interface RecordHistoryResponse {
  id: string
  recordId: string
  tableId: string
  action: 'create' | 'update' | 'delete' | 'restore'
  changes?: RecordChange[]
  previousData?: Record<string, any>
  newData?: Record<string, any>
  userId: string
  userName?: string
  userEmail?: string
  timestamp: string
  ipAddress?: string
  userAgent?: string
}

export interface RecordHistoryStats {
  totalChanges: number
  changesByAction: Record<string, number>
  changesByUser: Record<string, number>
  changesByField: Record<string, number>
  changesByDay: Record<string, number>
}

export class RecordHistoryService extends BaseService {
  /**
   * 获取记录历史列表
   */
  async getList(
    page = 1,
    perPage = 20,
    filter: RecordHistoryListFilter = {}
  ): Promise<PaginationResponse<RecordHistoryResponse>> {
    return this.send<PaginationResponse<RecordHistoryResponse>>('/api/v1/record-history', {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取所有记录历史
   */
  async getFullList(filter: RecordHistoryListFilter = {}): Promise<RecordHistoryResponse[]> {
    const result: RecordHistoryResponse[] = []
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
   * 获取单个历史记录
   */
  async getOne(id: string): Promise<RecordHistoryResponse> {
    return this.send<RecordHistoryResponse>(`/api/v1/record-history/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 获取记录的所有历史
   */
  async getByRecord(
    recordId: string,
    page = 1,
    perPage = 20
  ): Promise<PaginationResponse<RecordHistoryResponse>> {
    return this.send<PaginationResponse<RecordHistoryResponse>>(`/api/v1/records/${recordId}/history`, {
      method: 'GET',
      query: {
        page,
        perPage
      }
    })
  }

  /**
   * 获取表格的所有历史
   */
  async getByTable(
    tableId: string,
    page = 1,
    perPage = 20,
    filter: Omit<RecordHistoryListFilter, 'tableId'> = {}
  ): Promise<PaginationResponse<RecordHistoryResponse>> {
    return this.send<PaginationResponse<RecordHistoryResponse>>(`/api/v1/tables/${tableId}/history`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取字段的历史变更
   */
  async getByField(
    tableId: string,
    fieldId: string,
    page = 1,
    perPage = 20
  ): Promise<PaginationResponse<RecordHistoryResponse>> {
    return this.send<PaginationResponse<RecordHistoryResponse>>(`/api/v1/tables/${tableId}/fields/${fieldId}/history`, {
      method: 'GET',
      query: {
        page,
        perPage
      }
    })
  }

  /**
   * 获取用户的历史操作
   */
  async getByUser(
    userId: string,
    page = 1,
    perPage = 20,
    filter: Omit<RecordHistoryListFilter, 'userId'> = {}
  ): Promise<PaginationResponse<RecordHistoryResponse>> {
    return this.send<PaginationResponse<RecordHistoryResponse>>(`/api/v1/users/${userId}/history`, {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取历史统计
   */
  async getStats(
    filter: RecordHistoryListFilter = {}
  ): Promise<RecordHistoryStats> {
    return this.send<RecordHistoryStats>('/api/v1/record-history/stats', {
      method: 'GET',
      query: filter
    })
  }

  /**
   * 获取记录的历史统计
   */
  async getRecordStats(recordId: string): Promise<RecordHistoryStats> {
    return this.send<RecordHistoryStats>(`/api/v1/records/${recordId}/history/stats`, {
      method: 'GET'
    })
  }

  /**
   * 获取表格的历史统计
   */
  async getTableStats(tableId: string): Promise<RecordHistoryStats> {
    return this.send<RecordHistoryStats>(`/api/v1/tables/${tableId}/history/stats`, {
      method: 'GET'
    })
  }

  /**
   * 恢复记录到指定版本
   */
  async restoreToVersion(recordId: string, historyId: string): Promise<any> {
    return this.send(`/api/v1/records/${recordId}/restore`, {
      method: 'POST',
      body: {
        historyId
      }
    })
  }

  /**
   * 比较两个历史版本
   */
  async compareVersions(historyId1: string, historyId2: string): Promise<{
    differences: RecordChange[]
    summary: {
      added: number
      modified: number
      removed: number
    }
  }> {
    return this.send(`/api/v1/record-history/compare`, {
      method: 'POST',
      body: {
        historyId1,
        historyId2
      }
    })
  }

  /**
   * 导出历史数据
   */
  async export(
    format: 'csv' | 'json' | 'xlsx' = 'csv',
    filter: RecordHistoryListFilter = {}
  ): Promise<Blob> {
    return this.send<Blob>('/api/v1/record-history/export', {
      method: 'GET',
      query: {
        format,
        ...filter
      }
    })
  }

  /**
   * 清理历史数据
   */
  async cleanup(
    olderThan: string,
    filter: RecordHistoryListFilter = {}
  ): Promise<{
    deletedCount: number
    message: string
  }> {
    return this.send(`/api/v1/record-history/cleanup`, {
      method: 'POST',
      body: {
        olderThan,
        filter
      }
    })
  }
}
