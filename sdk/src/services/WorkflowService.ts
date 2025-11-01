import { BaseService } from './BaseService'
import { PaginationRequest, PaginationResponse } from '@/types/common'

export interface Workflow {
  id: string
  name: string
  description?: string
  tableId: string
  trigger: WorkflowTrigger
  actions: WorkflowAction[]
  isActive: boolean
  createdAt: string
  updatedAt: string
  createdBy: string
}

export interface WorkflowTrigger {
  type: 'record_created' | 'record_updated' | 'record_deleted' | 'field_changed' | 'manual'
  conditions?: WorkflowCondition[]
}

export interface WorkflowAction {
  type: 'send_notification' | 'update_field' | 'create_record' | 'send_email' | 'webhook'
  config: Record<string, any>
}

export interface WorkflowCondition {
  fieldId: string
  operator: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'not_contains'
  value: any
}

export interface WorkflowCreateRequest {
  name: string
  description?: string
  tableId: string
  trigger: WorkflowTrigger
  actions: WorkflowAction[]
  isActive?: boolean
}

export interface WorkflowUpdateRequest {
  name?: string
  description?: string
  trigger?: WorkflowTrigger
  actions?: WorkflowAction[]
  isActive?: boolean
}

export interface WorkflowListFilter {
  name?: string
  tableId?: string
  isActive?: boolean
  createdBy?: string
}

export interface WorkflowResponse {
  id: string
  name: string
  description?: string
  tableId: string
  trigger: WorkflowTrigger
  actions: WorkflowAction[]
  isActive: boolean
  createdAt: string
  updatedAt: string
  createdBy: string
}

export interface WorkflowExecution {
  id: string
  workflowId: string
  recordId: string
  status: 'pending' | 'running' | 'completed' | 'failed'
  result?: Record<string, any>
  error?: string
  startedAt: string
  completedAt?: string
}

export class WorkflowService extends BaseService {
  /**
   * 创建工作流
   */
  async create(data: WorkflowCreateRequest): Promise<WorkflowResponse> {
    return this.send<WorkflowResponse>('/api/v1/workflows', {
      method: 'POST',
      body: data
    })
  }

  /**
   * 获取工作流列表
   */
  async getList(
    page = 1,
    perPage = 20,
    filter: WorkflowListFilter = {}
  ): Promise<PaginationResponse<WorkflowResponse>> {
    return this.send<PaginationResponse<WorkflowResponse>>('/api/v1/workflows', {
      method: 'GET',
      query: {
        page,
        perPage,
        ...filter
      }
    })
  }

  /**
   * 获取所有工作流
   */
  async getFullList(filter: WorkflowListFilter = {}): Promise<WorkflowResponse[]> {
    const result: WorkflowResponse[] = []
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
   * 获取单个工作流
   */
  async getOne(id: string): Promise<WorkflowResponse> {
    return this.send<WorkflowResponse>(`/api/v1/workflows/${id}`, {
      method: 'GET'
    })
  }

  /**
   * 更新工作流
   */
  async update(id: string, data: WorkflowUpdateRequest): Promise<WorkflowResponse> {
    return this.send<WorkflowResponse>(`/api/v1/workflows/${id}`, {
      method: 'PATCH',
      body: data
    })
  }

  /**
   * 删除工作流
   */
  async delete(id: string): Promise<void> {
    await this.send(`/api/v1/workflows/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 激活工作流
   */
  async activate(id: string): Promise<WorkflowResponse> {
    return this.send<WorkflowResponse>(`/api/v1/workflows/${id}/activate`, {
      method: 'POST'
    })
  }

  /**
   * 停用工作流
   */
  async deactivate(id: string): Promise<WorkflowResponse> {
    return this.send<WorkflowResponse>(`/api/v1/workflows/${id}/deactivate`, {
      method: 'POST'
    })
  }

  /**
   * 手动触发工作流
   */
  async trigger(id: string, recordId: string): Promise<WorkflowExecution> {
    return this.send<WorkflowExecution>(`/api/v1/workflows/${id}/trigger`, {
      method: 'POST',
      body: {
        recordId
      }
    })
  }

  /**
   * 获取工作流执行历史
   */
  async getExecutions(
    id: string,
    page = 1,
    perPage = 20
  ): Promise<PaginationResponse<WorkflowExecution>> {
    return this.send<PaginationResponse<WorkflowExecution>>(`/api/v1/workflows/${id}/executions`, {
      method: 'GET',
      query: {
        page,
        perPage
      }
    })
  }

  /**
   * 获取执行详情
   */
  async getExecution(id: string, executionId: string): Promise<WorkflowExecution> {
    return this.send<WorkflowExecution>(`/api/v1/workflows/${id}/executions/${executionId}`, {
      method: 'GET'
    })
  }

  /**
   * 重试失败的执行
   */
  async retryExecution(id: string, executionId: string): Promise<WorkflowExecution> {
    return this.send<WorkflowExecution>(`/api/v1/workflows/${id}/executions/${executionId}/retry`, {
      method: 'POST'
    })
  }

  /**
   * 获取表格的工作流
   */
  async getByTable(tableId: string): Promise<WorkflowResponse[]> {
    return this.send<WorkflowResponse[]>(`/api/v1/tables/${tableId}/workflows`, {
      method: 'GET'
    })
  }
}
