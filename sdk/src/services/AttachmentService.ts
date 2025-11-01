import { BaseService } from './BaseService'
import {
  AttachmentSignatureResponse,
  GenerateSignatureRequest,
  AttachmentItem,
  NotifyUploadResponse,
  ListAttachmentsFilter,
  AttachmentStats
} from '@/types/attachment'

export class AttachmentService extends BaseService {
  /**
   * 生成上传签名
   * 对齐服务端 API: POST /api/v1/attachments/signature
   * 请求格式: { table_id, field_id, record_id, max_size?, allowed_types? }
   */
  async generateSignature(request: GenerateSignatureRequest): Promise<AttachmentSignatureResponse> {
    // 服务端期望 snake_case 格式
    const body = {
      table_id: request.table_id,
      field_id: request.field_id,
      record_id: request.record_id,
      ...(request.max_size && { max_size: request.max_size }),
      ...(request.allowed_types && { allowed_types: request.allowed_types })
    }
    
    const response = await this.send<AttachmentSignatureResponse>('/api/v1/attachments/signature', {
      method: 'POST',
      body
    })
    return response
  }

  /**
   * 上传文件
   * 对齐服务端 API: POST /api/v1/attachments/upload/:token
   * 请求格式: multipart/form-data { file: File }
   */
  async uploadFile(token: string, file: File | Blob, filename?: string): Promise<void> {
    const formData = new FormData()
    const fileToUpload = filename ? new File([file], filename) : file
    formData.append('file', fileToUpload)

    await this.send(`/api/v1/attachments/upload/${token}`, {
      method: 'POST',
      body: formData
    })
  }

  /**
   * 通知上传完成
   * 对齐服务端 API: POST /api/v1/attachments/notify/:token
   * 请求格式: { token, filename }
   */
  async notifyUpload(token: string, filename: string): Promise<NotifyUploadResponse> {
    const response = await this.send<NotifyUploadResponse>(`/api/v1/attachments/notify/${token}`, {
      method: 'POST',
      body: {
        token,
        filename
      }
    })
    return response
  }

  /**
   * 获取附件信息
   * 对齐服务端 API: GET /api/v1/attachments/:id
   */
  async getAttachment(id: string): Promise<AttachmentItem> {
    const response = await this.send<AttachmentItem>(`/api/v1/attachments/${id}`, {
      method: 'GET'
    })
    return response
  }

  /**
   * 列出附件
   * 对齐服务端 API: GET /api/v1/attachments
   * 查询参数: tableId, fieldId?, recordId?
   */
  async listAttachments(filter: ListAttachmentsFilter = {}): Promise<AttachmentItem[]> {
    const response = await this.send<AttachmentItem[]>('/api/v1/attachments', {
      method: 'GET',
      query: filter
    })
    
    // 服务端可能返回数组或对象，需要处理
    if (Array.isArray(response)) {
      return response
    }
    
    // 如果是对象格式，尝试提取 items 或 attachments 字段
    if (response && typeof response === 'object') {
      const items = (response as any).items || (response as any).attachments || (response as any).list
      return Array.isArray(items) ? items : []
    }
    
    return []
  }

  /**
   * 获取附件统计
   * 对齐服务端 API: GET /api/v1/tables/:tableId/attachments/stats
   */
  async getAttachmentStats(tableId: string): Promise<AttachmentStats> {
    const response = await this.send<AttachmentStats>(`/api/v1/tables/${tableId}/attachments/stats`, {
      method: 'GET'
    })
    return response
  }

  /**
   * 读取文件
   * 对齐服务端 API: GET /api/v1/attachments/read/*path
   * 注意：path 可能包含斜杠，需要使用通配符路由
   */
  async readFile(path: string, token?: string): Promise<Blob> {
    const url = `/api/v1/attachments/read/${path}`
    const query: Record<string, string> = {}
    if (token) {
      query.token = token
    }

    const response = await fetch(this.buildUrl(url) + (Object.keys(query).length > 0 ? '?' + new URLSearchParams(query).toString() : ''), {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${this.getAuthToken() || ''}`
      }
    })

    if (!response.ok) {
      let errorData: any = {}
      try {
        errorData = await response.json()
      } catch {
        errorData = { message: response.statusText }
      }
      throw new Error(`Failed to read file: ${errorData.message || response.statusText}`)
    }

    return await response.blob()
  }

  /**
   * 读取文件为文本
   */
  async readFileAsText(path: string, token?: string, encoding: string = 'utf-8'): Promise<string> {
    const blob = await this.readFile(path, token)
    return await blob.text()
  }

  /**
   * 读取文件为数组缓冲区
   */
  async readFileAsArrayBuffer(path: string, token?: string): Promise<ArrayBuffer> {
    const blob = await this.readFile(path, token)
    return await blob.arrayBuffer()
  }

  /**
   * 删除附件
   * 对齐服务端 API: DELETE /api/v1/attachments/:id
   */
  async deleteAttachment(id: string): Promise<void> {
    await this.send(`/api/v1/attachments/${id}`, {
      method: 'DELETE'
    })
  }

  /**
   * 完整的上传流程（生成签名 -> 上传文件 -> 通知完成）
   */
  async upload(
    request: GenerateSignatureRequest,
    file: File | Blob,
    filename?: string
  ): Promise<AttachmentItem> {
    // 1. 生成上传签名
    const signature = await this.generateSignature(request)

    // 2. 上传文件
    const fileToUpload = file instanceof File ? file : new File([file], filename || 'file')
    await this.uploadFile(signature.token, fileToUpload, filename)

    // 3. 通知上传完成
    const notifyResponse = await this.notifyUpload(signature.token, filename || fileToUpload.name)

    return notifyResponse.attachment
  }
}

