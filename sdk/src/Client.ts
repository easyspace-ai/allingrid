import { EventEmitter } from 'node:events'
import { BaseAuthStore } from '@/stores/BaseAuthStore'
import { LocalAuthStore } from '@/stores/LocalAuthStore'
import { ClientResponseError } from '@/ClientResponseError'
import { SendOptions, BeforeSendHook, AfterSendHook, AuthResponse } from '@/types/common'
import { BaseService } from '@/services/BaseService'
import { AuthService } from '@/services/AuthService'
import { SpaceService } from '@/services/SpaceService'
import { LuckDBBaseService } from '@/services/LuckDBBaseService'
import { TableService } from '@/services/TableService'
import { FieldService } from '@/services/FieldService'
import { RecordService } from '@/services/RecordService'
import { ViewService } from '@/services/ViewService'
import { UserService } from '@/services/UserService'
import { CollaboratorService } from '@/services/CollaboratorService'
import { RealtimeService } from '@/services/RealtimeService'
import { ShareDBService } from '@/services/ShareDBService'
import { ShareDBAdminService } from '@/services/ShareDBAdminService'
import { OrganizationService } from '@/services/OrganizationService'
import { WorkflowService } from '@/services/WorkflowService'
import { RecordHistoryService } from '@/services/RecordHistoryService'
import { NotificationService } from '@/services/NotificationService'
import { AttachmentService } from '@/services/AttachmentService'

export class LuckDBClient extends EventEmitter {
  baseURL: string
  lang: string
  authStore: BaseAuthStore

  // 请求钩子
  beforeSend?: BeforeSendHook
  afterSend?: AfterSendHook

  // 服务实例
  auth: AuthService
  spaces: SpaceService
  bases: LuckDBBaseService
  tables: TableService
  fields: FieldService
  records: RecordService
  views: ViewService
  users: UserService
  collaborators: CollaboratorService
  realtime: RealtimeService
  sharedb: ShareDBService
  sharedbAdmin: ShareDBAdminService
  organizations: OrganizationService
  workflows: WorkflowService
  recordHistory: RecordHistoryService
  notifications: NotificationService
  attachments: AttachmentService

  // 请求取消控制器
  private cancelControllers: Map<string, AbortController> = new Map()

  constructor(
    baseURL: string,
    authStore?: BaseAuthStore,
    lang = 'zh-CN'
  ) {
    super()

    this.baseURL = baseURL.replace(/\/+$/, '') // 移除末尾的斜杠
    this.lang = lang
    this.authStore = authStore || new LocalAuthStore()

    // 初始化所有服务
    this.auth = new AuthService(this)
    this.spaces = new SpaceService(this)
    this.bases = new LuckDBBaseService(this)
    this.tables = new TableService(this)
    this.fields = new FieldService(this)
    this.records = new RecordService(this)
    this.views = new ViewService(this)
    this.users = new UserService(this)
    this.collaborators = new CollaboratorService(this)
    this.realtime = new RealtimeService(this)
    this.sharedb = new ShareDBService(this)
    this.sharedbAdmin = new ShareDBAdminService(this)
    this.organizations = new OrganizationService(this)
    this.workflows = new WorkflowService(this)
    this.recordHistory = new RecordHistoryService(this)
    this.notifications = new NotificationService(this)
    this.attachments = new AttachmentService(this)

    // 监听认证状态变化
    this.authStore.onChange(() => {
      (this as any).emit('authChange', this.authStore.isValid)
    })
  }

  /**
   * 构建完整的 URL
   */
  buildUrl(path: string): string {
    if (path.startsWith('http')) {
      return path
    }

    const cleanPath = path.startsWith('/') ? path : '/' + path
    return this.baseURL + cleanPath
  }

  /**
   * 发送 HTTP 请求
   */
  async send<T>(path: string, options: SendOptions = {}): Promise<T> {
    const sendOptions = await this.initSendOptions({ ...options, path })
    // initSendOptions 已经构建了包含 query 参数的完整 URL，存储在 RequestInit 中
    // 但是 fetch 需要单独的 URL，我们需要从 sendOptions 中提取或重新构建
    const url = sendOptions.url || this.buildUrl(path)

    // 应用 beforeSend 钩子
    if (this.beforeSend) {
      const modifiedOptions = await this.beforeSend(url, sendOptions as SendOptions)
      Object.assign(sendOptions, modifiedOptions)
    }

    try {
      const response = await fetch(url, sendOptions)
      const data = await this.parseResponse(response)

      // 应用 afterSend 钩子
      if (this.afterSend) {
        return await this.afterSend(response, data)
      }

      return data
    } catch (error) {
      throw this.wrapError(error, url)
    }
  }

  /**
   * 初始化发送选项
   */
  private async initSendOptions(options: SendOptions & { path?: string }): Promise<RequestInit & { url?: string }> {
    const {
      method = 'GET',
      headers = {},
      body,
      query,
      cache = 'default',
      signal
    } = options

    // 处理查询参数
    let url = this.buildUrl(options.path || '')
    if (query) {
      const searchParams = new URLSearchParams()
      for (const [key, value] of Object.entries(query)) {
        if (value !== null && value !== undefined) {
          searchParams.append(key, String(value))
        }
      }
      const queryString = searchParams.toString()
      if (queryString) {
        url += (url.includes('?') ? '&' : '?') + queryString
      }
    }

    // 处理请求体
    let requestBody: any = body
    let contentType = headers['Content-Type'] || headers['content-type']

    if (body instanceof FormData) {
      requestBody = body
      // 不设置 Content-Type，让浏览器自动设置
      delete headers['Content-Type']
      delete headers['content-type']
    } else if (body && typeof body === 'object') {
      requestBody = JSON.stringify(body)
      contentType = 'application/json'
    }

    // 设置默认头部
    const requestHeaders: Record<string, string> = {
      'Accept': 'application/json',
      'Accept-Language': this.lang,
      ...headers
    }

    if (contentType) {
      requestHeaders['Content-Type'] = contentType
    }

    // 添加认证头
    const token = this.authStore.token
    if (token) {
      requestHeaders['Authorization'] = `Bearer ${token}`
      console.log('[Client.initSendOptions] 已添加认证头，token 长度:', token.length)
    } else {
      console.warn('[Client.initSendOptions] 没有 token，请求可能失败')
    }

    // 处理自动取消
    let requestSignal = signal
    if (!requestSignal && method !== 'GET') {
      const controller = new AbortController()
      const requestId = `${method}:${url}`
      this.cancelControllers.set(requestId, controller)
      requestSignal = controller.signal

      // 清理控制器
      requestSignal.addEventListener('abort', () => {
        this.cancelControllers.delete(requestId)
      })
    }

    const requestInit: RequestInit & { url?: string } = {
      method,
      headers: requestHeaders,
      body: requestBody,
      cache,
      signal: requestSignal || null
    }
    
    // 将构建好的 URL 存储起来，供 send 方法使用
    requestInit.url = url
    
    return requestInit
  }

  /**
   * 解析响应
   */
  private async parseResponse(response: Response): Promise<any> {
    if (!response.ok) {
      let errorData: any = {}
      try {
        errorData = await response.json()
      } catch {
        errorData = { message: response.statusText }
      }
      throw new ClientResponseError(response, errorData)
    }

    const contentType = response.headers.get('content-type')
    if (contentType?.includes('application/json')) {
      let data: any
      try {
        data = await response.json()
      } catch (error) {
        console.error('Failed to parse JSON response:', error)
        throw new Error('响应解析失败：无效的 JSON 格式')
      }

      // 统一解包服务端标准响应结构 { code, message, data, ... }
      if (data && typeof data === 'object' && 'code' in data && 'data' in data) {
        console.log('[Client] Detected standard response format with code and data')
        console.log('[Client] data.code:', data.code)
        console.log('[Client] data.data:', data.data)
        
        // 确保 data 不为 undefined 或 null
        if (data.data === undefined || data.data === null) {
          console.warn('[Client] Response data field is undefined or null, returning full response')
          return data
        }
        
        console.log('[Client] Returning unwrapped data:', data.data)
        return data.data
      }
      
      // 如果没有标准格式，直接返回数据
      if (data === undefined || data === null) {
        console.warn('[Client] Response data is undefined or null, returning empty object')
        return {}
      }
      
      console.log('[Client] Returning data as-is (no standard format):', data)
      return data
    }

    const text = await response.text()
    return text || {}
  }

  /**
   * 包装错误
   */
  private wrapError(error: any, url: string): Error {
    if (error instanceof ClientResponseError) {
      return error
    }

    if (error.name === 'AbortError') {
        return new ClientResponseError(
          new Response(null, { status: 408, statusText: 'Request Timeout' }),
          { code: 'REQUEST_CANCELLED', message: '请求被取消' },
          error,
          url
        )
    }

    // 网络连接错误
    if (error.code === 'ECONNREFUSED' || error.message?.includes('fetch failed')) {
      return new ClientResponseError(
        new Response(null, { status: 503, statusText: 'Service Unavailable' }),
        { code: 'CONNECTION_REFUSED', message: '无法连接到服务器，请检查服务器是否正在运行' },
        error,
        url
      )
    }

    // 其他网络错误
    return new ClientResponseError(
      new Response(null, { status: 500, statusText: 'Internal Server Error' }),
      { code: 'NETWORK_ERROR', message: error.message || '网络错误' },
      error,
      url
    )
  }

  /**
   * 取消指定请求
   */
  cancelRequest(requestId: string): boolean {
    const controller = this.cancelControllers.get(requestId)
    if (controller) {
      controller.abort()
      return true
    }
    return false
  }

  /**
   * 取消所有请求
   */
  cancelAllRequests(): void {
    for (const controller of this.cancelControllers.values()) {
      controller.abort()
    }
    this.cancelControllers.clear()
  }

  /**
   * 启用自动取消（非 GET 请求）
   */
  autoCancellation(enable = true): this {
    if (enable) {
      // 自动取消逻辑已在 initSendOptions 中实现
    }
    return this
  }

  /**
   * 构建过滤器（用于查询）
   */
  filter(expression: string): string {
    return expression
  }
}
