import { ErrorResponse } from '@/types/common'

export class ClientResponseError extends Error {
  url: string
  status: number
  code: string | number
  message: string
  details?: any
  response: Response
  isAbort: boolean
  originalError: Error | null

  constructor(response: Response, data: ErrorResponse, originalError?: Error, url?: string) {
    const message = ClientResponseError.getErrorMessage(response.status, data)
    super(message)

    this.name = 'ClientResponseError'
    this.url = url || response.url
    this.status = response.status
    this.code = (data as any)?.code || this.getDefaultErrorCode(response.status)
    this.message = (data as any)?.message || message
    // 兼容两种错误载荷位置：error.details 或 data
    const detailsFromError = (data as any)?.error?.details
    const detailsFromData = (data as any)?.data
    this.details = detailsFromError !== undefined ? detailsFromError : detailsFromData
    this.response = response
    this.isAbort = originalError?.name === 'AbortError'
    this.originalError = originalError || null

    // 保持堆栈跟踪
    if ((Error as any).captureStackTrace) {
      (Error as any).captureStackTrace(this, ClientResponseError)
    }
  }

  static getErrorMessage(status: number, data: ErrorResponse): string {
    // 处理常见的 HTTP 状态码
    switch (status) {
      case 400:
        return data.message || '请求参数错误'
      case 401:
        return '未授权访问，请先登录'
      case 403:
        return '权限不足，无法访问此资源'
      case 404:
        return '请求的资源不存在'
      case 409:
        return data.message || '资源冲突'
      case 422:
        return data.message || '数据验证失败'
      case 429:
        return '请求过于频繁，请稍后再试'
      case 500:
        return '服务器内部错误'
      case 503:
        return '服务暂时不可用'
      default:
        return data.message || `请求失败 (${status})`
    }
  }

  /**
   * 获取默认错误代码
   */
  private getDefaultErrorCode(status: number): string {
    switch (status) {
      case 400:
        return 'BAD_REQUEST'
      case 401:
        return 'UNAUTHORIZED'
      case 403:
        return 'FORBIDDEN'
      case 404:
        return 'NOT_FOUND'
      case 409:
        return 'CONFLICT'
      case 408:
        return 'TIMEOUT_ERROR'
      case 429:
        return 'TOO_MANY_REQUESTS'
      case 500:
        return 'INTERNAL_SERVER_ERROR'
      case 502:
        return 'BAD_GATEWAY'
      case 503:
        return 'SERVICE_UNAVAILABLE'
      case 504:
        return 'GATEWAY_TIMEOUT'
      default:
        return 'UNKNOWN_ERROR'
    }
  }

  // 检查是否为网络错误
  get isNetworkError(): boolean {
    return this.originalError?.message?.includes('ECONNREFUSED') || false
  }

  // 检查是否为超时错误
  get isTimeoutError(): boolean {
    return this.originalError?.message?.includes('timeout') || false
  }

  // 检查是否为取消错误
  get isCancelledError(): boolean {
    return this.isAbort
  }

  /**
   * 是否为认证错误
   */
  get isAuthError(): boolean {
    return this.status === 401 || this.code === 'UNAUTHORIZED' || this.code === 'INVALID_TOKEN' || this.code === 'TOKEN_EXPIRED'
  }

  /**
   * 是否为权限错误
   */
  get isPermissionError(): boolean {
    return this.status === 403 || this.code === 'FORBIDDEN' || this.code === 'PERMISSION_DENIED'
  }

  /**
   * 是否为验证错误
   */
  get isValidationError(): boolean {
    return this.status === 400 || this.code === 'VALIDATION_FAILED' || this.code === 'BAD_REQUEST'
  }

  /**
   * 是否为资源不存在错误
   */
  get isNotFoundError(): boolean {
    return this.status === 404 || this.code === 'NOT_FOUND'
  }

  /**
   * 是否为冲突错误
   */
  get isConflictError(): boolean {
    return this.status === 409 || this.code === 'CONFLICT'
  }

  /**
   * 是否为字段相关错误
   */
  get isFieldError(): boolean {
    if (typeof this.code === 'string') {
      return this.code.startsWith('FIELD_') || this.code.startsWith('INVALID_FIELD')
    }
    return false
  }

  /**
   * 转换为JSON格式
   */
  toJSON(): any {
    return {
      name: this.name,
      url: this.url,
      status: this.status,
      code: this.code,
      message: this.message,
      details: this.details,
      isNetworkError: this.isNetworkError,
      isTimeoutError: this.isTimeoutError,
      isCancelledError: this.isCancelledError,
      isAuthError: this.isAuthError,
      isPermissionError: this.isPermissionError,
      isValidationError: this.isValidationError,
      isNotFoundError: this.isNotFoundError,
      isConflictError: this.isConflictError,
      isFieldError: this.isFieldError
    }
  }
}
