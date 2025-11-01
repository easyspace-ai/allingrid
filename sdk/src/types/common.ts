// 通用类型定义
export interface PaginationRequest {
  page?: number
  perPage?: number
}

// 服务端分页响应格式
export interface PaginationResponse {
  total: number
  page: number
  pageSize: number
  totalPages: number
  hasNext: boolean
  hasPrevious: boolean
}

// 带数据的列表响应格式（服务端格式）
export interface ListResponse<T> {
  items: T[]
  pagination: PaginationResponse
}

// SDK兼容的分页响应格式（向后兼容）
export interface PaginationResponseWrapper<T> {
  items: T[]
  totalItems: number
  totalPages: number
  page: number
  perPage: number
  hasNext: boolean
  hasPrevious: boolean
}

export interface TimeRange {
  start?: string
  end?: string
}

export interface SortOrder {
  field: string
  direction: 'asc' | 'desc'
}

export interface OrderBy {
  field: string
  direction: 'asc' | 'desc'
}

// 请求选项
export interface SendOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'
  headers?: Record<string, string>
  body?: any
  query?: Record<string, any>
  cache?: RequestCache
  signal?: AbortSignal
}

// 认证相关
export interface AuthRecord {
  id: string
  email: string
  name?: string
  avatar?: string
  created: string
  updated: string
}

export interface AuthResponse {
  token: string
  record: AuthRecord
}

// 服务器原始响应格式
export interface ServerAuthResponse {
  code: number
  message: string
  data: {
    user: AuthRecord
    accessToken: string
    refreshToken: string
  }
}

// 错误类型
export interface ErrorResponse {
  code?: string | number
  message?: string
  data?: any
}

// 钩子函数类型
export type BeforeSendHook = (url: string, options: SendOptions) => Promise<SendOptions> | SendOptions
export type AfterSendHook = (response: Response, data: any) => Promise<any> | any
