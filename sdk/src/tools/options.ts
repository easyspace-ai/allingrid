import { SendOptions } from '@/types/common'

/**
 * 请求选项工具函数
 */

export interface RequestOptions extends SendOptions {
  timeout?: number
  retries?: number
  retryDelay?: number
}

/**
 * 合并请求选项
 */
export function mergeRequestOptions(
  defaultOptions: RequestOptions,
  userOptions: RequestOptions = {}
): RequestOptions {
  return {
    ...defaultOptions,
    ...userOptions,
    headers: {
      ...defaultOptions.headers,
      ...userOptions.headers
    },
    query: {
      ...defaultOptions.query,
      ...userOptions.query
    }
  }
}

/**
 * 创建默认请求选项
 */
export function createDefaultRequestOptions(): RequestOptions {
  return {
    method: 'GET',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    timeout: 30000,
    retries: 0,
    retryDelay: 1000
  }
}

/**
 * 创建 GET 请求选项
 */
export function createGetOptions(query?: Record<string, any>): RequestOptions {
  return {
    method: 'GET',
    ...(query && { query })
  }
}

/**
 * 创建 POST 请求选项
 */
export function createPostOptions(body?: any): RequestOptions {
  return {
    method: 'POST',
    body
  }
}

/**
 * 创建 PUT 请求选项
 */
export function createPutOptions(body?: any): RequestOptions {
  return {
    method: 'PUT',
    body
  }
}

/**
 * 创建 PATCH 请求选项
 */
export function createPatchOptions(body?: any): RequestOptions {
  return {
    method: 'PATCH',
    body
  }
}

/**
 * 创建 DELETE 请求选项
 */
export function createDeleteOptions(): RequestOptions {
  return {
    method: 'DELETE'
  }
}

/**
 * 创建文件上传选项
 */
export function createUploadOptions(
  file: File | Blob,
  filename?: string,
  additionalFields?: Record<string, any>
): RequestOptions {
  const formData = new FormData()
  
  if (filename) {
    formData.append('file', file, filename)
  } else {
    formData.append('file', file)
  }

  if (additionalFields) {
    for (const [key, value] of Object.entries(additionalFields)) {
      formData.append(key, String(value))
    }
  }

  return {
    method: 'POST',
    body: formData,
    headers: {
      // 不设置 Content-Type，让浏览器自动设置
    }
  }
}

/**
 * 创建分页选项
 */
export function createPaginationOptions(
  page = 1,
  perPage = 20,
  additionalQuery?: Record<string, any>
): RequestOptions {
  return {
    method: 'GET',
    query: {
      page,
      perPage,
      ...additionalQuery
    }
  }
}

/**
 * 创建排序选项
 */
export function createSortOptions(
  field: string,
  direction: 'asc' | 'desc' = 'asc',
  additionalQuery?: Record<string, any>
): RequestOptions {
  return {
    method: 'GET',
    query: {
      sort: `${field}:${direction}`,
      ...additionalQuery
    }
  }
}

/**
 * 创建过滤选项
 */
export function createFilterOptions(
  filter: string,
  additionalQuery?: Record<string, any>
): RequestOptions {
  return {
    method: 'GET',
    query: {
      filter,
      ...additionalQuery
    }
  }
}

/**
 * 创建搜索选项
 */
export function createSearchOptions(
  query: string,
  fields?: string[],
  additionalQuery?: Record<string, any>
): RequestOptions {
  const searchQuery: Record<string, any> = {
    q: query,
    ...additionalQuery
  }

  if (fields && fields.length > 0) {
    searchQuery.fields = fields.join(',')
  }

  return {
    method: 'GET',
    query: searchQuery
  }
}

/**
 * 验证请求选项
 */
export function validateRequestOptions(options: RequestOptions): boolean {
  if (!options.method) {
    return false
  }

  const validMethods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']
  if (!validMethods.includes(options.method)) {
    return false
  }

  if (options.timeout && options.timeout < 0) {
    return false
  }

  if (options.retries && options.retries < 0) {
    return false
  }

  if (options.retryDelay && options.retryDelay < 0) {
    return false
  }

  return true
}
