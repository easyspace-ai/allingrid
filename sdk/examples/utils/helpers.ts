/**
 * 等待指定时间
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * 等待条件满足
 */
export async function waitFor(
  condition: () => boolean,
  timeout: number = 5000,
  interval: number = 100
): Promise<boolean> {
  const startTime = Date.now()
  
  while (Date.now() - startTime < timeout) {
    if (condition()) {
      return true
    }
    await sleep(interval)
  }
  
  return false
}

/**
 * 安全执行，捕获错误
 */
export async function safeExecute<T>(
  fn: () => Promise<T>,
  errorMessage?: string
): Promise<T | null> {
  try {
    return await fn()
  } catch (error: any) {
    console.error(errorMessage || '执行失败:', error.message || error)
    return null
  }
}

/**
 * 格式化对象为 JSON
 */
export function formatJSON(obj: any): string {
  return JSON.stringify(obj, null, 2)
}

/**
 * 清理资源 ID 列表
 */
export interface ResourceIds {
  spaceIds?: string[]
  baseIds?: string[]
  tableIds?: string[]
  fieldIds?: string[]
  recordIds?: string[]
  viewIds?: string[]
}

/**
 * 验证资源 ID
 */
export function validateId(id: string | undefined, name: string): string {
  if (!id) {
    throw new Error(`${name} ID 不能为空`)
  }
  return id
}


