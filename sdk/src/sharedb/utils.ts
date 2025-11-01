import { ShareDBOperation } from '@/types/sharedb'

/**
 * ShareDB 工具函数
 */

/**
 * 创建插入操作
 */
export function createInsertOp(path: string[], value: any): ShareDBOperation[] {
  return [{
    p: path,
    oi: value
  }]
}

/**
 * 创建删除操作
 */
export function createDeleteOp(path: string[], oldValue: any): ShareDBOperation[] {
  return [{
    p: path,
    od: oldValue
  }]
}

/**
 * 创建替换操作
 */
export function createReplaceOp(path: string[], newValue: any, oldValue: any): ShareDBOperation[] {
  return [{
    p: path,
    oi: newValue,
    od: oldValue
  }]
}

/**
 * 创建列表插入操作
 */
export function createListInsertOp(path: string[], index: number, value: any): ShareDBOperation[] {
  return [{
    p: [...path, index.toString()],
    li: value
  }]
}

/**
 * 创建列表删除操作
 */
export function createListDeleteOp(path: string[], index: number, oldValue: any): ShareDBOperation[] {
  return [{
    p: [...path, index.toString()],
    ld: oldValue
  }]
}

/**
 * 创建数字加法操作
 */
export function createNumberAddOp(path: string[], value: number): ShareDBOperation[] {
  return [{
    p: path,
    na: value
  }]
}

/**
 * 创建字段更新操作
 */
export function createFieldUpdateOp(fieldName: string, newValue: any, oldValue?: any): ShareDBOperation[] {
  const op: ShareDBOperation = {
    p: [fieldName],
    oi: newValue
  }

  if (oldValue !== undefined) {
    op.od = oldValue
  }

  return [op]
}

/**
 * 创建字段删除操作
 */
export function createFieldDeleteOp(fieldName: string, oldValue: any): ShareDBOperation[] {
  return [{
    p: [fieldName],
    od: oldValue
  }]
}

/**
 * 创建数组追加操作
 */
export function createArrayAppendOp(arrayPath: string[], value: any): ShareDBOperation[] {
  return [{
    p: [...arrayPath, '-1'],
    li: value
  }]
}

/**
 * 创建数组插入操作
 */
export function createArrayInsertOp(arrayPath: string[], index: number, value: any): ShareDBOperation[] {
  return [{
    p: [...arrayPath, index.toString()],
    li: value
  }]
}

/**
 * 创建数组删除操作
 */
export function createArrayDeleteOp(arrayPath: string[], index: number, oldValue: any): ShareDBOperation[] {
  return [{
    p: [...arrayPath, index.toString()],
    ld: oldValue
  }]
}

/**
 * 创建对象合并操作
 */
export function createObjectMergeOp(objectPath: string[], updates: Record<string, any>): ShareDBOperation[] {
  const ops: ShareDBOperation[] = []

  for (const [key, value] of Object.entries(updates)) {
    ops.push({
      p: [...objectPath, key],
      oi: value
    })
  }

  return ops
}

/**
 * 解析操作类型
 */
export function getOpType(op: ShareDBOperation[]): string {
  if (op.length === 0) {
    return 'unknown'
  }

  const firstOp = op[0]
  if (firstOp?.oi !== undefined) {
    return 'insert'
  }
  if (firstOp?.od !== undefined) {
    return 'delete'
  }
  if (firstOp?.li !== undefined) {
    return 'listInsert'
  }
  if (firstOp?.ld !== undefined) {
    return 'listDelete'
  }
  if (firstOp?.na !== undefined) {
    return 'numberAdd'
  }

  return 'unknown'
}

/**
 * 解析操作路径
 */
export function getOpPath(op: ShareDBOperation[]): string[] {
  if (op.length === 0) {
    return []
  }
  return op[0]?.p || []
}

/**
 * 解析操作值
 */
export function getOpValue(op: ShareDBOperation[]): { newValue?: any; oldValue?: any } {
  if (op.length === 0) {
    return {}
  }

  const firstOp = op[0]
  return {
    newValue: firstOp?.oi || firstOp?.li,
    oldValue: firstOp?.od || firstOp?.ld
  }
}

/**
 * 检查操作是否影响指定路径
 */
export function isOpAffectingPath(op: ShareDBOperation[], path: string[]): boolean {
  if (op.length === 0) {
    return false
  }

  const opPath = op[0]?.p || []
  
  // 检查路径是否匹配
  if (opPath.length !== path.length) {
    return false
  }

  for (let i = 0; i < path.length; i++) {
    if (opPath[i] !== path[i]) {
      return false
    }
  }

  return true
}

/**
 * 检查操作是否影响指定字段
 */
export function isOpAffectingField(op: ShareDBOperation[], fieldName: string): boolean {
  const opPath = getOpPath(op)
  return opPath.length > 0 && opPath[0] === fieldName
}

/**
 * 合并多个操作
 */
export function mergeOps(ops: ShareDBOperation[][]): ShareDBOperation[] {
  const merged: ShareDBOperation[] = []
  
  for (const op of ops) {
    merged.push(...op)
  }
  
  return merged
}

/**
 * 验证操作格式
 */
export function validateOp(op: ShareDBOperation[]): boolean {
  if (op.length === 0) {
    return false
  }

  const firstOp = op[0]
  
  // 检查路径
  if (!Array.isArray(firstOp?.p)) {
    return false
  }

  // 检查至少有一个操作类型
  const hasOperation = firstOp?.oi !== undefined ||
                      firstOp?.od !== undefined ||
                      firstOp?.li !== undefined ||
                      firstOp?.ld !== undefined ||
                      firstOp?.na !== undefined

  return hasOperation
}
