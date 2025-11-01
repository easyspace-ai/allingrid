/**
 * FormData 工具函数
 */

export interface FormDataField {
  name: string
  value: string | Blob | File
  filename?: string
}

/**
 * 创建 FormData 对象
 */
export function createFormData(fields: FormDataField[]): FormData {
  const formData = new FormData()

  for (const field of fields) {
    if (field.filename && (field.value instanceof Blob || (field.value as any) instanceof File)) {
      formData.append(field.name, field.value as Blob, field.filename)
    } else {
      formData.append(field.name, field.value as string | Blob)
    }
  }

  return formData
}

/**
 * 从对象创建 FormData
 */
export function objectToFormData(obj: Record<string, any>): FormData {
  const formData = new FormData()

  for (const [key, value] of Object.entries(obj)) {
    if (value instanceof File) {
      formData.append(key, value)
    } else if (value instanceof Blob) {
      formData.append(key, value)
    } else if (Array.isArray(value)) {
      for (const item of value) {
        formData.append(`${key}[]`, String(item))
      }
    } else if (value !== null && value !== undefined) {
      formData.append(key, String(value))
    }
  }

  return formData
}

/**
 * 从 FormData 创建对象
 */
export function formDataToObject(formData: FormData): Record<string, any> {
  const obj: Record<string, any> = {}

  for (const [key, value] of (formData as any).entries()) {
    if (key.endsWith('[]')) {
      const arrayKey = key.slice(0, -2)
      if (!obj[arrayKey]) {
        obj[arrayKey] = []
      }
      obj[arrayKey].push(value)
    } else {
      obj[key] = value
    }
  }

  return obj
}

/**
 * 检查是否为文件
 */
export function isFile(value: any): value is File {
  return value instanceof File
}

/**
 * 检查是否为 Blob
 */
export function isBlob(value: any): value is Blob {
  return value instanceof Blob
}

/**
 * 检查是否为文件或 Blob
 */
export function isFileOrBlob(value: any): value is File | Blob {
  return isFile(value) || isBlob(value)
}

/**
 * 获取文件大小（字节）
 */
export function getFileSize(file: File | Blob): number {
  return file.size
}

/**
 * 获取文件类型
 */
export function getFileType(file: File | Blob): string {
  return file.type
}

/**
 * 检查文件类型是否匹配
 */
export function isFileType(file: File | Blob, type: string): boolean {
  return file.type === type
}

/**
 * 检查文件类型是否匹配（支持通配符）
 */
export function isFileTypeMatch(file: File | Blob, pattern: string): boolean {
  const fileType = file.type
  const regex = new RegExp(pattern.replace(/\*/g, '.*'))
  return regex.test(fileType)
}

/**
 * 验证文件大小
 */
export function validateFileSize(file: File | Blob, maxSize: number): boolean {
  return file.size <= maxSize
}

/**
 * 验证文件类型
 */
export function validateFileType(file: File | Blob, allowedTypes: string[]): boolean {
  return allowedTypes.includes(file.type)
}
