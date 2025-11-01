// 附件相关类型定义

/**
 * 上传签名响应
 * 对齐服务端响应格式
 */
export interface AttachmentSignatureResponse {
  token: string
  upload_url: string  // 服务端使用 snake_case
  expires_at: number  // 服务端使用 snake_case
  max_size: number    // 服务端使用 snake_case
  allowed_types: string[] // 服务端使用 snake_case
}

/**
 * 上传签名请求
 * 对齐服务端 API: 使用 snake_case 字段名
 */
export interface GenerateSignatureRequest {
  table_id: string  // 服务端使用 snake_case
  field_id: string  // 服务端使用 snake_case
  record_id: string // 服务端使用 snake_case
  max_size?: number
  allowed_types?: string[]
}

/**
 * 附件项
 * 对齐服务端响应格式
 */
export interface AttachmentItem {
  id: string
  name: string
  path: string
  token: string
  size: number
  mime_type?: string  // 服务端可能使用 mime_type
  mimetype?: string   // 兼容不同的命名
  presigned_url?: string
  width?: number
  height?: number
  sm_thumbnail_url?: string
  lg_thumbnail_url?: string
  created_time?: string
  updated_time?: string
  createdAt?: string  // 兼容字段
  updatedAt?: string  // 兼容字段
}

/**
 * 通知上传响应
 */
export interface NotifyUploadResponse {
  attachment: AttachmentItem
  success: boolean
  message: string
}

/**
 * 附件列表查询参数
 * 对齐服务端 API: 使用 snake_case 字段名
 */
export interface ListAttachmentsFilter {
  table_id?: string  // 服务端使用 snake_case
  field_id?: string  // 服务端使用 snake_case
  record_id?: string // 服务端使用 snake_case
}

/**
 * 附件统计信息
 */
export interface AttachmentStats {
  totalFiles: number
  totalSize: number
  imageFiles: number
  videoFiles: number
  audioFiles: number
  documentFiles: number
  otherFiles: number
}

