// 记录相关类型
export interface Record {
  id: string
  tableId: string
  fields: { [key: string]: any }
  createdAt: string
  updatedAt: string
  createdBy: string
  updatedBy: string
}

// 对齐服务端 CreateRecordRequest
export interface RecordCreateRequest {
  // 方式1：直接传递字段映射（推荐）
  fields?: { [key: string]: any }
  // 方式2：使用 data 字段（与服务端一致）
  data?: { [key: string]: any }
}

// 对齐服务端 UpdateRecordRequest
export interface RecordUpdateRequest {
  // Teable 格式支持
  fieldKeyType?: 'id' | 'name'
  record?: {
    fields: { [key: string]: any }
  }
  
  // 服务端格式（优先）
  data?: { [key: string]: any }
  version?: number // 乐观锁版本号
  
  // SDK兼容格式
  fields?: { [key: string]: any }
}

export interface RecordListFilter {
  fields?: { [key: string]: any }
  createdAfter?: string
  createdBefore?: string
  updatedAfter?: string
  updatedBefore?: string
  createdBy?: string
  updatedBy?: string
}

// 对齐服务端 RecordResponse
export interface RecordResponse {
  id: string
  tableId: string
  data: { [key: string]: any } // 服务端使用 data 字段
  createdAt: string
  updatedAt: string
  createdBy: string
  updatedBy: string
  version: number // 乐观锁版本号
  // SDK兼容字段（从 data 映射）
  fields?: { [key: string]: any }
}

// 对齐服务端批量操作请求
export interface BatchCreateRecordsRequest {
  records: Array<{
    fields: { [key: string]: any }
  }>
}

export interface BatchUpdateRecordsRequest {
  records: Array<{
    id: string
    fields: { [key: string]: any }
  }>
}

export interface BatchDeleteRecordsRequest {
  recordIds: string[]
}

// 对齐服务端批量操作响应
export interface BatchCreateRecordResponse {
  records: RecordResponse[]
  successCount: number
  failedCount: number
  errors?: string[]
}

export interface BatchUpdateRecordResponse {
  records: RecordResponse[]
  successCount: number
  failedCount: number
  errors?: string[]
}

export interface BatchDeleteRecordResponse {
  successCount: number
  failedCount: number
  errors?: string[]
}

// SDK兼容类型（向后兼容）
export interface BatchOperationResponse {
  success: boolean
  created?: number
  updated?: number
  deleted?: number
  errors?: Array<{
    recordId?: string
    error: string
  }>
}

// 视图相关类型
export interface View {
  id: string
  name: string
  description?: string
  tableId: string
  type: ViewType
  config: ViewConfig
  isShared: boolean
  shareId?: string
  ownerId: string
  createdAt: string
  updatedAt: string
}

export type ViewType = 'grid' | 'kanban' | 'calendar' | 'gallery' | 'form'

export interface ViewConfig {
  fields?: string[]
  filters?: ViewFilter[]
  sorts?: ViewSort[]
  groupBy?: string
  groupDirection?: 'asc' | 'desc'
  pageSize?: number
  layout?: { [key: string]: any }
}

export interface ViewFilter {
  field: string
  operator: FilterOperator
  value: any
}

export type FilterOperator = 
  | 'eq'        // 等于
  | 'ne'        // 不等于
  | 'gt'        // 大于
  | 'gte'       // 大于等于
  | 'lt'        // 小于
  | 'lte'       // 小于等于
  | 'in'        // 包含
  | 'nin'       // 不包含
  | 'like'      // 模糊匹配
  | 'nlike'     // 不模糊匹配
  | 'is'        // 是
  | 'isnt'      // 不是
  | 'between'   // 之间
  | 'nbetween'  // 不在之间

export interface ViewSort {
  field: string
  direction: 'asc' | 'desc'
}

export interface ViewCreateRequest {
  name: string
  description?: string
  type: ViewType
  config: ViewConfig
}

export interface ViewUpdateRequest {
  name?: string
  description?: string
  type?: ViewType
  config?: ViewConfig
}

export interface ViewListFilter {
  name?: string
  tableId?: string
  type?: ViewType
  isShared?: boolean
  ownerId?: string
}

export interface ViewResponse {
  id: string
  name: string
  description?: string
  tableId: string
  type: ViewType
  config: ViewConfig
  isShared: boolean
  shareId?: string
  ownerId: string
  createdAt: string
  updatedAt: string
}

export interface ViewShareRequest {
  isShared: boolean
  expiresAt?: string
}

export interface ViewShareResponse {
  shareId: string
  shareUrl: string
  expiresAt?: string
}
