// 表格相关类型
export interface Table {
  id: string
  name: string
  description?: string
  baseId: string
  ownerId: string
  createdAt: string
  updatedAt: string
  fields?: Field[]
}

export interface TableCreateRequest {
  name: string
  description?: string
  baseId: string
  fields?: FieldCreateRequest[]
  views?: ViewCreateRequest[]
}

export interface TableUpdateRequest {
  name?: string
  description?: string
}

export interface TableRenameRequest {
  name: string
}

export interface TableDuplicateRequest {
  name: string
  withData?: boolean
  withViews?: boolean
  withFields?: boolean
}

export type ViewType = 'grid' | 'kanban' | 'calendar' | 'gallery' | 'form'

export interface ViewCreateRequest {
  name: string
  type: ViewType
  description?: string
  columnMeta?: Array<Record<string, any>>
}

export interface TableListFilter {
  name?: string
  baseId?: string
  ownerId?: string
  createdAfter?: string
  createdBefore?: string
}

export interface TableResponse {
  id: string
  name: string
  description?: string
  baseId: string
  defaultViewId?: string
  fieldCount?: number
  recordCount?: number
  ownerId: string
  createdAt: string
  updatedAt: string
  fields?: Field[]
}

export interface TableUsage {
  recordCount: number
  fieldCount: number
  viewCount: number
  lastAccessedAt?: string
}

export interface TableManagementMenu {
  canRename: boolean
  canDelete: boolean
  canDuplicate: boolean
  canExport: boolean
  canImport: boolean
}

// 字段相关类型
export interface Field {
  id: string
  name: string
  type: FieldType
  description?: string
  required: boolean
  defaultValue?: any
  options?: FieldOptions
  tableId: string
  // 服务端返回包含的属性，对齐字段协议
  isPrimary?: boolean
  unique?: boolean
  createdAt: string
  updatedAt: string
}

export type FieldType = 
  | 'text'
  | 'singleLineText'
  | 'longText'
  | 'number'
  | 'date'
  | 'datetime'
  | 'boolean'
  | 'checkbox'
  | 'select'
  | 'singleSelect'
  | 'multipleSelect'
  | 'attachment'
  | 'user'
  | 'link'
  | 'formula'
  | 'rollup'
  | 'lookup'
  | 'count'
  | 'autoNumber'
  | 'createdTime'
  | 'createdBy'
  | 'lastModifiedTime'
  | 'lastModifiedBy'
  | 'ai'
  | 'email'
  | 'url'
  | 'phone'
  | 'rating'
  | 'duration'
  | 'percent'
  | 'currency'
  | 'button'

export interface FieldOptions {
  // 文本字段选项
  maxLength?: number
  minLength?: number
  pattern?: string
  
  // 数字字段选项
  precision?: number
  numberFormat?: 'decimal' | 'percent' | 'currency'
  currency?: string
  showCommas?: boolean
  min?: number
  max?: number
  minValue?: number
  maxValue?: number
  
  // 选择字段选项
  choices?: Array<{ id: string; name: string; color?: string }>
  preventAutoNewOptions?: boolean
  
  // 日期字段选项
  dateFormat?: string
  includeTime?: boolean
  timeFormat?: '12h' | '24h'
  timezone?: string
  
  // 公式字段选项
  expression?: string
  timeZone?: string
  
  // Rollup字段选项
  linkFieldId?: string
  rollupFieldId?: string
  aggregationFunc?: string
  
  // Lookup字段选项
  lookupFieldId?: string
  
  // Link字段选项
  linkedTableId?: string
  foreignKeyFieldId?: string
  symmetricFieldId?: string
  relationship?: 'one_to_one' | 'one_to_many' | 'many_to_one' | 'many_to_many'
  isSymmetric?: boolean
  allowMultiple?: boolean
  baseId?: string
  filterByViewId?: string
  visibleFieldIds?: string[]
  filterConfig?: FilterOptions
  
  // Count字段选项
  filterExpression?: string
  
  // Duration字段选项
  durationFormat?: string
  
  // Button字段选项
  label?: string
  action?: 'open_url' | 'run_script' | 'trigger_automation'
  buttonConfig?: Record<string, any>
  
  // User字段选项
  isMultiple?: boolean
  
  // Rating字段选项
  maxRating?: number
  icon?: 'star' | 'heart' | 'thumb'
  
  // AI字段选项
  provider?: string
  model?: string
  prompt?: string
  aiConfig?: Record<string, any>
  
  // 默认值
  defaultValue?: any
  
  // 通用配置
  showAs?: ShowAsOptions
  formatting?: FormattingOptions
}

export interface ShowAsOptions {
  type?: string
  color?: string
  config?: Record<string, any>
}

export interface FormattingOptions {
  type?: 'number' | 'date' | 'text'
  precision?: number
  dateFormat?: string
  timeFormat?: string
  timeZone?: string
  showCommas?: boolean
  currency?: string
}

export interface FilterOptions {
  conjunction?: 'and' | 'or'
  conditions?: FilterCondition[]
}

export interface FilterCondition {
  fieldId: string
  operator: string
  value: any
}

export interface FieldCreateRequest {
  name: string
  type: FieldType
  description?: string
  required?: boolean
  defaultValue?: any
  options?: FieldOptions
}

export interface FieldUpdateRequest {
  name?: string
  type?: FieldType
  description?: string
  required?: boolean
  defaultValue?: any
  options?: FieldOptions
}

export interface FieldListFilter {
  name?: string
  type?: FieldType
  tableId?: string
  required?: boolean
}

export interface FieldResponse {
  id: string
  name: string
  type: FieldType
  description?: string
  required: boolean
  defaultValue?: any
  options?: FieldOptions
  tableId: string
  // 服务端返回包含的属性，对齐字段协议
  isPrimary?: boolean
  unique?: boolean
  createdAt: string
  updatedAt: string
}
