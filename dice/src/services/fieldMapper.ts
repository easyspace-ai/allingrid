import type { FieldType, FieldOptions } from 'luckdb-sdk'

/**
 * Dice 单元格类型定义
 */
export type DiceCellVariant =
  | 'short-text'
  | 'long-text'
  | 'number'
  | 'select'
  | 'multi-select'
  | 'checkbox'
  | 'date'
  | 'link'
  | 'email'
  | 'phone'
  | 'rating'
  | 'user'
  | 'attachment'
  | 'formula'
  | 'ai'

/**
 * 将 SDK 字段类型映射到 Dice 单元格类型
 */
export function mapFieldTypeToCellVariant(
  fieldType: FieldType,
  options?: FieldOptions
): DiceCellVariant {
  switch (fieldType) {
    case 'singleLineText':
    case 'text':
      return 'short-text'
    
    case 'longText':
      return 'long-text'
    
    case 'number':
    case 'currency':
    case 'percent':
    case 'rating':
      return fieldType === 'rating' ? 'rating' : 'number'
    
    case 'singleSelect':
    case 'select':
      return 'select'
    
    case 'multipleSelect':
      return 'multi-select'
    
    case 'boolean':
    case 'checkbox':
      return 'checkbox'
    
    case 'date':
      return 'date'
    
    case 'datetime':
      // 日期时间也使用 date 类型，可以通过 options 区分
      return 'date'
    
    case 'email':
      return 'email'
    
    case 'url':
    case 'link':
      return 'link'
    
    case 'phone':
      return 'phone'
    
    case 'user':
      return 'user'
    
    case 'attachment':
      return 'attachment'
    
    case 'formula':
      return 'formula'
    
    case 'ai':
      return 'ai'
    
    default:
      // 默认使用短文本
      return 'short-text'
  }
}

/**
 * 将 Dice 单元格类型映射回 SDK 字段类型
 */
export function mapCellVariantToFieldType(cellVariant: DiceCellVariant): FieldType {
  switch (cellVariant) {
    case 'short-text':
      return 'singleLineText'
    
    case 'long-text':
      return 'longText'
    
    case 'number':
      return 'number'
    
    case 'select':
      return 'singleSelect'
    
    case 'multi-select':
      return 'multipleSelect'
    
    case 'checkbox':
      return 'checkbox'
    
    case 'date':
      return 'date'
    
    case 'email':
      return 'email'
    
    case 'link':
      return 'url'
    
    case 'phone':
      return 'phone'
    
    case 'rating':
      return 'rating'
    
    case 'user':
      return 'user'
    
    case 'attachment':
      return 'attachment'
    
    case 'formula':
      return 'formula'
    
    case 'ai':
      return 'ai'
    
    default:
      return 'singleLineText'
  }
}

/**
 * 将 SDK 字段选项转换为 Dice 单元格选项
 */
export function mapFieldOptionsToCellOptions(options?: FieldOptions): {
  options?: Array<{ label: string; value: string }>
  min?: number
  max?: number
  expression?: string
  [key: string]: any
} {
  if (!options) return {}

  const cellOptions: {
    options?: Array<{ label: string; value: string }>
    min?: number
    max?: number
    expression?: string
    [key: string]: any
  } = {}

  // 处理选择字段的选项
  if (options.choices && Array.isArray(options.choices)) {
    cellOptions.options = options.choices.map((choice) => ({
      label: choice.name || choice.id,
      value: choice.id || choice.name,
    }))
  }

  // 处理数字字段的范围
  if (options.min !== undefined) {
    cellOptions.min = options.min
  }
  if (options.max !== undefined) {
    cellOptions.max = options.max
  }

  // 处理公式字段的表达式
  if (options.expression) {
    cellOptions.expression = options.expression
  }

  // 保留其他选项
  Object.keys(options).forEach((key) => {
    if (!['choices', 'min', 'max', 'expression'].includes(key)) {
      cellOptions[key] = (options as any)[key]
    }
  })

  return cellOptions
}

