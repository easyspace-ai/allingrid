import type { Cell } from '@/types/data-grid'
import type { DiceCellVariant } from './fieldMapper'

/**
 * 根据单元格类型构建正确的 Cell 类型
 */
export function buildCellType(
  variant: DiceCellVariant,
  options: {
    options?: Array<{ label: string; value: string }>
    min?: number
    max?: number
    expression?: string
    [key: string]: unknown
  }
): Cell {
  switch (variant) {
    case 'short-text':
      return { variant: 'short-text' }
    
    case 'long-text':
      return { variant: 'long-text' }
    
    case 'number':
      return {
        variant: 'number',
        ...(options.min !== undefined && { min: options.min }),
        ...(options.max !== undefined && { max: options.max }),
      }
    
    case 'select':
      return {
        variant: 'select',
        options: options.options || [],
      }
    
    case 'multi-select':
      return {
        variant: 'multi-select',
        options: options.options || [],
      }
    
    case 'checkbox':
      return { variant: 'checkbox' }
    
    case 'date':
      return { variant: 'date' }
    
    case 'link':
      return { variant: 'link' }
    
    case 'email':
      return { variant: 'email' }
    
    case 'phone':
      return { variant: 'phone' }
    
    case 'rating':
      return {
        variant: 'rating',
        ...(options.max !== undefined && { max: options.max }),
      }
    
    case 'user':
      return {
        variant: 'user',
        ...(options.options && { options: options.options as Array<{ id: string; name: string; avatar?: string }> }),
      }
    
    case 'attachment':
      return { variant: 'attachment' }
    
    case 'formula':
      return {
        variant: 'formula',
        ...(options.expression && { expression: options.expression }),
      }
    
    case 'ai':
      return {
        variant: 'ai',
        ...(options.task && { task: options.task as any }),
        ...(options.prompt && { prompt: options.prompt }),
        ...(options.dependencies && { dependencies: options.dependencies }),
        ...(options.trigger && { trigger: options.trigger as any }),
        ...(options.cache !== undefined && { cache: options.cache }),
        ...(options.maxRetries !== undefined && { maxRetries: options.maxRetries }),
      }
    
    default:
      return { variant: 'short-text' }
  }
}

