import { LuckDBClient } from '../src/index'

export interface DemoContext {
  client: LuckDBClient
  spaceId?: string
  baseId?: string
  tableId?: string
  fieldIds: Record<string, string>
  recordIds: string[]
  viewIds: string[]
}

export function createDemoContext(client: LuckDBClient): DemoContext {
  return {
    client,
    fieldIds: {},
    recordIds: [],
    viewIds: []
  }
}

export interface DemoResult {
  success: boolean
  message: string
  data?: any
  error?: Error
}

export function createSuccessResult(message: string, data?: any): DemoResult {
  return {
    success: true,
    message,
    data
  }
}

export function createErrorResult(message: string, error: Error): DemoResult {
  return {
    success: false,
    message,
    error
  }
}


