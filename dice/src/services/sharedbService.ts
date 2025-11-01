import type { ShareDBOperation, ShareDBDocEvent } from 'luckdb-sdk'
import { luckdbClient } from '@/config/client'

/**
 * ShareDB 服务封装
 */
export class ShareDBService {
  private tableId: string | null = null
  private subscribedDocs: Map<string, any> = new Map() // recordId -> ShareDBDoc
  private eventHandlers: Map<string, Set<(event: ShareDBDocEvent) => void>> = new Map()

  /**
   * 初始化 ShareDB 连接
   */
  async initialize(): Promise<void> {
    await luckdbClient.sharedb.initialize()
  }

  /**
   * 连接 ShareDB
   */
  async connect(): Promise<void> {
    await luckdbClient.sharedb.connect()
  }

  /**
   * 断开 ShareDB 连接
   */
  disconnect(): void {
    luckdbClient.sharedb.disconnect()
  }

  /**
   * 检查是否已连接
   */
  get isConnected(): boolean {
    return luckdbClient.sharedb.isConnected
  }

  /**
   * 设置表格 ID（用于构建 collection 名称）
   */
  setTableId(tableId: string): void {
    this.tableId = tableId
  }

  /**
   * 获取 collection 名称
   */
  private getCollectionName(): string {
    if (!this.tableId) {
      throw new Error('Table ID is not set. Call setTableId() first.')
    }
    return `rec_${this.tableId}`
  }

  /**
   * 订阅记录文档
   */
  async subscribeRecord(recordId: string, onUpdate: (event: ShareDBDocEvent) => void): Promise<void> {
    if (!this.isConnected) {
      throw new Error('ShareDB is not connected. Call connect() first.')
    }

    const collection = this.getCollectionName()
    const doc = luckdbClient.sharedb.getDocument(collection, recordId)

    // 添加事件监听器
    if (!this.eventHandlers.has(recordId)) {
      this.eventHandlers.set(recordId, new Set())
    }
    this.eventHandlers.get(recordId)!.add(onUpdate)

    // 监听文档事件
    const handleLoad = ({ data }: { data: any }) => {
      onUpdate({ op: [], source: false, data })
    }

    const handleOp = (event: ShareDBDocEvent) => {
      onUpdate(event)
    }

    const handleError = (err: Error) => {
      console.error(`ShareDB document error for record ${recordId}:`, err)
      // 错误事件也可以通知，但数据结构可能不完整
      onUpdate({ op: [], source: false, data: doc.data || {} })
    }

    doc.on('load', handleLoad)
    doc.on('op', handleOp)
    doc.on('error', handleError)

    // 订阅文档
    await doc.subscribe()

    // 保存文档引用
    this.subscribedDocs.set(recordId, doc)

    // 如果文档已有数据，立即触发更新
    if (doc.data) {
      onUpdate({ op: [], source: false, data: doc.data })
    }
  }

  /**
   * 取消订阅记录文档
   */
  unsubscribeRecord(recordId: string): void {
    const doc = this.subscribedDocs.get(recordId)
    if (doc) {
      doc.destroy()
      this.subscribedDocs.delete(recordId)
    }
    this.eventHandlers.delete(recordId)
  }

  /**
   * 提交操作到 ShareDB（将字段更新转换为 JSON0 操作）
   */
  async submitFieldUpdate(
    recordId: string,
    fieldId: string,
    newValue: any,
    oldValue?: any
  ): Promise<void> {
    const doc = this.subscribedDocs.get(recordId)
    if (!doc) {
      throw new Error(`Document for record ${recordId} is not subscribed.`)
    }

    const op: ShareDBOperation[] = []

    if (newValue === undefined || newValue === null) {
      // 删除字段
      if (oldValue !== undefined) {
        op.push({ p: ['data', fieldId], od: oldValue })
      }
    } else {
      // 更新字段
      if (oldValue !== undefined) {
        op.push({ p: ['data', fieldId], oi: newValue, od: oldValue })
      } else {
        op.push({ p: ['data', fieldId], oi: newValue })
      }
    }

    if (op.length > 0) {
      await doc.submitOp(op)
    }
  }

  /**
   * 批量订阅多条记录
   */
  async subscribeRecords(
    recordIds: string[],
    onUpdate: (recordId: string, event: ShareDBDocEvent) => void
  ): Promise<void> {
    const promises = recordIds.map((recordId) =>
      this.subscribeRecord(recordId, (event) => onUpdate(recordId, event))
    )
    await Promise.all(promises)
  }

  /**
   * 批量取消订阅多条记录
   */
  unsubscribeRecords(recordIds: string[]): void {
    recordIds.forEach((recordId) => this.unsubscribeRecord(recordId))
  }

  /**
   * 取消订阅所有记录
   */
  unsubscribeAll(): void {
    const recordIds = Array.from(this.subscribedDocs.keys())
    this.unsubscribeRecords(recordIds)
  }

  /**
   * 清理资源
   */
  cleanup(): void {
    this.unsubscribeAll()
    this.disconnect()
    this.tableId = null
  }

  /**
   * 获取已订阅的文档数量
   */
  getSubscribedCount(): number {
    return this.subscribedDocs.size
  }
}

// 导出单例（可以根据需要创建多个实例）
export function createShareDBService(): ShareDBService {
  return new ShareDBService()
}

