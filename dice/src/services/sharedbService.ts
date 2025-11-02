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
    
    // 检查是否已经有订阅的文档，如果存在且未销毁，直接使用
    let doc = this.subscribedDocs.get(recordId)
    
    // 如果文档不存在或已被销毁，获取新的文档实例
    if (!doc || doc.destroyed) {
      doc = luckdbClient.sharedb.getDocument(collection, recordId)
      // 如果文档已被销毁，我们需要一个新的实例（ShareDBConnection 会缓存，所以需要特殊处理）
      // 注意：ShareDBConnection.getDocument 会返回缓存的实例，如果已经被销毁，我们需要特殊处理
      if (doc.destroyed) {
        // 如果文档已被销毁，我们需要从连接中移除它，然后获取新的实例
        // 但是 ShareDBConnection 不提供移除文档的方法，所以我们需要等待它自动清理
        // 或者我们可以尝试重新获取（如果连接已经清理了缓存）
        console.warn(`Document for record ${recordId} was destroyed, getting new instance...`)
        // 由于 ShareDBConnection 使用 Map 缓存，销毁的文档仍然在缓存中
        // 我们需要确保文档不会被再次使用，但实际上 ShareDBConnection 不应该返回已销毁的文档
        // 如果确实返回了已销毁的文档，说明缓存有问题
        // 这里我们直接尝试订阅，如果失败再处理
      }
    }

    // 添加事件监听器
    if (!this.eventHandlers.has(recordId)) {
      this.eventHandlers.set(recordId, new Set())
    }
    this.eventHandlers.get(recordId)!.add(onUpdate)

    // 监听文档事件
    const handleLoad = ({ data }: { data: any }) => {
      console.log(`[ShareDBService] load event for record ${recordId}`, { data })
      onUpdate({ op: [], source: false, data })
    }

    const handleOp = (ops: any[], source: boolean) => {
      console.log(`[ShareDBService] op event for record ${recordId}`, { 
        ops, 
        source, 
        data: doc.data 
      })
      onUpdate({ op: ops, source, data: doc.data })
    }

    const handleOpBatch = (ops: any[], source: boolean) => {
      console.log(`[ShareDBService] op batch event for record ${recordId}`, { 
        ops, 
        source, 
        data: doc.data 
      })
      onUpdate({ op: ops, source, data: doc.data })
    }

    const handleError = (err: Error) => {
      console.error(`[ShareDBService] error for record ${recordId}:`, err)
      // 错误事件也可以通知，但数据结构可能不完整
      onUpdate({ op: [], source: false, data: doc.data || {} })
    }

    // 移除旧的事件监听器（如果存在）以避免重复监听
    doc.removeAllListeners()
    
    doc.on('load', handleLoad)
    doc.on('op', handleOp)
    doc.on('op batch', handleOpBatch) // 添加 op batch 事件监听（参考 teable）
    doc.on('error', handleError)

    // 如果文档还未订阅，则订阅
    if (!doc.isSubscribed && !doc.destroyed) {
      await doc.subscribe()
    } else if (doc.destroyed) {
      throw new Error(`Cannot subscribe to destroyed document for record ${recordId}`)
    }

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
      // 先取消订阅，不要销毁文档（因为文档实例可能被缓存）
      doc.unsubscribe().catch((err) => {
        console.warn(`Failed to unsubscribe record ${recordId}:`, err)
      })
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
    const promises = recordIds.map(async (recordId) => {
      try {
        await this.subscribeRecord(recordId, (event) => onUpdate(recordId, event))
      } catch (error) {
        console.error(`Failed to subscribe record ${recordId}:`, error)
        // 如果订阅失败（比如文档已被销毁），我们尝试等待一小段时间后重试
        // 这可以处理在 refreshSubscriptions 中取消订阅后立即重新订阅的竞态条件
        if (error instanceof Error && error.message.includes('destroyed')) {
          console.warn(`Document ${recordId} was destroyed, waiting before retry...`)
          await new Promise(resolve => setTimeout(resolve, 100))
          // 重试一次
          try {
            await this.subscribeRecord(recordId, (event) => onUpdate(recordId, event))
          } catch (retryError) {
            console.error(`Failed to retry subscribe record ${recordId}:`, retryError)
          }
        }
      }
    })
    await Promise.allSettled(promises)
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

