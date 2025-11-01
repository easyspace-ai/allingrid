import { EventEmitter } from 'node:events'
import { BaseService } from '@/services/BaseService'
import { ShareDBConnection } from '@/sharedb/ShareDBConnection'
import { ShareDBDoc } from '@/sharedb/ShareDBDoc'
import { ShareDBPresence } from '@/sharedb/ShareDBPresence'
import { ShareDBConnectionOptions, ShareDBConnectionStatus } from '@/types/sharedb'

export class ShareDBService extends EventEmitter {
  private client: any
  private connection: ShareDBConnection | null = null
  private documents: Map<string, ShareDBDoc> = new Map()
  private presences: Map<string, ShareDBPresence> = new Map()
  private isInitialized: boolean = false

  constructor(client: any) {
    super()
    this.client = client
  }

  /**
   * 初始化 ShareDB 连接
   */
  async initialize(options: ShareDBConnectionOptions = {}): Promise<void> {
    if (this.isInitialized) {
      return
    }

    // 构建 WebSocket URL
    const wsURL = this.buildWebSocketURL()
    
    // 合并选项
    const connectionOptions: ShareDBConnectionOptions = {
      getAuthToken: () => this.client.authStore.token || undefined,
      ...options
    }

    // 创建连接
    this.connection = new ShareDBConnection(wsURL, connectionOptions as any)
    
    // 设置事件监听
    this.setupConnectionListeners()
    
    this.isInitialized = true
  }

  /**
   * 连接 ShareDB
   */
  async connect(): Promise<void> {
    if (!this.connection) {
      await this.initialize()
    }

    // 连接会自动建立，这里只是等待连接完成
    return new Promise((resolve, reject) => {
      if (!this.connection) {
        reject(new Error('ShareDB connection not initialized'))
        return
      }

      if (this.connection.isConnected) {
        resolve()
        return
      }

      const onConnected = () => {
        this.connection!.off('connected', onConnected)
        this.connection!.off('error', onError)
        console.log('ShareDB 连接已建立，准备就绪')
        resolve()
      }

      const onError = (err: Error) => {
        this.connection!.off('connected', onConnected)
        this.connection!.off('error', onError)
        console.error('ShareDB 连接错误:', err)
        reject(err)
      }

      this.connection.on('connected', onConnected)
      this.connection.on('error', onError)

      // 设置超时
      setTimeout(() => {
        this.connection!.off('connected', onConnected)
        this.connection!.off('error', onError)
        reject(new Error('ShareDB connection timeout'))
      }, 10000)
    })
  }

  /**
   * 断开 ShareDB 连接
   */
  disconnect(): void {
    if (this.connection) {
      this.connection.close()
    }
  }

  /**
   * 检查是否已连接
   */
  get isConnected(): boolean {
    return this.connection?.isConnected || false
  }

  /**
   * 获取连接状态
   */
  get connectionStatus(): ShareDBConnectionStatus | null {
    return this.connection?.connectionStatus || null
  }

  /**
   * 获取文档
   */
  getDocument(collection: string, id: string): ShareDBDoc {
    const key = `${collection}:${id}`
    
    if (!this.documents.has(key)) {
      if (!this.connection) {
        throw new Error('ShareDB connection not initialized')
      }
      
      const doc = this.connection.getDocument(collection, id)
      this.documents.set(key, doc)
    }

    return this.documents.get(key)!
  }

  /**
   * 获取 Presence
   */
  getPresence(_channel: string): ShareDBPresence {
    throw new Error('Presence 暂不支持（当前使用自定义 ShareDB 协议客户端）')
  }

  /**
   * 获取文档 Presence
   */
  getDocPresence(_collection: string, _id: string): ShareDBPresence {
    throw new Error('Doc Presence 暂不支持（当前使用自定义 ShareDB 协议客户端）')
  }

  /**
   * 销毁文档
   */
  destroyDocument(collection: string, id: string): void {
    const key = `${collection}:${id}`
    const doc = this.documents.get(key)
    
    if (doc) {
      doc.destroy()
      this.documents.delete(key)
    }
  }

  /**
   * 销毁 Presence
   */
  destroyPresence(channel: string): void {
    const presence = this.presences.get(channel)
    
    if (presence) {
      presence.destroy()
      this.presences.delete(channel)
    }
  }

  /**
   * 销毁文档 Presence
   */
  destroyDocPresence(collection: string, id: string): void {
    const channel = `doc:${collection}:${id}`
    this.destroyPresence(channel)
  }

  /**
   * 清理所有资源
   */
  cleanup(): void {
    // 销毁所有文档
    for (const doc of this.documents.values()) {
      doc.destroy()
    }
    this.documents.clear()

    // 销毁所有 Presence
    for (const presence of this.presences.values()) {
      presence.destroy()
    }
    this.presences.clear()

    // 断开连接
    if (this.connection) {
      this.connection.destroy()
      this.connection = null
    }

    this.isInitialized = false
  }

  /**
   * 构建 WebSocket URL
   */
  private buildWebSocketURL(): string {
    const baseURL = this.client.baseURL
    const token = this.client.authStore.token
    const wsURL = baseURL.replace(/^http/, 'ws') + '/socket'
    
    // 添加token作为查询参数
    if (token) {
      return `${wsURL}?token=${encodeURIComponent(token)}`
    }
    
    return wsURL
  }

  /**
   * 设置连接事件监听
   */
  private setupConnectionListeners(): void {
    if (!this.connection) {
      return
    }

    this.connection.on('connected', () => {
      this.emit('connected')
    })

    this.connection.on('disconnected', () => {
      this.emit('disconnected')
    })

    this.connection.on('error', (err: Error) => {
      console.error('ShareDB connection error:', err)
      this.emit('error', err)
    })

    this.connection.on('pong', () => {
      this.emit('heartbeat')
    })
  }

  /**
   * 获取所有文档
   */
  getAllDocuments(): ShareDBDoc[] {
    return Array.from(this.documents.values())
  }

  /**
   * 获取所有 Presence
   */
  getAllPresences(): ShareDBPresence[] {
    return Array.from(this.presences.values())
  }

  /**
   * 获取文档数量
   */
  getDocumentCount(): number {
    return this.documents.size
  }

  /**
   * 获取 Presence 数量
   */
  getPresenceCount(): number {
    return this.presences.size
  }
}
