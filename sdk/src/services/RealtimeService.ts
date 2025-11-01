import { BaseService } from './BaseService'
import { EventEmitter } from 'events'

export interface RealtimeSubscription {
  id: string
  topic: string
  listener: (data: any) => void
}

export class RealtimeService extends BaseService {
  private eventSource: EventSource | null = null
  private subscriptions: Map<string, RealtimeSubscription> = new Map()
  private reconnectAttempts: number = 0
  private maxReconnectAttempts: number = 10
  private reconnectDelay: number = 1000
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private isConnecting: boolean = false

  /**
   * 检查是否已连接
   */
  get isConnected(): boolean {
    return this.eventSource?.readyState === EventSource.OPEN
  }

  /**
   * 连接 SSE
   */
  async connect(): Promise<void> {
    if (this.isConnecting || this.isConnected) {
      return
    }

    this.isConnecting = true

    try {
      // 构建带 token 的 SSE URL，以通过 JWT 中间件验证
      const base = this.buildUrl('/api/realtime')
      const token = this.client.authStore?.token
      const url = token
        ? `${base}${base.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}`
        : base
      this.eventSource = new EventSource(url)

      this.setupEventListeners()
      
      // 等待连接建立
      await this.waitForConnection()
      
      this.reconnectAttempts = 0
    } catch (error) {
      this.isConnecting = false
      throw error
    }
  }

  /**
   * 断开连接
   */
  disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }

    if (this.eventSource) {
      this.eventSource.close()
      this.eventSource = null
    }

    this.subscriptions.clear()
    this.reconnectAttempts = 0
  }

  /**
   * 订阅主题
   */
  subscribe(topic: string, listener: (data: any) => void): string {
    const id = this.generateSubscriptionId()
    const subscription: RealtimeSubscription = {
      id,
      topic,
      listener
    }

    this.subscriptions.set(id, subscription)

    // 如果已连接，立即发送订阅请求
    if (this.isConnected) {
      this.sendSubscription(topic)
    }

    return id
  }

  /**
   * 取消订阅
   */
  unsubscribe(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId)
    if (subscription) {
      this.subscriptions.delete(subscriptionId)
      
      // 如果已连接，发送取消订阅请求
      if (this.isConnected) {
        this.sendUnsubscription(subscription.topic)
      }
    }
  }

  /**
   * 按主题取消订阅
   */
  unsubscribeByTopic(topic: string): void {
    const toRemove: string[] = []
    
    for (const [id, subscription] of this.subscriptions.entries()) {
      if (subscription.topic === topic) {
        toRemove.push(id)
      }
    }

    for (const id of toRemove) {
      this.unsubscribe(id)
    }
  }

  /**
   * 取消所有订阅
   */
  unsubscribeAll(): void {
    this.subscriptions.clear()
  }

  /**
   * 设置事件监听
   */
  private setupEventListeners(): void {
    if (!this.eventSource) {
      return
    }

    this.eventSource.onopen = () => {
      this.isConnecting = false
      this.reconnectAttempts = 0
      console.log('SSE connected')
    }

    this.eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        this.handleMessage(data)
      } catch (error) {
        console.error('Failed to parse SSE message:', error)
      }
    }

    this.eventSource.onerror = (error) => {
      console.error('SSE error:', error)
      this.handleConnectionError()
    }

    this.eventSource.addEventListener('ping', () => {
      // 响应心跳
      this.sendMessage({ type: 'pong' })
    })
  }

  /**
   * 处理消息
   */
  private handleMessage(data: any): void {
    if (data.type === 'subscription') {
      // 订阅确认
      return
    }

    if (data.type === 'unsubscription') {
      // 取消订阅确认
      return
    }

    // 分发消息给订阅者
    for (const subscription of this.subscriptions.values()) {
      if (subscription.topic === data.topic) {
        try {
          subscription.listener(data.payload)
        } catch (error) {
          console.error('Error in subscription listener:', error)
        }
      }
    }
  }

  /**
   * 处理连接错误
   */
  private handleConnectionError(): void {
    this.isConnecting = false

    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++
      const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1)
      
      console.log(`SSE reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`)
      
      this.reconnectTimer = setTimeout(() => {
        this.connect().catch(console.error)
      }, delay)
    } else {
      console.error('SSE max reconnection attempts reached')
    }
  }

  /**
   * 等待连接建立
   */
  private waitForConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.eventSource) {
        reject(new Error('EventSource not initialized'))
        return
      }

      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'))
      }, 10000)

      const onOpen = () => {
        clearTimeout(timeout)
        this.eventSource!.removeEventListener('open', onOpen)
        this.eventSource!.removeEventListener('error', onError)
        resolve()
      }

      const onError = () => {
        clearTimeout(timeout)
        this.eventSource!.removeEventListener('open', onOpen)
        this.eventSource!.removeEventListener('error', onError)
        reject(new Error('Connection failed'))
      }

      this.eventSource.addEventListener('open', onOpen)
      this.eventSource.addEventListener('error', onError)
    })
  }

  /**
   * 发送订阅请求
   */
  private sendSubscription(topic: string): void {
    this.sendMessage({
      type: 'subscribe',
      topic
    })
  }

  /**
   * 发送取消订阅请求
   */
  private sendUnsubscription(topic: string): void {
    this.sendMessage({
      type: 'unsubscribe',
      topic
    })
  }

  /**
   * 发送消息
   */
  private sendMessage(data: any): void {
    // SSE 是单向的，这里只是记录
    console.log('Would send SSE message:', data)
  }

  /**
   * 生成订阅 ID
   */
  private generateSubscriptionId(): string {
    return `sub_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  }

  /**
   * 获取订阅数量
   */
  getSubscriptionCount(): number {
    return this.subscriptions.size
  }

  /**
   * 获取所有订阅
   */
  getAllSubscriptions(): RealtimeSubscription[] {
    return Array.from(this.subscriptions.values())
  }
}
