import { EventEmitter } from 'events'
import ReconnectingWebSocket from 'reconnecting-websocket'
import { ShareDBDoc } from '@/sharedb/ShareDBDoc'

// 获取 WebSocket 类（Node.js 或浏览器环境）
let WebSocketClass: typeof WebSocket
if (typeof WebSocket !== 'undefined') {
  // 浏览器环境
  WebSocketClass = WebSocket
} else {
  // Node.js 环境，尝试使用 ws 包
  try {
    const ws = require('ws')
    WebSocketClass = ws.WebSocket as typeof WebSocket
  } catch (error) {
    throw new Error('No valid WebSocket class provided. Please install "ws" package for Node.js environment: npm install ws')
  }
}

export interface CustomShareDBMessage {
  a: string
  c?: string
  d?: string
  v?: number
  op?: Array<any>
  data?: any
  error?: {
    code: string
    message: string
    details?: string
  }
}

export interface ShareDBConnectionOptions {
  reconnectDelay?: number
  maxReconnectDelay?: number
  heartbeatInterval?: number
  heartbeatTimeout?: number
  getAuthToken?: () => string | undefined
}

export interface ShareDBConnectionStatus {
  connected: boolean
  reconnecting: boolean
  reconnectAttempts: number
  lastError?: Error
}

export class ShareDBConnection extends EventEmitter {
  private url: string
  private ws: ReconnectingWebSocket
  private options: ShareDBConnectionOptions
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null
  private reconnectAttempts = 0
  private lastPongTime = Date.now()
  private status: ShareDBConnectionStatus = {
    connected: false,
    reconnecting: false,
    reconnectAttempts: 0
  }

  private docs: Map<string, ShareDBDoc> = new Map()

  constructor(url: string, options: ShareDBConnectionOptions = {}) {
    super()
    this.url = url
    this.options = {
      reconnectDelay: 1000,
      maxReconnectDelay: 10000,
      heartbeatInterval: 30000,
      heartbeatTimeout: 60000,
      ...options
    }

    this.ws = new ReconnectingWebSocket(this.url, [], {
      WebSocket: WebSocketClass,
      maxEnqueuedMessages: 0,
      reconnectionDelayGrowFactor: 1.5,
      maxReconnectionDelay: this.options.maxReconnectDelay!,
      minReconnectionDelay: this.options.reconnectDelay!,
      connectionTimeout: 4000,
      maxRetries: Infinity
    })

    this.setupEventListeners()
    this.setupHeartbeat()
  }

  get isConnected(): boolean {
    return this.ws.readyState === WebSocketClass.OPEN && this.status.connected
  }

  get connectionStatus(): ShareDBConnectionStatus {
    return { ...this.status }
  }

  getDocument(collection: string, id: string): ShareDBDoc {
    const key = `${collection}:${id}`
    if (!this.docs.has(key)) {
      const doc = new ShareDBDoc(this, collection, id)
      this.docs.set(key, doc)
    }
    return this.docs.get(key)!
  }

  sendMessage(message: CustomShareDBMessage): void {
    if (!this.isConnected) return
    this.ws.send(JSON.stringify(message))
  }

  subscribe(collection: string, docId: string): void {
    this.sendMessage({ a: 's', c: collection, d: docId })
  }

  unsubscribe(collection: string, docId: string): void {
    this.sendMessage({ a: 'u', c: collection, d: docId })
  }

  fetch(collection: string, docId: string): Promise<any> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('获取文档超时')), 8000)
      const handler = (event: MessageEvent) => {
        try {
          const msg: CustomShareDBMessage = JSON.parse((event as any).data)
          if (msg.c === collection && msg.d === docId && msg.a === 'f') {
            clearTimeout(timeout)
            this.ws.removeEventListener('message', handler as any)
            if (msg.error) reject(new Error(msg.error.message))
            else resolve(msg.data)
          }
        } catch {}
      }
      this.ws.addEventListener('message', handler as any)
      this.sendMessage({ a: 'f', c: collection, d: docId })
    })
  }

  submitOp(collection: string, docId: string, ops: Array<any>): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('提交操作超时')), 8000)
      const handler = (event: MessageEvent) => {
        try {
          const msg: CustomShareDBMessage = JSON.parse((event as any).data)
          if (msg.c === collection && msg.d === docId && msg.a === 'op') {
            clearTimeout(timeout)
            this.ws.removeEventListener('message', handler as any)
            if (msg.error) reject(new Error(msg.error.message))
            else resolve()
          }
        } catch {}
      }
      this.ws.addEventListener('message', handler as any)
      this.sendMessage({ a: 'op', c: collection, d: docId, op: ops })
    })
  }

  private setupEventListeners(): void {
    this.ws.addEventListener('open', () => {
      this.reconnectAttempts = 0
      this.status.connected = true
      this.status.reconnecting = false
      delete this.status.lastError
      this.status.reconnectAttempts = 0
      this.emit('connected')
    })

    this.ws.addEventListener('close', () => {
      this.status.connected = false
      this.emit('disconnected')
    })

    this.ws.addEventListener('error', (err: any) => {
      this.status.lastError = new Error(err?.message || 'WebSocket error')
      this.emit('error', this.status.lastError)
    })

    this.ws.addEventListener('message', (event: MessageEvent) => {
      try {
        const msg: CustomShareDBMessage = JSON.parse((event as any).data)
        // 路由到文档
        if (msg.c && msg.d) {
          const key = `${msg.c}:${msg.d}`
          const doc = this.docs.get(key)
          if (doc) doc.handleMessage(msg)
        }
        // 心跳
        if (msg.a === 'pong') {
          this.lastPongTime = Date.now()
          this.emit('pong')
        }
      } catch {}
    })
  }

  private setupHeartbeat(): void {
    this.heartbeatInterval = setInterval(() => {
      if (this.isConnected) {
        this.sendMessage({ a: 'ping' })
      }
    }, this.options.heartbeatInterval!)
  }

  close(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval)
      this.heartbeatInterval = null
    }
    this.ws.close()
  }

  destroy(): void {
    this.close()
    this.removeAllListeners()
    this.docs.clear()
  }
}
