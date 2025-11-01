import { EventEmitter } from 'events'
import { ShareDBConnection } from '@/sharedb/ShareDBConnection'
import { ShareDBDocEvent } from '@/types/sharedb'

export interface ShareDBOperation {
  p: any[]
  oi?: any
  od?: any
  na?: number
}

export class ShareDBDoc extends EventEmitter {
  private connection: ShareDBConnection
  private collectionName: string
  private docId: string
  private subscribed = false
  private isDestroyed = false
  private _data: any = null
  private _version = 0

  constructor(connection: ShareDBConnection, collection: string, id: string) {
    super()
    this.connection = connection
    this.collectionName = collection
    this.docId = id
  }

  get id(): string { return this.docId }
  get collection(): string { return this.collectionName }
  get data(): any { return this._data }
  get version(): number { return this._version }
  get type(): string | null { return this._data ? 'json' : null }
  get isSubscribed(): boolean { return this.subscribed }
  get destroyed(): boolean { return this.isDestroyed }

  async subscribe(): Promise<void> {
    if (this.isDestroyed) throw new Error('Document has been destroyed')
    if (this.subscribed) return

    // 先发送订阅请求，服务端会返回初始数据
    this.connection.subscribe(this.collectionName, this.docId)
    this.subscribed = true

    // 不需要单独 fetch，因为订阅响应会包含初始数据
    // 如果 3 秒内没有收到订阅响应，则设置为空对象
    setTimeout(() => {
      if (this._data === null) {
        this._data = {}
        this._version = 0
        this.emit('load', { data: this._data })
      }
    }, 3000)
  }

  async unsubscribe(): Promise<void> {
    if (!this.subscribed) return
    this.connection.unsubscribe(this.collectionName, this.docId)
    this.subscribed = false
  }

  async submitOp(op: ShareDBOperation[]): Promise<void> {
    if (this.isDestroyed) throw new Error('Document has been destroyed')
    if (!this.subscribed) throw new Error('Document must be subscribed before submitting operations')
    await this.connection.submitOp(this.collectionName, this.docId, op as any)
  }

  async create(data: any): Promise<void> {
    if (this.isDestroyed) throw new Error('Document has been destroyed')
    await this.connection.submitOp(this.collectionName, this.docId, [{ p: [], oi: data }])
  }

  async del(): Promise<void> {
    if (this.isDestroyed) throw new Error('Document has been destroyed')
    await this.connection.submitOp(this.collectionName, this.docId, [{ p: [], od: this._data }])
  }

  handleMessage(message: { a: string; v?: number; data?: any; op?: ShareDBOperation[] }): void {
    try {
      switch (message.a) {
        case 's':
        case 'f':
          // 即使 data 为空对象，也应该触发 load 事件
          if (message.data !== undefined) {
            this._data = message.data
            this._version = message.v ?? this._version
            this.emit('load', { data: this._data })
          } else {
            // 如果没有 data 字段，设置为空对象并触发 load
            this._data = {}
            this._version = message.v ?? this._version
            this.emit('load', { data: this._data })
          }
          break
        case 'op':
          if (message.op) {
            this.applyOperations(message.op)
          }
          break
        default:
          break
      }
    } catch (err) {
      this.emit('error', err as any)
    }
  }

  private applyOperations(operations: ShareDBOperation[]): void {
    if (!this._data) this._data = {}
    operations.forEach(op => this.applyOperation(op))
    this._version++
    this.emit('op', { op: operations, source: false, data: this._data } as ShareDBDocEvent)
  }

  private applyOperation(operation: ShareDBOperation): void {
    const path = operation.p
    if (!Array.isArray(path) || path.length === 0) return
    let target = this._data
    for (let i = 0; i < path.length - 1; i++) {
      const key = path[i]
      if (!(key in target) || typeof target[key] !== 'object') target[key] = {}
      target = target[key]
    }
    const lastKey = path[path.length - 1]
    if (operation.oi !== undefined) target[lastKey] = operation.oi
    else if (operation.od !== undefined) delete target[lastKey]
    else if (operation.na !== undefined && typeof target[lastKey] === 'number') target[lastKey] += operation.na
  }

  destroy(): void {
    if (this.isDestroyed) return
    this.isDestroyed = true
    this.removeAllListeners()
  }
}
