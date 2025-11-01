import { EventEmitter } from 'events'
import * as ShareDB from 'sharedb/lib/client'
import { ShareDBPresenceEvent, ShareDBPresenceData } from '@/types/sharedb'

export class ShareDBPresence extends EventEmitter {
  private presence: ShareDB.Presence
  private localPresences: Map<string, ShareDB.LocalPresence>
  private isDestroyed: boolean = false

  constructor(connection: ShareDB.Connection, channel: string) {
    super()
    this.presence = connection.getPresence(channel)
    this.localPresences = new Map()
    this.setupListeners()
  }

  /**
   * 获取频道名称
   */
  get channel(): string {
    return this.presence.channel
  }

  /**
   * 检查是否已销毁
   */
  get destroyed(): boolean {
    return this.isDestroyed
  }

  /**
   * 订阅 Presence
   */
  async subscribe(): Promise<void> {
    if (this.isDestroyed) {
      throw new Error('Presence has been destroyed')
    }

    return new Promise((resolve, reject) => {
      this.presence.subscribe((err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  /**
   * 取消订阅
   */
  async unsubscribe(): Promise<void> {
    if (this.isDestroyed) {
      return
    }

    return new Promise((resolve) => {
      this.presence.unsubscribe((err) => {
        resolve()
      })
    })
  }

  /**
   * 创建本地 Presence
   */
  create(presenceId?: string): LocalPresenceWrapper {
    if (this.isDestroyed) {
      throw new Error('Presence has been destroyed')
    }

    const localPresence = this.presence.create(presenceId)
    const wrapper = new LocalPresenceWrapper(localPresence)

    if (presenceId) {
      this.localPresences.set(presenceId, localPresence)
    }

    return wrapper
  }

  /**
   * 获取本地 Presence
   */
  getLocalPresence(presenceId: string): LocalPresenceWrapper | null {
    const localPresence = this.localPresences.get(presenceId)
    if (!localPresence) {
      return null
    }

    return new LocalPresenceWrapper(localPresence)
  }

  /**
   * 销毁本地 Presence
   */
  destroyLocalPresence(presenceId: string): void {
    const localPresence = this.localPresences.get(presenceId)
    if (localPresence) {
      localPresence.destroy()
      this.localPresences.delete(presenceId)
    }
  }

  /**
   * 设置事件监听
   */
  private setupListeners(): void {
    this.presence.on('receive', (id, value) => {
      const event: ShareDBPresenceEvent = { id, value }
      this.emit('receive', event)
    })

    this.presence.on('error', (err) => {
      this.emit('error', err)
    })
  }

  /**
   * 销毁 Presence
   */
  destroy(): void {
    if (this.isDestroyed) {
      return
    }

    this.isDestroyed = true

    // 销毁所有本地 Presence
    for (const localPresence of this.localPresences.values()) {
      localPresence.destroy()
    }
    this.localPresences.clear()

    this.presence.destroy()
    this.removeAllListeners()
  }
}

export class LocalPresenceWrapper {
  private localPresence: ShareDB.LocalPresence

  constructor(localPresence: ShareDB.LocalPresence) {
    this.localPresence = localPresence
  }

  /**
   * 提交 Presence 数据
   */
  async submit(presence: ShareDBPresenceData): Promise<void> {
    return new Promise((resolve, reject) => {
      this.localPresence.submit(presence, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  /**
   * 发送 Presence 数据
   */
  async send(presence: ShareDBPresenceData): Promise<void> {
    return new Promise((resolve) => {
      (this.localPresence as any).send(presence)
      resolve()
    })
  }

  /**
   * 销毁本地 Presence
   */
  destroy(): void {
    this.localPresence.destroy()
  }

  /**
   * 获取 Presence ID
   */
  get id(): string {
    return (this.localPresence as any).id || 'unknown'
  }

  /**
   * 检查是否已销毁
   */
  get destroyed(): boolean {
    return (this.localPresence as any).destroyed || false
  }
}
