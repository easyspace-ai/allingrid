import { BaseAuthStore } from './BaseAuthStore'
import { AuthRecord } from '@/types/common'

export class LocalAuthStore extends BaseAuthStore {
  private storageKey = 'luckdb_auth'

  constructor() {
    super()
    this.load()
    this.setupCrossTabSync()
  }

  /**
   * 保存认证信息到 localStorage
   */
  save(token: string, record: AuthRecord): void {
    this._token = token
    this._record = record

    try {
      const authData = { token, record }
      localStorage.setItem(this.storageKey, JSON.stringify(authData))
    } catch {
      // localStorage 不可用，使用内存存储
    }

    this.triggerChange()
  }

  /**
   * 清除认证信息
   */
  clear(): void {
    this._token = null
    this._record = null

    try {
      localStorage.removeItem(this.storageKey)
    } catch {
      // localStorage 不可用，忽略
    }

    this.triggerChange()
  }

  /**
   * 从 localStorage 加载认证信息
   */
  load(): void {
    try {
      const authDataStr = localStorage.getItem(this.storageKey)
      if (authDataStr) {
        const authData = JSON.parse(authDataStr)
        if (authData.token && authData.record) {
          this._token = authData.token
          this._record = authData.record
        }
      }
    } catch {
      // 解析失败或 localStorage 不可用，使用默认值
      this._token = null
      this._record = null
    }
  }

  /**
   * 设置跨标签页同步
   */
  private setupCrossTabSync(): void {
    if (typeof window === 'undefined') {
      return
    }

    // 监听 storage 事件，实现跨标签页同步
    window.addEventListener('storage', (event) => {
      if (event.key === this.storageKey) {
        if (event.newValue) {
          try {
            const authData = JSON.parse(event.newValue)
            if (authData.token && authData.record) {
              this._token = authData.token
              this._record = authData.record
            }
          } catch {
            this._token = null
            this._record = null
          }
        } else {
          this._token = null
          this._record = null
        }
        this.triggerChange()
      }
    })
  }
}
