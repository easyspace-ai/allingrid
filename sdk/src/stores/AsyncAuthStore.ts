import { BaseAuthStore } from './BaseAuthStore'
import { AuthRecord } from '@/types/common'

export interface AsyncAuthStoreOptions {
  save: (token: string, record: AuthRecord) => Promise<void>
  load: () => Promise<{ token: string; record: AuthRecord } | null>
  clear: () => Promise<void>
}

export class AsyncAuthStore extends BaseAuthStore {
  private options: AsyncAuthStoreOptions
  private loadingPromise: Promise<void> | null = null

  constructor(options: AsyncAuthStoreOptions) {
    super()
    this.options = options
    this.load()
  }

  /**
   * 异步保存认证信息
   */
  async save(token: string, record: AuthRecord): Promise<void> {
    this._token = token
    this._record = record

    try {
      await this.options.save(token, record)
    } catch (error) {
      // 保存失败，回滚状态
      this._token = null
      this._record = null
      throw error
    }

    this.triggerChange()
  }

  /**
   * 异步清除认证信息
   */
  async clear(): Promise<void> {
    this._token = null
    this._record = null

    try {
      await this.options.clear()
    } catch (error) {
      // 清除失败，忽略错误
    }

    this.triggerChange()
  }

  /**
   * 异步加载认证信息
   */
  async load(): Promise<void> {
    // 防止重复加载
    if (this.loadingPromise) {
      return this.loadingPromise
    }

    this.loadingPromise = this.performLoad()
    return this.loadingPromise
  }

  private async performLoad(): Promise<void> {
    try {
      const authData = await this.options.load()
      if (authData) {
        this._token = authData.token
        this._record = authData.record
      } else {
        this._token = null
        this._record = null
      }
    } catch (error) {
      // 加载失败，使用默认值
      this._token = null
      this._record = null
    } finally {
      this.loadingPromise = null
    }
  }

  /**
   * 等待加载完成
   */
  async waitForLoad(): Promise<void> {
    if (this.loadingPromise) {
      await this.loadingPromise
    }
  }
}
