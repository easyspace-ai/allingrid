import { EventEmitter } from 'node:events'
import { AuthRecord } from '@/types/common'

export abstract class BaseAuthStore extends EventEmitter {
  protected _token: string | null = null
  protected _record: AuthRecord | null = null

  /**
   * 获取认证 token
   */
  get token(): string | null {
    return this._token
  }

  /**
   * 获取用户记录
   */
  get record(): AuthRecord | null {
    return this._record
  }

  /**
   * 检查认证是否有效
   */
  get isValid(): boolean {
    return !!this._token && !!this._record
  }

  /**
   * 检查是否为超级用户
   */
  get isSuperuser(): boolean {
    return this._record?.id === 'superuser' || false
  }

  /**
   * 保存认证信息
   */
  abstract save(token: string, record: AuthRecord): void

  /**
   * 清除认证信息
   */
  abstract clear(): void

  /**
   * 从存储中加载认证信息
   */
  abstract load(): void

  /**
   * 监听存储变化
   */
  onChange(callback: () => void): () => void {
    this.on('change', callback)
    return () => this.off('change', callback)
  }

  /**
   * 触发变化事件
   */
  protected triggerChange(): void {
    this.emit('change')
  }

  /**
   * 从 Cookie 加载（用于 SSR）
   */
  loadFromCookie(cookie: string): void {
    const cookies = this.parseCookies(cookie)
    const token = cookies['luckdb_auth_token']
    const recordStr = cookies['luckdb_auth_record']

    if (token && recordStr) {
      try {
        const record = JSON.parse(decodeURIComponent(recordStr))
        this.save(token, record)
      } catch {
        // 忽略解析错误
      }
    }
  }

  /**
   * 导出到 Cookie（用于 SSR）
   */
  exportToCookie(): string {
    const cookies: string[] = []

    if (this._token) {
      cookies.push(`luckdb_auth_token=${this._token}; Path=/; HttpOnly; SameSite=Strict`)
    }

    if (this._record) {
      const recordStr = encodeURIComponent(JSON.stringify(this._record))
      cookies.push(`luckdb_auth_record=${recordStr}; Path=/; HttpOnly; SameSite=Strict`)
    }

    return cookies.join('; ')
  }

  /**
   * 解析 Cookie 字符串
   */
  private parseCookies(cookie: string): Record<string, string> {
    const cookies: Record<string, string> = {}
    
    cookie.split(';').forEach(cookieStr => {
      const [name, value] = cookieStr.trim().split('=')
      if (name && value) {
        cookies[name] = value
      }
    })

    return cookies
  }
}
