import { LuckDBClient } from '@/Client'

export abstract class BaseService {
  protected client: LuckDBClient

  constructor(client: LuckDBClient) {
    this.client = client
  }

  /**
   * 构建完整的 API URL
   */
  protected buildUrl(path: string): string {
    return this.client.buildUrl(path)
  }

  /**
   * 发送请求的便捷方法
   */
  protected async send<T>(path: string, options: any = {}): Promise<T> {
    return this.client.send<T>(path, options)
  }

  /**
   * 获取认证 token
   */
  protected getAuthToken(): string | null {
    return this.client.authStore.token
  }

  /**
   * 检查是否已认证
   */
  protected isAuthenticated(): boolean {
    return this.client.authStore.isValid
  }
}
