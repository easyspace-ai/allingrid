import { BaseService } from './BaseService'

export class ShareDBAdminService extends BaseService {
  async getStats(): Promise<any> {
    return this.send('/api/v1/sharedb/stats', { method: 'GET' })
  }

  async getConnections(): Promise<any> {
    return this.send('/api/v1/sharedb/connections', { method: 'GET' })
  }

  async cleanup(): Promise<{ success: boolean }> {
    return this.send('/api/v1/sharedb/cleanup', { method: 'POST' })
  }
}



