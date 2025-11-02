import { BaseService } from './BaseService'
import { AuthResponse, AuthRecord } from '@/types/common'
import {
  LoginRequest,
  RegisterRequest,
  RefreshTokenRequest,
  PasswordResetRequest,
  PasswordResetConfirmRequest,
  EmailVerificationRequest,
  EmailVerificationConfirmRequest,
  EmailChangeRequest,
  EmailChangeConfirmRequest,
  UserUpdateRequest,
  PasswordUpdateRequest
} from '@/types/auth'

export class AuthService extends BaseService {
  /**
   * 用户登录
   */
  async login(email: string, password: string): Promise<AuthResponse> {
    const request: LoginRequest = { email, password }
    
    try {
      console.log('[AuthService] Starting login request...')
      const serverResponse = await this.send<{ user: AuthRecord, accessToken: string, refreshToken: string } | any>('/api/v1/auth/login', {
        method: 'POST',
        body: request
      })

      console.log('[AuthService] Received response:', serverResponse)
      console.log('[AuthService] Response type:', typeof serverResponse)
      console.log('[AuthService] Response keys:', serverResponse ? Object.keys(serverResponse) : 'null/undefined')

      // 防御性检查：确保响应存在
      if (!serverResponse) {
        console.error('[AuthService] Login response is undefined')
        throw new Error('登录失败：服务器响应为空')
      }

      // 额外检查：确保 serverResponse 是对象
      if (typeof serverResponse !== 'object') {
        console.error('[AuthService] Login response is not an object:', serverResponse)
        throw new Error(`登录失败：响应格式无效 - 期望对象，收到 ${typeof serverResponse}`)
      }

      // 检查响应格式：可能是 { accessToken, user } 或 { data: { accessToken, user } }
      let accessToken: string | undefined
      let user: AuthRecord | undefined

      if (serverResponse.accessToken) {
        // 直接格式：{ accessToken, user, refreshToken }
        accessToken = serverResponse.accessToken
        user = serverResponse.user
      } else if (serverResponse.data && serverResponse.data.accessToken) {
        // 嵌套格式：{ data: { accessToken, user, refreshToken } }
        accessToken = serverResponse.data.accessToken
        user = serverResponse.data.user
      } else {
        // 未知格式，尝试直接使用
        console.error('Unexpected login response format:', serverResponse)
        throw new Error(`登录失败：响应格式错误 - ${JSON.stringify(serverResponse).substring(0, 100)}`)
      }

      if (!accessToken) {
        throw new Error('登录失败：未收到访问令牌')
      }

      if (!user) {
        throw new Error('登录失败：未收到用户信息')
      }

      const response: AuthResponse = {
        token: accessToken,
        record: user
      }

      this.authResponse(response)
      return response
    } catch (error) {
      console.error('Login error details:', error)
      throw error
    }
  }

  /**
   * 用户注册
   */
  async register(email: string, password: string, passwordConfirm: string, name?: string): Promise<AuthResponse> {
    const request: RegisterRequest = { email, password, passwordConfirm, ...(name && { name }) }
    
    try {
      const serverResponse = await this.send<{ user: AuthRecord, accessToken: string, refreshToken: string } | any>('/api/v1/auth/register', {
        method: 'POST',
        body: request
      })

      // 防御性检查：确保响应存在
      if (!serverResponse) {
        throw new Error('注册失败：服务器响应为空')
      }

      // 检查响应格式：可能是 { accessToken, user } 或 { data: { accessToken, user } }
      let accessToken: string | undefined
      let user: AuthRecord | undefined

      if (serverResponse.accessToken) {
        accessToken = serverResponse.accessToken
        user = serverResponse.user
      } else if (serverResponse.data && serverResponse.data.accessToken) {
        accessToken = serverResponse.data.accessToken
        user = serverResponse.data.user
      } else {
        throw new Error(`注册失败：响应格式错误 - ${JSON.stringify(serverResponse).substring(0, 100)}`)
      }

      if (!accessToken || !user) {
        throw new Error('注册失败：未收到访问令牌或用户信息')
      }

      const response: AuthResponse = {
        token: accessToken,
        record: user
      }

      this.authResponse(response)
      return response
    } catch (error) {
      console.error('Register error details:', error)
      throw error
    }
  }

  /**
   * 用户登出
   */
  async logout(): Promise<void> {
    await this.send('/api/v1/auth/logout', {
      method: 'POST'
    })

    this.client.authStore.clear()
  }

  /**
   * 刷新访问令牌
   * 对齐服务端 API: POST /api/v1/auth/refresh
   * 请求格式: { refresh_token: string }
   */
  async refreshToken(): Promise<AuthResponse> {
    const token = this.client.authStore.token
    if (!token) {
      throw new Error('No token available for refresh')
    }

    // 服务端使用 refresh_token 字段名（snake_case）
    const serverResponse = await this.send<{ user: AuthRecord, accessToken: string, refreshToken: string }>('/api/v1/auth/refresh', {
      method: 'POST',
      body: { refresh_token: token } // 使用服务端期望的字段名
    })

    // parseResponse 已经解包了 data，所以直接使用 serverResponse
    const response: AuthResponse = {
      token: serverResponse.accessToken,
      record: serverResponse.user
    }

    this.authResponse(response)
    return response
  }

  /**
   * 获取当前用户信息
   * 对齐服务端 API: GET /api/v1/auth/me
   * 响应格式: { code, message, data: { userId, email, isAdmin, ... } }
   */
  async getCurrentUser(): Promise<AuthResponse> {
    const serverResponse = await this.send<{ userId: string, email: string, isAdmin: boolean }>('/api/v1/auth/me', {
      method: 'GET'
    })

    // 服务端返回的是 TokenClaims，需要获取完整用户信息
    // 这里先返回基本信息，如果需要完整信息，可以调用 /api/v1/users/:id
    const emailPrefix = serverResponse.email.split('@')[0]
    const record: AuthRecord = {
      id: serverResponse.userId,
      email: serverResponse.email,
      created: new Date().toISOString(),
      updated: new Date().toISOString()
    }
    
    // 只在有值时才添加可选属性
    if (emailPrefix) {
      record.name = emailPrefix
    }

    const response: AuthResponse = {
      token: this.client.authStore.token || '',
      record
    }

    return response
  }

  /**
   * 请求密码重置（当前服务端未提供，对齐策略：抛出不支持错误）
   */
  async requestPasswordReset(email: string): Promise<void> {
    throw new Error('Not supported by server: /api/v1/auth/password-reset')
  }

  /**
   * 确认密码重置
   */
  async confirmPasswordReset(token: string, password: string, passwordConfirm: string): Promise<AuthResponse> {
    throw new Error('Not supported by server: /api/v1/auth/password-reset/confirm')
  }

  /**
   * 请求邮箱验证
   */
  async requestEmailVerification(email: string): Promise<void> {
    throw new Error('Not supported by server: /api/v1/auth/email-verification')
  }

  /**
   * 确认邮箱验证
   */
  async confirmEmailVerification(token: string): Promise<AuthResponse> {
    throw new Error('Not supported by server: /api/v1/auth/email-verification/confirm')
  }

  /**
   * 请求邮箱更改
   */
  async requestEmailChange(newEmail: string): Promise<void> {
    throw new Error('Not supported by server: /api/v1/auth/email-change')
  }

  /**
   * 确认邮箱更改
   */
  async confirmEmailChange(token: string): Promise<AuthResponse> {
    throw new Error('Not supported by server: /api/v1/auth/email-change/confirm')
  }

  /**
   * 更新用户信息
   */
  async updateUser(data: UserUpdateRequest): Promise<AuthResponse> {
    const current = this.client.authStore.record
    if (!current?.id) {
      throw new Error('No authenticated user id for update')
    }
    const serverResponse = await this.send<{ user: AuthRecord, accessToken?: string }>(`/api/v1/users/${current.id}`, {
      method: 'PATCH',
      body: data
    })
    
    // parseResponse 已经解包了 data，所以直接使用 serverResponse
    const response: AuthResponse = {
      token: serverResponse.accessToken || this.client.authStore.token || '',
      record: serverResponse.user
    }
    
    this.authResponse(response)
    return response
  }

  /**
   * 更新密码
   */
  async updatePassword(oldPassword: string, newPassword: string, newPasswordConfirm: string): Promise<void> {
    const current = this.client.authStore.record
    if (!current?.id) {
      throw new Error('No authenticated user id for password update')
    }
    const request: PasswordUpdateRequest = { oldPassword, newPassword, newPasswordConfirm }
    await this.send(`/api/v1/users/${current.id}/password`, {
      method: 'PATCH',
      body: request
    })
  }

  /**
   * 处理认证响应
   */
  private authResponse(response: AuthResponse): void {
    if (response.token && response.record) {
      this.client.authStore.save(response.token, response.record)
    }
  }

  /**
   * 检查认证状态
   */
  isAuthenticated(): boolean {
    return this.client.authStore.isValid
  }

  /**
   * 获取当前用户
   */
  getCurrentUserSync(): AuthResponse['record'] | null {
    return this.client.authStore.record
  }

  /**
   * 获取访问令牌
   */
  getToken(): string | null {
    return this.client.authStore.token
  }
}
