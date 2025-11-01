/**
 * JWT 工具函数
 */

export interface JWTPayload {
  sub: string
  iat: number
  exp: number
  [key: string]: any
}

/**
 * 解析 JWT token（仅解析 payload，不验证签名）
 */
export function parseJWT(token: string): JWTPayload | null {
  try {
    const parts = token.split('.')
    if (parts.length !== 3) {
      return null
    }

    const payload = parts[1]
    const decoded = atob((payload || '').replace(/-/g, '+').replace(/_/g, '/'))
    return JSON.parse(decoded)
  } catch {
    return null
  }
}

/**
 * 检查 JWT token 是否过期
 */
export function isJWTExpired(token: string): boolean {
  const payload = parseJWT(token)
  if (!payload || !payload.exp) {
    return true
  }

  const now = Math.floor(Date.now() / 1000)
  return payload.exp <= now
}

/**
 * 检查 JWT token 是否即将过期（在指定秒数内）
 */
export function isJWTExpiringSoon(token: string, seconds = 300): boolean {
  const payload = parseJWT(token)
  if (!payload || !payload.exp) {
    return true
  }

  const now = Math.floor(Date.now() / 1000)
  return payload.exp <= now + seconds
}

/**
 * 获取 JWT token 的剩余有效时间（秒）
 */
export function getJWTTimeRemaining(token: string): number {
  const payload = parseJWT(token)
  if (!payload || !payload.exp) {
    return 0
  }

  const now = Math.floor(Date.now() / 1000)
  return Math.max(0, payload.exp - now)
}

/**
 * 获取 JWT token 的用户 ID
 */
export function getJWTUserId(token: string): string | null {
  const payload = parseJWT(token)
  return payload?.sub || null
}

/**
 * 检查 JWT token 是否有效（未过期且有用户 ID）
 */
export function isJWTValid(token: string): boolean {
  if (!token) {
    return false
  }

  const payload = parseJWT(token)
  return !!(payload && payload.sub && !isJWTExpired(token))
}
