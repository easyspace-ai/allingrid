/**
 * Cookie 工具函数
 */

export interface CookieOptions {
  expires?: Date | number
  maxAge?: number
  path?: string
  domain?: string
  secure?: boolean
  httpOnly?: boolean
  sameSite?: 'strict' | 'lax' | 'none'
}

/**
 * 设置 Cookie
 */
export function setCookie(
  name: string,
  value: string,
  options: CookieOptions = {}
): void {
  if (typeof document === 'undefined') {
    return
  }

  const {
    expires,
    maxAge,
    path = '/',
    domain,
    secure = false,
    httpOnly = false,
    sameSite = 'lax'
  } = options

  let cookieString = `${encodeURIComponent(name)}=${encodeURIComponent(value)}`

  if (expires) {
    const date = typeof expires === 'number' ? new Date(expires) : expires
    cookieString += `; Expires=${date.toUTCString()}`
  }

  if (maxAge) {
    cookieString += `; Max-Age=${maxAge}`
  }

  if (path) {
    cookieString += `; Path=${path}`
  }

  if (domain) {
    cookieString += `; Domain=${domain}`
  }

  if (secure) {
    cookieString += '; Secure'
  }

  if (httpOnly) {
    cookieString += '; HttpOnly'
  }

  if (sameSite) {
    cookieString += `; SameSite=${sameSite}`
  }

  document.cookie = cookieString
}

/**
 * 获取 Cookie
 */
export function getCookie(name: string): string | null {
  if (typeof document === 'undefined') {
    return null
  }

  const cookies = document.cookie.split(';')
  for (const cookie of cookies) {
    const [cookieName, cookieValue] = cookie.trim().split('=')
    if (cookieName === name) {
      return decodeURIComponent(cookieValue || '')
    }
  }

  return null
}

/**
 * 删除 Cookie
 */
export function deleteCookie(name: string, options: Pick<CookieOptions, 'path' | 'domain'> = {}): void {
  setCookie(name, '', {
    ...options,
    expires: new Date(0)
  })
}

/**
 * 解析所有 Cookie
 */
export function parseCookies(cookieString?: string): Record<string, string> {
  const cookies: Record<string, string> = {}
  const cookieStr = cookieString || (typeof document !== 'undefined' ? document.cookie : '')

  cookieStr.split(';').forEach(cookie => {
    const [name, value] = cookie.trim().split('=')
    if (name && value) {
      cookies[decodeURIComponent(name)] = decodeURIComponent(value)
    }
  })

  return cookies
}

/**
 * 检查 Cookie 是否支持
 */
export function isCookieSupported(): boolean {
  if (typeof document === 'undefined') {
    return false
  }

  try {
    const testKey = '__cookie_test__'
    setCookie(testKey, 'test')
    const supported = getCookie(testKey) === 'test'
    deleteCookie(testKey)
    return supported
  } catch {
    return false
  }
}
