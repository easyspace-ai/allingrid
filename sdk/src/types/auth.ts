// 认证相关类型
export interface LoginRequest {
  email: string
  password: string
}

export interface RegisterRequest {
  email: string
  password: string
  passwordConfirm: string
  name?: string
}

export interface RefreshTokenRequest {
  refreshToken: string
}

export interface PasswordResetRequest {
  email: string
}

export interface PasswordResetConfirmRequest {
  token: string
  password: string
  passwordConfirm: string
}

export interface EmailVerificationRequest {
  email: string
}

export interface EmailVerificationConfirmRequest {
  token: string
}

export interface EmailChangeRequest {
  newEmail: string
}

export interface EmailChangeConfirmRequest {
  token: string
}

export interface UserUpdateRequest {
  name?: string
  avatar?: string
}

export interface PasswordUpdateRequest {
  oldPassword: string
  newPassword: string
  newPasswordConfirm: string
}
