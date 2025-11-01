// 用户相关类型
export interface User {
  id: string
  email: string
  name?: string
  avatar?: string
  role: UserRole
  isActive: boolean
  createdAt: string
  updatedAt: string
}

export type UserRole = 'owner' | 'admin' | 'member' | 'viewer'

export interface UserCreateRequest {
  email: string
  name?: string
  role?: UserRole
}

export interface UserUpdateRequest {
  name?: string
  avatar?: string
  role?: UserRole
  isActive?: boolean
}

export interface UserListFilter {
  email?: string
  name?: string
  role?: UserRole
  isActive?: boolean
  createdAfter?: string
  createdBefore?: string
}

export interface UserResponse {
  id: string
  email: string
  name?: string
  avatar?: string
  role: UserRole
  isActive: boolean
  createdAt: string
  updatedAt: string
}

// 协作者相关类型
export interface Collaborator {
  id: string
  userId: string
  resourceType: CollaboratorResourceType
  resourceId: string
  role: CollaboratorRole
  permissions: CollaboratorPermission[]
  invitedBy: string
  invitedAt: string
  acceptedAt?: string
  isActive: boolean
  user?: User
}

export type CollaboratorResourceType = 'space' | 'base' | 'table' | 'view'

export type CollaboratorRole = 'owner' | 'admin' | 'editor' | 'viewer'

export type CollaboratorPermission = 
  | 'read'
  | 'write'
  | 'delete'
  | 'share'
  | 'manage'

export interface CollaboratorCreateRequest {
  userId: string
  role: CollaboratorRole
  permissions?: CollaboratorPermission[]
}

export interface CollaboratorUpdateRequest {
  role?: CollaboratorRole
  permissions?: CollaboratorPermission[]
  isActive?: boolean
}

export interface CollaboratorListFilter {
  userId?: string
  resourceType?: CollaboratorResourceType
  resourceId?: string
  role?: CollaboratorRole
  isActive?: boolean
  invitedAfter?: string
  invitedBefore?: string
}

export interface CollaboratorResponse {
  id: string
  userId: string
  resourceType: CollaboratorResourceType
  resourceId: string
  role: CollaboratorRole
  permissions: CollaboratorPermission[]
  invitedBy: string
  invitedAt: string
  acceptedAt?: string
  isActive: boolean
  user?: UserResponse
}

// 邀请相关类型
export interface InviteRequest {
  email: string
  role: CollaboratorRole
  permissions?: CollaboratorPermission[]
  message?: string
}

export interface InviteResponse {
  id: string
  email: string
  role: CollaboratorRole
  permissions: CollaboratorPermission[]
  message?: string
  invitedBy: string
  invitedAt: string
  expiresAt: string
  status: InviteStatus
}

export type InviteStatus = 'pending' | 'accepted' | 'declined' | 'expired'

export interface InviteAcceptRequest {
  token: string
}

export interface InviteDeclineRequest {
  token: string
}
