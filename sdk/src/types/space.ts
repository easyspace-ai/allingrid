// 空间相关类型
export interface Space {
  id: string
  name: string
  description?: string
  ownerId: string
  createdAt: string
  updatedAt: string
}

export interface SpaceCreateRequest {
  name: string
  description?: string
}

export interface SpaceUpdateRequest {
  name?: string
  description?: string
}

export interface SpaceListFilter {
  name?: string
  ownerId?: string
  createdAfter?: string
  createdBefore?: string
}

// 对齐服务端 SpaceResponse
export interface SpaceResponse {
  id: string
  name: string
  description?: string
  createdBy: string // 服务端使用 createdBy
  updatedBy?: string // 服务端使用 updatedBy
  createdAt: string
  updatedAt: string
  // SDK兼容字段
  ownerId?: string // 兼容旧代码，映射自 createdBy
  bases?: Base[]
}

export interface Base {
  id: string
  name: string
  description?: string
  spaceId: string
  ownerId: string
  createdAt: string
  updatedAt: string
}

// 对齐服务端 CreateBaseRequest
export interface BaseCreateRequest {
  name: string
  icon?: string // 服务端支持 icon
  // SDK兼容字段
  description?: string // 服务端暂不支持，但保留兼容性
}

export interface BaseUpdateRequest {
  name?: string
  description?: string
}

export interface BaseListFilter {
  name?: string
  spaceId?: string
  ownerId?: string
  createdAfter?: string
  createdBefore?: string
}

// 对齐服务端 BaseResponse
export interface BaseResponse {
  id: string
  name: string
  icon?: string // 服务端支持 icon
  spaceId: string
  createdBy: string // 服务端使用 createdBy
  createdAt: string
  updatedAt: string
  // SDK兼容字段
  ownerId?: string // 兼容旧代码，映射自 createdBy
  description?: string // 服务端暂不支持，但保留兼容性
}
