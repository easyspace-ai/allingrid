// ShareDB 相关类型
export interface ShareDBConnectionOptions {
  getAuthToken?: () => string | null
  reconnectDelay?: number
  maxReconnectDelay?: number
  heartbeatInterval?: number
  heartbeatTimeout?: number
}

export interface ShareDBOperation {
  p: string[]  // path
  oi?: any     // insert
  od?: any     // delete
  li?: any     // list insert
  ld?: any     // list delete
  na?: number  // number add
}

export interface ShareDBDocEvent {
  op: ShareDBOperation[]
  source: boolean
  data: any
}

export interface ShareDBPresenceEvent {
  id: string
  value: any
}

export interface ShareDBPresenceData {
  cursor?: { x: number; y: number }
  selection?: { start: number; end: number }
  user?: {
    id: string
    name: string
    avatar?: string
    color?: string
  }
  [key: string]: any
}

export interface ShareDBConnectionStatus {
  connected: boolean
  reconnecting: boolean
  lastError?: Error | undefined
  reconnectAttempts: number
}
