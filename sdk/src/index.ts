// 导出主要类
export * from '@/Client'
export * from '@/ClientResponseError'

// 导出服务
export * from '@/services/BaseService'
export * from '@/services/AuthService'
export * from '@/services/SpaceService'
export * from '@/services/LuckDBBaseService'
export * from '@/services/TableService'
export { FieldService } from '@/services/FieldService'
export * from '@/services/RecordService'
export * from '@/services/ViewService'
export * from '@/services/UserService'
export * from '@/services/CollaboratorService'
export * from '@/services/RealtimeService'
export * from '@/services/ShareDBService'
export * from '@/services/OrganizationService'
export * from '@/services/WorkflowService'
export * from '@/services/RecordHistoryService'
export * from '@/services/NotificationService'
export * from '@/services/AttachmentService'

// 导出存储
export * from '@/stores/BaseAuthStore'
export * from '@/stores/LocalAuthStore'
export * from '@/stores/AsyncAuthStore'

// 导出 ShareDB 相关
export * from '@/sharedb/ShareDBConnection'
export * from '@/sharedb/ShareDBDoc'
export * from '@/sharedb/ShareDBPresence'

// 导出类型
export * from '@/types/common'
export * from '@/types/auth'
export * from '@/types/attachment'
export type {
  ShareDBOperation,
  ShareDBDocEvent,
  ShareDBPresenceEvent,
  ShareDBPresenceData
} from '@/types/sharedb'

// 默认导出
import { LuckDBClient } from '@/Client'
export default LuckDBClient
