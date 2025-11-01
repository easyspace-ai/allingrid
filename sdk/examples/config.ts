import dotenv from 'dotenv'
import path from 'path'

// 加载环境变量（tsx 会自动处理 __dirname）
const envPath = path.join(__dirname || process.cwd(), '.env')
dotenv.config({ path: envPath })

export interface DemoConfig {
  serverURL: string
  websocketURL: string
  testEmail: string
  testPassword: string
  testName: string
  debug: boolean
  // ShareDB 测试数据
  sharedbTestSpaceId?: string
  sharedbTestBaseId?: string
  sharedbTestTableId?: string
  sharedbTestFieldId?: string
  sharedbTestRecordId?: string
  sharedbTestCollection?: string
}

export function loadConfig(): DemoConfig {
  const serverURL = process.env.SERVER_URL || 'http://localhost:8080'
  const websocketURL = process.env.WEBSOCKET_URL || serverURL.replace(/^http/, 'ws')
  const testEmail = process.env.TEST_EMAIL || 'demo@example.com'
  const testPassword = process.env.TEST_PASSWORD || 'Test123456!'
  const testName = process.env.TEST_NAME || 'Demo User'
  const debug = process.env.DEBUG === 'true'

  // ShareDB 测试数据
  const sharedbTestSpaceId = process.env.SHAREDB_TEST_SPACE_ID
  const sharedbTestBaseId = process.env.SHAREDB_TEST_BASE_ID
  const sharedbTestTableId = process.env.SHAREDB_TEST_TABLE_ID
  const sharedbTestFieldId = process.env.SHAREDB_TEST_FIELD_ID
  const sharedbTestRecordId = process.env.SHAREDB_TEST_RECORD_ID
  const sharedbTestCollection = process.env.SHAREDB_TEST_COLLECTION

  // 验证配置
  if (!serverURL) {
    throw new Error('SERVER_URL 环境变量未设置')
  }

  return {
    serverURL,
    websocketURL,
    testEmail,
    testPassword,
    testName,
    debug,
    sharedbTestSpaceId,
    sharedbTestBaseId,
    sharedbTestTableId,
    sharedbTestFieldId,
    sharedbTestRecordId,
    sharedbTestCollection
  }
}

export const config = loadConfig()

