import LuckDBClient from 'luckdb-sdk'
import { LocalAuthStore } from 'luckdb-sdk'

// 从环境变量获取服务器地址，默认使用 localhost:8080
const SERVER_URL = import.meta.env.VITE_LUCKDB_SERVER_URL || 'http://localhost:8080'

// 创建认证存储实例
const authStore = new LocalAuthStore()

// 创建全局 SDK 客户端实例
export const luckdbClient = new LuckDBClient(SERVER_URL, authStore, 'zh-CN')

// 导出客户端类型
export type { LuckDBClient }

