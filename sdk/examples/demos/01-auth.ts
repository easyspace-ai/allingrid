import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'

const logger = new Logger('Auth Demo')

export async function runAuthDemo(): Promise<boolean> {
  logger.section('认证 API 演示')

  // 创建客户端
  const client = new LuckDBClient(config.serverURL)
  
  try {
    // 1. 注册新用户
    logger.step(1, 7, '注册新用户...')
    try {
      const registerResult = await client.auth.register(
        config.testEmail,
        config.testPassword,
        config.testPassword,
        config.testName
      )
      logger.success(`注册成功: ${registerResult.record.email}`)
    } catch (error: any) {
      if (error.message?.includes('已存在') || error.status === 409) {
        logger.warning('用户已存在，跳过注册')
      } else {
        throw error
      }
    }

    // 2. 登录
    logger.step(2, 7, '用户登录...')
    const loginResult = await client.auth.login(config.testEmail, config.testPassword)
    logger.success(`登录成功: UserID=${loginResult.record.id}, Email=${loginResult.record.email}`)
    logger.info(`Token: ${loginResult.token.substring(0, 20)}...`)

    // 3. 获取当前用户信息
    logger.step(3, 7, '获取当前用户信息...')
    const currentUser = await client.auth.getCurrentUser()
    logger.success(`当前用户: ${currentUser.record.email}`)
    logger.info(`用户详情: ${JSON.stringify(currentUser.record, null, 2)}`)

    // 4. 检查认证状态
    logger.step(4, 7, '检查认证状态...')
    const isAuthenticated = client.auth.isAuthenticated()
    logger.success(`认证状态: ${isAuthenticated ? '已认证' : '未认证'}`)

    // 5. 获取同步用户信息
    logger.step(5, 7, '获取同步用户信息...')
    const syncUser = client.auth.getCurrentUserSync()
    if (syncUser) {
      logger.success(`同步用户: ${syncUser.email}`)
    } else {
      logger.warning('未获取到同步用户信息')
    }

    // 6. 更新用户信息
    logger.step(6, 7, '更新用户信息...')
    const updateResult = await safeExecute(async () => {
      const result = await client.auth.updateUser({
        name: `${config.testName} (Updated)`
      })
      return result
    }, '更新用户信息失败')
    
    if (updateResult && updateResult.record) {
      logger.success(`更新成功: ${updateResult.record.name || updateResult.record.email}`)
    }

    // 7. 刷新 Token
    logger.step(7, 7, '刷新访问令牌...')
    try {
      const refreshResult = await client.auth.refreshToken()
      logger.success('Token 刷新成功')
      logger.info(`新 Token: ${refreshResult.token.substring(0, 20)}...`)
    } catch (error: any) {
      logger.warning(`Token 刷新失败: ${error.message}`)
    }

    logger.success('\n✅ 认证 API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ 认证 API 演示失败: ${error.message}`)
    if (config.debug) {
      console.error(error)
    }
    return false
  }
}

// 如果直接运行此文件
if (require.main === module) {
  runAuthDemo()
    .then(success => {
      process.exit(success ? 0 : 1)
    })
    .catch(error => {
      console.error('未处理的错误:', error)
      process.exit(1)
    })
}

