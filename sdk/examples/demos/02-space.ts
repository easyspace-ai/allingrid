import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('Space Demo')

export async function runSpaceDemo(context: DemoContext): Promise<boolean> {
  logger.section('Space API 演示')

  const { client } = context

  try {
    // 1. 创建 Space
    logger.step(1, 8, '创建 Space...')
    const spaceCreateResult = await safeExecute(async () => {
      return await client.spaces.create({
        name: `Demo Space ${Date.now()}`,
        description: '这是一个演示空间'
      })
    }, '创建 Space 失败')

    if (!spaceCreateResult) {
      throw new Error('无法创建 Space')
    }

    context.spaceId = spaceCreateResult.id
    logger.success(`Space 创建成功: ID=${spaceCreateResult.id}, Name=${spaceCreateResult.name}`)

    // 2. 获取 Space 列表
    logger.step(2, 8, '获取 Space 列表...')
    const spaceList = await client.spaces.getList(1, 10)
    logger.success(`获取到 ${spaceList.items.length} 个 Space`)
    logger.info(`分页信息: 总数=${spaceList.pagination.total}, 当前页=${spaceList.pagination.page}`)

    // 3. 获取单个 Space
    logger.step(3, 8, '获取单个 Space...')
    const space = await client.spaces.getOne(context.spaceId!)
    logger.success(`获取成功: ${space.name}`)
    logger.info(`Space 详情: ${JSON.stringify(space, null, 2)}`)

    // 4. 更新 Space
    logger.step(4, 8, '更新 Space...')
    const updatedSpace = await client.spaces.update(context.spaceId!, {
      name: `${space.name} (Updated)`,
      description: '更新后的描述'
    })
    logger.success(`更新成功: ${updatedSpace.name}`)

    // 5. 获取 Space 的 Base 列表
    logger.step(5, 8, '获取 Space 的 Base 列表...')
    const baseList = await client.spaces.getBases(context.spaceId!, 1, 10)
    logger.success(`获取到 ${baseList.items.length} 个 Base`)
    logger.info(`分页信息: 总数=${baseList.pagination.total}`)

    // 6. 在 Space 中创建 Base
    logger.step(6, 8, '在 Space 中创建 Base...')
    const baseCreateResult = await safeExecute(async () => {
      return await client.spaces.createBase(context.spaceId!, {
        name: `Demo Base ${Date.now()}`,
        icon: '📊'
      })
    }, '创建 Base 失败')

    if (baseCreateResult) {
      context.baseId = baseCreateResult.id
      logger.success(`Base 创建成功: ID=${baseCreateResult.id}, Name=${baseCreateResult.name}`)
    }

    // 7. Space 协作者管理（如果服务端支持）
    logger.step(7, 8, '获取 Space 协作者列表...')
    try {
      const collaborators = await client.spaces.getCollaborators(context.spaceId!, 1, 10)
      logger.success(`获取到 ${collaborators.items.length} 个协作者`)
    } catch (error: any) {
      logger.warning(`协作者功能可能未实现: ${error.message}`)
    }

    // 8. 删除 Space（可选，用于清理）
    logger.step(8, 8, '清理: 删除 Space...')
    const shouldCleanup = process.env.CLEANUP !== 'false'
    if (shouldCleanup && context.spaceId) {
      await safeExecute(async () => {
        await client.spaces.delete(context.spaceId!)
        logger.success('Space 已删除')
      }, '删除 Space 失败')
    } else {
      logger.info('跳过清理（设置 CLEANUP=false 可保留资源）')
    }

    logger.success('\n✅ Space API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ Space API 演示失败: ${error.message}`)
    if (config.debug) {
      console.error(error)
    }
    return false
  }
}

// 如果直接运行此文件
if (require.main === module) {
  import('../config').then(({ config }) => {
    const client = new LuckDBClient(config.serverURL)
    
    // 先登录
    client.auth.login(config.testEmail, config.testPassword)
      .then(() => {
        return runSpaceDemo({ client, fieldIds: {}, recordIds: [], viewIds: [] })
      })
      .then(success => {
        process.exit(success ? 0 : 1)
      })
      .catch(error => {
        console.error('未处理的错误:', error)
        process.exit(1)
      })
  })
}

