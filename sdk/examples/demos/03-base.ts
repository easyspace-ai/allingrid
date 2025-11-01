import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('Base Demo')

export async function runBaseDemo(context: DemoContext): Promise<boolean> {
  logger.section('Base API 演示')

  const { client } = context

  if (!context.spaceId) {
    logger.error('需要先创建 Space')
    return false
  }

  try {
    // 1. 创建 Base
    logger.step(1, 8, '创建 Base...')
    const baseCreateResult = await safeExecute(async () => {
      return await client.bases.create(context.spaceId!, {
        name: `Demo Base ${Date.now()}`,
        icon: '📊'
      })
    }, '创建 Base 失败')

    if (!baseCreateResult) {
      throw new Error('无法创建 Base')
    }

    context.baseId = baseCreateResult.id
    logger.success(`Base 创建成功: ID=${baseCreateResult.id}, Name=${baseCreateResult.name}`)

    // 2. 获取 Base 列表（Space 下）
    logger.step(2, 8, '获取 Base 列表（Space 下）...')
    const baseListInSpace = await client.bases.getList(context.spaceId, 1, 10)
    logger.success(`获取到 ${baseListInSpace.items.length} 个 Base`)
    logger.info(`分页信息: 总数=${baseListInSpace.pagination.total}`)

    // 3. 获取 Base 列表（全局）
    logger.step(3, 8, '获取 Base 列表（全局）...')
    const baseListGlobal = await safeExecute(async () => {
      return await client.bases.getList(undefined, 1, 10)
    }, '获取全局 Base 列表失败')

    if (baseListGlobal) {
      logger.success(`获取到 ${baseListGlobal.items.length} 个 Base（全局）`)
    } else {
      logger.warning('全局 Base 列表 API 可能未实现')
    }

    // 4. 获取单个 Base
    logger.step(4, 8, '获取单个 Base...')
    const base = await client.bases.getOne(context.baseId!)
    logger.success(`获取成功: ${base.name}`)
    logger.info(`Base 详情: ${JSON.stringify(base, null, 2)}`)

    // 5. 更新 Base
    logger.step(5, 8, '更新 Base...')
    const updatedBase = await client.bases.update(context.baseId!, {
      name: `${base.name} (Updated)`,
      icon: '📈'
    })
    logger.success(`更新成功: ${updatedBase.name}`)

    // 6. 复制 Base
    logger.step(6, 8, '复制 Base...')
    const duplicatedBase = await safeExecute(async () => {
      return await client.bases.duplicate(context.baseId!, `Copy of ${updatedBase.name}`)
    }, '复制 Base 失败')

    if (duplicatedBase) {
      logger.success(`复制成功: ID=${duplicatedBase.id}, Name=${duplicatedBase.name}`)
    }

    // 7. 获取 Base 权限
    logger.step(7, 8, '获取 Base 权限...')
    try {
      const permission = await client.bases.getPermission(context.baseId!)
      logger.success('权限获取成功')
      logger.info(`权限详情: ${JSON.stringify(permission, null, 2)}`)
    } catch (error: any) {
      logger.warning(`权限功能可能未实现: ${error.message}`)
    }

    // 8. Base 协作者管理
    logger.step(8, 8, '获取 Base 协作者列表...')
    try {
      const collaborators = await client.bases.getCollaborators(context.baseId!, 1, 10)
      logger.success(`获取到 ${collaborators.items.length} 个协作者`)
    } catch (error: any) {
      logger.warning(`协作者功能可能未实现: ${error.message}`)
    }

    logger.success('\n✅ Base API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ Base API 演示失败: ${error.message}`)
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
    
    // 先登录并创建 Space
    client.auth.login(config.testEmail, config.testPassword)
      .then(() => {
        return client.spaces.create({
          name: `Demo Space ${Date.now()}`,
          description: 'Base Demo Space'
        })
      })
      .then(space => {
        return runBaseDemo({ 
          client, 
          spaceId: space.id,
          fieldIds: {}, 
          recordIds: [], 
          viewIds: [] 
        })
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

