import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('Table Demo')

export async function runTableDemo(context: DemoContext): Promise<boolean> {
  logger.section('Table API 演示')

  const { client } = context

  if (!context.baseId) {
    logger.error('需要先创建 Base')
    return false
  }

  try {
    // 1. 创建 Table（带字段和视图配置）
    logger.step(1, 8, '创建 Table（带字段和视图配置）...')
    const tableCreateResult = await safeExecute(async () => {
      return await client.tables.create(context.baseId!, {
        name: `Demo Table ${Date.now()}`,
        description: '这是一个演示表格',
        fields: [
          {
            name: 'Name',
            type: 'singleLineText',
            required: true
          },
          {
            name: 'Status',
            type: 'singleSelect',
            options: {
              choices: [
                { id: 'todo', name: '待办', color: '#ff0000' },
                { id: 'doing', name: '进行中', color: '#00ff00' },
                { id: 'done', name: '已完成', color: '#0000ff' }
              ]
            }
          }
        ],
        views: [
          {
            name: 'Grid View',
            type: 'grid'
          }
        ]
      })
    }, '创建 Table 失败')

    if (!tableCreateResult) {
      throw new Error('无法创建 Table')
    }

    context.tableId = tableCreateResult.id
    logger.success(`Table 创建成功: ID=${tableCreateResult.id}, Name=${tableCreateResult.name}`)

    // 2. 获取 Table 列表
    logger.step(2, 8, '获取 Table 列表...')
    const tableList = await client.tables.getList(context.baseId!, 1, 10)
    logger.success(`获取到 ${tableList.items.length} 个 Table`)
    logger.info(`分页信息: 总数=${tableList.pagination.total}`)

    // 3. 获取单个 Table
    logger.step(3, 8, '获取单个 Table...')
    const table = await client.tables.getOne(context.tableId!)
    logger.success(`获取成功: ${table.name}`)
    logger.info(`Table 详情: ${JSON.stringify(table, null, 2)}`)

    // 4. 更新 Table
    logger.step(4, 8, '更新 Table...')
    const updatedTable = await client.tables.update(context.tableId!, {
      name: `${table.name} (Updated)`,
      description: '更新后的描述'
    })
    logger.success(`更新成功: ${updatedTable.name}`)

    // 5. 重命名 Table
    logger.step(5, 8, '重命名 Table...')
    const renamedTable = await client.tables.rename(context.tableId!, {
      name: `Renamed ${updatedTable.name}`
    })
    logger.success(`重命名成功: ${renamedTable.name}`)

    // 6. 复制 Table
    logger.step(6, 8, '复制 Table...')
    const duplicatedTable = await safeExecute(async () => {
      return await client.tables.duplicate(context.tableId!, {
        name: `Copy of ${renamedTable.name}`,
        withData: true,
        withViews: true,
        withFields: true
      })
    }, '复制 Table 失败')

    if (duplicatedTable) {
      logger.success(`复制成功: ID=${duplicatedTable.id}, Name=${duplicatedTable.name}`)
    }

    // 7. 获取 Table 使用情况
    logger.step(7, 8, '获取 Table 使用情况...')
    try {
      const usage = await client.tables.getUsage(context.tableId!)
      logger.success('使用情况获取成功')
      logger.info(`使用情况: ${JSON.stringify(usage, null, 2)}`)
    } catch (error: any) {
      logger.warning(`使用情况功能可能未实现: ${error.message}`)
    }

    // 8. 获取 Table 管理菜单
    logger.step(8, 8, '获取 Table 管理菜单...')
    try {
      const menu = await client.tables.getManagementMenu(context.tableId!)
      logger.success('管理菜单获取成功')
      logger.info(`管理菜单: ${JSON.stringify(menu, null, 2)}`)
    } catch (error: any) {
      logger.warning(`管理菜单功能可能未实现: ${error.message}`)
    }

    logger.success('\n✅ Table API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ Table API 演示失败: ${error.message}`)
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
    
    // 先登录并创建 Space 和 Base
    client.auth.login(config.testEmail, config.testPassword)
      .then(() => {
        return client.spaces.create({
          name: `Demo Space ${Date.now()}`,
          description: 'Table Demo Space'
        })
      })
      .then(space => {
        return client.bases.create(space.id, {
          name: `Demo Base ${Date.now()}`,
          icon: '📊'
        }).then(base => ({ space, base }))
      })
      .then(({ space, base }) => {
        return runTableDemo({ 
          client, 
          spaceId: space.id,
          baseId: base.id,
          tableId: undefined,
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

