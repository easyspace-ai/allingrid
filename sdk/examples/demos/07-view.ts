import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('View Demo')

export async function runViewDemo(context: DemoContext): Promise<boolean> {
  logger.section('View API 演示')

  const { client } = context

  if (!context.tableId) {
    logger.error('需要先创建 Table')
    return false
  }

  try {
    // 1. 创建视图（Grid）
    logger.step(1, 10, '创建 Grid 视图...')
    const gridView = await safeExecute(async () => {
      return await client.views.create(context.tableId!, {
        name: 'Grid View',
        type: 'grid',
        description: '网格视图'
      })
    }, '创建 Grid 视图失败')

    if (gridView) {
      context.viewIds.push(gridView.id)
      logger.success(`Grid 视图创建成功: ID=${gridView.id}`)
    }

    // 2. 创建视图（Kanban）
    logger.step(2, 10, '创建 Kanban 视图...')
    const kanbanView = await safeExecute(async () => {
      return await client.views.create(context.tableId!, {
        name: 'Kanban View',
        type: 'kanban',
        description: '看板视图'
      })
    }, '创建 Kanban 视图失败')

    if (kanbanView) {
      context.viewIds.push(kanbanView.id)
      logger.success(`Kanban 视图创建成功: ID=${kanbanView.id}`)
    }

    // 3. 获取视图列表
    logger.step(3, 10, '获取视图列表...')
    const viewList = await client.views.getList(context.tableId!, 1, 10)
    logger.success(`获取到 ${viewList.items.length} 个视图`)
    logger.info(`分页信息: 总数=${viewList.pagination.total}`)

    // 4. 获取单个视图
    logger.step(4, 10, '获取单个视图...')
    if (gridView) {
      const view = await client.views.getOne(gridView.id)
      logger.success(`获取成功: ${view.name} (${view.type})`)
      logger.info(`视图详情: ${JSON.stringify(view, null, 2)}`)
    }

    // 5. 更新视图
    logger.step(5, 10, '更新视图...')
    if (gridView) {
      const updatedView = await client.views.update(gridView.id, {
        name: 'Updated Grid View',
        description: '更新后的网格视图'
      })
      logger.success(`更新成功: ${updatedView.name}`)
    }

    // 6. 更新视图配置
    logger.step(6, 10, '更新视图配置...')
    if (gridView) {
      // 更新过滤器
      await safeExecute(async () => {
        await client.views.updateFilter(gridView.id, {
          conjunction: 'and',
          conditions: []
        })
        logger.success('过滤器更新成功')
      }, '更新过滤器失败')

      // 更新排序
      await safeExecute(async () => {
        await client.views.updateSort(gridView.id, [
          { field: 'createdAt', direction: 'desc' }
        ])
        logger.success('排序更新成功')
      }, '更新排序失败')

      // 更新列配置
      await safeExecute(async () => {
        await client.views.updateColumnMeta(gridView.id, [
          { fieldId: 'id', width: 100 },
          { fieldId: 'name', width: 200 }
        ])
        logger.success('列配置更新成功')
      }, '更新列配置失败')
    }

    // 7. 视图分享功能
    logger.step(7, 10, '视图分享功能...')
    if (gridView) {
      // 启用分享
      await safeExecute(async () => {
        const shareResult = await client.views.share(gridView.id, {
          isShared: true
        })
        logger.success(`分享启用成功: ShareID=${shareResult.shareId}`)
      }, '启用分享失败')

      // 刷新分享 ID
      await safeExecute(async () => {
        const refreshResult = await client.views.refreshShareId(gridView.id)
        logger.success(`分享 ID 刷新成功: ${refreshResult.shareId}`)
      }, '刷新分享 ID 失败')

      // 禁用分享
      await safeExecute(async () => {
        await client.views.unshare(gridView.id)
        logger.success('分享已禁用')
      }, '禁用分享失败')
    }

    // 8. 视图锁定/解锁
    logger.step(8, 10, '视图锁定/解锁...')
    if (gridView) {
      await safeExecute(async () => {
        await client.views.lock(gridView.id)
        logger.success('视图锁定成功')
      }, '锁定视图失败')

      await safeExecute(async () => {
        await client.views.unlock(gridView.id)
        logger.success('视图解锁成功')
      }, '解锁视图失败')
    }

    // 9. 复制视图
    logger.step(9, 10, '复制视图...')
    if (gridView) {
      const duplicatedView = await safeExecute(async () => {
        return await client.views.duplicate(gridView.id, 'Copy of Grid View')
      }, '复制视图失败')

      if (duplicatedView) {
        logger.success(`复制成功: ID=${duplicatedView.id}`)
        context.viewIds.push(duplicatedView.id)
      }
    }

    // 10. 删除视图（可选，用于清理）
    logger.step(10, 10, '清理: 删除测试视图...')
    const shouldCleanup = process.env.CLEANUP !== 'false'
    if (shouldCleanup && context.viewIds.length > 0) {
      // 只删除一个测试视图
      const testViewId = context.viewIds[0]
      await safeExecute(async () => {
        await client.views.delete(testViewId!)
        logger.success('测试视图已删除')
      }, '删除视图失败')
      context.viewIds = context.viewIds.slice(1)
    } else {
      logger.info('跳过清理（设置 CLEANUP=false 可保留资源）')
    }

    logger.success('\n✅ View API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ View API 演示失败: ${error.message}`)
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
    
    // 先登录并创建 Table
    client.auth.login(config.testEmail, config.testPassword)
      .then(() => {
        return client.spaces.create({
          name: `Demo Space ${Date.now()}`,
          description: 'View Demo Space'
        })
      })
      .then(space => {
        return client.bases.create(space.id, {
          name: `Demo Base ${Date.now()}`,
          icon: '📊'
        }).then(base => ({ space, base }))
      })
      .then(({ space, base }) => {
        return client.tables.create(base.id, {
          name: `Demo Table ${Date.now()}`,
          description: 'View Demo Table'
        }).then(table => ({ space, base, table }))
      })
      .then(({ space, base, table }) => {
        return runViewDemo({ 
          client, 
          spaceId: space.id,
          baseId: base.id,
          tableId: table.id,
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

