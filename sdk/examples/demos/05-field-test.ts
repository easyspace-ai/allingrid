import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('Field Demo')

export async function runFieldDemo(context: DemoContext): Promise<boolean> {
  logger.section('Field API 演示')

  const { client } = context

  if (!context.tableId) {
    logger.error('需要先创建 Table')
    return false
  }

  try {
    // 1. 创建各种类型的字段
    logger.step(1, 6, '创建各种类型的字段...')
    
    const fieldTypes = [
      { name: 'Name', type: 'singleLineText', required: true },
      { name: 'Description', type: 'longText' },
      { name: 'Age', type: 'number', options: { precision: 0, min: 0, max: 150 } },
      
    ]

    for (const fieldConfig of fieldTypes) {
      const field = await safeExecute(async () => {
        return await client.fields.create(context.tableId!, fieldConfig)
      }, `创建字段 ${fieldConfig.name} 失败`)

      if (field) {
        context.fieldIds[fieldConfig.name] = field.id
        logger.success(`字段创建成功: ${field.name} (${field.type}), ID: ${field.id}`)
        logger.info(`字段完整信息: ${JSON.stringify(field, null, 2)}`)
      } else {
        logger.warning(`字段创建失败: ${fieldConfig.name}，返回 null`)
      }
    }

    logger.success(`成功创建 ${Object.keys(context.fieldIds).length} 个字段`)

    // 2. 获取字段列表
    logger.step(2, 6, '获取字段列表...')
    const fieldList = await client.fields.getList(context.tableId!, 1, 100)
    logger.success(`获取到 ${fieldList.items.length} 个字段`)
    logger.info(`分页信息: 总数=${fieldList.pagination.total}`)

    // 3. 获取所有字段
    logger.step(3, 6, '获取所有字段...')
    const allFields = await client.fields.getFullList(context.tableId!)
    logger.success(`获取到 ${allFields.length} 个字段（完整列表）`)

    // // 4. 获取单个字段
    // logger.step(4, 6, '获取单个字段...')
    // if (Object.keys(context.fieldIds).length > 0) {
    //   const firstFieldId = Object.values(context.fieldIds)[0]
    //   const field = await safeExecute(async () => {
    //     return await client.fields.getOne(firstFieldId!)
    //   }, '获取单个字段失败')
      
    //   if (field) {
    //     logger.success(`获取成功: ${field.name} (${field.type})`)
    //     logger.info(`字段详情: ${JSON.stringify(field, null, 2)}`)
    //   }
    // }

    // 5. 更新字段
    logger.step(5, 6, '更新字段...')
    if (context.fieldIds['Name']) {
      const fieldId = context.fieldIds['Name']
      logger.info(`尝试更新字段，ID: ${fieldId}`)
      
      // ❌ 关键修复：先等待一小段时间，确保数据库事务完成和缓存更新
      await new Promise(resolve => setTimeout(resolve, 200))
      
      // 先验证字段是否存在
      try {
        const existingField = await client.fields.getOne(fieldId)
        logger.info(`字段存在验证成功: ${existingField.name} (${existingField.id})`)
        
        // ❌ 关键修复：GetField 后等待一小段时间，确保缓存更新
        await new Promise(resolve => setTimeout(resolve, 100))
      } catch (error: any) {
        logger.error(`字段不存在或无法访问: ${error.message}`)
        logger.error(`尝试使用的字段ID: ${fieldId}`)
        logger.error(`所有已创建的字段ID: ${JSON.stringify(context.fieldIds, null, 2)}`)
        if (error.status) {
          logger.error(`HTTP状态码: ${error.status}`)
        }
      }
      
      try {
        const updatedField = await client.fields.update(fieldId, {
          name: 'Full Name',
          description: '完整姓名'
        })
        logger.success(`更新成功: ${updatedField.name}, Description: ${updatedField.description || '(空)'}`)
        logger.info(`更新后的字段信息: ${JSON.stringify(updatedField, null, 2)}`)
      } catch (error: any) {
        logger.error(`更新字段失败: ${error.message}`)
        if (error.status) {
          logger.error(`HTTP状态码: ${error.status}`)
        }
        if (error.details) {
          logger.error(`错误详情: ${JSON.stringify(error.details, null, 2)}`)
        }
        logger.error(`尝试使用的字段ID: ${fieldId}`)
        throw error
      }
    } else {
      logger.warning('Name 字段ID不存在，跳过更新')
    }

    // // 6. 删除字段（可选，用于清理）
    // logger.step(6, 6, '清理: 删除测试字段...')
    // const shouldCleanup = process.env.CLEANUP !== 'false'
    // if (shouldCleanup) {
    //   // 只删除一个测试字段
    //   const testFieldId = context.fieldIds['Is Active']
    //   if (testFieldId) {
    //     await safeExecute(async () => {
    //       await client.fields.delete(testFieldId)
    //       logger.success('测试字段已删除')
    //     }, '删除字段失败')
    //   }
    // } else {
    //   logger.info('跳过清理（设置 CLEANUP=false 可保留资源）')
    // }

    logger.success('\n✅ Field API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ Field API 演示失败: ${error.message}`)
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
    
    // 先登录并创建 Space、Base 和 Table
    client.auth.login(config.testEmail, config.testPassword)
      .then(() => {
        return client.spaces.create({
          name: `Demo Space ${Date.now()}`,
          description: 'Field Demo Space'
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
          description: 'Field Demo Table'
        }).then(table => ({ space, base, table }))
      })
      .then(({ space, base, table }) => {
        return runFieldDemo({ 
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

