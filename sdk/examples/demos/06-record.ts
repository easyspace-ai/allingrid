import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('Record Demo')

export async function runRecordDemo(context: DemoContext): Promise<boolean> {
  logger.section('Record API 演示')

  const { client } = context

  if (!context.tableId) {
    logger.error('需要先创建 Table')
    return false
  }

  if (Object.keys(context.fieldIds).length === 0) {
    logger.error('需要先创建 Field')
    return false
  }

  try {
    // 1. 创建记录
    logger.step(1, 9, '创建记录...')
    
    // 如果没有字段，先获取 Table 的字段列表
    if (Object.keys(context.fieldIds).length === 0) {
      logger.info('未找到字段，先获取 Table 的字段列表...')
      try {
        // 等待一下，确保字段创建已同步
        await sleep(500)
        const fieldList = await client.fields.getFullList(context.tableId!)
        if (fieldList.length > 0) {
          // 使用第一个字段
          const firstField = fieldList[0]
          context.fieldIds[firstField.name] = firstField.id
          logger.success(`使用现有字段: ${firstField.name} (${firstField.id})`)
        } else {
          // 如果没有字段，创建一个基本字段
          logger.info('Table 没有字段，创建一个基本字段...')
          const nameField = await safeExecute(async () => {
            return await client.fields.create(context.tableId!, {
              name: 'Name',
              type: 'singleLineText',
              required: false
            })
          }, '创建字段失败')
          
          if (nameField) {
            context.fieldIds['Name'] = nameField.id
            logger.success(`创建字段成功: ${nameField.id}`)
            // 等待字段同步
            await sleep(500)
          } else {
            throw new Error('无法创建必要的字段')
          }
        }
      } catch (error: any) {
        logger.error(`获取字段失败: ${error.message}`)
        throw error
      }
    }
    
    const recordCreateResult = await safeExecute(async () => {
      // 构建字段数据 - 至少需要一个字段
      const fields: Record<string, any> = {}
      
      // 使用现有字段 ID 填充数据
      if (context.fieldIds['Name']) {
        fields[context.fieldIds['Name']] = 'Demo Record 1'
      }
      if (context.fieldIds['Description']) {
        fields[context.fieldIds['Description']] = '这是一个演示记录'
      }
      if (context.fieldIds['Age']) {
        fields[context.fieldIds['Age']] = 25
      }
      if (context.fieldIds['Status']) {
        fields[context.fieldIds['Status']] = 'active'
      }
      if (context.fieldIds['Is Active']) {
        fields[context.fieldIds['Is Active']] = true
      }
      
      // 确保至少有一个字段
      if (Object.keys(fields).length === 0) {
        throw new Error('无法创建记录：没有可用的字段')
      }
      
      logger.info(`使用字段创建记录: ${JSON.stringify(Object.keys(fields))}`)

      return await client.records.create(context.tableId!, {
        data: fields
      })
    }, '创建记录失败')

    if (!recordCreateResult) {
      throw new Error('无法创建记录')
    }

    context.recordIds.push(recordCreateResult.id)
    logger.success(`记录创建成功: ID=${recordCreateResult.id}`)
    logger.info(`记录数据: ${JSON.stringify(recordCreateResult.data || recordCreateResult.fields, null, 2)}`)

    // 2. 获取记录列表
    logger.step(2, 9, '获取记录列表...')
    const recordList = await client.records.getList(context.tableId!, 1, 10)
    logger.success(`获取到 ${recordList.items.length} 条记录`)
    logger.info(`分页信息: 总数=${recordList.pagination.total}, 当前页=${recordList.pagination.page}`)

    // 3. 获取所有记录
    logger.step(3, 9, '获取所有记录...')
    const allRecords = await client.records.getFullList(context.tableId!)
    logger.success(`获取到 ${allRecords.length} 条记录（完整列表）`)

    // 4. 获取单个记录（新 API）
    logger.step(4, 9, '获取单个记录（新 API）...')
    const record = await client.records.getOne(context.tableId!, recordCreateResult.id)
    logger.success(`获取成功: ID=${record.id}`)
    logger.info(`记录详情: ${JSON.stringify(record.data || record.fields, null, 2)}`)

    // 5. 更新记录（支持乐观锁）
    logger.step(5, 9, '更新记录（支持乐观锁）...')
    const updatedRecord = await client.records.update(context.tableId!, recordCreateResult.id, {
      data: {
        [context.fieldIds['Name']]: 'Updated Demo Record 1'
      },
      version: record.version
    })
    logger.success(`更新成功: ID=${updatedRecord.id}`)
    logger.info(`更新后数据: ${JSON.stringify(updatedRecord.data, null, 2)}`)

    // 6. 批量创建记录
    logger.step(6, 9, '批量创建记录...')
    const batchCreateResult = await safeExecute(async () => {
      const fields: Record<string, any> = {}
      if (context.fieldIds['Name']) {
        fields[context.fieldIds['Name']] = 'Batch Record'
      }
      if (context.fieldIds['Age']) {
        fields[context.fieldIds['Age']] = 30
      }

      return await client.records.batchCreate(context.tableId!, {
        records: [
          { fields },
          { fields: { ...fields, [context.fieldIds['Name']]: 'Batch Record 2' } }
        ]
      })
    }, '批量创建记录失败')

    if (batchCreateResult) {
      logger.success(`批量创建成功: ${batchCreateResult.successCount} 条成功, ${batchCreateResult.failedCount} 条失败`)
      batchCreateResult.records.forEach(r => context.recordIds.push(r.id))
    }

    // 7. 批量更新记录
    logger.step(7, 9, '批量更新记录...')
    if (context.recordIds.length >= 2) {
      const batchUpdateResult = await safeExecute(async () => {
        return await client.records.batchUpdate(context.tableId!, {
          records: [
            {
              id: context.recordIds[0],
              fields: {
                [context.fieldIds['Name']]: 'Batch Updated Record 1'
              }
            },
            {
              id: context.recordIds[1],
              fields: {
                [context.fieldIds['Name']]: 'Batch Updated Record 2'
              }
            }
          ]
        })
      }, '批量更新记录失败')

      if (batchUpdateResult) {
        logger.success(`批量更新成功: ${batchUpdateResult.successCount} 条成功`)
      }
    }

    // 8. 批量删除记录
    logger.step(8, 9, '批量删除记录...')
    const shouldCleanup = process.env.CLEANUP !== 'false'
    if (shouldCleanup && context.recordIds.length >= 2) {
      const batchDeleteResult = await safeExecute(async () => {
        return await client.records.batchDelete(context.tableId!, {
          recordIds: context.recordIds.slice(0, 2) // 删除前两条
        })
      }, '批量删除记录失败')

      if (batchDeleteResult) {
        logger.success(`批量删除成功: ${batchDeleteResult.successCount} 条成功`)
        context.recordIds = context.recordIds.slice(2)
      }
    } else {
      logger.info('跳过批量删除（设置 CLEANUP=false 可保留资源）')
    }

    // 9. 搜索记录（如果服务端支持）
    logger.step(9, 9, '搜索记录...')
    try {
      const searchResult = await client.records.search(context.tableId!, 'Demo', undefined, 1, 10)
      logger.success(`搜索成功: 找到 ${searchResult.items.length} 条记录`)
    } catch (error: any) {
      logger.warning(`搜索功能可能未实现: ${error.message}`)
    }

    logger.success('\n✅ Record API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ Record API 演示失败: ${error.message}`)
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
    
    // 先登录并创建完整的资源链
    client.auth.login(config.testEmail, config.testPassword)
      .then(() => {
        return client.spaces.create({
          name: `Demo Space ${Date.now()}`,
          description: 'Record Demo Space'
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
          description: 'Record Demo Table'
        }).then(table => ({ space, base, table }))
      })
      .then(({ space, base, table }) => {
        // 创建字段
        return Promise.all([
          client.fields.create(table.id, { name: 'Name', type: 'singleLineText', required: true }),
          client.fields.create(table.id, { name: 'Description', type: 'longText' }),
          client.fields.create(table.id, { name: 'Age', type: 'number' })
        ]).then(fields => {
          const fieldIds: Record<string, string> = {}
          fields.forEach(f => {
            if (f.name === 'Name') fieldIds['Name'] = f.id
            if (f.name === 'Description') fieldIds['Description'] = f.id
            if (f.name === 'Age') fieldIds['Age'] = f.id
          })
          return { space, base, table, fieldIds }
        })
      })
      .then(({ space, base, table, fieldIds }) => {
        return runRecordDemo({ 
          client, 
          spaceId: space.id,
          baseId: base.id,
          tableId: table.id,
          fieldIds, 
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

