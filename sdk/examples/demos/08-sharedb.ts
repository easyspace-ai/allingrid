import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { sleep, safeExecute } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('ShareDB Demo')

// 从 .env 读取数据的测试方式（重点测试实时同步）
async function runShareDBDemoFromEnv(): Promise<boolean> {
  logger.section('ShareDB 实时同步测试')
  
  // 从 .env 读取测试数据
  if (!config.sharedbTestTableId || !config.sharedbTestFieldId || !config.sharedbTestRecordId || !config.sharedbTestCollection) {
    logger.error('缺少 ShareDB 测试数据，请先运行: npm run setup:data')
    logger.info('提示: 运行 npm run setup:data 创建测试数据并保存到 .env 文件')
    return false
  }
  
  const tableId = config.sharedbTestTableId
  const fieldId = config.sharedbTestFieldId
  const recordId = config.sharedbTestRecordId
  const collection = config.sharedbTestCollection
  
  logger.info('使用测试数据:')
  logger.info(`  Table ID: ${tableId}`)
  logger.info(`  Field ID: ${fieldId}`)
  logger.info(`  Record ID: ${recordId}`)
  logger.info(`  Collection: ${collection}`)
  
  // 创建客户端1
  const client1 = new LuckDBClient(config.serverURL)
  
  try {
    // 1. 客户端1登录并连接
    logger.step(1, 5, '客户端1登录并连接...')
    await client1.auth.login(config.testEmail, config.testPassword)
    logger.success('客户端1登录成功')
    
    await client1.sharedb.initialize()
    await client1.sharedb.connect()
    logger.success('客户端1 WebSocket 连接成功')
    
    await sleep(500)
    
    // 2. 客户端1订阅文档
    logger.step(2, 5, '客户端1订阅文档...')
    const doc1 = client1.sharedb.getDocument(collection, recordId)
    
    let doc1Loaded = false
    doc1.on('load', ({ data }) => {
      doc1Loaded = true
      logger.info(`客户端1文档加载完成: ${JSON.stringify(data, null, 2)}`)
      if (data && typeof data === 'object' && data.data && typeof data.data === 'object') {
        const fieldCount = Object.keys(data.data).length
        if (fieldCount > 0) {
          logger.success(`✅ 客户端1收到完整数据，包含 ${fieldCount} 个字段`)
        } else {
          logger.warning('⚠️ 客户端1文档数据为空')
        }
      }
    })
    
    doc1.on('op', ({ op, source, data }) => {
      logger.success(`客户端1收到操作: ${JSON.stringify(op)}`)
      logger.info(`客户端1新数据: ${JSON.stringify(data, null, 2)}`)
    })
    
    doc1.on('error', (err) => {
      logger.error(`客户端1文档错误: ${err.message}`)
    })
    
    await doc1.subscribe()
    logger.success('客户端1订阅成功')
    
    // 等待文档加载
    await sleep(2000)
    
    if (!doc1Loaded) {
      logger.warning('⚠️ 客户端1文档未加载')
    }
    
    // 3. 客户端2连接并订阅
    logger.step(3, 5, '客户端2连接并订阅...')
    const client2 = new LuckDBClient(config.serverURL)
    await client2.auth.login(config.testEmail, config.testPassword)
    await client2.sharedb.initialize()
    await client2.sharedb.connect()
    logger.success('客户端2 WebSocket 连接成功')
    
    await sleep(500)
    
    const doc2 = client2.sharedb.getDocument(collection, recordId)
    
    let doc2Loaded = false
    let receivedOp = false
    let receivedValue: string | null = null
    
    doc2.on('load', ({ data }) => {
      doc2Loaded = true
      logger.info(`客户端2文档加载完成: ${JSON.stringify(data, null, 2)}`)
      if (data && typeof data === 'object' && data.data && typeof data.data === 'object') {
        const fieldCount = Object.keys(data.data).length
        if (fieldCount > 0) {
          logger.success(`✅ 客户端2收到完整数据，包含 ${fieldCount} 个字段`)
          logger.info(`当前字段值: ${JSON.stringify(data.data, null, 2)}`)
        } else {
          logger.warning('⚠️ 客户端2文档数据为空')
        }
      }
    })
    
    doc2.on('op', ({ op, source, data }) => {
      receivedOp = true
      logger.success(`✅ 客户端2收到操作: ${JSON.stringify(op)}`)
      logger.info(`客户端2新数据: ${JSON.stringify(data, null, 2)}`)
      
      // 提取字段值
      if (data && typeof data === 'object' && data.data && typeof data.data === 'object' && data.data[fieldId]) {
        receivedValue = String(data.data[fieldId])
      }
    })
    
    doc2.on('error', (err) => {
      logger.error(`客户端2文档错误: ${err.message}`)
    })
    
    await doc2.subscribe()
    logger.success('客户端2订阅成功')
    
    // 等待客户端2文档加载
    logger.info('等待客户端2文档加载...')
    await sleep(3000)
    
    if (!doc2Loaded) {
      logger.warning('⚠️ 客户端2文档未加载')
    }
    
    // 4. 客户端1修改字段值
    logger.step(4, 5, '客户端1修改字段值...')
    const newValue = `Updated at ${Date.now()} - ${new Date().toISOString()}`
    logger.info(`准备更新字段值: ${newValue}`)
    
    await doc1.submitOp([
      { p: ['data', fieldId], oi: newValue }
    ])
    
    logger.success(`客户端1已提交操作: ${newValue}`)
    
    // 5. 验证客户端2收到更新
    logger.step(5, 5, '验证实时同步...')
    logger.info('等待客户端2接收操作（5秒）...')
    await sleep(5000)
    
    if (receivedOp) {
      logger.success('✅ 实时同步测试成功！')
      logger.success(`✅ 客户端2收到了客户端1的操作`)
      if (receivedValue && receivedValue === newValue) {
        logger.success(`✅ 字段值验证成功: "${receivedValue}"`)
        logger.success('\n🎉 ShareDB 实时同步功能验证通过！')
      } else {
        logger.warning(`⚠️ 字段值不匹配: 期望 "${newValue}", 实际 "${receivedValue}"`)
      }
    } else {
      logger.error('❌ 实时同步测试失败：客户端2未收到操作')
      logger.info('可能原因：')
      logger.info('  1. 服务端 PubSub 未正确配置')
      logger.info('  2. 客户端2订阅未完成')
      logger.info('  3. 操作格式或 channel 名称不匹配')
      logger.info('  4. 网络延迟导致操作未及时到达')
      logger.info(`调试信息: 客户端2订阅=${doc2.isSubscribed}, 连接=${client2.sharedb.isConnected}`)
      return false
    }
    
    // 清理资源
    logger.info('清理资源...')
    doc1.destroy()
    doc2.destroy()
    client2.sharedb.disconnect()
    client1.sharedb.disconnect()
    logger.success('资源清理完成')
    
    return true
    
  } catch (error: any) {
    logger.error(`❌ ShareDB 测试失败: ${error.message}`)
    if (config.debug) {
      console.error(error)
    }
    return false
  } finally {
    // 确保断开连接
    if (client1.sharedb.isConnected) {
      client1.sharedb.disconnect()
    }
  }
}

// 旧版本实现（兼容 runner.ts）
async function runShareDBDemoOld(context: DemoContext): Promise<boolean> {
  logger.section('ShareDB WebSocket API 演示')

  const { client } = context

  if (!context.tableId) {
    logger.error('需要先创建 Table')
    return false
  }

  if (context.recordIds.length === 0) {
    // 如果没有记录，尝试创建一个
    logger.info('未找到记录，先创建一个记录...')
    
    // 确保有字段 - 先尝试获取 Table 的字段
    if (Object.keys(context.fieldIds).length === 0) {
      logger.info('未找到字段，先获取 Table 的字段列表...')
      try {
        // 等待一下，确保字段创建已同步
        await sleep(500)
        const fieldList = await client.fields.getFullList(context.tableId!)
        if (fieldList.length > 0) {
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
            logger.success(`字段创建成功: ${nameField.id}`)
            // 等待字段同步
            await sleep(500)
          } else {
            logger.error('无法创建字段')
            return false
          }
        }
      } catch (error: any) {
        logger.error(`获取字段失败: ${error.message}`)
        return false
      }
    }
    
    if (Object.keys(context.fieldIds).length > 0) {
      const record = await safeExecute(async () => {
        return await client.records.create(context.tableId!, {
          data: {
            [Object.values(context.fieldIds)[0]]: 'ShareDB Test Record'
          }
        })
      }, '创建记录失败')
      
      if (record) {
        context.recordIds.push(record.id)
        logger.success(`记录创建成功: ${record.id}`)
      } else {
        logger.error('无法创建记录')
        return false
      }
    } else {
      logger.error('无法创建记录：缺少字段')
      return false
    }
  }

  try {
    // 1. 初始化 ShareDB 连接
    logger.step(1, 6, '初始化 ShareDB 连接...')
    await client.sharedb.initialize()
    logger.success('ShareDB 初始化成功')

    // 2. 连接 WebSocket
    logger.step(2, 6, '连接 WebSocket...')
    await client.sharedb.connect()
    logger.success('WebSocket 连接成功')
    logger.info(`连接状态: ${client.sharedb.connectionStatus}`)

    // 3. 获取文档并订阅
    logger.step(3, 6, '获取文档并订阅...')
    let recordId = context.recordIds[0] // 使用 let，以便在需要时更新
    const collection = `rec_${context.tableId}`
    
    // 如果没有记录ID，先创建一个记录
    if (!recordId) {
      logger.info('没有现有记录，先创建记录...')
      if (Object.keys(context.fieldIds).length > 0) {
        const firstFieldId = Object.values(context.fieldIds)[0]
        const record = await client.records.create(context.tableId!, {
          data: {
            [firstFieldId]: 'ShareDB Demo Record - Initial Data'
          }
        })
        recordId = record.id
        context.recordIds.push(recordId)
        logger.success(`记录创建成功: ${recordId}`)
        // 等待记录保存
        await sleep(1000)
      } else {
        logger.error('没有字段，无法创建记录')
        return false
      }
    }
    
    const doc = client.sharedb.getDocument(collection, recordId)
    
    logger.info(`文档: collection=${collection}, id=${recordId}`)

    // 监听文档事件
    doc.on('load', ({ data }) => {
      logger.info(`文档加载完成: ${JSON.stringify(data, null, 2)}`)
    })

    doc.on('op', ({ op, source, data }) => {
      logger.success(`收到操作: ${JSON.stringify(op)}`)
      logger.info(`新数据: ${JSON.stringify(data, null, 2)}`)
    })

    doc.on('error', (err) => {
      logger.error(`文档错误: ${err.message}`)
    })

    // 订阅文档
    await doc.subscribe()
    logger.success('文档订阅成功')
    
    // 等待文档加载
    await sleep(1000)
    
    if (doc.data) {
      logger.info(`文档数据: ${JSON.stringify(doc.data, null, 2)}`)
      logger.info(`文档版本: ${doc.version}`)
    } else {
      logger.warning('文档数据为空，可能需要先创建记录')
    }

    // 4. 客户端1创建初始数据（使用 ShareDB 操作确保数据可用）
    logger.step(4, 6, '客户端1创建初始数据...')
    if (Object.keys(context.fieldIds).length > 0) {
      const firstFieldId = Object.values(context.fieldIds)[0]
      const initialValue = `Initial data created by client1 at ${new Date().toISOString()}`
      
      // 使用 ShareDB 操作创建数据，确保客户端2订阅时能收到
      logger.info('通过 ShareDB 操作创建初始数据...')
      await doc.submitOp([
        { p: ['data', firstFieldId], oi: initialValue }
      ])
      logger.success(`初始数据创建成功（ShareDB）: 字段 ${firstFieldId} = ${initialValue}`)
      
      // 等待操作提交完成
      await sleep(2000)
      
      // 验证文档数据已更新
      if (doc.data && doc.data.data && doc.data.data[firstFieldId]) {
        logger.success(`✅ 文档数据验证成功: 包含字段数据`)
      } else {
        logger.warning('⚠️ 文档数据验证失败，但继续测试')
      }
    }

    // 5. 多客户端实时同步测试
    logger.step(5, 6, '多客户端实时同步测试...')
    
    // 在客户端2订阅之前，先验证数据已存在
    logger.info('验证数据已创建并同步...')
    if (Object.keys(context.fieldIds).length > 0) {
      const firstFieldId = Object.values(context.fieldIds)[0]
      let dataReady = false
      
      // 最多重试5次，每次等待1秒
      for (let i = 0; i < 5; i++) {
        try {
          const verifyRecord = await client.records.getOne(context.tableId!, recordId)
          if (verifyRecord && verifyRecord.data && verifyRecord.data[firstFieldId]) {
            logger.success(`✅ 数据验证成功: 记录包含字段数据（尝试 ${i + 1}/5）`)
            dataReady = true
            break
          } else {
            logger.info(`数据验证中...（尝试 ${i + 1}/5）`)
          }
        } catch (error: any) {
          logger.warning(`数据验证失败（尝试 ${i + 1}/5）: ${error.message}`)
        }
        
        if (i < 4) {
          await sleep(1000)
        }
      }
      
      if (!dataReady) {
        logger.warning('⚠️ 数据验证失败，但继续测试实时同步功能')
      }
    }
    
    logger.info('创建第二个客户端连接...')
    
    const client2 = new LuckDBClient(config.serverURL)
    
    // 使用相同 token 登录（或创建新用户）
    await client2.auth.login(config.testEmail, config.testPassword)
    await client2.sharedb.initialize()
    await client2.sharedb.connect()
    
    // 等待连接建立
    await sleep(500)
    
    logger.info(`客户端2连接状态: ${JSON.stringify(client2.sharedb.connectionStatus)}`)
    
    const doc2 = client2.sharedb.getDocument(collection, recordId)
    
    logger.info(`客户端2文档: collection=${collection}, id=${recordId}`)
    
    let receivedOp = false
    let doc2Loaded = false
    
    // 监听文档加载事件
    doc2.on('load', ({ data }) => {
      doc2Loaded = true
      logger.info(`客户端2文档加载完成: ${JSON.stringify(data, null, 2)}`)
      // 检查 data.data 是否有字段数据（服务端返回格式：{ "data": { "fieldId": "value" } }）
      if (data && typeof data === 'object' && data.data && typeof data.data === 'object') {
        const fieldCount = Object.keys(data.data).length
        if (fieldCount > 0) {
          logger.success(`✅ 客户端2收到完整数据，包含 ${fieldCount} 个字段`)
        } else {
          logger.warning('⚠️ 客户端2文档数据为空（记录存在但无字段数据）')
        }
      } else {
        logger.warning('⚠️ 客户端2文档数据格式不正确')
      }
    })
    
    // 监听操作事件
    doc2.on('op', ({ op, source, data }) => {
      receivedOp = true
      logger.success(`✅ 客户端2收到操作: ${JSON.stringify(op)}`)
      logger.info(`客户端2新数据: ${JSON.stringify(data, null, 2)}`)
    })
    
    doc2.on('error', (err) => {
      logger.error(`客户端2文档错误: ${err.message}`)
    })
    
    // 订阅文档
    logger.info('客户端2开始订阅文档...')
    await doc2.subscribe()
    logger.info('客户端2已订阅文档')
    
    // 等待文档加载和订阅完成
    logger.info('等待文档加载和订阅完成（3秒）...')
    await sleep(3000)
    
    if (!doc2Loaded) {
      logger.warning('⚠️ 客户端2文档未加载（可能文档为空）')
    }
    
    logger.info(`客户端2订阅状态: ${doc2.isSubscribed}`)
    logger.info(`客户端2连接状态: ${client2.sharedb.isConnected}`)
    
    // 从客户端1提交操作
    if (Object.keys(context.fieldIds).length > 0) {
      const firstFieldId = Object.values(context.fieldIds)[0]
      const syncValue = `Sync Test at ${Date.now()}`
      
      logger.info(`客户端1准备提交操作: ${syncValue}`)
      
      await doc.submitOp([
        { p: ['data', firstFieldId], oi: syncValue }
      ])
      logger.info(`客户端1操作已提交: ${syncValue}`)
      
      // 等待客户端2接收（增加等待时间）
      logger.info('等待客户端2接收操作（5秒）...')
      await sleep(5000)
      
      if (receivedOp) {
        logger.success('✅ 实时同步测试成功：客户端2收到了客户端1的操作')
      } else {
        logger.warning('⚠️ 实时同步测试：客户端2未收到操作')
        logger.info('可能原因：')
        logger.info('  1. 服务端 PubSub 未正确配置（Redis 或本地 PubSub）')
        logger.info('  2. 客户端2订阅未完成')
        logger.info('  3. 操作格式或 channel 名称不匹配')
        logger.info('  4. 网络延迟导致操作未及时到达')
        logger.info('  5. 服务端订阅处理器未正确调用')
        logger.info(`调试信息: 客户端2订阅=${doc2.isSubscribed}, 连接=${client2.sharedb.isConnected}`)
      }
    }

    // 6. 清理资源
    logger.step(6, 6, '清理资源...')
    doc.destroy()
    doc2.destroy()
    client2.sharedb.disconnect()
    logger.success('文档和连接已清理')

    logger.success('\n✅ ShareDB WebSocket API 演示完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ ShareDB WebSocket API 演示失败: ${error.message}`)
    if (config.debug) {
      console.error(error)
    }
    return false
  } finally {
    // 确保断开连接
    if (client.sharedb.isConnected) {
      client.sharedb.disconnect()
    }
  }
}

// 主入口函数（兼容 runner.ts）
export async function runShareDBDemo(context: DemoContext): Promise<boolean> {
  // 如果从 .env 读取到了数据，使用新的测试方式
  if (config.sharedbTestTableId && config.sharedbTestFieldId && config.sharedbTestRecordId && config.sharedbTestCollection) {
    return runShareDBDemoFromEnv()
  }
  
  // 否则使用旧的测试方式
  logger.warning('未找到 .env 测试数据，使用旧的测试方式')
  logger.warning('建议运行: npm run setup:data 创建测试数据')
  return runShareDBDemoOld(context)
}

// 如果直接运行此文件
if (require.main === module) {
  runShareDBDemoFromEnv()
    .then(success => {
      process.exit(success ? 0 : 1)
    })
    .catch(error => {
      console.error('未处理的错误:', error)
      process.exit(1)
    })
}
