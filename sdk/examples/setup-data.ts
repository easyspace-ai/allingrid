#!/usr/bin/env tsx
/**
 * 数据准备脚本
 * 创建所有必要的资源（space, base, table, field, record）并保存到 .env 文件
 */

import LuckDBClient from '../src/index'
import { config } from './config'
import { Logger } from './utils/logger'
import { sleep } from './utils/helpers'
import * as fs from 'fs'
import * as path from 'path'

const logger = new Logger('Setup Data')

async function setupData(): Promise<void> {
  logger.section('数据准备脚本')
  
  const client = new LuckDBClient(config.serverURL)
  
  try {
    // 1. 登录
    logger.info('正在登录...')
    await client.auth.login(config.testEmail, config.testPassword)
    logger.success('登录成功')
    
    // 2. 创建 Space
    logger.info('创建 Space...')
    const space = await client.spaces.create({
      name: `ShareDB Test Space ${Date.now()}`,
      description: '用于 ShareDB 实时同步测试的 Space'
    })
    logger.success(`Space 创建成功: ${space.id}`)
    
    // 等待一下
    await sleep(500)
    
    // 3. 创建 Base
    logger.info('创建 Base...')
    const base = await client.bases.create(space.id, {
      name: `ShareDB Test Base ${Date.now()}`,
      icon: '📊'
    })
    logger.success(`Base 创建成功: ${base.id}`)
    
    await sleep(500)
    
    // 4. 创建 Table
    logger.info('创建 Table...')
    const table = await client.tables.create(base.id, {
      name: `ShareDB Test Table ${Date.now()}`,
      description: '用于 ShareDB 实时同步测试的 Table'
    })
    logger.success(`Table 创建成功: ${table.id}`)
    
    await sleep(500)
    
    // 5. 创建 Field
    logger.info('创建 Field...')
    const field = await client.fields.create(table.id, {
      name: 'Name',
      type: 'singleLineText',
      required: false
    })
    logger.success(`Field 创建成功: ${field.id}`)
    
    await sleep(2000) // 增加等待时间，确保字段同步到数据库
    
    // 6. 验证字段已创建（最多重试5次）
    logger.info('验证字段已创建...')
    let fieldVerified = false
    for (let i = 0; i < 5; i++) {
      try {
        await sleep(1000)
        const verifyField = await client.fields.getOne(field.id)
        if (verifyField && verifyField.id === field.id) {
          logger.success(`字段验证成功: ${verifyField.name}`)
          fieldVerified = true
          break
        } else {
          logger.info(`字段验证中...（尝试 ${i + 1}/5）`)
        }
      } catch (error: any) {
        logger.warning(`字段验证失败（尝试 ${i + 1}/5）: ${error.message}`)
      }
    }
    
    if (!fieldVerified) {
      logger.error('❌ 字段验证失败: 字段可能不存在或被删除')
      logger.error('提示: 字段创建可能失败，或字段查询有延迟')
      throw new Error('字段验证失败，无法继续创建记录')
    }
    
    await sleep(1000) // 额外等待，确保字段完全同步
    
    // 7. 创建 Record（带初始数据）
    logger.info('创建 Record（带初始数据）...')
    let record
    try {
      record = await client.records.create(table.id, {
        data: {
          [field.id]: `Initial Record Data - ${new Date().toISOString()}`
        }
      })
      logger.success(`Record 创建成功: ${record.id}`)
      
      // 检查创建响应
      logger.info(`记录创建响应: ${JSON.stringify(record, null, 2)}`)
      if (record.data && record.data[field.id]) {
        logger.success(`✅ 记录创建响应包含数据: ${record.data[field.id]}`)
      }
    } catch (error: any) {
      logger.error(`记录创建失败: ${error.message}`)
      if (error.response) {
        logger.error(`错误详情: ${JSON.stringify(error.response.data, null, 2)}`)
      }
      throw error
    }
    
    // 等待一下，确保记录保存
    await sleep(3000)
    
    // 8. 验证记录已创建并有数据（最多重试10次，每次等待2秒）
    logger.info('验证记录已创建并有数据...')
    let verifySuccess = false
    for (let i = 0; i < 10; i++) {
      try {
        await sleep(2000)
        const verifyRecord = await client.records.getOne(table.id, record.id)
        if (verifyRecord && verifyRecord.data && verifyRecord.data[field.id]) {
          logger.success(`✅ 记录验证成功: 包含字段数据（尝试 ${i + 1}/10）`)
          logger.info(`字段值: ${verifyRecord.data[field.id]}`)
          verifySuccess = true
          break
        } else {
          logger.info(`记录验证中...（尝试 ${i + 1}/10）`)
          if (verifyRecord) {
            logger.info(`记录存在但数据为空，数据: ${JSON.stringify(verifyRecord.data)}`)
            // 如果记录存在但数据为空，通过 ShareDB 添加数据
            if (i === 4) { // 第5次尝试时，如果数据仍为空，通过 ShareDB 添加
              logger.info('记录数据为空，尝试通过 ShareDB 添加数据...')
              try {
                await client.sharedb.initialize()
                await client.sharedb.connect()
                await sleep(1000)
                
                const collection = `rec_${table.id}`
                const doc = client.sharedb.getDocument(collection, record.id)
                
                await doc.subscribe()
                await sleep(2000)
                
                const shareDBValue = `Initial Record Data (ShareDB) - ${new Date().toISOString()}`
                logger.info(`通过 ShareDB 设置字段值: ${shareDBValue}`)
                
                await doc.submitOp([
                  { p: ['data', field.id], oi: shareDBValue }
                ])
                
                logger.success('✅ 通过 ShareDB 操作添加数据成功')
                await sleep(3000)
                
                client.sharedb.disconnect()
              } catch (error: any) {
                logger.warning(`ShareDB 操作失败: ${error.message}`)
              }
            }
          } else {
            logger.warning(`记录不存在`)
          }
        }
      } catch (error: any) {
        logger.warning(`记录验证失败（尝试 ${i + 1}/10）: ${error.message}`)
      }
    }
    
    if (!verifySuccess) {
      logger.warning('⚠️ 记录验证失败: 记录数据可能为空')
      logger.warning('提示: 记录已创建，但字段数据可能通过 ShareDB 操作添加')
      logger.warning('ShareDB 测试仍可正常进行，因为 ShareDB 操作会实时同步')
    }
    
    // 9. 将数据保存到 .env 文件
    logger.info('保存数据到 .env 文件...')
    const envPath = path.join(__dirname, '.env')
    const envContent = `
# ShareDB 测试数据（自动生成）
# 生成时间: ${new Date().toISOString()}

# Space ID
SHAREDB_TEST_SPACE_ID=${space.id}

# Base ID
SHAREDB_TEST_BASE_ID=${base.id}

# Table ID
SHAREDB_TEST_TABLE_ID=${table.id}

# Field ID
SHAREDB_TEST_FIELD_ID=${field.id}

# Record ID
SHAREDB_TEST_RECORD_ID=${record.id}

# Collection Name (for ShareDB)
SHAREDB_TEST_COLLECTION=rec_${table.id}
`
    
    // 读取现有的 .env 文件（如果存在）
    let existingEnv = ''
    if (fs.existsSync(envPath)) {
      existingEnv = fs.readFileSync(envPath, 'utf-8')
      // 移除旧的 ShareDB 测试数据部分
      existingEnv = existingEnv.replace(/# ShareDB 测试数据.*?# Collection Name.*?\n/gs, '')
    }
    
    // 合并内容
    const finalEnv = existingEnv.trim() + envContent
    
    fs.writeFileSync(envPath, finalEnv.trim() + '\n')
    logger.success(`数据已保存到: ${envPath}`)
    
    logger.success('\n✅ 数据准备完成！')
    logger.info('\n数据摘要:')
    logger.info(`  Space ID: ${space.id}`)
    logger.info(`  Base ID: ${base.id}`)
    logger.info(`  Table ID: ${table.id}`)
    logger.info(`  Field ID: ${field.id}`)
    logger.info(`  Record ID: ${record.id}`)
    logger.info(`  Collection: rec_${table.id}`)
    
  } catch (error: any) {
    logger.error(`数据准备失败: ${error.message}`)
    console.error(error)
    process.exit(1)
  }
}

// 运行脚本
setupData().catch((error) => {
  logger.error(`脚本执行失败: ${error.message}`)
  console.error(error)
  process.exit(1)
})

