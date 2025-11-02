import LuckDBClient from '../../src/index'
import { config } from '../config'
import { Logger } from '../utils/logger'
import { safeExecute } from '../utils/helpers'

const logger = new Logger('Test Table Demo')

// 测试表格配置
const testTable = {
  spaceId: 'spc_rtpLk96gJHLeYTv7JJMlo',
  baseId: '7ec1e878-91b9-4c1b-ad86-05cdf801318f',
  tableId: 'tbl_Pweb3NpbtiUb4Fwbi90WP'
}

export async function runTestTableDemo(): Promise<boolean> {
  logger.section('测试表格连接 - 查看字段和记录')

  // 创建客户端
  const client = new LuckDBClient(config.serverURL)

  // 使用提供的账号信息
  const testEmail = 'admin@126.com'
  const testPassword = 'Pmker123'

  logger.info(`服务器地址: ${config.serverURL}`)
  logger.info(`账号: ${testEmail}`)
  logger.info(`表格 ID: ${testTable.tableId}`)

  try {
    // 1. 先登录
    logger.step(1, 5, '用户登录...')
    try {
      const loginResult = await client.auth.login(testEmail, testPassword)
      logger.success(`登录成功: ${loginResult.record.email}`)
    } catch (error: any) {
      logger.error(`登录失败: ${error.message}`)
      if (error.details) {
        logger.error(`详细信息: ${error.details}`)
      }
      if (error.status) {
        logger.error(`状态码: ${error.status}`)
      }
      logger.warning('请检查:')
      logger.warning('  1. 服务器地址是否正确（当前: ' + config.serverURL + '）')
      logger.warning('  2. 账号和密码是否正确')
      logger.warning('  3. 密码是否符合服务端的验证规则')
      throw error
    }

    // 2. 获取表格信息
    logger.step(2, 5, '获取表格信息...')
    const table = await safeExecute(async () => {
      return await client.tables.getOne(testTable.tableId)
    }, '获取表格失败')

    if (!table) {
      throw new Error('无法获取表格信息')
    }

    logger.success(`表格名称: ${table.name}`)
    logger.info(`表格 ID: ${table.id}`)
    if (table.description) {
      logger.info(`表格描述: ${table.description}`)
    }

    // 3. 获取所有字段
    logger.step(3, 5, '获取所有字段...')
    const fields = await safeExecute(async () => {
      return await client.fields.getFullList(testTable.tableId)
    }, '获取字段列表失败')

    if (!fields) {
      throw new Error('无法获取字段列表')
    }

    logger.success(`找到 ${fields.length} 个字段`)
    console.log('\n字段列表:')
    console.log('='.repeat(80))
    
    fields.forEach((field, index) => {
      console.log(`\n[字段 ${index + 1}]`)
      console.log(`  ID: ${field.id}`)
      console.log(`  名称: ${field.name}`)
      console.log(`  类型: ${field.type}`)
      if (field.description) {
        console.log(`  描述: ${field.description}`)
      }
      if (field.required !== undefined) {
        console.log(`  必填: ${field.required ? '是' : '否'}`)
      }
      if (field.options) {
        console.log(`  选项: ${JSON.stringify(field.options, null, 2)}`)
      }
      if (field.defaultValue !== undefined) {
        console.log(`  默认值: ${JSON.stringify(field.defaultValue)}`)
      }
    })
    console.log('='.repeat(80))

    // 4. 获取所有记录
    logger.step(4, 5, '获取所有记录...')
    const records = await safeExecute(async () => {
      return await client.records.getFullList(testTable.tableId)
    }, '获取记录列表失败')

    if (!records) {
      throw new Error('无法获取记录列表')
    }

    logger.success(`找到 ${records.length} 条记录`)
    console.log('\n记录列表:')
    console.log('='.repeat(80))

    if (records.length === 0) {
      console.log('\n(暂无记录)')
    } else {
      records.forEach((record, index) => {
        console.log(`\n[记录 ${index + 1}]`)
        console.log(`  ID: ${record.id}`)
        if (record.version !== undefined) {
          console.log(`  版本: ${record.version}`)
        }
        if (record.createdAt) {
          console.log(`  创建时间: ${record.createdAt}`)
        }
        if (record.updatedAt) {
          console.log(`  更新时间: ${record.updatedAt}`)
        }
        console.log(`  字段数据:`)
        
        // 显示字段数据
        const fieldData = record.data || record.fields || {}
        if (Object.keys(fieldData).length === 0) {
          console.log(`    (无字段数据)`)
        } else {
          // 将字段 ID 映射到字段名称以便显示
          const fieldMap: Record<string, string> = {}
          fields.forEach(field => {
            fieldMap[field.id] = field.name
          })

          Object.entries(fieldData).forEach(([fieldId, value]) => {
            const fieldName = fieldMap[fieldId] || fieldId
            const displayValue = value === null || value === undefined 
              ? '(空)' 
              : typeof value === 'object' 
                ? JSON.stringify(value, null, 2)
                : String(value)
            console.log(`    ${fieldName} (${fieldId}): ${displayValue}`)
          })
        }
      })
    }
    console.log('='.repeat(80))

    // 5. 汇总信息
    logger.step(5, 5, '汇总信息...')
    console.log('\n汇总信息:')
    console.log('='.repeat(80))
    console.log(`表格 ID: ${testTable.tableId}`)
    console.log(`表格名称: ${table.name}`)
    console.log(`字段数量: ${fields.length}`)
    console.log(`记录数量: ${records.length}`)
    console.log('='.repeat(80))

    // 字段类型统计
    const fieldTypeStats: Record<string, number> = {}
    fields.forEach(field => {
      fieldTypeStats[field.type] = (fieldTypeStats[field.type] || 0) + 1
    })
    
    if (Object.keys(fieldTypeStats).length > 0) {
      console.log('\n字段类型统计:')
      Object.entries(fieldTypeStats).forEach(([type, count]) => {
        console.log(`  ${type}: ${count}`)
      })
    }

    logger.success('\n✅ 测试表格连接完成')
    return true

  } catch (error: any) {
    logger.error(`\n❌ 测试表格连接失败: ${error.message}`)
    if (config.debug) {
      console.error(error)
    }
    return false
  }
}

// 如果直接运行此文件
if (require.main === module) {
  runTestTableDemo()
    .then(success => {
      process.exit(success ? 0 : 1)
    })
    .catch(error => {
      console.error('未处理的错误:', error)
      process.exit(1)
    })
}

