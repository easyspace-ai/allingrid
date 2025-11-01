import LuckDBClient from '../src/index'
import { config } from './config'
import { Logger } from './utils/logger'
import { sleep } from './utils/helpers'
import { runAuthDemo } from './demos/01-auth'
import { runSpaceDemo } from './demos/02-space'
import { runBaseDemo } from './demos/03-base'
import { runTableDemo } from './demos/04-table'
import { runFieldDemo } from './demos/05-field'
import { runRecordDemo } from './demos/06-record'
import { runViewDemo } from './demos/07-view'
import { runShareDBDemo } from './demos/08-sharedb'
import { runAttachmentDemo } from './demos/09-attachment'
import { createDemoContext } from './utils/types'

const logger = new Logger('Demo Runner')

interface DemoModule {
  name: string
  run: (context: any) => Promise<boolean>
  requiresAuth?: boolean
  requiresSpace?: boolean
  requiresBase?: boolean
  requiresTable?: boolean
  requiresFields?: boolean
  requiresRecords?: boolean
}

const modules: DemoModule[] = [
  { name: 'auth', run: runAuthDemo, requiresAuth: false },
  { name: 'space', run: runSpaceDemo, requiresAuth: true },
  { name: 'base', run: runBaseDemo, requiresAuth: true, requiresSpace: true },
  { name: 'table', run: runTableDemo, requiresAuth: true, requiresBase: true },
  { name: 'field', run: runFieldDemo, requiresAuth: true, requiresTable: true },
  { name: 'record', run: runRecordDemo, requiresAuth: true, requiresTable: true, requiresFields: true },
  { name: 'view', run: runViewDemo, requiresAuth: true, requiresTable: true },
  { name: 'sharedb', run: runShareDBDemo, requiresAuth: true, requiresTable: true, requiresRecords: true },
  { name: 'attachment', run: runAttachmentDemo, requiresAuth: true, requiresTable: true }
]

async function setupResources(client: LuckDBClient, modulesToRun: string[]): Promise<any> {
  const context = createDemoContext(client)

    // 如果需要 Space，先创建（需要先登录）
    if (modulesToRun.some(m => ['space', 'base', 'table', 'field', 'record', 'view', 'sharedb', 'attachment'].includes(m))) {
    if (!modulesToRun.includes('space')) {
      logger.info('自动创建 Space（后续模块需要）...')
      const space = await client.spaces.create({
        name: `Auto Demo Space ${Date.now()}`,
        description: '自动创建的演示空间'
      })
      context.spaceId = space.id
      logger.success(`Space 创建成功: ${space.id}`)
    }
  }

    // 如果需要 Base，先创建（需要先登录）
    if (modulesToRun.some(m => ['base', 'table', 'field', 'record', 'view', 'sharedb', 'attachment'].includes(m))) {
    if (!modulesToRun.includes('base') && context.spaceId) {
      logger.info('自动创建 Base（后续模块需要）...')
      const base = await client.bases.create(context.spaceId, {
        name: `Auto Demo Base ${Date.now()}`,
        icon: '📊'
      })
      context.baseId = base.id
      logger.success(`Base 创建成功: ${base.id}`)
    }
  }

    // 如果需要 Table，先创建（需要先登录）
    if (modulesToRun.some(m => ['table', 'field', 'record', 'view', 'sharedb', 'attachment'].includes(m))) {
    if (!modulesToRun.includes('table') && context.baseId) {
      logger.info('自动创建 Table（后续模块需要）...')
      const table = await client.tables.create(context.baseId, {
        name: `Auto Demo Table ${Date.now()}`,
        description: '自动创建的演示表格'
      })
      context.tableId = table.id
      logger.success(`Table 创建成功: ${table.id}`)
    }
  }

  // 如果需要 Field，先创建（需要先登录）
  if (modulesToRun.some(m => ['field', 'record', 'sharedb'].includes(m))) {
    if (!modulesToRun.includes('field') && context.tableId) {
      logger.info('自动创建 Field（后续模块需要）...')
      try {
        const field = await client.fields.create(context.tableId, {
          name: 'Name',
          type: 'singleLineText',
          required: false  // 改为非必填
        })
        context.fieldIds['Name'] = field.id
        logger.success(`Field 创建成功: ${field.id}`)
        // 等待字段同步到数据库，并验证字段确实存在
        await sleep(500)
        try {
          const verifyField = await client.fields.getOne(field.id)
          if (verifyField) {
            logger.info(`字段验证成功: ${verifyField.name}`)
          }
        } catch (error: any) {
          logger.warning(`字段验证失败: ${error.message}`)
        }
      } catch (error: any) {
        logger.warning(`自动创建 Field 失败: ${error.message}`)
      }
    }
  }

  // 如果需要 Record，先创建（需要先登录）
  if (modulesToRun.includes('sharedb') && context.tableId && Object.keys(context.fieldIds).length > 0) {
    logger.info('自动创建 Record（ShareDB 模块需要）...')
    try {
      // 等待字段创建完成
      await sleep(500)
      
      const firstFieldId = Object.values(context.fieldIds)[0]
      const record = await client.records.create(context.tableId, {
        data: {
          [firstFieldId]: 'ShareDB Demo Record - Initial Data'
        }
      })
      context.recordIds.push(record.id)
      logger.success(`Record 创建成功: ${record.id}`)
      
      // 验证记录已创建并有数据（最多重试3次）
      await sleep(500)
      let verifySuccess = false
      for (let i = 0; i < 3; i++) {
        try {
          const verifyRecord = await client.records.getOne(context.tableId, record.id)
          if (verifyRecord && verifyRecord.data && verifyRecord.data[firstFieldId]) {
            logger.success(`✅ 记录验证成功: 包含字段数据（尝试 ${i + 1}/3）`)
            verifySuccess = true
            break
          } else {
            logger.info(`记录验证中...（尝试 ${i + 1}/3）`)
          }
        } catch (error: any) {
          logger.warning(`记录验证失败（尝试 ${i + 1}/3）: ${error.message}`)
        }
        if (i < 2) {
          await sleep(1000)
        }
      }
      
      if (!verifySuccess) {
        logger.warning('⚠️ 记录验证失败: 记录数据为空')
      }
    } catch (error: any) {
      logger.warning(`自动创建 Record 失败: ${error.message}`)
    }
  }

  return context
}

async function runAllDemos(): Promise<void> {
  logger.section('LuckDB SDK 完整演示')

  const client = new LuckDBClient(config.serverURL)
  const startTime = Date.now()

  try {
    // 解析命令行参数
    const args = process.argv.slice(2)
    const moduleArg = args.find(arg => arg.startsWith('--module='))
    const skipCleanup = args.includes('--no-cleanup')
    
    if (skipCleanup) {
      process.env.CLEANUP = 'false'
    }

    let modulesToRun: string[]
    if (moduleArg) {
      const moduleName = moduleArg.split('=')[1]
      modulesToRun = [moduleName]
      logger.info(`运行指定模块: ${moduleName}`)
    } else {
      modulesToRun = modules.map(m => m.name)
      logger.info('运行所有模块')
    }

    // 登录（除了 auth 模块，其他模块都需要先登录）
    if (!modulesToRun.includes('auth')) {
      logger.info('正在登录...')
      await client.auth.login(config.testEmail, config.testPassword)
      logger.success('登录成功')
    }

    // 设置资源
    const context = await setupResources(client, modulesToRun)

    // 运行各个模块
    const results: Record<string, boolean> = {}
    
    for (const module of modules) {
      if (!modulesToRun.includes(module.name)) {
        continue
      }

      logger.section(`模块: ${module.name.toUpperCase()}`)
      
      try {
        const success = await module.run(context)
        results[module.name] = success
        
        if (success) {
          logger.success(`✅ ${module.name} 模块完成`)
        } else {
          logger.error(`❌ ${module.name} 模块失败`)
        }
      } catch (error: any) {
        logger.error(`❌ ${module.name} 模块异常: ${error.message}`)
        results[module.name] = false
      }

      // 模块之间稍作等待
      await new Promise(resolve => setTimeout(resolve, 500))
    }

    // 输出总结（仅当有结果时）
    if (typeof results !== 'undefined' && Object.keys(results).length > 0) {
      const endTime = Date.now()
      const duration = ((endTime - startTime) / 1000).toFixed(2)

      logger.section('演示总结')
      logger.info(`总耗时: ${duration} 秒`)
      logger.info('\n模块执行结果:')
      
      const successCount = Object.values(results).filter(r => r).length
      const totalCount = Object.keys(results).length

      for (const [name, success] of Object.entries(results)) {
        const icon = success ? '✅' : '❌'
        logger.info(`  ${icon} ${name}`)
      }

      logger.info(`\n成功: ${successCount}/${totalCount}`)
      
      if (successCount === totalCount) {
        logger.success('\n🎉 所有演示完成！')
        process.exit(0)
      } else {
        logger.warning('\n⚠️  部分演示失败')
        process.exit(1)
      }
    } else {
      // auth 模块单独处理
      process.exit(0)
    }

  } catch (error: any) {
    logger.error(`\n❌ 演示运行失败: ${error.message}`)
    if (config.debug) {
      console.error(error)
    }
    process.exit(1)
  }
}

// 运行演示
if (require.main === module) {
  runAllDemos().catch(error => {
    console.error('未处理的错误:', error)
    process.exit(1)
  })
}

export { runAllDemos }

