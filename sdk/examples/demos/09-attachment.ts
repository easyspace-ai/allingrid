import LuckDBClient from '../../src/index'
import { Logger } from '../utils/logger'
import { sleep } from '../utils/helpers'
import type { DemoContext } from '../utils/types'

const logger = new Logger('Attachment Demo')

/**
 * 附件上传功能演示
 * 
 * 演示内容：
 * 1. 生成上传签名
 * 2. 上传文件
 * 3. 通知上传完成
 * 4. 获取附件信息
 * 5. 列出附件
 * 6. 获取附件统计
 * 7. 读取文件
 * 8. 删除附件
 */
export async function runAttachmentDemo(context: DemoContext): Promise<boolean> {
  logger.section('Attachment API 演示')

  const { client } = context

  if (!context.tableId) {
    logger.error('需要先创建 Table')
    return false
  }

  try {
    // 0. 确保有附件字段
    let attachmentFieldId: string | undefined
    
    // 尝试从现有字段中找到附件字段
    if (Object.keys(context.fieldIds).length > 0) {
      for (const [name, fieldId] of Object.entries(context.fieldIds)) {
        try {
          const field = await client.fields.get(fieldId)
          if (field.type === 'attachment') {
            attachmentFieldId = fieldId
            logger.info(`使用现有附件字段: ${name} (${fieldId})`)
            break
          }
        } catch (error) {
          // 忽略错误，继续查找
        }
      }
    }

    // 如果没有附件字段，创建一个
    if (!attachmentFieldId) {
      logger.info('未找到附件字段，创建一个...')
      const field = await client.fields.create(context.tableId, {
        name: 'Attachment',
        type: 'attachment',
        required: false
      })
      attachmentFieldId = field.id
      context.fieldIds['Attachment'] = field.id
      logger.success(`✅ 附件字段创建成功: ID=${field.id}`)
      await sleep(500)
    }

    // 确保有 Record
    let recordId: string
    if (!context.recordIds || context.recordIds.length === 0) {
      logger.info('未找到 Record，创建一个...')
      const record = await client.records.create(context.tableId, {
        data: {
          [attachmentFieldId]: [] // 附件字段初始为空数组
        }
      })
      recordId = record.id
      if (!context.recordIds) {
        context.recordIds = []
      }
      context.recordIds.push(record.id)
      logger.success(`✅ Record 创建成功: ID=${record.id}`)
      await sleep(500)
    } else {
      recordId = context.recordIds[0]
    }

    // 3. 测试附件上传流程
    logger.info('📝 步骤 3: 测试附件上传流程')
    logger.info('-'.repeat(62))

    // 3.1 生成上传签名
    logger.step(1, 8, '生成上传签名...')
    const signature = await client.attachments.generateSignature({
      table_id: context.tableId,
      field_id: attachmentFieldId,
      record_id: recordId
    })
    logger.success('✅ 签名生成成功')
    logger.info(`     - Token: ${signature.token}`)
    logger.info(`     - 上传URL: ${signature.upload_url}`)
    logger.info(`     - 过期时间: ${signature.expires_at}`)
    logger.info(`     - 最大文件大小: ${signature.max_size} bytes`)
    logger.info(`     - 允许的文件类型: ${signature.allowed_types.join(', ')}`)
    await sleep(500)

    // 3.2 创建测试文件
    logger.step(2, 8, '创建测试文件...')
    const testFileContent = '这是一个测试文件内容\n用于测试附件上传功能'
    const testFile = new Blob([testFileContent], { type: 'text/plain' })
    const testFileName = 'test_file.txt'
    logger.success(`✅ 测试文件创建成功: ${testFileName} (${testFile.size} bytes)`)
    await sleep(500)

    // 3.3 上传文件
    logger.step(3, 8, '上传文件...')
    await client.attachments.uploadFile(signature.token, testFile, testFileName)
    logger.success('✅ 文件上传成功')
    await sleep(500)

    // 3.4 通知上传完成
    logger.step(4, 8, '通知上传完成...')
    const notifyResponse = await client.attachments.notifyUpload(signature.token, testFileName)
    const attachmentId = notifyResponse.attachment.id
    logger.success('✅ 通知成功')
    logger.info(`     - AttachmentID: ${attachmentId}`)
    logger.info(`     - 文件名: ${notifyResponse.attachment.name}`)
    logger.info(`     - 文件大小: ${notifyResponse.attachment.size} bytes`)
    logger.info(`     - MIME类型: ${notifyResponse.attachment.mimetype || notifyResponse.attachment.mime_type}`)
    logger.info(`     - 文件路径: ${notifyResponse.attachment.path}`)
    await sleep(500)

    // 4. 测试附件查询功能
    logger.step(5, 8, '获取附件信息...')
    const attachment = await client.attachments.getAttachment(attachmentId)
    logger.success('✅ 附件信息获取成功')
    logger.info(`     - ID: ${attachment.id}`)
    logger.info(`     - 名称: ${attachment.name}`)
    logger.info(`     - 大小: ${attachment.size} bytes`)
    logger.info(`     - MIME类型: ${attachment.mimetype || attachment.mime_type}`)
    await sleep(500)

    // 5. 列出附件
    logger.step(6, 8, '列出附件...')
    const attachments = await client.attachments.listAttachments({
      table_id: context.tableId,
      field_id: attachmentFieldId,
      record_id: recordId
    })
    logger.success(`✅ 附件列表获取成功: 共 ${attachments.length} 个附件`)
    attachments.forEach((att, index) => {
      logger.info(`     [${index + 1}] ID=${att.id}, Name=${att.name}, Size=${att.size}`)
    })
    await sleep(500)

    // 6. 获取附件统计
    logger.step(7, 8, '获取附件统计...')
    const stats = await client.attachments.getAttachmentStats(context.tableId)
    logger.success('✅ 附件统计获取成功')
    logger.info(`     - 总文件数: ${stats.total_files}`)
    logger.info(`     - 总大小: ${stats.total_size} bytes`)
    logger.info(`     - 图片文件: ${stats.image_files}`)
    logger.info(`     - 视频文件: ${stats.video_files}`)
    logger.info(`     - 音频文件: ${stats.audio_files}`)
    logger.info(`     - 文档文件: ${stats.document_files}`)
    logger.info(`     - 其他文件: ${stats.other_files}`)
    await sleep(500)

    // 7. 测试文件读取
    logger.step(8, 8, '读取文件...')
    const fileContent = await client.attachments.readFileAsText(attachment.path)
    logger.success('✅ 文件读取成功')
    logger.info(`     - 文件内容长度: ${fileContent.length} bytes`)
    logger.info(`     - 文件内容预览: ${fileContent.substring(0, 100)}`)
    await sleep(500)

    // 8. 测试文件删除
    logger.step(9, 9, '删除附件...')
    await client.attachments.deleteAttachment(attachmentId)
    logger.success(`✅ 附件删除成功: ID=${attachmentId}`)
    await sleep(500)

    // 验证删除
    logger.info('验证删除...')
    try {
      await client.attachments.getAttachment(attachmentId)
      logger.warn('⚠️  删除验证失败: 附件仍然存在')
    } catch (error: any) {
      if (error.status === 404 || error.message?.includes('不存在')) {
        logger.success('✅ 删除验证成功: 附件已不存在')
      } else {
        throw error
      }
    }

    logger.success('✅ 附件上传功能演示完成')
    return true

  } catch (error: any) {
    logger.error(`❌ 附件上传功能演示失败: ${error.message}`)
    if (error.data) {
      logger.error(`   错误详情: ${JSON.stringify(error.data)}`)
    }
    return false
  }
}

