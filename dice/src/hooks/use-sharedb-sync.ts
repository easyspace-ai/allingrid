import { useCallback, useEffect, useRef, useState, useMemo } from 'react'
import type { ShareDBDocEvent } from 'luckdb-sdk'
import { createShareDBService } from '@/services/sharedbService'
import type { ShareDBService } from '@/services/sharedbService'

interface UseShareDBSyncOptions<TData> {
  tableId: string | null
  recordIds: string[]
  onRecordUpdate: (recordId: string, data: any) => void
  enabled?: boolean
}

interface UseShareDBSyncReturn {
  isConnected: boolean
  subscribedCount: number
  connect: () => Promise<void>
  disconnect: () => void
  refreshSubscriptions: () => Promise<void>
  submitFieldUpdate: (recordId: string, fieldId: string, newValue: any, oldValue?: any) => Promise<void>
}

/**
 * ShareDB 实时同步 Hook
 * 
 * 功能：
 * 1. 管理 ShareDB 连接
 * 2. 订阅/取消订阅记录文档
 * 3. 监听 'op' 事件并更新本地状态
 * 4. 防止循环更新（通过标记本地操作）
 */
export function useShareDBSync<TData extends { id: string }>(
  options: UseShareDBSyncOptions<TData>
): UseShareDBSyncReturn {
  const { tableId, recordIds, onRecordUpdate, enabled = true } = options

  const serviceRef = useRef<ShareDBService | null>(null)
  const isInitializedRef = useRef(false)
  const localOperationRef = useRef<Set<string>>(new Set()) // 标记本地操作，防止循环更新
  const [isConnected, setIsConnected] = useState(false)
  const [subscribedCount, setSubscribedCount] = useState(0)
  
  // 使用 useRef 保存 onRecordUpdate 回调，避免因回调变化导致重新创建订阅函数
  const onRecordUpdateRef = useRef(onRecordUpdate)
  useEffect(() => {
    onRecordUpdateRef.current = onRecordUpdate
  }, [onRecordUpdate])
  
  // 使用 useMemo 稳定化 recordIds，只在实际内容变化时更新
  const recordIdsKey = useMemo(() => {
    return recordIds.slice().sort().join(',')
  }, [recordIds])
  
  // 保存上次的 recordIdsKey，用于比较
  const prevRecordIdsKeyRef = useRef<string>('')
  
  // 防止重复执行 refreshSubscriptions 的标志
  const isRefreshingRef = useRef(false)
  
  // 连接检查防抖：记录上次连接检查时间
  const lastConnectionCheckRef = useRef<number>(0)
  const CONNECTION_CHECK_INTERVAL = 5000 // 从2秒改为5秒

  // 初始化服务
  useEffect(() => {
    if (!serviceRef.current) {
      serviceRef.current = createShareDBService()
    }

    return () => {
      if (serviceRef.current) {
        serviceRef.current.cleanup()
        serviceRef.current = null
      }
    }
  }, [])

  // 连接 ShareDB
  const connect = useCallback(async () => {
    if (!serviceRef.current || !enabled || !tableId) {
      return
    }

    try {
      // 初始化连接
      await serviceRef.current.initialize()
      
      // 设置表格 ID
      serviceRef.current.setTableId(tableId)
      
      // 连接
      await serviceRef.current.connect()
      
      setIsConnected(true)
      isInitializedRef.current = true
    } catch (error) {
      console.error('Failed to connect ShareDB:', error)
      setIsConnected(false)
    }
  }, [enabled, tableId])

  // 断开连接
  const disconnect = useCallback(() => {
    if (serviceRef.current) {
      serviceRef.current.disconnect()
      setIsConnected(false)
      isInitializedRef.current = false
    }
  }, [])

  // 处理文档更新事件（使用 ref 中的回调，避免依赖变化）
  const handleDocumentUpdate = useCallback(
    (recordId: string, event: ShareDBDocEvent) => {
      console.log(`[useShareDBSync] handleDocumentUpdate for record ${recordId}`, {
        hasOp: !!event.op && event.op.length > 0,
        opLength: event.op?.length || 0,
        source: event.source,
        hasData: !!event.data
      })

      // 使用 source 参数判断是否本地操作（参考 teable）
      // source === true: 本地操作（本客户端触发的）
      // source === false: 远程操作（服务器广播的）
      
      // 如果是本地操作，且我们已经乐观更新了，可以跳过或更新一次（避免重复）
      if (event.source === true) {
        console.log(`[useShareDBSync] 本地操作，recordId: ${recordId}`)
        // 本地操作可能是 ShareDB 确认的，但我们已经在 handleDataChange 中乐观更新了
        // 可以选择跳过，或者更新一次确保数据一致
        // 这里我们选择更新一次，确保 ShareDB 文档数据和本地状态一致
        const doc = (serviceRef.current as any)?.subscribedDocs?.get?.(recordId)
        if (doc?.data) {
          console.log(`[useShareDBSync] 本地操作确认，更新本地状态`, doc.data)
          onRecordUpdateRef.current(recordId, { data: doc.data })
        }
        return
      }

      // 处理远程更新（source === false 或 undefined）
      console.log(`[useShareDBSync] 远程操作，recordId: ${recordId}`, { 
        hasEventData: !!event.data, 
        eventData: event.data,
        hasOps: !!event.op && event.op.length > 0 
      })
      
      // 从 ShareDB 文档获取最新数据（op 事件的数据已经在文档中应用）
      // 使用 requestAnimationFrame 和 setTimeout 确保 ShareDB 的操作已经完全应用到文档
      // requestAnimationFrame 确保在下一个渲染帧读取，setTimeout 确保在下一个事件循环读取
      requestAnimationFrame(() => {
        setTimeout(() => {
          const doc = (serviceRef.current as any)?.subscribedDocs?.get?.(recordId)
          // 优先使用 doc.data（最新状态），如果不可用则使用 event.data
          const shareDBData = doc?.data || event.data
          if (shareDBData) {
            console.log(`[useShareDBSync] 应用远程更新`, { 
              fromDoc: !!doc?.data, 
              fromEvent: !!event.data,
              data: shareDBData 
            })
            // ShareDB 文档格式：doc.data 是 { [fieldId]: value, ... }
            // 但 handleRecordUpdate 期望 { data: { [fieldId]: value, ... } }
            const finalData = shareDBData && typeof shareDBData === 'object' && 'data' in shareDBData
              ? shareDBData
              : { data: shareDBData }
            onRecordUpdateRef.current(recordId, finalData)
          } else {
            console.warn(`[useShareDBSync] 无法获取 ShareDB 数据，recordId: ${recordId}`)
          }
        }, 0)
      })
    },
    [] // 不依赖 onRecordUpdate，使用 ref
  )
  
  // 订阅文档（使用稳定的 recordIds 引用）
  const subscribeDocuments = useCallback(async () => {
    if (!serviceRef.current || !isInitializedRef.current || !enabled || recordIds.length === 0) {
      console.log('[useShareDBSync] subscribeDocuments 跳过:', {
        hasService: !!serviceRef.current,
        isInitialized: isInitializedRef.current,
        enabled,
        recordCount: recordIds.length
      })
      return
    }

    try {
      console.log('[useShareDBSync] 开始订阅文档，记录数:', recordIds.length)
      // 批量订阅所有记录
      await serviceRef.current.subscribeRecords(recordIds, (recordId, event) => {
        // 处理文档更新事件
        handleDocumentUpdate(recordId, event)
      })

      console.log('[useShareDBSync] 订阅完成，记录数:', recordIds.length)
      setSubscribedCount(recordIds.length)
    } catch (error) {
      console.error('[useShareDBSync] Failed to subscribe documents:', error)
    }
  }, [recordIds, enabled, handleDocumentUpdate])

  // 标记本地操作（防止循环更新）
  const markLocalOperation = useCallback((recordId: string, op: any[]) => {
    const operationKey = `${recordId}:${JSON.stringify(op)}`
    localOperationRef.current.add(operationKey)

    // 3秒后自动清除标记（防止内存泄漏）
    setTimeout(() => {
      localOperationRef.current.delete(operationKey)
    }, 3000)
  }, [])

  // 刷新订阅（当记录列表变化时）- 添加防重复机制
  const refreshSubscriptions = useCallback(async () => {
    if (!serviceRef.current || !isInitializedRef.current || !enabled) {
      return
    }
    
    // 防止并发执行
    if (isRefreshingRef.current) {
      console.log('[ShareDB] refreshSubscriptions already in progress, skipping...')
      return
    }
    
    isRefreshingRef.current = true

    try {
      // 取消所有现有订阅
      const currentRecordIds = Array.from(
        (serviceRef.current as any)?.subscribedDocs?.keys() || []
      )
      if (currentRecordIds.length > 0) {
        serviceRef.current.unsubscribeRecords(currentRecordIds)
        // 等待一小段时间，确保取消订阅操作完成
        await new Promise(resolve => setTimeout(resolve, 50))
      }

      // 重新订阅
      await subscribeDocuments()
    } finally {
      isRefreshingRef.current = false
    }
  }, [subscribeDocuments, enabled])

  // 自动连接
  useEffect(() => {
    if (enabled && tableId && !isInitializedRef.current) {
      connect()
    }
  }, [enabled, tableId, connect])

  // 当记录列表变化时，更新订阅（使用稳定的 recordIdsKey）
  useEffect(() => {
    // 只在 recordIdsKey 真正变化时执行
    if (recordIdsKey === prevRecordIdsKeyRef.current) {
      return
    }
    
    prevRecordIdsKeyRef.current = recordIdsKey
    
    if (isInitializedRef.current && enabled) {
      refreshSubscriptions()
    }
  }, [recordIdsKey, enabled, refreshSubscriptions])

  // 监听连接状态和自动重连（增加检查间隔，添加防抖）
  useEffect(() => {
    if (!serviceRef.current || !enabled) return

    const checkConnection = () => {
      const now = Date.now()
      // 防抖：如果距离上次检查时间太短，跳过
      if (now - lastConnectionCheckRef.current < CONNECTION_CHECK_INTERVAL) {
        return
      }
      lastConnectionCheckRef.current = now
      
      if (serviceRef.current) {
        const wasConnected = isConnected
        const nowConnected = serviceRef.current.isConnected
        setIsConnected(nowConnected)
        setSubscribedCount(serviceRef.current.getSubscribedCount())

        // 如果连接断开，尝试重连
        if (wasConnected && !nowConnected && tableId) {
          console.warn('ShareDB connection lost, attempting to reconnect...')
          connect().catch((err) => {
            console.error('Failed to reconnect ShareDB:', err)
          })
        }
      }
    }

    const interval = setInterval(checkConnection, CONNECTION_CHECK_INTERVAL)
    return () => clearInterval(interval)
  }, [enabled, isConnected, tableId, connect])

  // 重试订阅文档（当连接恢复时）- 使用稳定的 recordIdsKey
  useEffect(() => {
    // 只在连接状态真正变化时执行，避免重复订阅
    if (isInitializedRef.current && isConnected && enabled && recordIds.length > 0) {
      subscribeDocuments().catch((err) => {
        console.error('Failed to resubscribe documents after reconnect:', err)
      })
    }
  }, [isConnected, recordIdsKey, enabled, subscribeDocuments])

  // 导出标记本地操作的函数（供外部使用）
  ;(useShareDBSync as any).markLocalOperation = markLocalOperation

  // 提交字段更新到 ShareDB
  const submitFieldUpdate = useCallback(
    async (recordId: string, fieldId: string, newValue: any, oldValue?: any) => {
      if (!serviceRef.current) {
        throw new Error('ShareDB service is not initialized')
      }
      await serviceRef.current.submitFieldUpdate(recordId, fieldId, newValue, oldValue)
    },
    []
  )

  return {
    isConnected,
    subscribedCount,
    connect,
    disconnect,
    refreshSubscriptions,
    submitFieldUpdate,
  }
}

// 导出辅助函数：标记本地操作
export function markShareDBLocalOperation(recordId: string, op: any[]): void {
  // 这个函数会被外部调用来标记本地操作
  if ((useShareDBSync as any).markLocalOperation) {
    ;(useShareDBSync as any).markLocalOperation(recordId, op)
  }
}

