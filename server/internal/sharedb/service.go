package sharedb

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/events"
	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/monitoring"
	"github.com/easyspace-ai/luckdb/server/pkg/sharedb"
	"github.com/easyspace-ai/luckdb/server/pkg/sharedb/opbuilder"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// ShareDBService ShareDB 核心服务
type ShareDBService struct {
	adapter        Adapter
	pubsub         PubSub
	presence       PresenceManager
	middleware     []Middleware
	connections    sync.Map // connection ID -> *Connection
	wsConnections  sync.Map // connection ID -> *websocket.Conn
	documents      sync.Map // document ID -> *Document
	eventHook      *TransactionHook
	eventConverter *OpsToEventsConverter
	errorHandler   *errors.ErrorHandler
	perfMonitor    *monitoring.PerformanceMonitor
	perfMiddleware *monitoring.PerformanceMiddleware
	logger         *zap.Logger
	ctx            context.Context
	writeMutex     sync.Mutex // 保护 WebSocket 写入的互斥锁
	cancel         context.CancelFunc
	mu             sync.RWMutex
}

// Document 文档信息
type Document struct {
	ID        string
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
	mu        sync.RWMutex
}

// NewService 创建 ShareDB 服务
func NewService(adapter Adapter, pubsub PubSub, presence PresenceManager, logger *zap.Logger) *ShareDBService {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建性能监控器
	perfMonitor := monitoring.NewPerformanceMonitor(logger)
	perfMiddleware := monitoring.NewPerformanceMiddleware(perfMonitor, logger)

	// 启动定期日志记录
	perfMonitor.StartPeriodicLogging(30 * time.Second)

	service := &ShareDBService{
		adapter:        adapter,
		pubsub:         pubsub,
		presence:       presence,
		middleware:     make([]Middleware, 0),
		errorHandler:   errors.NewErrorHandler(true, true), // 开发环境启用详细错误
		perfMonitor:    perfMonitor,
		perfMiddleware: perfMiddleware,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
	}

	// 启动清理协程
	go service.cleanupRoutine()

	return service
}

// SetEventManager 设置事件管理器
func (s *ShareDBService) SetEventManager(eventManager *events.BusinessEventManager) {
	s.eventHook = NewTransactionHook(eventManager, s.logger)
	s.eventConverter = NewOpsToEventsConverter(eventManager, s.logger)
	s.logger.Info("✅ ShareDB 事件管理器已设置")
}

// AddMiddleware 添加中间件
func (s *ShareDBService) AddMiddleware(middleware Middleware) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.middleware = append(s.middleware, middleware)
}

// cleanupConnection 清理连接资源，确保所有资源都被正确释放
func (s *ShareDBService) cleanupConnection(conn *websocket.Conn, connection *Connection) {
	if connection == nil {
		return
	}

	// 取消所有订阅
	connection.mu.Lock()
	for channel, cancel := range connection.subCancelFuncs {
		s.logger.Debug("取消订阅",
			zap.String("connection_id", connection.ID),
			zap.String("channel", channel))
		cancel()
	}
	connection.subCancelFuncs = nil
	connection.mu.Unlock()

	// 标记连接为非活跃状态
	connection.IsActive = false
	connection.LastSeen = time.Now()

	// 从连接映射中删除
	s.connections.Delete(connection.ID)
	s.wsConnections.Delete(connection.ID)

	// 关闭 WebSocket 连接
	if conn != nil {
		// 发送关闭帧
		conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Connection cleanup"),
			time.Now().Add(time.Second))
		conn.Close()
	}

	// 记录连接清理和当前资源状态
	s.logConnectionMetrics("connection_cleaned_up", connection.ID, connection.UserID)
}

// logConnectionMetrics 记录连接和 goroutine 指标
func (s *ShareDBService) logConnectionMetrics(event string, connectionID, userID string) {
	// 统计活跃连接数
	activeConnections := 0
	s.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*Connection); ok && conn.IsActive {
			activeConnections++
		}
		return true
	})

	s.logger.Info("ShareDB connection metrics",
		zap.String("event", event),
		zap.String("connection_id", connectionID),
		zap.String("user_id", userID),
		zap.Int("active_connections", activeConnections),
		zap.Int("total_goroutines", runtime.NumGoroutine()))
}

// validateConnectionLimits 验证连接限制
func (s *ShareDBService) validateConnectionLimits(userID string) error {
	const (
		maxConnectionsPerUser = 50   // 每个用户最大连接数（开发环境放宽限制）
		maxTotalConnections   = 1000 // 总最大连接数
	)

	// 统计当前连接数
	totalConnections := 0
	userConnections := 0

	s.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*Connection); ok && conn.IsActive {
			totalConnections++
			if conn.UserID == userID {
				userConnections++
			}
		}
		return true
	})

	// 检查总连接数限制
	if totalConnections >= maxTotalConnections {
		return fmt.Errorf("server connection limit exceeded: %d/%d", totalConnections, maxTotalConnections)
	}

	// 检查用户连接数限制
	if userConnections >= maxConnectionsPerUser {
		return fmt.Errorf("user connection limit exceeded: %d/%d for user %s", userConnections, maxConnectionsPerUser, userID)
	}

	return nil
}

// HandleWebSocket 处理 WebSocket 连接
func (s *ShareDBService) HandleWebSocket(c *gin.Context) {
	// 记录详细的请求信息用于调试
	s.logger.Info("WebSocket upgrade request received",
		zap.String("connection_header", c.Request.Header.Get("Connection")),
		zap.String("upgrade_header", c.Request.Header.Get("Upgrade")),
		zap.String("sec_websocket_key", c.Request.Header.Get("Sec-WebSocket-Key")),
		zap.String("sec_websocket_version", c.Request.Header.Get("Sec-WebSocket-Version")),
		zap.String("user_id", c.GetString("user_id")),
		zap.String("remote_addr", c.Request.RemoteAddr),
		zap.String("user_agent", c.Request.Header.Get("User-Agent")),
		zap.String("origin", c.Request.Header.Get("Origin")),
		zap.String("query", c.Request.URL.RawQuery))

	// 升级到 WebSocket
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 在生产环境中应该进行更严格的检查
		},
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		// 禁用压缩以避免 flate stream 错误
		EnableCompression: false,
		// 设置握手超时
		HandshakeTimeout: 10 * time.Second,
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		s.logger.Error("Failed to upgrade WebSocket connection",
			zap.Error(err),
			zap.String("connection_header", c.Request.Header.Get("Connection")),
			zap.String("upgrade_header", c.Request.Header.Get("Upgrade")),
			zap.String("user_id", c.GetString("user_id")))
		return
	}
	defer conn.Close()

	// 创建连接信息
	userID := c.GetString("user_id")
	if userID == "" {
		s.logger.Warn("WebSocket 连接没有 user_id，检查 JWT 中间件",
			zap.String("connection_header", c.Request.Header.Get("Connection")),
			zap.String("upgrade_header", c.Request.Header.Get("Upgrade")))
	} else {
		s.logger.Info("WebSocket 连接已验证",
			zap.String("user_id", userID),
			zap.String("connection_header", c.Request.Header.Get("Connection")),
			zap.String("upgrade_header", c.Request.Header.Get("Upgrade")))
	}

	// 连接限制检查
	if err := s.validateConnectionLimits(userID); err != nil {
		s.logger.Warn("Connection rejected due to limits",
			zap.String("user_id", userID),
			zap.Error(err))
		conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Connection limit exceeded"),
			time.Now().Add(time.Second))
		conn.Close()
		return
	}

	connection := &Connection{
		ID:            generateConnectionID(),
		UserID:        userID, // 从中间件获取用户ID
		LastSeen:      time.Now(),
		IsActive:      true,
		CreatedAt:     time.Now(), // 记录连接创建时间用于超时检查
		subCancelFuncs: make(map[string]context.CancelFunc),
	}

	// 注册连接
	s.connections.Store(connection.ID, connection)
	s.wsConnections.Store(connection.ID, conn)

	// 确保在所有退出路径上都进行清理
	defer func() {
		s.cleanupConnection(conn, connection)
	}()

	s.logger.Info("ShareDB WebSocket connection established",
		zap.String("connection_id", connection.ID),
		zap.String("user_id", connection.UserID))

	// 记录连接建立时的指标
	s.logConnectionMetrics("connection_established", connection.ID, connection.UserID)

	// 设置 ping/pong 处理器
	conn.SetPingHandler(func(message string) error {
		s.logger.Debug("Received ping", zap.String("connection_id", connection.ID))
		return conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second))
	})

	conn.SetPongHandler(func(message string) error {
		s.logger.Debug("Received pong", zap.String("connection_id", connection.ID))
		connection.LastSeen = time.Now()
		return nil
	})

	// 创建 done channel 用于控制 ping goroutine 的生命周期
	pingDone := make(chan struct{})
	defer close(pingDone)

	// 启动 ping 协程
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("Panic in ping goroutine - stopping",
					zap.Any("panic", r),
					zap.String("connection_id", connection.ID))
			}
		}()

		for {
			select {
			case <-pingTicker.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second)); err != nil {
					s.logger.Debug("Failed to send ping, connection may be dead",
						zap.String("connection_id", connection.ID),
						zap.Error(err))
					return
				}
			case <-pingDone:
				s.logger.Debug("Ping goroutine stopping",
					zap.String("connection_id", connection.ID))
				return
			case <-s.ctx.Done():
				s.logger.Debug("Ping goroutine stopping due to context cancellation",
					zap.String("connection_id", connection.ID))
				return
			}
		}
	}()

	// 处理消息
	s.handleConnection(conn, connection)
}

// handleConnection 处理连接消息
func (s *ShareDBService) handleConnection(conn *websocket.Conn, connection *Connection) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Panic in ShareDB connection handler - cleaning up and exiting",
				zap.Any("panic", r),
				zap.String("connection_id", connection.ID),
				zap.String("user_id", connection.UserID))

			// 立即清理连接资源并退出
			s.cleanupConnection(conn, connection)
			return
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// 读取消息 - 添加panic恢复
		var data []byte
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("Panic during WebSocket read",
						zap.Any("panic", r),
						zap.String("connection_id", connection.ID))
					err = fmt.Errorf("panic during read: %v", r)
				}
			}()
			_, data, err = conn.ReadMessage()
		}()

		if err != nil {
			// 检查是否是超时错误
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.logger.Debug("WebSocket read timeout, sending ping",
					zap.String("connection_id", connection.ID))
				// 发送 ping 检查连接是否还活着
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second)); err != nil {
					s.logger.Debug("Failed to send ping, connection is dead",
						zap.String("connection_id", connection.ID),
						zap.Error(err))
					return
				}
				continue
			}

			// 检查是否是正常的关闭错误
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived) {
				s.logger.Info("ShareDB WebSocket connection closed normally",
					zap.String("connection_id", connection.ID),
					zap.String("error", err.Error()))
				break
			}

			// 检查是否是连接重置或管道破裂错误
			if strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "use of closed network connection") ||
				strings.Contains(err.Error(), "repeated read on failed websocket connection") {
				s.logger.Debug("WebSocket connection lost (client disconnected)",
					zap.String("connection_id", connection.ID),
					zap.String("error", err.Error()))
				break
			}

			// 其他错误
			s.logger.Error("ShareDB WebSocket connection error",
				zap.Error(err),
				zap.String("connection_id", connection.ID))
			break
		}

		// 更新最后活跃时间
		connection.LastSeen = time.Now()

		// 解析消息
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			s.logger.Error("Failed to parse ShareDB message", zap.Error(err))
			continue
		}

		// 处理消息
		if err := s.handleMessage(conn, connection, &msg); err != nil {
			// 检查是否是连接丢失错误
			if strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "use of closed network connection") ||
				strings.Contains(err.Error(), "connection lost") {
				s.logger.Debug("ShareDB connection lost (client disconnected)",
					zap.String("connection_id", connection.ID),
					zap.String("error", err.Error()))
				break
			}

			s.logger.Error("Failed to handle ShareDB message", zap.Error(err))

			// 发送错误响应
			s.sendError(conn, &msg, errors.NewShareDBError("SERVER_ERROR", err.Error()))
		}
	}
}

// handleMessage 处理消息
func (s *ShareDBService) handleMessage(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 应用中间件
	for _, middleware := range s.middleware {
		if err := middleware.Handle(s.ctx, connection, msg); err != nil {
			return err
		}
	}

	// 根据消息类型处理
	switch msg.Action {
	case "hs": // handshake
		return s.handleHandshake(conn, connection, msg)
	case "f": // fetch
		return s.handleFetch(conn, connection, msg)
	case "s": // subscribe
		return s.handleSubscribe(conn, connection, msg)
	case "us": // unsubscribe
		return s.handleUnsubscribe(conn, connection, msg)
	case "op": // operation
		return s.handleOperation(conn, connection, msg)
	case "p": // presence
		return s.handlePresence(conn, connection, msg)
	case "pp": // presence ping
		return s.handlePresencePing(conn, connection, msg)
	default:
		return s.sendError(conn, msg, errors.NewShareDBError(errors.ErrOperationInvalid, "Unknown action: "+msg.Action))
	}
}

// handleHandshake 处理握手
func (s *ShareDBService) handleHandshake(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 发送握手响应 - ShareDB 客户端期望直接包含 protocol 和 type 字段
	response := Message{
		Action:   "hs",
		Protocol: 1, // ShareDB 客户端期望数字版本
		Type:     "json0",
		ID:       connection.ID,
	}
	return s.sendMessage(conn, &response)
}

// handleFetch 处理获取请求
func (s *ShareDBService) handleFetch(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 获取文档快照
	snapshot, err := s.adapter.GetSnapshot(s.ctx, msg.Collection, msg.DocID, nil)
	if err != nil {
		return err
	}

	// 发送快照
	response := Message{
		Action:     "f",
		Collection: msg.Collection,
		DocID:      msg.DocID,
		Data:       snapshot,
	}
	return s.sendMessage(conn, &response)
}

// handleSubscribe 处理订阅
func (s *ShareDBService) handleSubscribe(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 订阅文档
	channel := msg.Collection + "." + msg.DocID
	collection := msg.Collection
	docID := msg.DocID

	s.logger.Info("处理订阅请求",
		zap.String("connection_id", connection.ID),
		zap.String("collection", collection),
		zap.String("docID", docID),
		zap.String("channel", channel))

	// 为每个连接创建独立的 context，当连接断开时取消订阅
	subCtx, cancel := context.WithCancel(s.ctx)
	
	// 存储 cancel 函数，以便在连接断开时取消订阅
	connection.mu.Lock()
	if connection.subCancelFuncs == nil {
		connection.subCancelFuncs = make(map[string]context.CancelFunc)
	}
	// 如果已经订阅过这个 channel，先取消之前的订阅
	if oldCancel, exists := connection.subCancelFuncs[channel]; exists {
		oldCancel()
	}
	connection.subCancelFuncs[channel] = cancel
	connection.mu.Unlock()

	// 订阅操作更新
	if err := s.pubsub.Subscribe(subCtx, channel, func(op *opbuilder.Operation) {
		s.logger.Info("收到 PubSub 操作",
			zap.String("connection_id", connection.ID),
			zap.String("channel", channel),
			zap.Int("op_path_len", len(op.Path)))

		// 检查连接是否还活着
		if !s.isConnectionAlive(conn) {
			s.logger.Debug("Connection is dead, skipping message send",
				zap.String("connection_id", connection.ID),
				zap.String("channel", channel))
			return
		}

		// 发送操作到客户端
		// op.Path 的结构是 []interface{}{[]OTOperation}，需要提取操作数组
		var ops []OTOperation
		if len(op.Path) > 0 {
			if opsArray, ok := op.Path[0].([]OTOperation); ok {
				ops = opsArray
			} else if opsInterface, ok := op.Path[0].([]interface{}); ok {
				// 兼容处理：如果是 []interface{}，尝试转换
				for _, opItem := range opsInterface {
					if otOp, ok := opItem.(map[string]interface{}); ok {
						ops = append(ops, OTOperation(otOp))
					}
				}
			}
		}
		
		response := Message{
			Action:     "op",
			Collection: collection,
			DocID:      docID,
			Op:         ops,
			Version:    0, // 版本号会在后续从数据库获取
		}

		s.logger.Info("向客户端发送操作",
			zap.String("connection_id", connection.ID),
			zap.String("channel", channel),
			zap.Int("ops_count", len(ops)))

		if err := s.sendMessage(conn, &response); err != nil {
			// 连接丢失错误는 Debug 级别로 처리
			if strings.Contains(err.Error(), "connection lost") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "connection reset") {
				s.logger.Debug("Connection lost while sending operation",
					zap.String("connection_id", connection.ID),
					zap.String("channel", channel))
			} else {
				s.logger.Error("发送操作到客户端失败",
					zap.Error(err),
					zap.String("channel", channel))
			}
		} else {
			s.logger.Debug("操作已成功发送到客户端",
				zap.String("connection_id", connection.ID),
				zap.String("channel", channel))
		}
	}); err != nil {
		s.logger.Error("订阅失败",
			zap.Error(err),
			zap.String("channel", channel))
		// 清理 cancel 函数
		connection.mu.Lock()
		delete(connection.subCancelFuncs, channel)
		connection.mu.Unlock()
		cancel()
		return err
	}

	s.logger.Info("订阅成功",
		zap.String("connection_id", connection.ID),
		zap.String("channel", channel))

	// 获取文档当前状态
	// PostgresAdapter.GetSnapshot 期望 collection 字符串，它会内部解析
	s.logger.Debug("获取文档快照",
		zap.String("collection", msg.Collection),
		zap.String("doc_id", msg.DocID))
	
	snapshot, err := s.adapter.GetSnapshot(s.ctx, msg.Collection, msg.DocID, nil)
	if err != nil {
		s.logger.Warn("获取文档快照失败，返回空数据（记录可能尚未创建）",
			zap.Error(err),
			zap.String("collection", msg.Collection),
			zap.String("doc_id", msg.DocID))
		// 即使获取失败，也发送一个空数据，确保客户端收到 load 事件
		// 这样可以支持客户端先订阅，后创建数据的场景
		response := Message{
			Action:     "s",
			Collection: msg.Collection,
			DocID:      msg.DocID,
			Data:       map[string]interface{}{"data": map[string]interface{}{}}, // 返回格式化的空数据
			Version:    0,
		}
		if sendErr := s.sendMessage(conn, &response); sendErr != nil {
			return sendErr
		}
		return nil // 不返回错误，允许继续订阅
	}

	// 发送订阅确认，包含文档数据和版本
	response := Message{
		Action:     "s",
		Collection: msg.Collection,
		DocID:      msg.DocID,
		Data:       snapshot.Data,
		Version:    snapshot.Version,
	}
	
	s.logger.Debug("发送订阅确认",
		zap.String("connection_id", connection.ID),
		zap.String("collection", msg.Collection),
		zap.String("doc_id", msg.DocID),
		zap.Any("data", snapshot.Data),
		zap.Int64("version", snapshot.Version))
	
	return s.sendMessage(conn, &response)
}

// handleUnsubscribe 处理取消订阅
func (s *ShareDBService) handleUnsubscribe(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 取消订阅文档
	channel := msg.Collection + "." + msg.DocID

	s.logger.Info("处理取消订阅请求",
		zap.String("connection_id", connection.ID),
		zap.String("channel", channel))

	// 取消订阅操作更新
	if err := s.pubsub.Unsubscribe(s.ctx, channel); err != nil {
		s.logger.Error("取消订阅失败",
			zap.Error(err),
			zap.String("channel", channel))
		return err
	}

	// 发送取消订阅确认
	response := Message{
		Action:     "us",
		Collection: msg.Collection,
		DocID:      msg.DocID,
	}
	return s.sendMessage(conn, &response)
}

// handleOperation 处理操作
func (s *ShareDBService) handleOperation(conn *websocket.Conn, connection *Connection, msg *Message) (err error) {
	// 添加 panic 恢复机制
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("处理操作时发生 panic",
				zap.Any("panic", r),
				zap.String("collection", msg.Collection),
				zap.String("docID", msg.DocID))
			err = fmt.Errorf("处理操作时发生 panic: %v", r)
		}
	}()

	// 使用性能监控包装操作
	return s.perfMiddleware.WrapOperation("handle_operation", func() error {
		return s.doHandleOperation(conn, connection, msg)
	})
}

// doHandleOperation 实际的操作处理逻辑
func (s *ShareDBService) doHandleOperation(conn *websocket.Conn, connection *Connection, msg *Message) error {
	s.logger.Info("处理 ShareDB 操作",
		zap.String("collection", msg.Collection),
		zap.String("docID", msg.DocID),
		zap.Int64("version", msg.Version),
		zap.Int("opCount", len(msg.Op)))

	// 基本验证
	if len(msg.Op) == 0 {
		s.logger.Error("操作列表为空")
		return s.sendError(conn, msg, errors.NewShareDBError(errors.ErrOperationInvalid, "operation list cannot be empty"))
	}

	// 创建操作
	op := &opbuilder.Operation{
		Path:     []interface{}{msg.Op},
		OldValue: nil,
		NewValue: nil,
		Type:     opbuilder.OpTypeSet,
	}

	// 提交操作
	if err := s.SubmitOp(s.ctx, msg.Collection, msg.DocID, op); err != nil {
		return err
	}

	// 发布操作
	s.logger.Info("开始发布操作",
		zap.String("collection", msg.Collection),
		zap.String("docID", msg.DocID),
		zap.Int64("version", msg.Version))

	if err := s.PublishOp(s.ctx, msg.Collection, msg.DocID, op); err != nil {
		s.logger.Error("发布操作失败", zap.Error(err))
		return err
	}

	s.logger.Info("操作发布成功")

	// 发送操作确认响应
	response := &Message{
		Action:     "op",
		Collection: msg.Collection,
		DocID:      msg.DocID,
		Version:    msg.Version,
		Op:         msg.Op,
	}

	s.logger.Debug("发送操作确认响应",
		zap.String("collection", msg.Collection),
		zap.String("docID", msg.DocID),
		zap.Int64("version", msg.Version))

	return s.sendMessage(conn, response)
}

// handlePresence 处理在线状态
func (s *ShareDBService) handlePresence(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 解析在线状态数据
	var presenceData PresenceData
	if msg.Presence != nil {
		presenceData = PresenceData{
			UserID:    connection.UserID,
			Data:      msg.Presence,
			Timestamp: time.Now().Unix(),
		}
	}

	// 提交在线状态
	channel := msg.Collection + "." + msg.DocID
	if err := s.presence.Submit(s.ctx, channel, connection.ID, presenceData); err != nil {
		return err
	}

	// 广播在线状态到其他客户端
	presences, err := s.presence.GetPresences(s.ctx, channel)
	if err != nil {
		return err
	}

	// 转换在线状态数据
	presenceMap := make(map[string]interface{})
	for clientID, presence := range presences {
		presenceMap[clientID] = presence
	}

	// 发送在线状态更新
	response := Message{
		Action:     "p",
		Collection: msg.Collection,
		DocID:      msg.DocID,
		Presence:   presenceMap,
	}
	return s.sendMessage(conn, &response)
}

// SubmitOp 提交操作
func (s *ShareDBService) SubmitOp(ctx context.Context, collection, docID string, op *opbuilder.Operation) error {
	// 获取或创建事务上下文
	txCtx := sharedb.GetOrCreateTransactionContext(ctx)

	// 将操作添加到事务上下文中
	opMap := map[string]opbuilder.Operation{
		docID: *op,
	}
	txCtx.AddRawOpMap(opMap)

	s.logger.Info("Operation submitted",
		zap.String("collection", collection),
		zap.String("doc_id", docID),
		zap.String("op_type", string(op.Type)))

	// 触发事件钩子
	if s.eventHook != nil {
		// 转换为 Operation 类型
		operation := &Operation{
			Op: []OTOperation{},
		}
		s.eventHook.AfterCommit(collection, docID, operation)
	}

	return nil
}

// WithTransaction 在事务中执行操作
func (s *ShareDBService) WithTransaction(fn func(context.Context) error) error {
	// 创建新的事务上下文
	txCtx := sharedb.NewTransactionContext()
	ctx := sharedb.WithTransactionContext(context.Background(), txCtx)

	// 执行业务逻辑
	if err := fn(ctx); err != nil {
		return err
	}

	// 事务提交后发布所有操作
	return s.publishOpsInTransaction(txCtx)
}

// publishOpsInTransaction 在事务中发布所有操作
func (s *ShareDBService) publishOpsInTransaction(txCtx *sharedb.TransactionContext) error {
	if txCtx.IsEmpty() {
		return nil
	}

	// 发布所有原始操作映射
	for _, opMap := range txCtx.GetRawOpMaps() {
		for docID, op := range opMap {
			// 这里需要从操作中提取 collection 信息
			// 暂时使用默认的 collection 格式
			collection := "rec_" + docID // 这里需要根据实际情况调整

			if err := s.PublishOp(context.Background(), collection, docID, &op); err != nil {
				s.logger.Error("Failed to publish operation in transaction",
					zap.String("collection", collection),
					zap.String("doc_id", docID),
					zap.Error(err))
				return err
			}
		}
	}

	// 清理缓存键
	cacheKeys := txCtx.GetCacheKeys()
	if len(cacheKeys) > 0 {
		s.logger.Info("Clearing cache keys", zap.Strings("keys", cacheKeys))
		// 这里可以调用缓存清理逻辑
		// s.cacheService.ClearKeys(cacheKeys)
	}

	// 清空事务上下文
	txCtx.Clear()

	return nil
}

// PublishOp 发布操作（对齐 Teable）
func (s *ShareDBService) PublishOp(ctx context.Context, collection, docID string, op *opbuilder.Operation) error {
	// 直接发布到 pubsub

	channels := []string{
		collection,               // 广播到整个 collection (rec_tableId)
		collection + "." + docID, // 广播到特定文档 (rec_tableId.recordId)
	}

	s.logger.Info("发布 ShareDB 操作",
		zap.String("collection", collection),
		zap.String("docID", docID),
		zap.Strings("channels", channels),
		zap.Int("op_path_len", len(op.Path)))

	if err := s.pubsub.Publish(ctx, channels, op); err != nil {
		s.logger.Error("PubSub 发布失败",
			zap.Error(err),
			zap.String("collection", collection),
			zap.String("docID", docID))
		return err
	}

	s.logger.Info("ShareDB 操作发布成功",
		zap.String("collection", collection),
		zap.String("docID", docID),
		zap.Strings("channels", channels))

	return nil
}

// isConnectionAlive 检查连接是否还活着
func (s *ShareDBService) isConnectionAlive(conn *websocket.Conn) bool {
	// 尝试发送 ping 来检查连接状态
	err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second))
	return err == nil
}

// sendMessage 发送消息
func (s *ShareDBService) sendMessage(conn *websocket.Conn, msg *Message) error {
	// 使用互斥锁保护 WebSocket 写入，避免并发写入
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// 设置写入超时
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	err = conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		// 检查是否是连接相关的错误
		if strings.Contains(err.Error(), "broken pipe") ||
			strings.Contains(err.Error(), "connection reset") ||
			strings.Contains(err.Error(), "use of closed network connection") ||
			strings.Contains(err.Error(), "repeated read on failed websocket connection") {
			return fmt.Errorf("connection lost: %w", err)
		}
		return err
	}

	return nil
}

// sendError 发送错误
func (s *ShareDBService) sendError(conn *websocket.Conn, msg *Message, err *errors.ShareDBError) error {
	response := Message{
		Action:     msg.Action,
		Collection: msg.Collection,
		DocID:      msg.DocID,
		Error:      err,
	}
	return s.sendMessage(conn, &response)
}

// GetStats 获取统计信息
func (s *ShareDBService) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"connections": 0,
		"documents":   0,
		"timestamp":   time.Now().Unix(),
	}

	// 统计连接数
	connectionCount := 0
	s.connections.Range(func(key, value interface{}) bool {
		connectionCount++
		return true
	})
	stats["connections"] = connectionCount

	// 统计文档数
	documentCount := 0
	s.documents.Range(func(key, value interface{}) bool {
		documentCount++
		return true
	})
	stats["documents"] = documentCount

	// 添加性能指标
	if s.perfMonitor != nil {
		perfMetrics := s.perfMonitor.GetMetrics()
		stats["performance"] = perfMetrics
	}

	return stats
}

// GetPerformanceMetrics 获取性能指标
func (s *ShareDBService) GetPerformanceMetrics() *monitoring.PerformanceMetrics {
	if s.perfMonitor != nil {
		return s.perfMonitor.GetMetrics()
	}
	return nil
}

// GetOperationStats 获取操作统计
func (s *ShareDBService) GetOperationStats() map[string]interface{} {
	if s.perfMonitor != nil {
		return s.perfMonitor.GetOperationStats()
	}
	return nil
}

// Shutdown 关闭服务
func (s *ShareDBService) Shutdown() error {
	s.cancel()

	// 关闭所有连接
	s.connections.Range(func(key, value interface{}) bool {
		// 这里可以关闭连接
		return true
	})

	// 关闭适配器
	if s.adapter != nil {
		s.adapter.Close()
	}

	// 关闭发布订阅
	if s.pubsub != nil {
		s.pubsub.Close()
	}

	// 关闭在线状态管理器
	if s.presence != nil {
		s.presence.Close()
	}

	s.logger.Info("ShareDB service shutdown completed")
	return nil
}

// cleanupRoutine 清理协程
func (s *ShareDBService) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanupInactiveConnections()
		}
	}
}

// cleanupInactiveConnections 清理非活跃连接
func (s *ShareDBService) cleanupInactiveConnections() {
	now := time.Now()
	timeout := 2 * time.Minute // 缩短超时时间，更快清理

	cleanedCount := 0
	s.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		if now.Sub(conn.LastSeen) > timeout {
			conn.IsActive = false
			s.connections.Delete(key)
			cleanedCount++
			s.logger.Info("Cleaned up inactive connection",
				zap.String("connection_id", conn.ID),
				zap.String("user_id", conn.UserID),
				zap.Duration("inactive_duration", now.Sub(conn.LastSeen)))
		}
		return true
	})

	if cleanedCount > 0 {
		s.logger.Info("Connection cleanup completed",
			zap.Int("cleaned_connections", cleanedCount))
	}
}

// ForceCleanupAllConnections 强制清理所有连接（开发环境使用）
func (s *ShareDBService) ForceCleanupAllConnections() {
	cleanedCount := 0
	s.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.IsActive = false
		s.connections.Delete(key)
		cleanedCount++
		return true
	})

	s.logger.Info("Force cleanup all connections completed",
		zap.Int("cleaned_connections", cleanedCount))
}

// generateConnectionID 生成连接ID
func generateConnectionID() string {
	return "conn_" + time.Now().Format("20060102150405") + "_" + randomString(8)
}

// randomString 生成随机字符串
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}

// handlePresencePing 处理 presence ping
func (s *ShareDBService) handlePresencePing(conn *websocket.Conn, connection *Connection, msg *Message) error {
	// 简单的 presence ping 响应
	response := Message{
		Action: "pp",
	}
	return s.sendMessage(conn, &response)
}

// BroadcastOperation 广播ShareDB操作到所有连接的客户端
func (s *ShareDBService) BroadcastOperation(tableID, recordID string, ops []OTOperation) error {
	collection := fmt.Sprintf("rec_%s", tableID)

	// 创建操作消息
	msg := Message{
		Action:     "op",
		Collection: collection,
		DocID:      recordID,
		Op:         ops,
		Version:    1, // 版本号，实际应该从数据库获取
	}

	// 广播到所有连接的客户端
	count := 0
	s.wsConnections.Range(func(key, value interface{}) bool {
		connID := key.(string)
		conn := value.(*websocket.Conn)

		if err := s.sendMessage(conn, &msg); err != nil {
			s.logger.Debug("广播操作失败",
				zap.String("connection_id", connID),
				zap.Error(err))
		} else {
			count++
		}
		return true
	})

	s.logger.Info("ShareDB操作已广播",
		zap.String("collection", collection),
		zap.String("doc_id", recordID),
		zap.Int("connections", count))

	return nil
}
