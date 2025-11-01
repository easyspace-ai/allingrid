package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ShareDBMessage ShareDB 协议消息
type ShareDBMessage struct {
	Action     string        `json:"a"`              // action: hs, s, op, etc.
	Collection string        `json:"c,omitempty"`    // collection
	DocID      string        `json:"d,omitempty"`    // document ID
	Version    int64         `json:"v,omitempty"`   // version
	Op         []interface{} `json:"op,omitempty"`  // operations
	Data       interface{}   `json:"data,omitempty"` // data
	Protocol   int           `json:"protocol,omitempty"`
	Type       string        `json:"type,omitempty"`
	ID         interface{}   `json:"id,omitempty"`
	Error      interface{}   `json:"error,omitempty"`
}

// ListenerClient 监听客户端
type ListenerClient struct {
	wsURL      string
	token      string
	conn       *websocket.Conn
	messages   chan *ShareDBMessage
	received   []*ShareDBMessage
	mu         sync.RWMutex
	collection string
	docID      string
	connected  bool
}

// NewListenerClient 创建监听客户端
func NewListenerClient(wsURL, token string) *ListenerClient {
	return &ListenerClient{
		wsURL:    wsURL,
		token:    token,
		messages: make(chan *ShareDBMessage, 100),
		received: make([]*ShareDBMessage, 0),
	}
}

// Connect 连接到 WebSocket 服务器
func (c *ListenerClient) Connect() error {
	// 构建 WebSocket URL，添加认证头
	url := c.wsURL + "/socket"
	
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+c.token)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(url, header)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}

	c.conn = conn
	c.connected = true

	// 启动消息接收协程
	go c.receiveMessages()

	// 发送握手消息
	if err := c.handshake(); err != nil {
		c.conn.Close()
		c.connected = false
		return fmt.Errorf("handshake failed: %w", err)
	}

	log.Println("✅ WebSocket 连接成功，等待握手响应...")

	return nil
}

// handshake 发送握手消息
func (c *ListenerClient) handshake() error {
	msg := ShareDBMessage{
		Action: "hs",
	}

	return c.sendMessage(&msg)
}

// Subscribe 订阅文档
func (c *ListenerClient) Subscribe(collection, docID string) error {
	c.collection = collection
	c.docID = docID

	msg := ShareDBMessage{
		Action:     "s", // subscribe
		Collection: collection,
		DocID:      docID,
	}

	log.Printf("📡 订阅文档: collection=%s, docID=%s\n", collection, docID)

	if err := c.sendMessage(&msg); err != nil {
		return fmt.Errorf("send subscribe message: %w", err)
	}

	// 等待订阅确认
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case msg := <-c.messages:
			if msg.Action == "s" && msg.Collection == collection && msg.DocID == docID {
				log.Printf("✅ 订阅成功，收到初始数据: version=%d\n", msg.Version)
				return nil
			}
		case <-timeout.C:
			return fmt.Errorf("订阅超时")
		}
	}
}

// receiveMessages 接收消息
func (c *ListenerClient) receiveMessages() {
	for {
		var msg ShareDBMessage
		if err := c.conn.ReadJSON(&msg); err != nil {
			if c.connected {
				log.Printf("❌ 读取消息失败: %v\n", err)
			}
			return
		}

		// 处理握手响应
		if msg.Action == "hs" {
			log.Printf("✅ 收到握手响应: protocol=%d, type=%s\n", msg.Protocol, msg.Type)
		}

		// 存储消息
		c.mu.Lock()
		c.received = append(c.received, &msg)
		c.mu.Unlock()

		// 发送到通道
		select {
		case c.messages <- &msg:
		default:
			log.Println("⚠️  消息通道已满，丢弃消息")
		}
	}
}

// sendMessage 发送消息
func (c *ListenerClient) sendMessage(msg *ShareDBMessage) error {
	if c.conn == nil {
		return fmt.Errorf("websocket not connected")
	}

	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.conn.WriteJSON(msg)
}

// WaitForOperation 等待操作消息
func (c *ListenerClient) WaitForOperation(timeout time.Duration) (*ShareDBMessage, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case msg := <-c.messages:
			if msg.Action == "op" && msg.Collection == c.collection && msg.DocID == c.docID {
				log.Printf("📨 收到操作消息: version=%d, opCount=%d\n", msg.Version, len(msg.Op))
				return msg, nil
			}
		case <-timer.C:
			return nil, fmt.Errorf("等待操作消息超时")
		}
	}
}

// GetReceivedMessages 获取已接收的消息
func (c *ListenerClient) GetReceivedMessages() []*ShareDBMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*ShareDBMessage, len(c.received))
	copy(result, c.received)
	return result
}

// Close 关闭连接
func (c *ListenerClient) Close() error {
	c.connected = false
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// PrintReceivedMessages 打印已接收的消息
func (c *ListenerClient) PrintReceivedMessages() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	log.Printf("\n📋 已接收的消息总数: %d\n", len(c.received))
	for i, msg := range c.received {
		msgJSON, _ := json.MarshalIndent(msg, "", "  ")
		log.Printf("消息 %d:\n%s\n", i+1, string(msgJSON))
	}
}

