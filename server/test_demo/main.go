package main

import (
	"fmt"
	"log"
	"strings"
	"time"
)

func main() {
	log.Println("🚀 开始 ShareDB 实时同步测试演示")
	log.Println(strings.Repeat("=", 62))

	// 1. 加载配置
	config := LoadConfig()
	log.Printf("📋 配置信息:")
	log.Printf("   - 服务器地址: %s", config.ServerURL)
	log.Printf("   - WebSocket 地址: %s", config.WebSocketURL)
	log.Printf("   - 测试邮箱: %s", config.TestEmail)
	log.Println()

	// 2. 创建 HTTP 客户端
	httpClient := NewHTTPClient(config.ServerURL)

	// 3. 注册并登录
	log.Println("📝 步骤 1: 注册/登录测试账号")
	authResp, err := registerAndLogin(httpClient, config.TestEmail, config.TestPassword, config.TestName)
	if err != nil {
		log.Fatalf("❌ 注册/登录失败: %v\n", err)
	}
	log.Printf("✅ 登录成功: UserID=%s, Email=%s\n", authResp.Data.User.ID, authResp.Data.User.Email)
	httpClient.SetToken(authResp.Data.AccessToken)
	log.Println()

	// 4. 创建资源链
	log.Println("📝 步骤 2: 创建资源链 (Space -> Base -> Table -> Field -> Record)")

	// 4.1 创建 Space
	log.Println("  创建 Space...")
	spaceResp, err := createSpace(httpClient, "测试空间")
	if err != nil {
		log.Fatalf("❌ 创建 Space 失败: %v\n", err)
	}
	spaceID := spaceResp.Data.ID
	log.Printf("  ✅ Space 创建成功: ID=%s\n", spaceID)

	// 4.2 创建 Base
	log.Println("  创建 Base...")
	baseResp, err := createBase(httpClient, spaceID, "测试Base")
	if err != nil {
		log.Fatalf("❌ 创建 Base 失败: %v\n", err)
	}
	baseID := baseResp.Data.ID
	log.Printf("  ✅ Base 创建成功: ID=%s\n", baseID)

	// 4.3 创建 Table
	log.Println("  创建 Table...")
	tableResp, err := createTable(httpClient, baseID, "测试表")
	if err != nil {
		log.Fatalf("❌ 创建 Table 失败: %v\n", err)
	}
	tableID := tableResp.Data.ID
	log.Printf("  ✅ Table 创建成功: ID=%s\n", tableID)

	// 4.4 创建 Field
	log.Println("  创建 Field...")
	fieldResp, err := createField(httpClient, tableID, "标题", "singleLineText")
	if err != nil {
		log.Fatalf("❌ 创建 Field 失败: %v\n", err)
	}
	fieldID := fieldResp.Data.ID
	log.Printf("  ✅ Field 创建成功: ID=%s, Name=%s\n", fieldID, fieldResp.Data.Name)

	// 4.5 创建 Record
	log.Println("  创建 Record...")
	recordFields := map[string]interface{}{
		fieldID: "初始值",
	}
	recordResp, err := createRecord(httpClient, tableID, recordFields)
	if err != nil {
		log.Fatalf("❌ 创建 Record 失败: %v\n", err)
	}
	recordID := recordResp.Data.ID
	log.Printf("  ✅ Record 创建成功: ID=%s\n", recordID)
	log.Println()

	// 5. 启动监听客户端
	log.Println("📝 步骤 3: 启动监听客户端")
	listener := NewListenerClient(config.WebSocketURL, authResp.Data.AccessToken)

	if err := listener.Connect(); err != nil {
		log.Fatalf("❌ WebSocket 连接失败: %v\n", err)
	}

	// 订阅记录
	collection := fmt.Sprintf("rec_%s", tableID)
	log.Printf("📡 订阅记录: collection=%s, docID=%s\n", collection, recordID)

	if err := listener.Subscribe(collection, recordID); err != nil {
		log.Fatalf("❌ 订阅失败: %v\n", err)
	}
	log.Println()

	// 等待一段时间确保订阅完成
	time.Sleep(1 * time.Second)

	// 6. 创建更新客户端并执行更新
	log.Println("📝 步骤 4: 执行记录更新并验证广播")
	updater := NewUpdaterClient(httpClient, tableID, recordID, fieldID)

	// 准备多个更新值
	updateValues := []string{
		"第一次更新",
		"第二次更新",
		"第三次更新",
		"最终值",
	}

	// 在协程中执行更新
	updateDone := make(chan error, 1)
	go func() {
		time.Sleep(2 * time.Second) // 给监听客户端一些时间准备
		if err := updater.UpdateFieldMultiple(updateValues, 2*time.Second); err != nil {
			updateDone <- err
			return
		}
		updateDone <- nil
	}()

	// 等待接收操作消息
	log.Println("⏳ 等待接收操作消息...")
	receivedOps := make([]*ShareDBMessage, 0)

	for i := 0; i < len(updateValues); i++ {
		msg, err := listener.WaitForOperation(10 * time.Second)
		if err != nil {
			log.Printf("⚠️  等待操作消息失败: %v\n", err)
			break
		}
		receivedOps = append(receivedOps, msg)
		log.Printf("✅ 收到操作消息 %d/%d\n", i+1, len(updateValues))
	}

	// 等待更新完成
	if err := <-updateDone; err != nil {
		log.Printf("⚠️  更新过程中出错: %v\n", err)
	}

	log.Println()

	// 7. 验证结果
	log.Println("📝 步骤 5: 验证测试结果")
	log.Println(strings.Repeat("=", 62))

	if len(receivedOps) == 0 {
		log.Println("❌ 测试失败: 未收到任何操作消息")
		log.Println("💡 可能的原因:")
		log.Println("   1. ShareDB 服务未正确配置")
		log.Println("   2. 记录更新未触发广播")
		log.Println("   3. WebSocket 连接问题")
		log.Println()
		log.Println("📋 所有接收到的消息:")
		listener.PrintReceivedMessages()
		return
	}

	log.Printf("✅ 测试成功！收到 %d 条操作消息\n", len(receivedOps))
	log.Println()

	// 打印收到的操作消息详情
	log.Println("📨 收到的操作消息详情:")
	for i, msg := range receivedOps {
		log.Printf("消息 %d:", i+1)
		log.Printf("  - Action: %s", msg.Action)
		log.Printf("  - Collection: %s", msg.Collection)
		log.Printf("  - DocID: %s", msg.DocID)
		log.Printf("  - Version: %d", msg.Version)
		log.Printf("  - Operations: %d", len(msg.Op))
		if len(msg.Op) > 0 {
			log.Printf("  - Op[0]: %v", msg.Op[0])
		}
		log.Println()
	}

	// 打印所有消息
	log.Println("📋 所有接收到的消息:")
	listener.PrintReceivedMessages()

	// 清理
	log.Println("\n🧹 清理资源...")
	if err := listener.Close(); err != nil {
		log.Printf("⚠️  关闭监听客户端失败: %v\n", err)
	}

	log.Println("\n✅ 测试演示完成！")
}

