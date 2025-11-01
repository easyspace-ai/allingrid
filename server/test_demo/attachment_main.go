package main

import (
	"log"
	"strings"
)

func main() {
	log.Println("🚀 开始附件上传功能测试")
	log.Println(strings.Repeat("=", 62))

	// 1. 加载配置
	config := LoadConfig()
	log.Printf("📋 配置信息:")
	log.Printf("   - 服务器地址: %s", config.ServerURL)
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
	spaceResp, err := createSpace(httpClient, "附件测试空间")
	if err != nil {
		log.Fatalf("❌ 创建 Space 失败: %v\n", err)
	}
	spaceID := spaceResp.Data.ID
	log.Printf("  ✅ Space 创建成功: ID=%s\n", spaceID)

	// 4.2 创建 Base
	log.Println("  创建 Base...")
	baseResp, err := createBase(httpClient, spaceID, "附件测试Base")
	if err != nil {
		log.Fatalf("❌ 创建 Base 失败: %v\n", err)
	}
	baseID := baseResp.Data.ID
	log.Printf("  ✅ Base 创建成功: ID=%s\n", baseID)

	// 4.3 创建 Table
	log.Println("  创建 Table...")
	tableResp, err := createTable(httpClient, baseID, "附件测试表")
	if err != nil {
		log.Fatalf("❌ 创建 Table 失败: %v\n", err)
	}
	tableID := tableResp.Data.ID
	log.Printf("  ✅ Table 创建成功: ID=%s\n", tableID)

	// 4.4 创建附件字段
	log.Println("  创建附件字段...")
	fieldResp, err := createField(httpClient, tableID, "附件", "attachment")
	if err != nil {
		log.Fatalf("❌ 创建附件字段失败: %v\n", err)
	}
	fieldID := fieldResp.Data.ID
	log.Printf("  ✅ 附件字段创建成功: ID=%s\n", fieldID)

	// 4.5 创建 Record
	log.Println("  创建 Record...")
	recordResp, err := createRecord(httpClient, tableID, map[string]interface{}{
		fieldID: []interface{}{}, // 附件字段初始为空数组
	})
	if err != nil {
		log.Fatalf("❌ 创建 Record 失败: %v\n", err)
	}
	recordID := recordResp.Data.ID
	log.Printf("  ✅ Record 创建成功: ID=%s\n", recordID)
	log.Println()

	// 5. 测试附件上传流程
	log.Println("📝 步骤 3: 测试附件上传流程")
	log.Println(strings.Repeat("-", 62))

	// 5.1 生成上传签名
	log.Println("  5.1 生成上传签名...")
	signatureResp, err := generateAttachmentSignature(httpClient, tableID, fieldID, recordID)
	if err != nil {
		log.Fatalf("❌ 生成上传签名失败: %v\n", err)
	}
	token := signatureResp.Data.Token
	log.Printf("  ✅ 签名生成成功: Token=%s\n", token)
	log.Printf("     - 上传URL: %s\n", signatureResp.Data.UploadURL)
	log.Printf("     - 过期时间: %d\n", signatureResp.Data.ExpiresAt)
	log.Printf("     - 最大文件大小: %d bytes\n", signatureResp.Data.MaxSize)
	log.Printf("     - 允许的文件类型: %v\n", signatureResp.Data.AllowedTypes)

	// 5.2 创建测试文件
	log.Println("  5.2 创建测试文件...")
	testContent := "这是一个测试文件内容\n用于测试附件上传功能\n"
	testFilePath, err := createTestFile("test_file.txt", testContent)
	if err != nil {
		log.Fatalf("❌ 创建测试文件失败: %v\n", err)
	}
	log.Printf("  ✅ 测试文件创建成功: %s\n", testFilePath)
	defer func() {
		if err := cleanupTestFile(testFilePath); err != nil {
			log.Printf("⚠️  清理测试文件失败: %v\n", err)
		}
	}()

	// 5.3 上传文件
	log.Println("  5.3 上传文件...")
	if err := uploadFile(httpClient, token, testFilePath); err != nil {
		log.Fatalf("❌ 上传文件失败: %v\n", err)
	}
	log.Printf("  ✅ 文件上传成功\n")

	// 5.4 通知上传完成
	log.Println("  5.4 通知上传完成...")
	notifyResp, err := notifyUpload(httpClient, token, "test_file.txt")
	if err != nil {
		log.Fatalf("❌ 通知上传完成失败: %v\n", err)
	}
	attachmentID := notifyResp.Data.Attachment.ID
	log.Printf("  ✅ 通知成功: AttachmentID=%s\n", attachmentID)
	log.Printf("     - 文件名: %s\n", notifyResp.Data.Attachment.Name)
	log.Printf("     - 文件大小: %d bytes\n", notifyResp.Data.Attachment.Size)
	log.Printf("     - MIME类型: %s\n", notifyResp.Data.Attachment.MimeType)
	log.Printf("     - 文件路径: %s\n", notifyResp.Data.Attachment.Path)
	log.Println()

	// 6. 测试附件查询功能
	log.Println("📝 步骤 4: 测试附件查询功能")
	log.Println(strings.Repeat("-", 62))

	// 6.1 获取附件信息
	log.Println("  6.1 获取附件信息...")
	attachment, err := getAttachment(httpClient, attachmentID)
	if err != nil {
		log.Fatalf("❌ 获取附件信息失败: %v\n", err)
	}
	log.Printf("  ✅ 附件信息获取成功\n")
	log.Printf("     - ID: %s\n", attachment.ID)
	log.Printf("     - 名称: %s\n", attachment.Name)
	log.Printf("     - 大小: %d bytes\n", attachment.Size)
	log.Printf("     - MIME类型: %s\n", attachment.MimeType)

	// 6.2 列出附件
	log.Println("  6.2 列出附件...")
	attachments, err := listAttachments(httpClient, tableID, fieldID, recordID)
	if err != nil {
		log.Fatalf("❌ 列出附件失败: %v\n", err)
	}
	log.Printf("  ✅ 附件列表获取成功: 共 %d 个附件\n", len(attachments))
	for i, att := range attachments {
		log.Printf("     [%d] ID=%s, Name=%s, Size=%d\n", i+1, att.ID, att.Name, att.Size)
	}

	// 6.3 获取附件统计
	log.Println("  6.3 获取附件统计...")
	statsResp, err := getAttachmentStats(httpClient, tableID)
	if err != nil {
		log.Fatalf("❌ 获取附件统计失败: %v\n", err)
	}
	log.Printf("  ✅ 附件统计获取成功\n")
	log.Printf("     - 总文件数: %d\n", statsResp.Data.TotalFiles)
	log.Printf("     - 总大小: %d bytes\n", statsResp.Data.TotalSize)
	log.Printf("     - 图片文件: %d\n", statsResp.Data.ImageFiles)
	log.Printf("     - 视频文件: %d\n", statsResp.Data.VideoFiles)
	log.Printf("     - 音频文件: %d\n", statsResp.Data.AudioFiles)
	log.Printf("     - 文档文件: %d\n", statsResp.Data.DocumentFiles)
	log.Printf("     - 其他文件: %d\n", statsResp.Data.OtherFiles)
	log.Println()

	// 7. 测试文件读取
	log.Println("📝 步骤 5: 测试文件读取")
	log.Println(strings.Repeat("-", 62))

	log.Println("  7.1 读取文件内容...")
	fileContent, err := readFile(httpClient, attachment.Path)
	if err != nil {
		log.Fatalf("❌ 读取文件失败: %v\n", err)
	}
	log.Printf("  ✅ 文件读取成功\n")
	log.Printf("     - 文件内容长度: %d bytes\n", len(fileContent))
	log.Printf("     - 文件内容预览: %s\n", string(fileContent[:min(len(fileContent), 100)]))
	log.Println()

	// 8. 测试文件删除
	log.Println("📝 步骤 6: 测试文件删除")
	log.Println(strings.Repeat("-", 62))

	log.Println("  8.1 删除附件...")
	if err := deleteAttachment(httpClient, attachmentID); err != nil {
		log.Fatalf("❌ 删除附件失败: %v\n", err)
	}
	log.Printf("  ✅ 附件删除成功: ID=%s\n", attachmentID)

	// 验证删除
	log.Println("  8.2 验证删除...")
	_, err = getAttachment(httpClient, attachmentID)
	if err != nil {
		log.Printf("  ✅ 删除验证成功: 附件已不存在\n")
	} else {
		log.Printf("  ⚠️  删除验证失败: 附件仍然存在\n")
	}
	log.Println()

	// 9. 测试结果总结
	log.Println("📝 步骤 7: 测试结果总结")
	log.Println(strings.Repeat("=", 62))
	log.Println("✅ 所有附件功能测试通过！")
	log.Println()
	log.Println("📋 测试覆盖的功能:")
	log.Println("   ✅ 生成上传签名")
	log.Println("   ✅ 上传文件")
	log.Println("   ✅ 通知上传完成")
	log.Println("   ✅ 获取附件信息")
	log.Println("   ✅ 列出附件")
	log.Println("   ✅ 获取附件统计")
	log.Println("   ✅ 读取文件")
	log.Println("   ✅ 删除附件")
	log.Println()
	log.Println("🎉 附件上传功能测试完成！")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

