package http

import (
	"embed"

	"github.com/gin-gonic/gin"

	"github.com/easyspace-ai/luckdb/server/internal/container"
	"github.com/easyspace-ai/luckdb/server/pkg/logger"
)

// SetupRoutes 设置所有API路由
func SetupRoutes(router *gin.Engine, cont *container.Container, staticFiles embed.FS) {
	// 先设置 WebSocket 路由（必须在静态文件路由之前）
	setupWebSocketRoutes(router, cont)

	// 设置静态文件服务（前端应用）
	setupStaticFiles(router, staticFiles)

	// API v1路由组
	v1 := router.Group("/api/v1")

	// 监控端点（无需认证）
	setupMonitoringRoutes(v1, cont)

	// 认证相关路由（无需JWT中间件）
	setupAuthRoutes(v1, cont)

	// 需要JWT认证的路由组
	authRequired := v1.Group("")
	authRequired.Use(JWTAuthMiddleware(cont.AuthService()))
	{
		// 用户相关路由
		setupUserRoutes(authRequired, cont)

		// 用户配置路由
		setupUserConfigRoutes(authRequired, cont)

		// 空间相关路由
		setupSpaceRoutes(authRequired, cont)

		// Base相关路由
		setupBaseRoutes(authRequired, cont)

		// 表格相关路由
		setupTableRoutes(authRequired, cont)

		// 字段相关路由
		setupFieldRoutes(authRequired, cont)

		// 记录相关路由
		setupRecordRoutes(authRequired, cont)

		// 视图相关路由
		setupViewRoutes(authRequired, cont)

		// 附件相关路由 ✨
		setupAttachmentRoutes(authRequired, cont)

	}

	// WebSocket 路由（需要认证）✨
	//setupWebSocketRoutes(router, cont)

	// JSVM 管理路由（需要认证）✨
	setupJSVMRoutes(v1, cont)

	// 实时通信路由（无需认证）✨
	setupRealtimeRoutes(router, cont)

	// ShareDB 路由（需要认证）✨
	setupShareDBRoutes(v1, cont)

	// WebSocket 路由已在前面设置
}

// setupUserConfigRoutes 设置用户配置路由
func setupUserConfigRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewUserConfigHandler(cont.UserConfigService())

	// 用户配置路由
	userConfig := rg.Group("/user")
	{
		userConfig.GET("/config", handler.GetUserConfig)
		userConfig.PUT("/config", handler.UpdateUserConfig)
	}
}

// setupSpaceRoutes 设置空间路由
func setupSpaceRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewSpaceHandler(cont.SpaceService())
	collabHandler := NewCollaboratorHandler(cont.CollaboratorService())

	// 空间路由
	spaces := rg.Group("/spaces")
	{
		spaces.POST("", handler.CreateSpace)
		spaces.GET("", handler.ListSpaces)
		spaces.GET("/:spaceId", handler.GetSpace)
		spaces.PATCH("/:spaceId", handler.UpdateSpace) // ✅ 部分更新使用PATCH
		spaces.DELETE("/:spaceId", handler.DeleteSpace)

		// Space协作者管理 ✨
		spaces.GET("/:spaceId/collaborators", collabHandler.ListSpaceCollaborators)
		spaces.POST("/:spaceId/collaborators", collabHandler.AddSpaceCollaborator)
		spaces.PATCH("/:spaceId/collaborators/:collaboratorId", collabHandler.UpdateSpaceCollaborator)
		spaces.DELETE("/:spaceId/collaborators/:collaboratorId", collabHandler.RemoveSpaceCollaborator)
	}
}

// setupBaseRoutes 设置Base路由（对齐原版）
func setupBaseRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewBaseHandler(cont.BaseService(), cont.TableService())
	collabHandler := NewCollaboratorHandler(cont.CollaboratorService())

	// Space下的Base
	spaces := rg.Group("/spaces")
	{
		spaces.POST("/:spaceId/bases", handler.CreateBase)
		spaces.GET("/:spaceId/bases", handler.ListBases)
	}

	// Base路由
	bases := rg.Group("/bases")
	{
		bases.GET("/:baseId", handler.GetBase)
		bases.PATCH("/:baseId", handler.UpdateBase)
		bases.DELETE("/:baseId", handler.DeleteBase)

		// Base子资源
		// Note: GET /:baseId/tables 由TableHandler处理（避免重复注册）
		bases.POST("/:baseId/duplicate", handler.DuplicateBase)
		bases.GET("/:baseId/permission", handler.GetBasePermission)

		// Base协作者管理 ✨
		bases.GET("/:baseId/collaborators", collabHandler.ListBaseCollaborators)
		bases.POST("/:baseId/collaborators", collabHandler.AddBaseCollaborator)
		bases.PATCH("/:baseId/collaborators/:collaboratorId", collabHandler.UpdateBaseCollaborator)
		bases.DELETE("/:baseId/collaborators/:collaboratorId", collabHandler.RemoveBaseCollaborator)
	}
}

// setupTableRoutes 设置表格路由
func setupTableRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewTableHandler(cont.TableService())

	// Base下的表格
	bases := rg.Group("/bases")
	{
		bases.GET("/:baseId/tables", handler.ListTables)
		bases.POST("/:baseId/tables", handler.CreateTable)
	}

	// 表格路由
	tables := rg.Group("/tables")
	{
		tables.GET("/:tableId", handler.GetTable)
		tables.PATCH("/:tableId", handler.UpdateTable) // ✅ 部分更新使用PATCH
		tables.DELETE("/:tableId", handler.DeleteTable)

		// 表管理路由
		tables.PUT("/:tableId/rename", handler.RenameTable)          // 重命名表
		tables.POST("/:tableId/duplicate", handler.DuplicateTable)   // 复制表
		tables.GET("/:tableId/usage", handler.GetTableUsage)         // 获取表用量
		tables.GET("/:tableId/menu", handler.GetTableManagementMenu) // 获取表管理菜单
	}
}

// setupFieldRoutes 设置字段路由
func setupFieldRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewFieldHandler(cont.FieldService())

	// 表格下的字段
	tables := rg.Group("/tables")
	{
		tables.GET("/:tableId/fields", handler.ListFields)
		tables.POST("/:tableId/fields", handler.CreateField)
	}

	// 字段路由
	fields := rg.Group("/fields")
	{
		fields.GET("/:fieldId", handler.GetField)
		fields.PATCH("/:fieldId", handler.UpdateField) // ✅ 部分更新使用PATCH
		fields.DELETE("/:fieldId", handler.DeleteField)
	}
}

// setupRecordRoutes 设置记录路由
func setupRecordRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewRecordHandler(
		cont.RecordService(),
		cont.FieldService(),       // ✅ 添加
		cont.CalculationService(), // ✅ 添加
		cont.RecordRepository(),   // ✅ 添加
	)

	// 表格下的记录（对齐 Teable 架构：所有记录操作都需要 tableId）
	tables := rg.Group("/tables")
	{
		// 列表和创建
		tables.GET("/:tableId/records", handler.ListRecords)
		tables.POST("/:tableId/records", handler.CreateRecord)
		tables.POST("/:tableId/records/batch", handler.BatchCreateRecords)

		// 单条记录操作（需要 tableId 和 recordId）
		tables.GET("/:tableId/records/:recordId", handler.GetRecord)
		tables.PATCH("/:tableId/records/:recordId", handler.UpdateRecord) // ✅ 对齐 Teable
		tables.DELETE("/:tableId/records/:recordId", handler.DeleteRecord)

		// 批量操作
		tables.PATCH("/:tableId/records/batch", handler.BatchUpdateRecords)
		tables.DELETE("/:tableId/records/batch", handler.BatchDeleteRecords)
	}

	// 记录路由（保留旧路由以兼容，但标记为废弃）
	// ⚠️ 废弃：建议使用 /api/v1/tables/:tableId/records/:recordId
	// 废弃原因：旧路由缺少tableId参数，无法确定记录所属的表
	// 迁移建议：客户端应使用新的表级路由，提供完整的上下文信息
	// 计划移除：在下一个主要版本中移除这些路由
	records := rg.Group("/records")
	{
		records.GET("/:recordId", handler.GetRecord)
		records.PATCH("/:recordId", handler.UpdateRecord)
		records.DELETE("/:recordId", handler.DeleteRecord)
		records.PATCH("/batch", handler.BatchUpdateRecords)
		records.DELETE("/batch", handler.BatchDeleteRecords)
	}
}

// setupUserRoutes 设置用户路由
func setupUserRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewUserHandler(cont.UserService())

	users := rg.Group("/users")
	{
		users.POST("", handler.CreateUser)                   // 创建用户
		users.GET("/:id", handler.GetUser)                   // 获取用户信息
		users.PATCH("/:id", handler.UpdateUser)              // ✅ 部分更新使用PATCH
		users.DELETE("/:id", handler.DeleteUser)             // 删除用户
		users.GET("", handler.ListUsers)                     // 用户列表
		users.PATCH("/:id/password", handler.ChangePassword) // ✅ 修改密码用PATCH
	}
}

// setupAuthRoutes 设置认证路由
func setupAuthRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewAuthHandler(cont.AuthService())

	auth := rg.Group("/auth")
	{
		auth.POST("/register", handler.Register)    // 注册
		auth.POST("/login", handler.Login)          // 登录
		auth.POST("/logout", handler.Logout)        // 登出
		auth.POST("/refresh", handler.RefreshToken) // 刷新Token
		auth.GET("/me", handler.GetCurrentUser)     // 获取当前用户信息
	}
}

// setupViewRoutes 设置视图路由
func setupViewRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewViewHandler(cont.ViewService())

	// 表格下的视图
	tables := rg.Group("/tables")
	{
		tables.GET("/:tableId/views", handler.ListViews)        // 获取表格的所有视图
		tables.POST("/:tableId/views", handler.CreateView)      // 创建视图
		tables.GET("/:tableId/views/count", handler.CountViews) // 统计视图数量
	}

	// 视图路由
	views := rg.Group("/views")
	{
		// 基本操作
		views.GET("/:viewId", handler.GetView)       // 获取视图详情
		views.PATCH("/:viewId", handler.UpdateView)  // ✅ 部分更新使用PATCH
		views.DELETE("/:viewId", handler.DeleteView) // 删除视图

		// 视图配置（这些是完整替换特定字段，用PATCH更合理）
		views.PATCH("/:viewId/filter", handler.UpdateViewFilter)          // ✅ 更新过滤器
		views.PATCH("/:viewId/sort", handler.UpdateViewSort)              // ✅ 更新排序
		views.PATCH("/:viewId/group", handler.UpdateViewGroup)            // ✅ 更新分组
		views.PATCH("/:viewId/column-meta", handler.UpdateViewColumnMeta) // ✅ 更新列配置
		views.PATCH("/:viewId/options", handler.UpdateViewOptions)        // ✅ 更新选项
		views.PATCH("/:viewId/order", handler.UpdateViewOrder)            // ✅ 更新排序位置

		// 分享功能
		views.POST("/:viewId/enable-share", handler.EnableShare)        // 启用分享
		views.POST("/:viewId/disable-share", handler.DisableShare)      // 禁用分享
		views.POST("/:viewId/refresh-share-id", handler.RefreshShareID) // 刷新分享ID
		views.PATCH("/:viewId/share-meta", handler.UpdateShareMeta)     // ✅ 更新分享元数据

		// 锁定功能
		views.POST("/:viewId/lock", handler.LockView)     // 锁定视图
		views.POST("/:viewId/unlock", handler.UnlockView) // 解锁视图

		// 复制功能
		views.POST("/:viewId/duplicate", handler.DuplicateView) // 复制视图
	}

	// 分享视图访问
	share := rg.Group("/share")
	{
		share.GET("/views/:shareId", handler.GetViewByShareID) // 通过分享ID获取视图
	}
}

// setupAttachmentRoutes 设置附件路由 ✨
func setupAttachmentRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewAttachmentHandler(cont.AttachmentService(), logger.Logger)

	// 附件路由
	attachments := rg.Group("/attachments")
	{
		// 生成上传签名
		attachments.POST("/signature", handler.GenerateSignature)

		// 上传文件（使用令牌）
		attachments.POST("/upload/:token", handler.UploadFile)

		// 通知上传完成
		attachments.POST("/notify/:token", handler.NotifyUpload)

		// 读取文件（使用通配符以支持路径中的斜杠）
		attachments.GET("/read/*path", handler.ReadFile)

		// 删除文件
		attachments.DELETE("/:id", handler.DeleteFile)

		// 获取附件信息
		attachments.GET("/:id", handler.GetAttachment)

		// 列出附件
		attachments.GET("", handler.ListAttachments)

		// 清理过期令牌
		attachments.POST("/cleanup-tokens", handler.CleanupExpiredTokens)
	}

	// 表格附件统计
	tables := rg.Group("/tables")
	{
		tables.GET("/:tableId/attachments/stats", handler.GetAttachmentStats)
	}
}

// setupWebSocketRoutes 设置WebSocket路由 ✨
// 旧 WebSocket 路由已移除

// setupMonitoringRoutes 设置监控路由
func setupMonitoringRoutes(rg *gin.RouterGroup, cont *container.Container) {
	handler := NewMonitoringHandler(cont.DB())

	monitoring := rg.Group("/monitoring")
	{
		monitoring.GET("/db-stats", handler.GetDBStats)
	}
}

// setupJSVMRoutes 设置 JSVM 管理路由
func setupJSVMRoutes(rg *gin.RouterGroup, cont *container.Container) {
	// 检查 JSVM 是否启用
	if cont.JSVMManager() == nil {
		return
	}

	// JSVM 管理路由（需要认证）
	jsvm := rg.Group("/jsvm")
	jsvm.Use(JWTAuthMiddleware(cont.AuthService()))
	{
		// 钩子管理
		jsvm.GET("/hooks", func(c *gin.Context) {
			hookManager := cont.JSVMManager().GetHookManager()
			hooks := hookManager.GetAllHooks()
			c.JSON(200, gin.H{"hooks": hooks})
		})

		// 插件管理
		jsvm.GET("/plugins", func(c *gin.Context) {
			pluginManager := cont.JSVMManager().GetPluginManager()
			plugins := pluginManager.ListPlugins()
			c.JSON(200, gin.H{"plugins": plugins})
		})

		// JSVM 统计
		jsvm.GET("/stats", func(c *gin.Context) {
			hookManager := cont.JSVMManager().GetHookManager()
			pluginManager := cont.JSVMManager().GetPluginManager()

			stats := map[string]interface{}{
				"hooks":   hookManager.GetHookCount(),
				"plugins": pluginManager.GetPluginCount(),
			}
			c.JSON(200, gin.H{"stats": stats})
		})
	}

	// 实时通信统计路由（需要认证）
	realtime := rg.Group("/realtime")
	realtime.Use(JWTAuthMiddleware(cont.AuthService()))
	{
		realtime.GET("/stats", func(c *gin.Context) {
			if cont.RealtimeManager() != nil {
				stats := cont.RealtimeManager().GetStats()
				c.JSON(200, gin.H{"stats": stats})
			} else {
				c.JSON(200, gin.H{"stats": map[string]interface{}{}})
			}
		})
	}
}

// setupRealtimeRoutes 设置实时通信路由
func setupRealtimeRoutes(router *gin.Engine, cont *container.Container) {
	// 检查实时通信管理器是否启用
	if cont.RealtimeManager() == nil {
		return
	}

	// SSE 路由（需要认证）
	router.GET("/api/realtime", JWTAuthMiddleware(cont.AuthService()), cont.RealtimeManager().HandleSSE)
	router.POST("/api/realtime", JWTAuthMiddleware(cont.AuthService()), cont.RealtimeManager().HandleSSESubscription)
}

// setupShareDBRoutes 设置 ShareDB 路由
func setupShareDBRoutes(rg *gin.RouterGroup, cont *container.Container) {
	// 检查 ShareDB 服务是否启用
	if cont.RealtimeManager() == nil || cont.RealtimeManager().GetShareDBService() == nil {
		return
	}

	// ShareDB 统计和管理路由（需要认证）
	sharedb := rg.Group("/sharedb")
	sharedb.Use(JWTAuthMiddleware(cont.AuthService()))
	{
		handler := NewShareDBHandler(cont.RealtimeManager().GetShareDBService(), nil)

		// 统计信息
		sharedb.GET("/stats", handler.GetStats)

		// 连接管理
		sharedb.GET("/connections", handler.GetConnections)

		// 开发环境：强制清理所有连接
		sharedb.POST("/cleanup", handler.ForceCleanupConnections)
	}
}

// setupWebSocketRoutes 设置 WebSocket 路由
func setupWebSocketRoutes(router *gin.Engine, cont *container.Container) {
	// 检查实时通信管理器是否启用
	if cont.RealtimeManager() == nil {
		return
	}

	// ShareDB 协作 WebSocket 路由（需要认证）
	router.GET("/socket", JWTAuthMiddleware(cont.AuthService()), cont.RealtimeManager().HandleShareDBWebSocket)
	router.GET("/socket/*path", JWTAuthMiddleware(cont.AuthService()), cont.RealtimeManager().HandleShareDBWebSocket)

}

// setupStaticFiles 设置静态文件服务
func setupStaticFiles(router *gin.Engine, staticFiles embed.FS) {
	// 创建静态文件处理器
	staticHandler, err := NewStaticHandler(staticFiles, "web")
	if err != nil {
		// 如果静态文件不存在，只打印警告，不影响API服务
		println("Warning: Failed to setup static files:", err.Error())
		return
	}

	// 使用 NoRoute 来处理所有未匹配的路由
	// 这样可以支持前端的 SPA 路由
	router.NoRoute(staticHandler.ServeSPA())
}
