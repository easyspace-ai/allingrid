// 审计日志插件示例
// 这个插件会自动记录所有用户和记录操作

console.log("审计日志插件正在初始化...");

// 插件配置
const pluginConfig = {
    name: "audit_logger",
    version: "1.0.0",
    description: "自动记录所有操作的审计日志插件",
    enabled: true
};

// 审计日志存储（在实际应用中，这里应该连接到数据库或日志系统）
const auditLogs = [];

// 记录审计日志的函数
function logAuditEvent(eventType, data) {
    const auditEntry = {
        id: `audit_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        event_type: eventType,
        data: data,
        timestamp: new Date().toISOString(),
        plugin: pluginConfig.name
    };
    
    auditLogs.push(auditEntry);
    
    console.log(`[审计日志] ${eventType}:`, auditEntry);
    
    // 在实际应用中，这里应该将日志保存到数据库或发送到日志系统
    // 例如：await database.saveAuditLog(auditEntry);
}

// 注册用户操作钩子
app.onUserCreate(function(data) {
    logAuditEvent("user.create", {
        user_id: data.user_id,
        email: data.email,
        name: data.name,
        action: "用户创建",
        details: `新用户 ${data.name} (${data.email}) 已创建`
    });
});

app.onUserUpdate(function(data) {
    logAuditEvent("user.update", {
        user_id: data.user_id,
        updates: data.updates,
        action: "用户更新",
        details: `用户 ${data.user_id} 的信息已更新: ${JSON.stringify(data.updates)}`
    });
});

app.onUserDelete(function(data) {
    logAuditEvent("user.delete", {
        user_id: data.user_id,
        action: "用户删除",
        details: `用户 ${data.user_id} 已被删除`
    });
});

// 注册记录操作钩子
app.onRecordCreate(function(data) {
    logAuditEvent("record.create", {
        table_id: data.table_id,
        record_id: data.record_id,
        action: "记录创建",
        details: `在表 ${data.table_id} 中创建了新记录 ${data.record_id}`
    });
});

app.onRecordUpdate(function(data) {
    logAuditEvent("record.update", {
        table_id: data.table_id,
        record_id: data.record_id,
        action: "记录更新",
        details: `表 ${data.table_id} 中的记录 ${data.record_id} 已更新`
    });
});

app.onRecordDelete(function(data) {
    logAuditEvent("record.delete", {
        table_id: data.table_id,
        record_id: data.record_id,
        action: "记录删除",
        details: `表 ${data.table_id} 中的记录 ${data.record_id} 已被删除`
    });
});

// 插件初始化完成
console.log("审计日志插件初始化完成");
console.log("插件配置:", pluginConfig);

// 导出插件信息（JSVM兼容版本）
// 在JSVM环境中，我们使用全局变量而不是module.exports
if (typeof global !== 'undefined') {
    global.auditLoggerPlugin = {
        config: pluginConfig,
        getAuditLogs: function() {
            return auditLogs;
        },
        clearAuditLogs: function() {
            auditLogs.length = 0;
        }
    };
}

// 兼容性：如果存在module对象，也导出
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        config: pluginConfig,
        getAuditLogs: function() {
            return auditLogs;
        },
        clearAuditLogs: function() {
            auditLogs.length = 0;
        }
    };
}