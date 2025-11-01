// 用户相关钩子示例
// 这些钩子会在用户操作时自动触发

// 用户创建钩子
app.onUserCreate = function(callback) {
    console.log("用户创建钩子已注册");
    
    // 注册钩子回调
    this.onUserCreateCallback = callback;
};

// 用户更新钩子
app.onUserUpdate = function(callback) {
    console.log("用户更新钩子已注册");
    
    // 注册钩子回调
    this.onUserUpdateCallback = callback;
};

// 用户删除钩子
app.onUserDelete = function(callback) {
    console.log("用户删除钩子已注册");
    
    // 注册钩子回调
    this.onUserDeleteCallback = callback;
};

// 记录创建钩子
app.onRecordCreate = function(callback) {
    console.log("记录创建钩子已注册");
    
    // 注册钩子回调
    this.onRecordCreateCallback = callback;
};

// 记录更新钩子
app.onRecordUpdate = function(callback) {
    console.log("记录更新钩子已注册");
    
    // 注册钩子回调
    this.onRecordUpdateCallback = callback;
};

// 记录删除钩子
app.onRecordDelete = function(callback) {
    console.log("记录删除钩子已注册");
    
    // 注册钩子回调
    this.onRecordDeleteCallback = callback;
};

// 示例：用户创建时的处理逻辑
app.onUserCreate(function(data) {
    console.log("用户创建事件触发:", data);
    
    // 可以在这里添加自定义逻辑，比如：
    // - 发送欢迎邮件
    // - 创建默认配置
    // - 记录审计日志
    // - 触发其他业务逻辑
    
    log.info("新用户创建", {
        user_id: data.user_id,
        email: data.email,
        name: data.name,
        timestamp: data.timestamp
    });
});

// 示例：用户更新时的处理逻辑
app.onUserUpdate(function(data) {
    console.log("用户更新事件触发:", data);
    
    log.info("用户信息更新", {
        user_id: data.user_id,
        updates: data.updates,
        timestamp: data.timestamp
    });
});

// 示例：记录创建时的处理逻辑
app.onRecordCreate(function(data) {
    console.log("记录创建事件触发:", data);
    
    log.info("新记录创建", {
        table_id: data.table_id,
        record_id: data.record_id,
        data: data.data,
        timestamp: data.timestamp
    });
    
    // 可以在这里添加自定义逻辑，比如：
    // - 数据验证
    // - 自动计算字段
    // - 发送通知
    // - 记录审计日志
});

// 示例：记录更新时的处理逻辑
app.onRecordUpdate(function(data) {
    console.log("记录更新事件触发:", data);
    
    log.info("记录更新", {
        table_id: data.table_id,
        record_id: data.record_id,
        data: data.data,
        timestamp: data.timestamp
    });
});

// 示例：记录删除时的处理逻辑
app.onRecordDelete(function(data) {
    console.log("记录删除事件触发:", data);
    
    log.info("记录删除", {
        table_id: data.table_id,
        record_id: data.record_id,
        timestamp: data.timestamp
    });
});

console.log("用户钩子文件已加载");