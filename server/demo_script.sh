#!/bin/bash

# Yjs + SSE 实时协作系统演示脚本
# 服务器地址
SERVER="http://localhost:8082"

echo "🚀 Yjs + SSE 实时协作系统演示"
echo "=================================="
echo ""

# 1. 检查服务器状态
echo "1. 📊 检查服务器状态..."
echo "   实时通信统计:"
curl -s "$SERVER/admin/realtime/stats" | jq '.'
echo ""
echo "   JSVM 统计:"
curl -s "$SERVER/admin/jsvm/stats" | jq '.'
echo ""

# 2. 测试用户 API
echo "2. 👥 测试用户 API..."
echo "   获取用户列表:"
curl -s "$SERVER/api/v1/users/" | jq '.'
echo ""

# 3. 测试记录 API（触发实时事件）
echo "3. 📝 测试记录 API（触发实时事件）..."
echo "   创建记录:"
curl -s -X POST "$SERVER/api/v1/records/" \
  -H "Content-Type: application/json" \
  -d '{"name":"测试记录","description":"这是一个测试记录"}' | jq '.'
echo ""

echo "   更新记录:"
curl -s -X PUT "$SERVER/api/v1/records/record_1" \
  -H "Content-Type: application/json" \
  -d '{"name":"更新的记录","status":"active"}' | jq '.'
echo ""

echo "   删除记录:"
curl -s -X DELETE "$SERVER/api/v1/records/record_1" | jq '.'
echo ""

# 4. 测试 WebSocket 连接（基础）
echo "4. 🔌 测试 WebSocket 连接..."
echo "   基础 WebSocket 端点: ws://localhost:8082/ws"
echo "   Yjs 协作端点: ws://localhost:8082/yjs/ws?document=table1&user=user1"
echo ""

# 5. 测试 SSE 连接
echo "5. 📡 测试 SSE 连接..."
echo "   SSE 端点: $SERVER/api/realtime?client_id=demo&user_id=demo"
echo "   订阅管理: POST $SERVER/api/realtime"
echo ""

# 6. 显示前端示例
echo "6. 🌐 前端示例..."
echo "   打开浏览器访问: file://$(pwd)/examples/frontend_integration.html"
echo "   或者使用以下命令启动简单的 HTTP 服务器:"
echo "   cd examples && python3 -m http.server 3000"
echo "   然后访问: http://localhost:3000/frontend_integration.html"
echo ""

# 7. 显示可用的 API 端点
echo "7. 📋 可用的 API 端点..."
echo "   GET  $SERVER/api/v1/users/          - 获取用户列表"
echo "   POST $SERVER/api/v1/users/          - 创建用户"
echo "   GET  $SERVER/api/v1/records/        - 获取记录列表"
echo "   POST $SERVER/api/v1/records/        - 创建记录"
echo "   PUT  $SERVER/api/v1/records/:id     - 更新记录"
echo "   DELETE $SERVER/api/v1/records/:id   - 删除记录"
echo "   GET  $SERVER/admin/realtime/stats   - 实时通信统计"
echo "   GET  $SERVER/admin/jsvm/stats       - JSVM 统计"
echo ""

# 8. 显示实时通信端点
echo "8. 🔄 实时通信端点..."
echo "   WebSocket:"
echo "     - ws://localhost:8082/ws                    (基础 WebSocket)"
echo "     - ws://localhost:8082/yjs/ws?document=doc1&user=user1  (Yjs 协作)"
echo ""
echo "   Server-Sent Events:"
echo "     - GET  $SERVER/api/realtime?client_id=xxx&user_id=xxx"
echo "     - POST $SERVER/api/realtime (订阅管理)"
echo ""

echo "🎉 演示完成！"
echo ""
echo "💡 提示:"
echo "   - 使用前端示例页面测试实时协作功能"
echo "   - 多个浏览器窗口可以测试多用户协作"
echo "   - 查看服务器日志了解实时事件触发情况"
echo "   - 参考 SHAREDB_TO_YJS_MIGRATION.md 了解迁移指南"
