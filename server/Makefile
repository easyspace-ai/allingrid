# Makefile for LuckDB Server

# 版本信息
VERSION ?= 0.1.0
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -X main.Version=$(VERSION) -X main.GitCommit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)

.PHONY: help test test-unit test-integration test-coverage test-all clean build run

help: ## 显示帮助信息
	@echo "LuckDB Server - 可用命令："
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ==================== 构建命令 ====================

build: ## 构建 LuckDB 服务器（开发版本）
	@echo "🔨 构建 LuckDB 服务器..."
	@mkdir -p bin
	go build -o bin/luckdb ./cmd/luckdb
	@echo "✅ 构建完成: bin/luckdb"

build-prod: ## 构建生产版本（包含版本信息）
	@echo "🔨 构建生产版本..."
	@echo "   版本: $(VERSION)"
	@echo "   提交: $(COMMIT)"
	@echo "   时间: $(BUILD_TIME)"
	@mkdir -p bin
	go build -ldflags="$(LDFLAGS)" -o bin/luckdb ./cmd/luckdb
	@echo "✅ 构建完成: bin/luckdb"

build-linux: ## 构建 Linux 版本
	@echo "🐧 构建 Linux 版本..."
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/luckdb-linux ./cmd/luckdb
	@echo "✅ Linux 版本构建完成: bin/luckdb-linux"

build-macos: ## 构建 macOS 版本
	@echo "🍎 构建 macOS 版本..."
	@mkdir -p bin
	GOOS=darwin GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/luckdb-darwin-amd64 ./cmd/luckdb
	GOOS=darwin GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o bin/luckdb-darwin-arm64 ./cmd/luckdb
	@echo "✅ macOS 版本构建完成"

build-windows: ## 构建 Windows 版本
	@echo "🪟 构建 Windows 版本..."
	@mkdir -p bin
	GOOS=windows GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/luckdb-windows.exe ./cmd/luckdb
	@echo "✅ Windows 版本构建完成: bin/luckdb-windows.exe"

build-cross: build-linux build-macos build-windows ## 交叉编译所有平台

install: build ## 安装到 $GOPATH/bin
	@echo "📦 安装 LuckDB..."
	cp bin/luckdb $(GOPATH)/bin/luckdb
	@echo "✅ 安装完成: $(GOPATH)/bin/luckdb"

clean: ## 清理构建文件
	@echo "🧹 清理构建文件..."
	rm -rf bin/
	rm -f coverage.out coverage.html
	rm -rf coverage/
	go clean -testcache
	@echo "✅ 清理完成"

# ==================== 运行命令 ====================

run: ## 运行 API 服务器（开发模式）
	@echo "🚀 启动 LuckDB API 服务器..."
	go run ./cmd/luckdb serve

dev: run ## 别名：运行 API 服务器


# ==================== 数据库命令 ====================

migrate: ## 执行数据库迁移
	@echo "📊 执行数据库迁移..."
	go run ./cmd/luckdb migrate up

migrate-up: migrate ## 别名：执行数据库迁移

migrate-down: ## 回滚数据库迁移
	@echo "📊 回滚数据库迁移..."
	go run ./cmd/luckdb migrate down

migrate-version: ## 显示当前迁移版本
	@echo "📊 查看迁移版本..."
	go run ./cmd/luckdb migrate version

migrate-force: ## 强制设置迁移版本（需要 VERSION 参数）
	@echo "📊 强制设置迁移版本 $(VERSION)..."
	go run ./cmd/luckdb migrate force $(VERSION)

migrate-drop: ## 删除所有表（危险！）
	@echo "⚠️  删除所有表..."
	go run ./cmd/luckdb migrate drop

# ==================== 工具命令 ====================

password: ## 生成密码哈希（需要 PWD 参数）
	@echo "🔑 生成密码哈希..."
	go run ./cmd/luckdb util generate-password --password $(PWD)

debug-config: ## 调试配置
	@echo "🔍 调试配置..."
	go run ./cmd/luckdb util debug-config

# ==================== 测试命令 ====================

test: ## 运行所有测试
	@echo "🧪 运行所有测试..."
	go test ./... -v -count=1

test-unit: ## 运行单元测试
	@echo "🧪 运行单元测试..."
	go test ./internal/domain/... ./internal/application/... -v -count=1

test-integration: ## 运行集成测试
	@echo "🧪 运行集成测试..."
	go test ./internal/testing/integration/... -v -count=1 -tags=integration

test-coverage: ## 生成测试覆盖率报告
	@echo "📊 生成测试覆盖率报告..."
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html
	go tool cover -func=coverage.out | grep total
	@echo "📁 覆盖率报告生成：coverage.html"

test-coverage-detailed: ## 生成详细的测试覆盖率报告
	@echo "📊 生成详细测试覆盖率报告..."
	@mkdir -p coverage
	go test ./internal/domain/... -coverprofile=coverage/domain.out
	go test ./internal/application/... -coverprofile=coverage/application.out
	go test ./internal/infrastructure/... -coverprofile=coverage/infrastructure.out
	go test ./internal/interfaces/... -coverprofile=coverage/interfaces.out
	go test ./internal/commands/... -coverprofile=coverage/commands.out
	@echo "✅ 详细报告生成在 coverage/ 目录"

test-domain: ## 测试领域层
	@echo "🧪 测试领域层..."
	go test ./internal/domain/... -v -count=1

test-application: ## 测试应用层
	@echo "🧪 测试应用层..."
	go test ./internal/application/... -v -count=1

test-infrastructure: ## 测试基础设施层
	@echo "🧪 测试基础设施层..."
	go test ./internal/infrastructure/... -v -count=1

test-interfaces: ## 测试接口层
	@echo "🧪 测试接口层..."
	go test ./internal/interfaces/... -v -count=1


test-bench: ## 运行性能测试
	@echo "⚡ 运行性能测试..."
	go test ./... -bench=. -benchmem -run=^Benchmark

test-race: ## 运行竞态检测
	@echo "🔍 运行竞态检测..."
	go test ./... -race -count=1

test-short: ## 运行快速测试（跳过慢速测试）
	@echo "⚡ 运行快速测试..."
	go test ./... -short -count=1

test-all: test test-coverage ## 运行所有测试并生成覆盖率报告

# ==================== 代码质量命令 ====================

lint: ## 运行代码检查
	@echo "🔍 运行代码检查..."
	golangci-lint run ./...

fmt: ## 格式化代码
	@echo "✨ 格式化代码..."
	go fmt ./...
	goimports -w .

vet: ## 运行代码审查
	@echo "🔍 运行代码审查..."
	go vet ./...

# ==================== 依赖管理 ====================

deps: ## 安装依赖
	@echo "📦 安装依赖..."
	go mod download
	go mod tidy

deps-update: ## 更新依赖
	@echo "📦 更新依赖..."
	go get -u ./...
	go mod tidy

deps-check: ## 检查依赖更新
	@echo "🔍 检查依赖更新..."
	go list -u -m all

# ==================== Docker 命令 ====================

docker-build: ## 构建 Docker 镜像
	@echo "🐳 构建 Docker 镜像..."
	docker build -t luckdb:$(VERSION) -t luckdb:latest -f ../docker/Dockerfile.server ..

docker-run: ## 运行 Docker 容器
	@echo "🐳 运行 Docker 容器..."
	docker run -p 2345:2345 luckdb:latest

docker-push: ## 推送 Docker 镜像
	@echo "🐳 推送 Docker 镜像..."
	docker push luckdb:$(VERSION)
	docker push luckdb:latest

# ==================== 快速启动命令 ====================

start: build ## 编译并启动服务器
	@echo "🚀 启动 LuckDB..."
	./bin/luckdb serve

stop: ## 停止所有 LuckDB 进程
	@echo "🛑 停止 LuckDB..."
	@pkill -f "luckdb" || true

restart: stop start ## 重启服务器

# ==================== 完整测试流程 ====================

full-test: clean deps build migrate test ## 完整测试流程（清理+依赖+构建+迁移+测试）

ci: deps build test lint ## CI 流程（依赖+构建+测试+检查）

# ==================== 版本信息 ====================

version: ## 显示版本信息
	@echo "📦 LuckDB 版本信息:"
	@echo "  版本: $(VERSION)"
	@echo "  提交: $(COMMIT)"
	@echo "  构建时间: $(BUILD_TIME)"

.DEFAULT_GOAL := help
