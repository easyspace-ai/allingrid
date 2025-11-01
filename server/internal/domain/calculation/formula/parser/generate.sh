#!/bin/bash

# ANTLR4 Go代码生成脚本
# 从Grammar文件生成Go解析器代码

set -e

echo "======================================"
echo "  ANTLR4 Go代码生成"
echo "======================================"
echo ""

# 检查antlr是否安装
if ! command -v antlr &> /dev/null; then
    echo "错误: antlr未安装"
    echo "请运行: brew install antlr"
    exit 1
fi

# 当前目录
PARSER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PARSER_DIR"

echo "📁 工作目录: $PARSER_DIR"
echo ""

# 清理旧的生成文件
echo "🧹 清理旧文件..."
rm -f FormulaLexer.go FormulaLexer.interp FormulaLexer.tokens
rm -f Formula.go Formula.interp Formula.tokens
rm -f formula_*.go
echo "✅ 清理完成"
echo ""

# 生成Go代码
echo "🔨 生成Go代码..."
antlr -Dlanguage=Go \
  -o . \
  -package parser \
  -visitor \
  -no-listener \
  FormulaLexer.g4 Formula.g4

if [ $? -eq 0 ]; then
    echo "✅ 生成成功"
    echo ""
    echo "生成的文件:"
    ls -lh *.go 2>/dev/null | awk '{print "  " $9, "(" $5 ")"}'
else
    echo "❌ 生成失败"
    exit 1
fi

echo ""
echo "======================================"
echo "  生成完成"
echo "======================================"

