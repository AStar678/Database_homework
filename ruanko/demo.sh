#!/bin/bash
# Ruanko DBMS 终端演示脚本

set -e

echo "========================================"
echo "  Ruanko DBMS 终端功能演示"
echo "========================================"
echo ""

# 进入项目目录
cd "$(dirname "$0")"

# 确保二进制已编译
if [ ! -f "./ruanko_cli" ]; then
    echo "正在编译 Ruanko CLI..."
    export GOCACHE="$(pwd)/gocache"
    export GOTMPDIR="$(pwd)/tmp"
    mkdir -p "$GOCACHE" "$GOTMPDIR"
    go build -o ruanko_cli cmd/ruanko/main.go
    echo "编译完成"
    echo ""
fi

# 清理旧数据，确保演示环境干净
echo "清理演示环境..."
rm -rf dbms_root
echo ""

# 执行演示 SQL
echo "开始演示..."
echo "========================================"
cat demo.sql | ./ruanko_cli

echo ""
echo "========================================"
echo "  演示结束"
echo "========================================"
