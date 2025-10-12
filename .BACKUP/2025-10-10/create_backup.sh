#!/bin/bash

# 創建備份腳本
echo "🗂️  創建備份資料夾..."
mkdir -p .BACKUP/2025-10-07

echo "📦 開始備份檔案..."
rsync -av --exclude='.BACKUP' --exclude='node_modules' . .BACKUP/2025-10-07/

echo "✅ 備份完成！"
echo "📊 備份統計："
echo "   備份資料夾：.BACKUP/2025-10-07/"
echo "   備份檔案數：$(find .BACKUP/2025-10-07 -type f | wc -l)"
echo ""
echo "📋 主要備份內容："
ls -la .BACKUP/2025-10-07/