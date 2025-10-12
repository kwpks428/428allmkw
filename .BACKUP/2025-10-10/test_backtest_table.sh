#!/bin/bash

echo "======================================"
echo "回測表格功能測試腳本"
echo "======================================"
echo ""

# 檢查文件是否存在
echo "1. 檢查修改的文件..."
files=(
    "server/app.html"
    "backtest/backtest.js"
)

for file in "${files[@]}"; do
    if [ -f "/private/tmp/allmrw-temp/$file" ]; then
        echo "✓ $file 存在"
    else
        echo "✗ $file 不存在"
    fi
done
echo ""

# 檢查 HTML 中是否有新的表格
echo "2. 檢查 HTML 中的表格元素..."
if grep -q "backtest-table" /private/tmp/allmrw-temp/server/app.html; then
    echo "✓ backtest-table 元素已添加"
else
    echo "✗ 未找到 backtest-table 元素"
fi

if grep -q "renderBacktestTable" /private/tmp/allmrw-temp/server/app.html; then
    echo "✓ renderBacktestTable 函數已添加"
else
    echo "✗ 未找到 renderBacktestTable 函數"
fi
echo ""

# 檢查回測函數是否有 history
echo "3. 檢查回測函數中的 history 字段..."
if grep -q "history: \[\]" /private/tmp/allmrw-temp/backtest/backtest.js; then
    echo "✓ history 數組已添加到策略結果中"
else
    echo "✗ 未找到 history 數組"
fi

if grep -q "results.history.push" /private/tmp/allmrw-temp/backtest/backtest.js; then
    echo "✓ history.push 記錄已添加"
else
    echo "✗ 未找到 history.push 記錄"
fi
echo ""

# 檢查預測時間設置
echo "4. 檢查預測時間配置..."
if grep -q "5000" /private/tmp/allmrw-temp/backtest/backtest.js | grep -q "finalAdvanceMs"; then
    echo "✓ 預測時間已設置為鎖倉前5秒"
else
    echo "⚠ 預測時間設置可能不正確，請檢查 DYNAMIC_CONFIG.finalAdvanceMs"
fi
echo ""

# 檢查 CSS 樣式
echo "5. 檢查 CSS 樣式..."
css_classes=(
    "prediction-circle"
    "result-bg-up"
    "result-bg-down"
    "winrate-high"
)

for class in "${css_classes[@]}"; do
    if grep -q "\.$class" /private/tmp/allmrw-temp/server/app.html; then
        echo "✓ CSS class .$class 已定義"
    else
        echo "✗ 未找到 CSS class .$class"
    fi
done
echo ""

# 語法檢查
echo "6. Node.js 語法檢查..."
cd /private/tmp/allmrw-temp/backtest
if node -c backtest.js 2>/dev/null; then
    echo "✓ backtest.js 語法正確"
else
    echo "✗ backtest.js 語法錯誤"
    node -c backtest.js
fi
echo ""

# 總結
echo "======================================"
echo "測試完成！"
echo "======================================"
echo ""
echo "下一步："
echo "1. 重啟服務："
echo "   cd /private/tmp/allmrw-temp/server && node server.js &"
echo "   cd /private/tmp/allmrw-temp/backtest && node backtest.js &"
echo ""
echo "2. 在瀏覽器中打開系統，查看新的回測表格"
echo ""
echo "3. 等待新局次，觀察預測和結果的實時更新"
echo ""
echo "詳細說明請參閱: BACKTEST_TABLE_CHANGES.md"
