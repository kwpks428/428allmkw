# 回測表格顯示系統 - 更新說明

## 功能概述

重新設計了回測數據在前端的顯示方式，創建了一個清晰的表格視圖，顯示三種策略在最近50局的預測表現。

## 表格結構

### 佈局：3行 × 51列

- **第1列**：策略名稱
- **第2列**：總體勝率（勝/總數）
- **第3-51列**：最近49局的預測結果（本局到前48局）

### 三種策略

1. **動量策略**：基於時間序列特徵分析（連續趨勢、資金流動、成交量動量等）
2. **跟隨最高勝率**：跟隨近12局勝率最高的錢包
3. **反向最低勝率**：反向操作勝率最低的錢包

## 視覺設計

### 預測顯示
- **綠色實心圓** (●)：預測上漲（UP）
- **紅色實心圓** (●)：預測下跌（DOWN）

### 結果著色
當局次結果出來後：
- **綠色背景**：該局實際結果為上漲（UP）
- **紅色背景**：該局實際結果為下跌（DOWN）

### 視覺效果
- ✅ **預測正確**：圓圈與背景顏色相同，融為一體
- ❌ **預測錯誤**：圓圈與背景顏色對比明顯（綠圓+紅底 或 紅圓+綠底）

## 技術實現

### 前端修改 (`server/app.html`)

#### 1. CSS 樣式
```css
/* 新增回測表格相關樣式 */
.backtest-section        # 表格容器
.backtest-table          # 表格主體
.prediction-circle       # 預測圓圈
.result-bg-up            # UP結果背景（綠色）
.result-bg-down          # DOWN結果背景（紅色）
.winrate-high/medium/low # 勝率顏色標識
```

#### 2. JavaScript 邏輯
- `renderBacktestTable(backtestData)`: 渲染完整的回測表格
- 自動處理歷史數據數組，創建49個格子
- 根據預測和結果動態著色
- 添加 tooltip 顯示詳細信息（局次、預測、結果、勝負）

### 後端修改 (`backtest/backtest.js`)

#### 1. 回測函數增強
為三個策略函數添加了 `history` 數組：

**backtestFollowBest()**
```javascript
results.history.push({
  epoch: 局次號碼,
  prediction: 'UP' or 'DOWN',
  result: 實際結果,
  isWin: true/false/null
});
```

**backtestReverseLow()**
- 同上結構
- 注意：prediction 是反向預測結果

**backtestMomentumStrategy()**
- 同上結構
- 添加了錯誤處理，無數據時記錄為 null

#### 2. 數據結構
```javascript
{
  timestamp: "ISO時間戳",
  startEpoch: 起始局次,
  currentEpoch: 當前局次,
  strategies: {
    momentum: {
      name: "時間序列動量",
      winRate: 52.08,
      wins: 25,
      total: 48,
      history: [          // 新增！
        {
          epoch: 12345,
          prediction: "UP",
          result: "UP",
          isWin: true
        },
        // ... 48個記錄
      ]
    },
    // follow_best 和 reverse_low 同樣結構
  }
}
```

### 預測時間統一

#### 配置修改
```javascript
// backtest/backtest.js
DYNAMIC_CONFIG.finalAdvanceMs = 5000; // 統一在鎖倉前5秒
```

#### 工作流程
1. 系統在每局開始時初始化局次上下文
2. 計算鎖倉時間，排程最終預測
3. 在鎖倉前5秒，所有三個策略統一給出最終預測
4. 預測通過 Redis pub/sub 推送給前端
5. 局次結束後，結果更新，表格背景著色

## 部署步驟

### 1. 停止服務
```bash
# 停止所有相關服務
pkill -f "node.*server.js"
pkill -f "node.*backtest.js"
```

### 2. 更新代碼
```bash
cd /private/tmp/allmrw-temp
# 代碼已更新完成
```

### 3. 重啟服務
```bash
# 重啟 Web 服務器
cd server && node server.js &

# 重啟回測服務
cd backtest && node backtest.js &
```

### 4. 環境變量（可選）
```bash
# 如需自定義預測提前時間
export FINAL_ADVANCE_MS=5000  # 鎖倉前5秒（默認值）
```

## 數據流

```
┌─────────────┐
│ 新局次開始  │
└──────┬──────┘
       │
       ├─→ 初始化局次上下文
       │
       ├─→ 排程最終預測（鎖倉前5秒）
       │
       ▼
┌──────────────┐
│ 鎖倉前5秒    │ ← 所有策略統一在此時給出最終預測
└──────┬───────┘
       │
       ├─→ calculateMomentumPrediction()
       ├─→ followBestQuery（查詢最高勝率錢包）
       ├─→ reverseLowQuery（查詢最低勝率錢包）
       │
       ▼
┌──────────────┐
│ 發布預測     │
└──────┬───────┘
       │
       ├─→ Redis pub/sub: live_predictions
       ├─→ 前端接收並顯示（圓圈）
       │
       ▼
┌──────────────┐
│ 局次結束     │
└──────┬───────┘
       │
       ├─→ 結果寫入資料庫
       ├─→ 執行回測（包含歷史數據）
       ├─→ Redis pub/sub: backtest_results
       │
       ▼
┌──────────────┐
│ 前端更新表格 │
└──────┬───────┘
       │
       └─→ 著色背景（綠/紅）
           顯示勝負對比
```

## 驗證測試

### 1. 檢查回測數據
```bash
# 查看 Redis 中的最新回測數據
redis-cli -u $REDIS_URL GET latest_backtest
```

### 2. 檢查前端顯示
1. 打開瀏覽器訪問系統
2. 查看新增的「策略回測表現」區塊
3. 確認三行策略都正確顯示
4. 確認每行有49個格子（歷史數據）
5. 檢查圓圈顏色和背景著色

### 3. 測試實時更新
1. 等待新局次開始
2. 觀察鎖倉前5秒是否出現預測（應該在上方AI預測區和表格中）
3. 局次結束後，確認表格自動更新並著色

## 性能優化

### 前端
- 使用 CSS transitions 實現平滑動畫
- 批量DOM更新，減少重繪
- Tooltip使用原生title屬性

### 後端
- 並行執行三個策略回測（Promise.all）
- 歷史數據限制在49局
- Redis快取最新回測結果（1小時過期）

## 故障排查

### 表格不顯示
1. 檢查瀏覽器控制台是否有錯誤
2. 確認 WebSocket 連接正常
3. 檢查 `backtestData.strategies.momentum.history` 是否存在

### 預測時間不對
1. 檢查環境變數 `FINAL_ADVANCE_MS`
2. 查看回測服務日誌中的排程信息
3. 確認系統時間正確

### 顏色不對
1. 檢查 CSS 是否正確載入
2. 確認 prediction 和 result 值為 'UP' 或 'DOWN'
3. 查看瀏覽器開發者工具中元素的 class

## 未來改進

1. **更多局次**：支持配置顯示局次數量（目前固定49局）
2. **策略性能圖表**：添加折線圖顯示勝率趨勢
3. **過濾器**：允許用戶選擇顯示哪些策略
4. **導出功能**：支持導出回測數據為 CSV
5. **響應式設計**：優化移動端顯示

## 相關文件

- `/private/tmp/allmrw-temp/server/app.html` - 前端UI
- `/private/tmp/allmrw-temp/backtest/backtest.js` - 回測邏輯
- `/private/tmp/allmrw-temp/server/server.js` - WebSocket服務器

## 更新日期

2025-01-08

## 作者

Warp AI - 根據用戶需求實現
