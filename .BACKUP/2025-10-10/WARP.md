# WARP.md

此文件為 WARP (warp.dev) 在此儲存庫中工作時提供指引。

## 專案概述

這是一個 **BSC (幣安智能鏈) 預測市場數據處理系統**，用於監控和分析預測市場智能合約上的下注活動。系統由多個微服務組成，協同工作來收集、處理和提供即時下注數據和分析。

### 核心架構

系統採用**分散式微服務架構**，包含以下組件：

1. **即時數據收集** (`realbet/`) - WebSocket 監聽器，捕獲即時下注事件
2. **歷史數據處理** (`hisbet/`) - 批次處理器，處理歷史下注數據和錢包分析
3. **網頁介面** (`server/`) - HTTP/WebSocket 伺服器，提供即時儀表板
4. **回測引擎** (`backtest/`) - 策略分析和勝率計算

### 數據流架構

```
智能合約事件 → WebSocket → Redis Stream → 資料庫
                                    ↓
歷史處理器 ← Redis Pub/Sub ← 即時監聽器
      ↓
分析表 → Web 儀表板 → Redis 快取 → 客戶端
```

## 常用開發指令

### 環境設定
```bash
# 為所有服務安裝依賴
npm install
cd hisbet && npm install
cd ../realbet && npm install  
cd ../server && npm install
cd ../backtest && npm install
```

### 資料庫設定
```bash
# 初始化 TimescaleDB 架構
psql $DATABASE_URL -f init_timescaledb.sql
```

### 執行服務

**啟動個別服務：**
```bash
# 歷史數據處理器
cd hisbet && node hisbet.js

# 即時事件監聽器
cd realbet && node realbet.js

# 網頁儀表板伺服器
cd server && node server.js

# 回測服務
cd backtest && node backtest.js
```

**開發環境自動重啟：**
```bash
# 使用 nodemon 進行開發（全域安裝：npm i -g nodemon）
cd hisbet && nodemon hisbet.js
cd realbet && nodemon realbet.js  
cd server && nodemon server.js
```

### 測試和除錯

**資料庫查詢除錯：**
```bash
# 檢查最新處理的局次
psql $DATABASE_URL -c "SELECT MAX(epoch) FROM hisbet;"
psql $DATABASE_URL -c "SELECT MAX(epoch) FROM realbet;"

# 檢查錢包分析數據
psql $DATABASE_URL -c "SELECT wallet_address, epoch, short_12_win_rate FROM wallet_analysis ORDER BY epoch DESC LIMIT 10;"

# 監控失敗局次
psql $DATABASE_URL -c "SELECT * FROM failed_epochs ORDER BY failed_at DESC;"
```

**Redis 除錯：**
```bash
# 監控 Redis streams 和 pub/sub
redis-cli -u $REDIS_URL MONITOR

# 檢查 Redis streams
redis-cli -u $REDIS_URL XINFO STREAM bet_stream

# 檢查最新回測結果
redis-cli -u $REDIS_URL GET latest_backtest
```

**日誌分析：**
```bash
# 即時監控服務日誌（使用 Railway）
railway logs --service=hisbet
railway logs --service=realbet
railway logs --service=server
```

## 架構詳情

### 智能合約整合
- **合約地址**：存放在 `CONTRACT_ADDR` 環境變數中
- **ABI 定義**：位於 `abi.json`（跨服務共用）
- **監控事件**：`BetBull`、`BetBear`、`Claim`、`EndRound`、`LockRound`
- **網路**：BSC 主網（鏈 ID：56）

### 資料庫架構 (TimescaleDB)
- **核心表**：`round`、`hisbet`、`realbet`、`claim`、`multi_claim`
- **分析表**：`wallet_analysis`（計算勝率和盈虧）
- **營運表**：`finepoch`、`failed_epochs`
- **時間序列優化**：所有表使用 TimescaleDB hypertables
- **地址標準化**：所有錢包地址以小寫存放，並加上約束

### Redis 架構
- **Streams**：`bet_stream` 用於可靠的事件處理
- **Pub/Sub 頻道**：`round_update_channel`、`backtest_results`
- **消費者群組**：`bet_processors` 具備故障轉移支援
- **快取**：區塊時間戳、局次數據、分析結果

### 服務通訊模式

**即時流程：**
1. `realbet.js` 監聽 WebSocket 事件 → Redis Stream
2. Stream 消費者處理事件 → `realbet` 表
3. `hisbet.js` 整合數據 → `hisbet` 表 + 分析
4. Redis pub/sub 通知其他服務更新

**分析管道：**
1. 計算 12 局和 48 局窗口的錢包分析
2. 對歷史數據執行回測策略
3. 結果快取在 Redis 中並透過 WebSocket 提供

## 開發指引

### 程式碼模式
- **地址處理**：始終使用 `normalizeAddress()` 轉換為小寫
- **時間處理**：使用 `toTaipeiTimeString()` 確保時區格式一致
- **錯誤處理**：區分可重試和致命錯誤
- **快取**：為頻繁存取的數據實作 LRU 快取（區塊時間戳、局次）

### 配置管理
- 所有服務使用包含 Railway 專用 URL 的 `.env` 文件
- 資料庫連接使用連接池（最多 10-15 個連接）
- Redis 客戶端實作指數退避重試策略
- WebSocket 連接包含心跳監控

### Railway 部署
- 每個服務有自己的 `railway.json` 和重啟策略
- 服務配置為 `ON_FAILURE` 重啟，最多重試 10 次
- 環境變數透過 Railway 的儀表板管理
- 日誌可透過 `railway logs --service=<名稱>` 存取

### 數據一致性模式
- **局次處理**：透過 `finepoch` 表追蹤的序列處理
- **重複預防**：使用基於交易的 upsert 和衝突解決
- **區塊範圍計算**：基於歷史數據的動態範圍計算
- **重試邏輯**：指數退避，具有最大重試限制

## 效能考量

### 批次處理
- `hisbet.js` 以可配置的批次大小處理（預設：100,000 筆記錄）
- 區塊範圍計算透過分析歷史模式優化 RPC 呼叫
- LRU 快取減少重複的區塊鏈查詢

### 即時處理
- Redis streams 透過確認追蹤確保無事件遺失
- WebSocket 連接包含心跳的自動重連
- 消費者群組提供負載分散和故障轉移

### 資料庫優化
- TimescaleDB hypertables 提供自動分區
- 在 `epoch`、`wallet_address` 和時間欄位上的策略性索引
- 臨時表的保留策略（`realbet` 自動清理）

## 重要環境變數

從 `.env` 文件中，系統需要以下關鍵配置：
- `DATABASE_URL`：TimescaleDB 連接字串
- `REDIS_URL`：Redis 連接字串
- `CONTRACT_ADDR`：BSC 預測市場合約地址
- `RPC_URL`：BSC RPC 端點（用於歷史數據）
- `WSS_URL`：BSC WebSocket 端點（用於即時數據）

## 常見問題排除

### 服務無法啟動
檢查環境變數是否正確設定，特別是資料庫和 Redis URL

### 數據處理停滯
檢查 `failed_epochs` 表找出失敗的局次，檢視 Redis streams 的積壓情況

### WebSocket 連接問題
監控心跳日誌，確保 WSS_URL 可達且網路穩定

### 效能問題
檢查資料庫連接池使用情況，監控 Redis 記憶體使用，考慮增加快取大小