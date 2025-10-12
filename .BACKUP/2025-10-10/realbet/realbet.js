/**
 * BSC 預測市場即時監聽服務 - Redis Stream 優化版
 * 
 * 架構：
 * - WebSocket 監聽 → Redis Stream (傳輸層)
 * - Stream Consumer → realbet 表 (暫存層)
 * - 數據不丟失，自動重試
 */

const dotenv = require("dotenv");
dotenv.config();

const { ethers } = require("ethers");
const { Pool } = require("pg");
const fs = require("fs");
const { createClient } = require("redis");

// 配置檢查
if (!process.env.DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!process.env.WSS_URL) throw new Error("Missing WSS_URL");
if (!process.env.CONTRACT_ADDR) throw new Error("Missing CONTRACT_ADDR");
if (!process.env.REDIS_URL) throw new Error("Missing REDIS_URL");

const WSS_URL = process.env.WSS_URL;
const CONTRACT_ADDR = process.env.CONTRACT_ADDR;
const CONTRACT_ABI = JSON.parse(fs.readFileSync("./abi.json", "utf8"));

// Redis 客戶端（確保使用 .env 的 REDIS_URL）
const redisPublisher = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) return new Error('Max retries reached');
      return Math.min(retries * 100, 3000);
    }
  }
});
const redisConsumer = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) return new Error('Max retries reached');
      return Math.min(retries * 100, 3000);
    }
  }
});

// 資料庫連接池
const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
});

// Stream 配置
const STREAM_NAME = 'bet_stream';
const GROUP_NAME = 'bet_processors';
const CONSUMER_NAME = 'consumer_1';
const BATCH_SIZE = 100;
const BATCH_TIMEOUT = 1000;

// LRU 快取
class LRUCache {
  constructor(limit = 1000) {
    this.limit = limit;
    this.cache = new Map();
  }
  get(key) {
    if (!this.cache.has(key)) return null;
    const val = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, val);
    return val;
  }
  set(key, val) {
    if (this.cache.has(key)) this.cache.delete(key);
    else if (this.cache.size >= this.limit) {
      this.cache.delete(this.cache.keys().next().value);
    }
    this.cache.set(key, val);
  }
}

const blockTimestampCache = new LRUCache();

// 工具函數
function toTaipeiTimeString(ts) {
  if (ts === null || ts === undefined || isNaN(Number(ts))) {
    ts = Math.floor(Date.now() / 1000);
  }
  return new Date(Number(ts) * 1000).toLocaleString('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).replace(',', '');
}

function normalizeAddress(address) {
  if (!address || typeof address !== 'string') throw new Error(`無效地址: ${address}`);
  const trimmed = address.trim();
  if (!/^0x[0-9a-fA-F]{40}$/.test(trimmed)) throw new Error(`格式錯誤: ${trimmed}`);
  return trimmed.toLowerCase();
}

async function getBlockTimestamp(provider, blockNumber) {
  const cached = blockTimestampCache.get(blockNumber);
  if (cached) return cached;
  try {
    const block = await provider.getBlock(blockNumber);
    if (block && block.timestamp) {
      blockTimestampCache.set(blockNumber, block.timestamp);
      return block.timestamp;
    }
  } catch (error) {
    console.error(`獲取區塊 ${blockNumber} 時間戳失敗:`, error.message);
  }
  return Math.floor(Date.now() / 1000);
}

// ========================================
// WebSocket 監聽服務
// ========================================

class BettingListenerService {
  constructor() {
    this.provider = null;
    this.contract = null;
    this.isConnected = false;
    this.reconnectTimeout = null;
    this.heartbeatInterval = null;
    this.lastHeartbeat = Date.now();
  }

  async connect() {
    console.log(`🔌 連接 WebSocket: ${WSS_URL}`);
    try {
      this.provider = new ethers.WebSocketProvider(WSS_URL);
      this.contract = new ethers.Contract(CONTRACT_ADDR, CONTRACT_ABI, this.provider);

      this.setupEventListeners();
      await this.provider.getNetwork();
      this.startHeartbeat();

    } catch (error) {
      console.error(`❌ WebSocket 初始化失敗: ${error.message}`);
      this.scheduleReconnect();
    }
  }

  startHeartbeat() {
    this.heartbeatInterval = setInterval(async () => {
      const now = Date.now();
      const timeSinceLastBeat = now - this.lastHeartbeat;

      if (timeSinceLastBeat > 120000) { // 2分鐘才判定超時
        console.warn(`💔 心跳超時 (${Math.floor(timeSinceLastBeat / 1000)}秒)，重新連接...`);
        this.scheduleReconnect();
        return;
      }

      try {
        await this.provider.getBlockNumber();
        this.lastHeartbeat = Date.now();
      } catch (error) {
        console.warn(`💔 心跳檢查失敗: ${error.message}`);
        this.scheduleReconnect();
      }
    }, 60000); // 1分鐘檢查一次，平衡可靠性與 RPC 消耗
  }

  stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  setupEventListeners() {
    if (!this.provider || !this.contract) return;

    this.provider.websocket.on('open', () => {
      this.isConnected = true;
      this.lastHeartbeat = Date.now();
      console.log("✅ WebSocket 連接成功");
      if (this.reconnectTimeout) {
        clearTimeout(this.reconnectTimeout);
        this.reconnectTimeout = null;
      }
    });

    this.provider.websocket.on('close', (code) => {
      this.isConnected = false;
      console.warn(`🔌 WebSocket 斷線 (${code})`);
      this.scheduleReconnect();
    });

    this.provider.websocket.on('error', (err) => {
      console.error(`❌ WebSocket 錯誤: ${err.message}`);
    });

    this.contract.on("BetBull", (sender, epoch, amount, event) => {
      this.lastHeartbeat = Date.now();
      console.log(`🐂 UP ${epoch} ${ethers.formatEther(amount)} BNB`);
      handleNewBet(this, sender, epoch, amount, "UP", event);
    });

    this.contract.on("BetBear", (sender, epoch, amount, event) => {
      this.lastHeartbeat = Date.now();
      console.log(`🐻 DOWN ${epoch} ${ethers.formatEther(amount)} BNB`);
      handleNewBet(this, sender, epoch, amount, "DOWN", event);
    });

    console.log("🎧 事件監聽器已啟動");
  }

  scheduleReconnect() {
    if (this.reconnectTimeout) return;
    this.reconnectTimeout = setTimeout(() => {
      console.log("🔄 重連中...");
      this.disconnect();
      this.connect();
    }, 5000);
  }

  disconnect() {
    this.stopHeartbeat();
    if (this.contract) {
      this.contract.removeAllListeners();
    }
    if (this.provider) {
      this.provider.destroy();
    }
    this.provider = null;
    this.contract = null;
    this.isConnected = false;
  }
}

// ========================================
// 事件處理：寫入 Redis Stream
// ========================================

async function handleNewBet(service, sender, epoch, amount, direction, { log }) {
  try {
    const blockTimestamp = await getBlockTimestamp(service.provider, log.blockNumber);
    const betData = {
      epoch: Number(epoch),
      bet_time: toTaipeiTimeString(blockTimestamp),
      wallet_address: normalizeAddress(sender),
      bet_direction: direction,
      bet_amount: parseFloat(ethers.formatEther(amount)),
      block_number: log.blockNumber,
      tx_hash: log.transactionHash.toLowerCase(),
    };

    // 寫入 Redis Stream（持久化）
    await redisPublisher.xAdd(STREAM_NAME, '*', {
      data: JSON.stringify(betData)
    });

    // 立即推送給前端（不等資料庫）
    await redisPublisher.publish('instant_bet_channel', JSON.stringify({
      type: 'instant_bet',
      data: betData
    }));

  } catch (error) {
    console.error(`❌ 處理下注事件失敗:`, error.message);
  }
}

// ========================================
// Stream 消費者：批次寫入資料庫
// ========================================

async function startStreamConsumer() {
  console.log('🔄 啟動 Stream 消費者');

  // 建立消費者群組
  try {
    await redisConsumer.xGroupCreate(STREAM_NAME, GROUP_NAME, '0', { MKSTREAM: true });
    console.log(`✅ 建立消費者群組: ${GROUP_NAME}`);
  } catch (error) {
    if (error.message.includes('BUSYGROUP')) {
      console.log(`ℹ️  消費者群組已存在: ${GROUP_NAME}`);
    } else {
      throw error;
    }
  }

  let batchBuffer = [];
  let lastFlush = Date.now();

  while (true) {
    try {
      // 讀取訊息（阻塞 1 秒）
      const messages = await redisConsumer.xReadGroup(
        GROUP_NAME,
        CONSUMER_NAME,
        [{ key: STREAM_NAME, id: '>' }],
        { COUNT: BATCH_SIZE, BLOCK: 1000 }
      );

      if (messages && messages[0]) {
        for (const msg of messages[0].messages) {
          try {
            const betData = JSON.parse(msg.message.data);
            batchBuffer.push({ id: msg.id, data: betData });
          } catch (parseError) {
            console.error('解析訊息失敗:', parseError.message);
            // ACK 無效訊息，避免重複處理
            await redisConsumer.xAck(STREAM_NAME, GROUP_NAME, msg.id);
          }
        }
      }

      // 批次寫入條件
      const shouldFlush = batchBuffer.length >= BATCH_SIZE || 
                         (batchBuffer.length > 0 && Date.now() - lastFlush > BATCH_TIMEOUT);

      if (shouldFlush) {
        await flushBatch(batchBuffer);
        batchBuffer = [];
        lastFlush = Date.now();
      }

    } catch (error) {
      console.error('消費者錯誤:', error.message);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function flushBatch(batch) {
  if (batch.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 批次插入
    const values = [];
    const params = [];
    let idx = 1;

    for (const item of batch) {
      const bet = item.data;
      values.push(`($${idx},$${idx+1},$${idx+2},$${idx+3},$${idx+4},$${idx+5},$${idx+6})`);
      params.push(
        bet.epoch,
        bet.bet_time,
        bet.wallet_address,
        bet.bet_direction,
        bet.bet_amount,
        bet.block_number,
        bet.tx_hash
      );
      idx += 7;
    }

    await client.query(`
      INSERT INTO realbet(epoch, bet_time, wallet_address, bet_direction, bet_amount, block_number, tx_hash)
      VALUES ${values.join(',')}
      ON CONFLICT (bet_time, tx_hash) DO NOTHING
    `, params);

    await client.query('COMMIT');

    // ACK 所有訊息
    for (const item of batch) {
      await redisConsumer.xAck(STREAM_NAME, GROUP_NAME, item.id);
    }

    console.log(`💾 批次寫入: ${batch.length} 筆`);

    // 推送分析請求
    const analysisPromises = batch.map(item => {
      return redisPublisher.publish('analysis_channel', JSON.stringify({
        type: 'analysis_request',
        bet: item.data
      }));
    });
    await Promise.all(analysisPromises);

  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`❌ 批次寫入失敗: ${error.message}`);
    // 不 ACK，Redis 會自動重新投遞
  } finally {
    client.release();
  }
}

// ========================================
// 監控：查看 Stream 狀態
// ========================================

async function monitorStream() {
  setInterval(async () => {
    try {
      const length = await redisPublisher.xLen(STREAM_NAME);
      const pending = await redisPublisher.xPending(STREAM_NAME, GROUP_NAME);
      
      if (length > 1000) {
        console.warn(`⚠️  Stream 積壓: ${length} 條訊息`);
      }
      
      if (pending && pending.pending > 100) {
        console.warn(`⚠️  未處理訊息: ${pending.pending} 條`);
      }
    } catch (error) {
      // 監控失敗不影響主流程
    }
  }, 60000); // 每分鐘檢查一次
}

// ========================================
// 主程序啟動
// ========================================

(async () => {
  try {
    console.log("🚀 啟動即時下注監聽服務 (Redis Stream 版)");

    // 連接 Redis
    await redisPublisher.connect();
    await redisConsumer.connect();
    console.log('✅ Redis 連接成功');

    redisPublisher.on('error', err => console.error('Redis Publisher 錯誤:', err.message));
    redisConsumer.on('error', err => console.error('Redis Consumer 錯誤:', err.message));

    // 測試資料庫連接
    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    console.log('✅ 資料庫連接成功');

    // 啟動 WebSocket 監聽器
    const listenerService = new BettingListenerService();
    listenerService.connect();

    // 啟動 Stream 消費者（背景執行）
    startStreamConsumer();

    // 啟動監控
    monitorStream();

    // 優雅關閉
    process.on('SIGTERM', async () => {
      console.log('📴 收到 SIGTERM，優雅關閉...');
      listenerService.disconnect();
      await redisPublisher.quit();
      await redisConsumer.quit();
      await pool.end();
      process.exit(0);
    });

    process.on('SIGINT', async () => {
      console.log('📴 收到 SIGINT，優雅關閉...');
      listenerService.disconnect();
      await redisPublisher.quit();
      await redisConsumer.quit();
      await pool.end();
      process.exit(0);
    });

  } catch (err) {
    console.error(`❌ 啟動失敗: ${err.message}`);
    process.exit(1);
  }
})();