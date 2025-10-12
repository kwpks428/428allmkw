const dotenv = require("dotenv");
dotenv.config(); // Load .env from the current directory

const { genkit, z } = require("genkit");

const { ethers } = require("ethers");
const { Pool } = require("pg");
const { createClient } = require("ioredis");
const fs = require("fs");

// 1. GENKIT INITIALIZATION
// ========================================

const ai = genkit({
  plugins: [],
  logLevel: "debug",
  enableTracingAndMetrics: true,
});

// 2. CONFIGURATION & SHARED RESOURCES
// ========================================

// Config validation
if (!process.env.DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!process.env.WSS_URL) throw new Error("Missing WSS_URL");
if (!process.env.CONTRACT_ADDR) throw new Error("Missing CONTRACT_ADDR");
if (!process.env.REDIS_URL) throw new Error("Missing REDIS_URL");

const WSS_URL = process.env.WSS_URL;
const CONTRACT_ADDR = process.env.CONTRACT_ADDR;
const CONTRACT_ABI = JSON.parse(fs.readFileSync("./abi.json", "utf8"));

// Shared DB Pool
const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
});

// Redis Client for real-time publishing
const redisPublisher = createClient(process.env.REDIS_URL, {
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    if (times > 3) return null;
    return Math.min(times * 1000, 3000);
  }
});
redisPublisher.on('error', (err) => console.error('Redis Publisher Error:', err.message));

// 3. GENKIT FLOW DEFINITION
// ========================================

const BetDataSchema = z.object({
  epoch: z.number(),
  bet_time: z.string(),
  wallet_address: z.string(),
  bet_direction: z.string(),
  bet_amount: z.number(),
  block_number: z.number(),
  tx_hash: z.string(),
});

const realtimeBetFlow = ai.defineFlow(
  {
    name: "realtimeBetFlow",
    inputSchema: BetDataSchema,
    outputSchema: z.object({ success: z.boolean(), tx_hash: z.string() }),
  },
  async (bet) => {
    const client = await pool.connect();
    try {
      const query = `
        INSERT INTO realbet(epoch, bet_time, wallet_address, bet_direction, bet_amount, block_number, tx_hash)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (bet_time, tx_hash) DO NOTHING
      `;
      const params = [
        bet.epoch,
        bet.bet_time,
        bet.wallet_address,
        bet.bet_direction,
        bet.bet_amount,
        bet.block_number,
        bet.tx_hash,
      ];
      
      await client.query(query, params);
      console.log(`[Genkit Flow] 💾 資料庫寫入成功: ${bet.tx_hash}`);
      
      // 🚀 推送到前端 (Redis Pub/Sub)
      const frontendData = {
        type: 'realtime_bet',
        timestamp: new Date().toISOString(),
        data: {
          epoch: bet.epoch,
          direction: bet.bet_direction,
          amount: bet.bet_amount,
          wallet: bet.wallet_address.toLowerCase(),
          tx_hash: bet.tx_hash,
          block_number: bet.block_number
        }
      };
      
      try {
        await redisPublisher.publish('realtime_bets_channel', JSON.stringify(frontendData));
        console.log(`[Genkit Flow] 📡 前端推送成功: ${bet.bet_direction} ${bet.bet_amount} BNB`);
      } catch (publishError) {
        console.error(`[Genkit Flow] ⚠️ 前端推送失敗:`, publishError.message);
        // 不影響主流程，繼續執行
      }
      
      return { success: true, tx_hash: bet.tx_hash };

    } catch (error) {
      console.error(`[Genkit Flow] ❌ 資料庫寫入失敗:`, error.message);
      throw error; // Let Genkit handle the error state
    } finally {
      client.release();
    }
  }
);

// 4. WEBSOCKET LISTENER SERVICE
// ========================================

// LRU Cache for block timestamps
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

// Helper functions
function toTaipeiTimeString(ts) {
  return new Date(Number(ts) * 1000).toLocaleString('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
}

function normalizeAddress(address) {
  return address.toLowerCase();
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
    console.error(`[Listener] 獲取區塊 ${blockNumber} 時間戳失敗:`, error.message);
  }
  return Math.floor(Date.now() / 1000);
}

class BettingListenerService {
  constructor() {
    this.provider = null;
    this.contract = null;
    this.reconnectTimeout = null;
  }

  connect() {
    console.log(`[Listener] 🔌 連接 WebSocket: ${WSS_URL}`);
    try {
      this.provider = new ethers.WebSocketProvider(WSS_URL);
      this.contract = new ethers.Contract(CONTRACT_ADDR, CONTRACT_ABI, this.provider);
      this.setupEventListeners();
    } catch (error) {
      console.error(`[Listener] ❌ WebSocket 初始化失敗: ${error.message}`);
      this.scheduleReconnect();
    }
  }

  setupEventListeners() {
    this.provider.websocket.on('open', () => {
      console.log("[Listener] ✅ WebSocket 連接成功");
      if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
    });

    this.provider.websocket.on('close', (code) => {
      console.warn(`[Listener] 🔌 WebSocket 斷線 (${code})`);
      this.scheduleReconnect();
    });

    this.provider.websocket.on('error', (err) => {
      console.error(`[Listener] ❌ WebSocket 錯誤: ${err.message}`);
    });

    this.contract.on("BetBull", (sender, epoch, amount, event) => {
      console.log(`[Listener] 🐂 UP ${epoch} ${ethers.formatEther(amount)} BNB`);
      this.handleNewBet(sender, epoch, amount, "UP", event.log);
    });

    this.contract.on("BetBear", (sender, epoch, amount, event) => {
      console.log(`[Listener] 🐻 DOWN ${epoch} ${ethers.formatEther(amount)} BNB`);
      this.handleNewBet(sender, epoch, amount, "DOWN", event.log);
    });

    console.log("[Listener] 🎧 事件監聽器已啟動");
  }

  async handleNewBet(sender, epoch, amount, direction, log) {
    try {
      const blockTimestamp = await getBlockTimestamp(this.provider, log.blockNumber);
      const betData = {
        epoch: Number(epoch),
        bet_time: toTaipeiTimeString(blockTimestamp),
        wallet_address: normalizeAddress(sender),
        bet_direction: direction,
        bet_amount: parseFloat(ethers.formatEther(amount)),
        block_number: log.blockNumber,
        tx_hash: log.transactionHash.toLowerCase(),
      };

      // *** CALL THE GENKIT FLOW ***
      realtimeBetFlow(betData).catch(flowError => {
        console.error(`[Listener] ❌ Flow 執行失敗 for tx ${betData.tx_hash}:`, flowError.message);
      });

    } catch (error) {
      console.error(`[Listener] ❌ 處理下注事件失敗:`, error.message);
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimeout) return;
    this.reconnectTimeout = setTimeout(() => {
      console.log("[Listener] 🔄 重連中...");
      this.disconnect();
      this.connect();
    }, 5000);
  }

  disconnect() {
    if (this.contract) this.contract.removeAllListeners();
    if (this.provider) this.provider.destroy();
  }
}

// 5. MAIN EXECUTION
// ========================================

async function main() {
  try {
    console.log("🚀 啟動 Genkit 即時下注服務");

    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    console.log('✅ 資料庫連接成功');

    const listenerService = new BettingListenerService();
    listenerService.connect();

    console.log("✅ 服務已啟動。使用 'genkit start' 來查看 Flow 追蹤。");

  } catch (err) {
    console.error(`❌ 啟動失敗: ${err.message}`);
    process.exit(1);
  }
}

// Export for external use
module.exports = {
  realtimeBetFlow,
};

// If this script is run directly (e.g., `node gk_realbet.js`)
if (require.main === module) {
  main();
}
