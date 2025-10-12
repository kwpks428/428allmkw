/**
 * BSC 預測市場即時監控系統 - Web 伺服器（完整優化版）
 * 
 * 改進：
 * 1. 統一地址標準化（資料庫層 + 應用層）
 * 2. 分析計算加鎖（防止重複計算）
 * 3. 完善錯誤處理（區分致命/可恢復錯誤）
 * 4. Redis 重連機制
 * 5. 修正圖表生成邏輯
 * 6. 完整的客戶端消息處理
 */

const dotenv = require("dotenv");
dotenv.config();

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");
const { createClient } = require("redis");
const { ethers } = require("ethers");

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

// 環境變數檢查
if (!process.env.DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!process.env.REDIS_URL) throw new Error("Missing REDIS_URL");
if (!process.env.RPC_URL) throw new Error("Missing RPC_URL");
if (!process.env.CONTRACT_ADDR) throw new Error("Missing CONTRACT_ADDR");

// ========================================
// 工具函數模組
// ========================================

/**
 * 標準化錢包地址為小寫
 */
function normalizeAddress(address) {
  if (!address || typeof address !== 'string') {
    throw new Error(`無效地址: ${address}`);
  }
  const trimmed = address.trim();
  if (!/^0x[0-9a-fA-F]{40}$/.test(trimmed)) {
    throw new Error(`地址格式錯誤: ${trimmed}`);
  }
  return trimmed.toLowerCase();
}

/**
 * 將 Unix 秒時間戳轉換為台北時區字串
 * 格式：YYYY-MM-DD HH:mm:ss
 */
function toTaipeiTimeString(ts) {
  if (ts === null || ts === undefined || isNaN(Number(ts))) {
    console.warn(`⚠️ 無效時間戳: ${ts}，使用當前時間`);
    ts = Math.floor(Date.now() / 1000);
  }
  
  const date = new Date(Number(ts) * 1000);
  
  return date.toLocaleString('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).replace(',', '');
}

/**
 * 延遲函數
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ========================================
// 初始化
// ========================================

const contractAddr = process.env.CONTRACT_ADDR;
const contractAbi = JSON.parse(fs.readFileSync("./abi.json", "utf8"));

const dbPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 15,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// Redis 客戶端（使用 .env 的 REDIS_URL）
// 訂閱專用客戶端
const redisSubscriber = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) return new Error('Max retries reached');
      return Math.min(retries * 100, 3000);
    }
  }
});

// 普通操作客戶端（用於鎖、計數等）
const redisClient = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) return new Error('Max retries reached');
      return Math.min(retries * 100, 3000);
    }
  }
});

const provider = new ethers.JsonRpcProvider(process.env.RPC_URL);
const contract = new ethers.Contract(contractAddr, contractAbi, provider);

// HTTP 伺服器
const server = http.createServer((req, res) => {
  if (req.url === '/') {
    const filePath = path.join(__dirname, 'app.html');
    fs.readFile(filePath, 'utf8', (err, content) => {
      if (err) {
        console.error('❌ 讀取 app.html 錯誤:', err);
        res.writeHead(500);
        res.end('Error loading app.html');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(content);
    });
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

const wss = new WebSocketServer({ server });

// ========================================
// 資料庫初始化
// ========================================

async function initDatabase() {
  let client;
  try {
    console.log('📊 初始化資料庫...');
    client = await dbPool.connect();

    // 創建 wallet_analysis 表
    await client.query(`
      CREATE TABLE IF NOT EXISTS wallet_analysis (
        wallet_address VARCHAR(42) NOT NULL,
        epoch BIGINT NOT NULL,
        calculated_at TIMESTAMPTZ DEFAULT NOW(),
        short_12_total_bets INTEGER NOT NULL DEFAULT 0,
        short_12_win_rate NUMERIC(5, 2),
        short_12_profit_loss NUMERIC(20, 8),
        mid_48_total_bets INTEGER NOT NULL DEFAULT 0,
        mid_48_win_rate NUMERIC(5, 2),
        mid_48_profit_loss NUMERIC(20, 8),
        PRIMARY KEY (wallet_address, epoch)
      );
    `);
    
    // 確保地址小寫約束
    try {
      await client.query(`
        ALTER TABLE wallet_analysis 
        ADD CONSTRAINT check_wallet_analysis_lowercase 
        CHECK (wallet_address = LOWER(wallet_address))
      `);
    } catch (e) {
      // 約束可能已存在
    }

    // 創建索引
    await client.query('CREATE INDEX IF NOT EXISTS idx_wallet_analysis_epoch ON wallet_analysis (epoch);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_wallet_analysis_calculated ON wallet_analysis (calculated_at);');
    
    console.log('✅ wallet_analysis 表初始化完成');
    
    // 創建 prediction_snapshots 表
    await client.query(`
      CREATE TABLE IF NOT EXISTS prediction_snapshots (
        epoch BIGINT PRIMARY KEY,
        snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        follow_best_prediction VARCHAR(10),
        follow_best_wallet VARCHAR(42),
        follow_best_win_rate_12 NUMERIC(5, 2),
        follow_best_win_rate_48 NUMERIC(5, 2),
        reverse_low_prediction VARCHAR(10),
        reverse_low_wallet VARCHAR(42),
        reverse_low_win_rate_12 NUMERIC(5, 2),
        reverse_low_win_rate_48 NUMERIC(5, 2),
        momentum_prediction VARCHAR(10),
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    
    // 創建索引
    await client.query('CREATE INDEX IF NOT EXISTS idx_prediction_snapshots_epoch ON prediction_snapshots(epoch DESC);');
    
    console.log('✅ prediction_snapshots 表初始化完成');

  } catch (error) {
    console.error('❌ 初始化資料庫失敗:', error.message);
    throw error;
  } finally {
    if (client) client.release();
  }
}

// ========================================
// Redis 連接管理
// ========================================

async function setupRedisSubscriptions() {
  try {
    await redisSubscriber.subscribe('instant_bet_channel', handleInstantBet);
    await redisSubscriber.subscribe('analysis_channel', handleAnalysisRequest);
    await redisSubscriber.subscribe('round_update_channel', handleRoundUpdate);
    await redisSubscriber.subscribe('backtest_results', handleBacktestResults);
    await redisSubscriber.subscribe('live_predictions', handleLivePredictions);
    await redisSubscriber.subscribe('tx_benchmark', handleTxBenchmark);
    console.log('✅ Redis 訂閱已建立');
  } catch (error) {
    console.error('❌ Redis 訂閱失敗:', error);
    throw error;
  }
}

redisSubscriber.on('error', async (err) => {
  console.error('❌ Redis 錯誤:', err);
});

redisSubscriber.on('reconnecting', () => {
  console.log('🔄 Redis 重新連接中...');
});

redisSubscriber.on('ready', async () => {
  console.log('✅ Redis 連接成功');
  await setupRedisSubscriptions();
});

// ========================================
// 局次處理
// ========================================

async function processNewRound(epoch, roundDataFromChain) {
  const client = await dbPool.connect();
  try {
    await client.query(`
      INSERT INTO round(epoch, start_time, lock_time, close_time, lock_price, close_price, result, 
        total_bet_amount, up_bet_amount, down_bet_amount, up_payout, down_payout)
      VALUES($1, $2, $3, $4, 0, 0, NULL, 0, 0, 0, 0, 0)
      ON CONFLICT (start_time, epoch) DO NOTHING
    `, [
      epoch,
      toTaipeiTimeString(roundDataFromChain.startTimestamp),
      toTaipeiTimeString(roundDataFromChain.lockTimestamp),
      toTaipeiTimeString(roundDataFromChain.closeTimestamp)
    ]);
    
    await broadcastRoundUpdate(epoch, roundDataFromChain);
  } catch(error) {
    console.error('❌ 處理新局次錯誤:', error.message);
  } finally {
    client.release();
  }
}

async function broadcastRoundUpdate(epoch, roundDataFromChain) {
  const now = Date.now();
  const lockTime = Number(roundDataFromChain.lockTimestamp) * 1000;
  const closeTime = Number(roundDataFromChain.closeTimestamp) * 1000;
  
  let status = 'ENDED';
  if (now < lockTime) status = 'LIVE';
  else if (now < closeTime) status = 'LOCKED';

  const roundDataForWs = {
    epoch: epoch,
    lockTimestamp: Number(roundDataFromChain.lockTimestamp),
    closeTimestamp: Number(roundDataFromChain.closeTimestamp),
    bullAmount: 0,
    bearAmount: 0,
    totalAmount: 0,
    status: status,
    result: null,
    closePrice: null
  };
  
  // 廣播到 WebSocket 客戶端
  broadcast({ type: 'round_update', data: roundDataForWs });
  
  // 發布到 Redis channel 供 backtest 服務訂閱
  try {
    await redisClient.publish('round_update_channel', JSON.stringify(roundDataForWs));
  } catch (error) {
    console.error('❌ 發布到 Redis round_update_channel 失敗:', error.message);
  }
  
  console.log(`📡 已廣播局次 ${epoch} (${status})`);
}

// ========================================
// 錢包分析 - 主函數（帶鎖機制）
// ========================================

async function calculateWeightedFailureRate(client, walletAddress, currentEpoch) {
  // 標準化地址
  try {
    walletAddress = normalizeAddress(walletAddress);
  } catch (error) {
    console.error('❌ 地址標準化失敗:', error.message);
    return null;
  }

  console.log(`📊 計算錢包 ${walletAddress} 在局次 ${currentEpoch}`);

  // 1. 嘗試從快取讀取
  const cached = await getAnalysisFromCache(client, walletAddress, currentEpoch);
  if (cached) {
    console.log(`📋 使用快取: ${walletAddress}`);
    return cached;
  }

  // 2. 使用 Redis 鎖防止重複計算
  const lockKey = `analysis:${walletAddress}:${currentEpoch}`;
  const lockAcquired = await redisClient.set(lockKey, 'true', {
    EX: 60,
    NX: true
  });

  if (!lockAcquired) {
    console.log(`🔒 ${walletAddress} 正在計算中，等待...`);
    await sleep(1000);
    // 重試讀取快取
    return await getAnalysisFromCache(client, walletAddress, currentEpoch);
  }

  try {
    // 雙重檢查
    const cachedAgain = await getAnalysisFromCache(client, walletAddress, currentEpoch);
    if (cachedAgain) {
      console.log(`📋 雙重檢查命中快取: ${walletAddress}`);
      return cachedAgain;
    }

    // 3. 計算
    console.log(`💻 重新計算: ${walletAddress}`);
    const baseEpoch = currentEpoch - 2;

    const short12 = await calculateWindowStats(client, walletAddress, baseEpoch - 11, baseEpoch, 12);
    const mid48 = await calculateWindowStats(client, walletAddress, baseEpoch - 47, baseEpoch, 48);

    console.log(`📊 計算結果: 12局 ${short12.total_bets} 筆, 48局 ${mid48.total_bets} 筆`);

    // 4. 存入快取
    await saveAnalysisToCache(client, walletAddress, currentEpoch, short12, mid48);

    // 5. 查詢錢包標籤和圖表數據
    const walletTags = await analyzeWalletTags(client, walletAddress);
    const chartData = await get50RoundsChartData(client, walletAddress, currentEpoch);

    return {
      short_12_rounds: {
        ...short12,
        score: short12.bare_score
      },
      mid_48_rounds: {
        ...mid48,
        score: mid48.bare_score
      },
      wallet_tags: walletTags,
      chart_data: chartData
    };

  } finally {
    // 釋放鎖
    await redisClient.del(lockKey);
  }
}

// ========================================
// 錢包分析 - 子函數
// ========================================

/**
 * 從快取讀取分析結果
 */
async function getAnalysisFromCache(client, walletAddress, currentEpoch) {
  try {
    const result = await client.query(`
      SELECT short_12_total_bets, short_12_win_rate as short_12_bare_score, short_12_profit_loss,
             mid_48_total_bets, mid_48_win_rate as mid_48_bare_score, mid_48_profit_loss
      FROM wallet_analysis
      WHERE wallet_address = $1 AND epoch = $2
    `, [walletAddress, currentEpoch]);

    if (!result || result.rows.length === 0) return null;

    const cached = result.rows[0];
    const walletTags = await analyzeWalletTags(client, walletAddress);
    const chartData = await get50RoundsChartData(client, walletAddress, currentEpoch);

    return {
      short_12_rounds: {
        total_bets: cached.short_12_total_bets,
        bare_score: cached.short_12_bare_score,
        score: cached.short_12_bare_score,
        profit_loss: cached.short_12_profit_loss
      },
      mid_48_rounds: {
        total_bets: cached.mid_48_total_bets,
        bare_score: cached.mid_48_bare_score,
        score: cached.mid_48_bare_score,
        profit_loss: cached.mid_48_profit_loss
      },
      wallet_tags: walletTags,
      chart_data: chartData
    };
  } catch (error) {
    console.error('❌ 讀取快取失敗:', error.message);
    return null;
  }
}

/**
 * 計算單個時間窗口的統計數據
 */
async function calculateWindowStats(client, walletAddress, startEpoch, endEpoch, windowSize) {
  if (startEpoch > endEpoch) {
    return { total_bets: 0, bare_score: null, profit_loss: null };
  }

  try {
    const result = await client.query(`
      SELECT COUNT(*) as total_bets,
             COUNT(CASE WHEN h.result = 'WIN' THEN 1 END) as wins,
             COUNT(CASE WHEN h.result = 'LOSS' THEN 1 END) as losses,
             SUM(CASE
               WHEN h.result = 'WIN' AND h.bet_direction = 'UP' 
                 THEN h.bet_amount * (COALESCE(r.up_payout, 0) - 1)
               WHEN h.result = 'WIN' AND h.bet_direction = 'DOWN' 
                 THEN h.bet_amount * (COALESCE(r.down_payout, 0) - 1)
               WHEN h.result = 'LOSS' THEN -h.bet_amount
               ELSE 0
             END) as total_profit_loss
      FROM hisbet h
      LEFT JOIN round r ON h.epoch = r.epoch
      WHERE h.wallet_address = $1 AND h.epoch BETWEEN $2 AND $3
    `, [walletAddress, startEpoch, endEpoch]);

    if (!result || result.rows.length === 0) {
      return { total_bets: 0, bare_score: null, profit_loss: null };
    }

    const { total_bets, wins, losses, total_profit_loss } = result.rows[0];
    const totalBets = parseInt(total_bets);
    const winCount = parseInt(wins);
    const lossCount = parseInt(losses);
    const profitLoss = total_profit_loss ? parseFloat(total_profit_loss) : 0;

    if (totalBets === 0) {
      return { total_bets: 0, bare_score: null, profit_loss: null };
    }

    const rawScore = winCount - lossCount;
    const bareScore = (rawScore + windowSize) * 100 / (windowSize * 2);

    return {
      total_bets: totalBets,
      bare_score: parseFloat(bareScore.toFixed(2)),
      profit_loss: parseFloat(profitLoss.toFixed(8))
    };
  } catch (error) {
    console.error('❌ 計算窗口統計失敗:', error.message);
    return { total_bets: 0, bare_score: null, profit_loss: null };
  }
}

/**
 * 分析錢包標籤（鯨魚、機器人）
 */
async function analyzeWalletTags(client, walletAddress) {
  const tags = {
    whale_level: 0,
    is_bot: false
  };

  try {
    const multiClaimResult = await client.query(`
      SELECT SUM(total_amount) as total_claim_amount,
             SUM(num_claimed_epochs) as total_claim_epochs,
             MAX(num_claimed_epochs) as max_epochs
      FROM multi_claim
      WHERE wallet_address = $1
    `, [walletAddress]);

    if (!multiClaimResult || multiClaimResult.rows.length === 0 || !multiClaimResult.rows[0].total_claim_amount) {
      return tags;
    }

    const { total_claim_amount, total_claim_epochs, max_epochs } = multiClaimResult.rows[0];
    const totalAmount = parseFloat(total_claim_amount);
    const totalEpochs = parseInt(total_claim_epochs);

    // 鯨魚判斷
    if (totalEpochs > 0) {
      const avgAmountPerRound = totalAmount / totalEpochs;
      if (avgAmountPerRound >= 1) {
        tags.whale_level = Math.floor(avgAmountPerRound);
      }
    }

    // 機器人判斷
    if (parseInt(max_epochs) >= 10) {
      const hisbetResult = await client.query(`
        SELECT bet_amount
        FROM hisbet
        WHERE wallet_address = $1
        ORDER BY bet_time DESC
        LIMIT 50
      `, [walletAddress]);

      if (hisbetResult && hisbetResult.rows.length >= 10) {
        let zeroEndingCount = 0;
        const totalBets = hisbetResult.rows.length;

        for (const row of hisbetResult.rows) {
          const amount = parseFloat(row.bet_amount);
          const amountStr = amount.toFixed(8);
          if (amountStr.endsWith('0000')) {
            zeroEndingCount++;
          }
        }

        const botPercentage = zeroEndingCount / totalBets;
        if (botPercentage >= 0.7) {
          tags.is_bot = true;
        }
      }
    }

  } catch (error) {
    console.error(`❌ 分析錢包標籤失敗 ${walletAddress}:`, error.message);
  }

  return tags;
}

/**
 * 查詢 50 局的詳細歷史數據
 */
async function get50RoundsChartData(client, walletAddress, currentEpoch) {
  try {
    const result = await client.query(`
      WITH epochs AS (
        SELECT generate_series($2, $2 - 49, -1) AS epoch
      ),
      combined_data AS (
        SELECT epoch, bet_amount, result
        FROM hisbet
        WHERE wallet_address = $1 AND epoch BETWEEN $2 - 49 AND $2
        UNION ALL
        SELECT epoch, bet_amount, NULL as result
        FROM realbet
        WHERE wallet_address = $1 AND epoch BETWEEN $2 - 49 AND $2
      )
      SELECT e.epoch,
             COALESCE(c.bet_amount, 0) as bet_amount,
             c.result
      FROM epochs e
      LEFT JOIN combined_data c ON c.epoch = e.epoch
      ORDER BY e.epoch DESC
    `, [walletAddress, currentEpoch]);

    if (!result || result.rows.length === 0) return [];

    return result.rows.map(row => ({
      epoch: parseInt(row.epoch),
      amount: parseFloat(row.bet_amount),
      result: row.result
    }));
  } catch (error) {
    console.error(`❌ 獲取圖表數據失敗 ${walletAddress}:`, error.message);
    return [];
  }
}

/**
 * 保存分析結果到快取
 */
async function saveAnalysisToCache(client, walletAddress, currentEpoch, short12, mid48) {
  try {
    await client.query(`
      INSERT INTO wallet_analysis (
        wallet_address, epoch,
        short_12_total_bets, short_12_win_rate, short_12_profit_loss,
        mid_48_total_bets, mid_48_win_rate, mid_48_profit_loss
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (wallet_address, epoch) DO UPDATE SET
        calculated_at = NOW(),
        short_12_total_bets = EXCLUDED.short_12_total_bets,
        short_12_win_rate = EXCLUDED.short_12_win_rate,
        short_12_profit_loss = EXCLUDED.short_12_profit_loss,
        mid_48_total_bets = EXCLUDED.mid_48_total_bets,
        mid_48_win_rate = EXCLUDED.mid_48_win_rate,
        mid_48_profit_loss = EXCLUDED.mid_48_profit_loss
    `, [
      walletAddress, currentEpoch,
      short12.total_bets, short12.bare_score, short12.profit_loss,
      mid48.total_bets, mid48.bare_score, mid48.profit_loss
    ]);
    console.log(`💾 已快取: ${walletAddress} epoch ${currentEpoch}`);
  } catch (error) {
    console.error('❌ 快取存儲失敗:', error.message);
  }
}

// ========================================
// 圖表生成（修正版）
// ========================================

function generateChartSVG(chartData) {
  if (!chartData || chartData.length === 0) {
    return '<div style="color: #666; font-size: 11px; text-align: center;">無圖表數據</div>';
  }

  const layout = calculateChartLayout();
  const dataPoints = prepareChartData(chartData, layout);
  return renderChartSVG(dataPoints, layout);
}

function calculateChartLayout() {
  const width = 420;
  const height = 50;
  const padding = { top: 5, right: 10, bottom: 5, left: 10 };
  const chartHeight = height - padding.top - padding.bottom;
  
  return {
    width,
    height,
    padding,
    chartHeight,
    maxAmount: 1.25,
    gap1: 12,
    gap2: 10,
    pointSpacing: 10
  };
}

function prepareChartData(chartData, layout) {
  const { padding, maxAmount, gap1, gap2, pointSpacing, chartHeight } = layout;
  
  const yScale = (amount) => chartHeight - (amount / maxAmount * chartHeight * 0.8);
  
  const xPositions = [];
  let currentX = padding.left;
  
  // 第1局
  xPositions[0] = currentX;
  currentX += pointSpacing;
  
  // 第2局
  xPositions[1] = currentX;
  currentX += gap1;
  
  // 第3-50局
  for (let i = 2; i < 50; i++) {
    xPositions[i] = currentX;
    currentX += pointSpacing;
    if ((i - 2) > 0 && (i - 2) % 12 === 11 && i < 49) {
      currentX += gap2;
    }
  }
  
  return chartData.slice(0, 50).map((point, index) => ({
    x: xPositions[index],
    y: padding.top + yScale(point.amount),
    y0: padding.top + yScale(0),  // 正確的零點位置
    amount: point.amount,
    result: point.result,
    index: index
  }));
}

function renderChartSVG(dataPoints, layout) {
  const { width, height } = layout;
  
  const pathSegments = [];
  let currentSegment = [];
  const markers = [];
  
  for (const point of dataPoints) {
    const hasBet = point.amount > 0 || point.index < 2;
    
    if (hasBet) {
      currentSegment.push(`${point.x},${point.y}`);
    } else {
      if (currentSegment.length > 0) {
        pathSegments.push(currentSegment);
        currentSegment = [];
      }
    }
    
    let marker = '';
    if (point.index === 0) {
      // 第1局：X符號
      const size = 3;
      marker = `<g>
        <line x1="${point.x-size}" y1="${point.y-size}" x2="${point.x+size}" y2="${point.y+size}" stroke="#FFD700" stroke-width="1.5"/>
        <line x1="${point.x-size}" y1="${point.y+size}" x2="${point.x+size}" y2="${point.y-size}" stroke="#FFD700" stroke-width="1.5"/>
      </g>`;
    } else if (point.index === 1) {
      // 第2局：三角形
      const size = 3;
      marker = `<polygon points="${point.x},${point.y-size} ${point.x-size},${point.y+size} ${point.x+size},${point.y+size}" fill="none" stroke="#00BFFF" stroke-width="1.2"/>`;
    } else {
      // 第3-50局：圓圈
      if (point.result === 'WIN') {
        marker = `<circle cx="${point.x}" cy="${point.y}" r="2.5" fill="#00FF00" stroke="none"/>`;
      } else if (point.result === 'LOSS') {
        marker = `<circle cx="${point.x}" cy="${point.y}" r="2.5" fill="#FF0000" stroke="none"/>`;
      } else {
        // 未下注：空心圓在零點
        marker = `<circle cx="${point.x}" cy="${point.y0}" r="2" fill="none" stroke="#888" stroke-width="1"/>`;
      }
    }
    markers.push(marker);
  }
  
  if (currentSegment.length > 0) {
    pathSegments.push(currentSegment);
  }
  
  const paths = pathSegments.map(segment => {
    if (segment.length < 2) return '';
    return `<path d="M${segment.join(' L')}" fill="none" stroke="#555" stroke-width="1"/>`;
  }).join('');
  
  return `
    <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
      ${paths}
      ${markers.join('')}
    </svg>
  `;
}

// ========================================
// 下注數據獲取
// ========================================

async function fetchBetsWithAnalysis(client, tableName, epoch) {
  try {
    // 查詢時強制地址小寫
    const betsRes = await client.query(
      `SELECT *, LOWER(wallet_address) as wallet_address
       FROM ${tableName}
       WHERE epoch = $1
       ORDER BY bet_time DESC
       LIMIT 100`,
      [epoch]
    );

    if (!betsRes || betsRes.rows.length === 0) return [];

    const bets = betsRes.rows.map(b => ({
      epoch: b.epoch,
      bet_time: b.bet_time,
      wallet_address: b.wallet_address,  // 已經是小寫
      bet_direction: b.bet_direction,
      amount: b.bet_amount,
      tx_hash: b.tx_hash,
      analysis: null
    }));

    const uniqueWallets = [...new Set(bets.map(b => b.wallet_address))];
    console.log(`📊 找到 ${bets.length} 筆下注，來自 ${uniqueWallets.length} 個錢包 (epoch ${epoch})`);

    // 並發計算分析並立即推送
    const analysisMap = new Map();
    const analysisPromises = uniqueWallets.map(async (wallet) => {
      try {
        const analysisData = await calculateWeightedFailureRate(client, wallet, epoch);
        if (analysisData) {
          analysisMap.set(wallet, analysisData);

          // 立即廣播分析結果給所有客戶端
          broadcast({
            type: 'bet_analysis',
            data: {
              wallet_address: wallet,
              analysis: analysisData
            }
          });
          console.log(`📤 即時推送分析: ${wallet.substring(0, 8)}...`);
        }
      } catch (error) {
        console.error(`❌ 計算分析失敗 ${wallet}:`, error.message);
      }
    });

    await Promise.all(analysisPromises);

    // 附加分析數據
    bets.forEach(bet => {
      if (analysisMap.has(bet.wallet_address)) {
        bet.analysis = analysisMap.get(bet.wallet_address);
      }
    });

    return bets;
  } catch (error) {
    console.error(`❌ 獲取下注數據失敗 (${tableName}, epoch ${epoch}):`, error.message);
    return [];
  }
}

// ========================================
// Redis 事件處理
// ========================================

async function handleInstantBet(message, channel) {
  try {
    const payload = JSON.parse(message);
    if (payload.type === 'instant_bet') {
      broadcast({ type: "new_bet", data: payload.data });
    }
  } catch (error) {
    console.error('❌ 處理即時下注錯誤:', error.message);
  }
}

// 追蹤最新局次，避免廣播舊局次
let latestBroadcastEpoch = 0;

// 追蹤已設置的鎖倉前5秒快照計時器
const snapshotTimers = new Map(); // epoch -> timeoutId

/**
 * 鎖倉前5秒快照計算：分析當前局所有下注，找出勝率最高/最低錢包的下注方向
 */
async function calculatePredictionSnapshot(epoch) {
  let client;
  try {
    console.log(`📸 開始計算局次 ${epoch} 的鎖倉前5秒快照...`);
    client = await dbPool.connect();
    
    // 獲取當前局所有下注（包含 realbet 和 hisbet）
    const betsQuery = await client.query(`
      SELECT DISTINCT ON (wallet_address)
        wallet_address, 
        bet_direction,
        bet_time
      FROM (
        SELECT wallet_address, bet_direction, bet_time FROM realbet WHERE epoch = $1
        UNION ALL
        SELECT wallet_address, bet_direction, bet_time FROM hisbet WHERE epoch = $1
      ) combined
      ORDER BY wallet_address, bet_time DESC
    `, [epoch]);
    
    if (betsQuery.rows.length === 0) {
      console.log(`⚠️ 局次 ${epoch} 沒有下注數據，跳過快照`);
      return;
    }
    
    console.log(`📋 局次 ${epoch} 共有 ${betsQuery.rows.length} 筆下注`);
    
    // 獲取所有錢包的勝率數據
    const walletsWithWinRates = [];
    
    for (const bet of betsQuery.rows) {
      const walletAddress = normalizeAddress(bet.wallet_address);
      
      // 獲取該錢包的勝率數據
      const analysisQuery = await client.query(`
        SELECT short_12_win_rate, mid_48_win_rate, short_12_total_bets, mid_48_total_bets
        FROM wallet_analysis
        WHERE wallet_address = $1 AND epoch = $2
      `, [walletAddress, epoch]);
      
      if (analysisQuery.rows.length > 0) {
        const analysis = analysisQuery.rows[0];
        // 只有有歷史數據的錢包才參與比較
        if (analysis.short_12_total_bets > 0 || analysis.mid_48_total_bets > 0) {
          walletsWithWinRates.push({
            wallet_address: walletAddress,
            bet_direction: bet.bet_direction,
            short_12_win_rate: analysis.short_12_win_rate || 0,
            mid_48_win_rate: analysis.mid_48_win_rate || 0,
            short_12_total_bets: analysis.short_12_total_bets || 0,
            mid_48_total_bets: analysis.mid_48_total_bets || 0
          });
        }
      }
    }
    
    if (walletsWithWinRates.length === 0) {
      console.log(`⚠️ 局次 ${epoch} 沒有有歷史數據的下注，跳過快照`);
      return;
    }
    
    console.log(`📋 局次 ${epoch} 共有 ${walletsWithWinRates.length} 個錢包有歷史數據`);
    
    // 排序：先比12局勝率，内12局相同再看48局
    walletsWithWinRates.sort((a, b) => {
      if (a.short_12_win_rate !== b.short_12_win_rate) {
        return b.short_12_win_rate - a.short_12_win_rate; // 降序
      }
      return b.mid_48_win_rate - a.mid_48_win_rate; // 降序
    });
    
    // 找出勝率最高和最低的錢包
    const highestWallet = walletsWithWinRates[0];
    const lowestWallet = walletsWithWinRates[walletsWithWinRates.length - 1];
    
    console.log(`🔼 勝率最高: ${highestWallet.wallet_address.substring(0,8)}... (12局: ${highestWallet.short_12_win_rate}%, 48局: ${highestWallet.mid_48_win_rate}%) -> ${highestWallet.bet_direction}`);
    console.log(`🔽 勝率最低: ${lowestWallet.wallet_address.substring(0,8)}... (12局: ${lowestWallet.short_12_win_rate}%, 48局: ${lowestWallet.mid_48_win_rate}%) -> ${lowestWallet.bet_direction}`);
    
    // 計算三個策略的預測
    const snapshot = {
      epoch,
      timestamp: Math.floor(Date.now() / 1000),
      strategies: {
        follow_best: {
          prediction: highestWallet.bet_direction,
          wallet: highestWallet.wallet_address,
          winRate_12: highestWallet.short_12_win_rate,
          winRate_48: highestWallet.mid_48_win_rate
        },
        reverse_low: {
          prediction: lowestWallet.bet_direction === 'UP' ? 'DOWN' : 'UP', // 反向
          wallet: lowestWallet.wallet_address,
          winRate_12: lowestWallet.short_12_win_rate,
          winRate_48: lowestWallet.mid_48_win_rate
        },
        momentum: {
          prediction: await calculateMomentumPredictionSnapshot(client, epoch, betsQuery.rows)
        }
      }
    };
    
    // 儲存快照到資料庫（永久儲存）
    try {
      await client.query(`
        INSERT INTO prediction_snapshots (
          epoch, snapshot_time,
          follow_best_prediction, follow_best_wallet, follow_best_win_rate_12, follow_best_win_rate_48,
          reverse_low_prediction, reverse_low_wallet, reverse_low_win_rate_12, reverse_low_win_rate_48,
          momentum_prediction
        ) VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (epoch) DO UPDATE SET
          snapshot_time = NOW(),
          follow_best_prediction = EXCLUDED.follow_best_prediction,
          follow_best_wallet = EXCLUDED.follow_best_wallet,
          follow_best_win_rate_12 = EXCLUDED.follow_best_win_rate_12,
          follow_best_win_rate_48 = EXCLUDED.follow_best_win_rate_48,
          reverse_low_prediction = EXCLUDED.reverse_low_prediction,
          reverse_low_wallet = EXCLUDED.reverse_low_wallet,
          reverse_low_win_rate_12 = EXCLUDED.reverse_low_win_rate_12,
          reverse_low_win_rate_48 = EXCLUDED.reverse_low_win_rate_48,
          momentum_prediction = EXCLUDED.momentum_prediction
      `, [
        epoch,
        snapshot.strategies.follow_best.prediction,
        snapshot.strategies.follow_best.wallet,
        snapshot.strategies.follow_best.winRate_12,
        snapshot.strategies.follow_best.winRate_48,
        snapshot.strategies.reverse_low.prediction,
        snapshot.strategies.reverse_low.wallet,
        snapshot.strategies.reverse_low.winRate_12,
        snapshot.strategies.reverse_low.winRate_48,
        snapshot.strategies.momentum.prediction
      ]);
      
      console.log(`💾 局次 ${epoch} 快照已存入資料庫`);
    } catch (dbError) {
      console.error(`❌ 存入資料庫失敗:`, dbError.message);
    }
    
    // 同時也儲存到 Redis 作為快取（可選）
    const snapshotKey = `prediction_at_lock:${epoch}`;
    await redisClient.setEx(snapshotKey, 21600, JSON.stringify(snapshot)); // TTL 6小時
    
    console.log(`✅ 局次 ${epoch} 快照已儲存: follow_best=${snapshot.strategies.follow_best.prediction}, reverse_low=${snapshot.strategies.reverse_low.prediction}, momentum=${snapshot.strategies.momentum.prediction}`);
    
  } catch (error) {
    console.error(`❌ 局次 ${epoch} 快照計算失敗:`, error.message);
  } finally {
    if (client) client.release();
  }
}

/**
 * 計算動量策略的快照預測
 */
async function calculateMomentumPredictionSnapshot(client, epoch, currentBets) {
  try {
    // 獲取當前局的資金流動數據
    const currentRound = await client.query(`
      SELECT up_bet_amount, down_bet_amount, total_bet_amount
      FROM round WHERE epoch = $1
    `, [epoch]);
    
    if (currentRound.rows.length === 0) {
      return null;
    }
    
    const currentUp = parseFloat(currentRound.rows[0].up_bet_amount) || 0;
    const currentDown = parseFloat(currentRound.rows[0].down_bet_amount) || 0;
    const currentTotal = currentUp + currentDown;
    const upRatio = currentTotal > 0 ? currentUp / currentTotal : 0.5;
    
    // 獲取最近5局的趨勢
    const trendQuery = await client.query(`
      SELECT result, up_bet_amount, down_bet_amount
      FROM round 
      WHERE epoch BETWEEN $1 AND $2 AND result IS NOT NULL
      ORDER BY epoch DESC
      LIMIT 5
    `, [epoch - 5, epoch - 1]);
    
    if (trendQuery.rows.length < 3) {
      return null;
    }
    
    // 簡單的動量判斷：如果 UP 資金占比 > 0.6，預測 UP
    if (upRatio > 0.6) {
      return 'UP';
    } else if (upRatio < 0.4) {
      return 'DOWN';
    }
    
    // 否則看趨勢：最近3局的結果
    const recent3 = trendQuery.rows.slice(0, 3);
    const upCount = recent3.filter(r => r.result === 'UP').length;
    return upCount >= 2 ? 'UP' : 'DOWN';
    
  } catch (error) {
    console.error('❌ 動量策略快照計算失敗:', error.message);
    return null;
  }
}

async function handleRoundUpdate(message, channel) {
  try {
    const roundData = JSON.parse(message);
    const epoch = roundData.epoch;

    // 初始化：第一次收到時設定基準
    if (latestBroadcastEpoch === 0) {
      latestBroadcastEpoch = epoch;
      console.log(`🔄 收到局次 ${epoch} 更新（初始化）`);
    } else if (epoch < latestBroadcastEpoch - 5) {
      // 過濾：跳過比最新局次舊太多的（超過 5 局）
      console.log(`⏭️  跳過舊局次 ${epoch}（當前最新: ${latestBroadcastEpoch}）`);
      return;
    } else if (epoch > latestBroadcastEpoch) {
      // 更新追蹤的最新局次
      latestBroadcastEpoch = epoch;
      console.log(`🔄 收到局次 ${epoch} 更新（新局次）`);
    } else {
      // 同一局次的更新
      console.log(`🔄 收到局次 ${epoch} 更新（同局次）`);
    }

  const now = Date.now();
  // roundData.lock_time 格式為 "YYYY-MM-DD HH:mm:ss" 台北時間
  // 需要明確指定為台北時區來避免時區混淆
  const lockTime = new Date(roundData.lock_time + ' GMT+0800').getTime();
  const closeTime = new Date(roundData.close_time + ' GMT+0800').getTime();

    let status = 'ENDED';
    if (now < lockTime) status = 'LIVE';
    else if (now < closeTime) status = 'LOCKED';

    const roundDataForWs = {
      epoch: epoch,
      lockTimestamp: Math.floor(lockTime / 1000),
      closeTimestamp: Math.floor(closeTime / 1000),
      bullAmount: roundData.up_bet_amount,
      bearAmount: roundData.down_bet_amount,
      totalAmount: roundData.total_bet_amount,
      status: status,
      result: roundData.result,
      closePrice: roundData.close_price
    };

    broadcast({ type: 'round_update', data: roundDataForWs });
    console.log(`📡 已廣播局次 ${epoch} (${status})`);
    
    // 如果是 LIVE 狀態，設置鎖倉前5秒的快照計算
    if (status === 'LIVE' && !snapshotTimers.has(epoch)) {
      const currentTime = Date.now();
      const lockTimestamp = new Date(roundData.lock_time + ' GMT+0800').getTime();
      const timeUntilSnapshot = lockTimestamp - currentTime - 5000; // 鎖倉前5秒
      
      if (timeUntilSnapshot > 0) {
        const secondsUntil = Math.floor(timeUntilSnapshot / 1000);
        console.log(`⏰ 將在 ${secondsUntil} 秒後計算局次 ${epoch} 的快照（鎖倉時間: ${new Date(lockTimestamp).toLocaleString('zh-TW', {timeZone: 'Asia/Taipei'})}）`);
        const timerId = setTimeout(() => {
          calculatePredictionSnapshot(epoch).catch(err => {
            console.error(`❌ 快照計算失敗:`, err.message);
          });
          snapshotTimers.delete(epoch); // 清除記錄
        }, timeUntilSnapshot);
        
        snapshotTimers.set(epoch, timerId);
      } else if (timeUntilSnapshot > -10000) {
        // 如果已經過了5秒但不超過10秒，立即計算
        console.log(`⚡ 立即計算局次 ${epoch} 的快照（已接近鎖倉）`);
        calculatePredictionSnapshot(epoch).catch(err => {
          console.error(`❌ 快照計算失敗:`, err.message);
        });
      }
    }
  } catch (error) {
    console.error('❌ 處理局次更新錯誤:', error.message);
  }
}

async function handleAnalysisRequest(message, channel) {
  try {
    const payload = JSON.parse(message);
    if (payload.type === 'analysis_request') {
      console.log(`🧮 收到分析請求: ${payload.bet.wallet_address.substring(0,8)}... epoch ${payload.bet.epoch}`);
      // 不要 await，讓它在背景執行，避免阻塞
      processAnalysisInBackground(payload.bet).catch(err => {
        console.error('❌ 背景分析失敗:', err.message);
      });
    }
  } catch (error) {
    console.error('❌ 處理分析請求錯誤:', error.message);
  }
}

async function processAnalysisInBackground(betData) {
  let client;
  try {
    client = await dbPool.connect();
    console.log(`⏳ 開始計算分析: ${betData.wallet_address.substring(0,8)}... epoch ${betData.epoch}`);

    const analysis = await calculateWeightedFailureRate(client, betData.wallet_address, betData.epoch);

    if (analysis) {
      const analysisPayload = {
        type: "bet_analysis",
        data: {
          wallet_address: normalizeAddress(betData.wallet_address),
          tx_hash: betData.tx_hash,
          analysis: analysis
        }
      };

      broadcast(analysisPayload);
      console.log(`✅ 已推送分析: ${betData.wallet_address.substring(0,8)}... (12局: ${analysis.short_12_rounds?.total_bets || 0} 筆)`);
    }
  } catch (error) {
    console.error('❌ 後台分析失敗:', error.message);
  } finally {
    if (client) client.release();
  }
}

async function handleBacktestResults(message, channel) {
  try {
    const backtestData = JSON.parse(message);
    console.log(`📊 收到回測結果: 跟隨最高 ${backtestData.strategies.follow_best.winRate}%, 反向最低 ${backtestData.strategies.reverse_low.winRate}%`);
    
    // 廣播給所有 WebSocket 客戶端
    broadcast({ type: 'backtest_update', data: backtestData });
  } catch (error) {
    console.error('❌ 處理回測結果錯誤:', error.message);
  }
}

async function handleLivePredictions(message, channel) {
  try {
    const predictionsData = JSON.parse(message);
    const momentum = predictionsData.strategies?.momentum;
    
    if (momentum) {
      console.log(`🔮 收到實時預測: 動量策略 -> ${momentum.prediction} (信心度: ${momentum.confidence})`);
    }
    
    // 廣播給所有 WebSocket 客戶端
    broadcast({ type: 'live_predictions', data: predictionsData });
  } catch (error) {
    console.error('❌ 處理實時預測錯誤:', error.message);
  }
}

async function handleTxBenchmark(message, channel) {
  try {
    const data = JSON.parse(message);
    // 只做轉發，前端維護彙總
    broadcast({ type: 'tx_benchmark', data });
  } catch (error) {
    console.error('❌ 處理 tx_benchmark 錯誤:', error.message);
  }
}

// ========================================
// WebSocket 客戶端消息處理
// ========================================

async function handleClientMessage(ws, message) {
  const client = await dbPool.connect();
  try {
    const data = JSON.parse(message);
    
    switch (data.type) {
      case 'get_current_round': {
        try {
          // 從鏈上獲取最新局次號碼
          const currentEpoch = await contract.currentEpoch();
          const latestEpoch = Number(currentEpoch);

          // 嘗試從資料庫獲取該局次的詳細信息
          const roundRes = await client.query('SELECT * FROM round WHERE epoch = $1', [latestEpoch]);

          let roundData;
          if (roundRes && roundRes.rows.length > 0) {
            // 資料庫有資料
            const round = roundRes.rows[0];
            const now = Date.now();
            const lockTime = new Date(round.lock_time).getTime();
            const closeTime = new Date(round.close_time).getTime();

            let status = 'ENDED';
            if (now < lockTime) status = 'LIVE';
            else if (now < closeTime) status = 'LOCKED';

            roundData = {
              epoch: round.epoch,
              lockTimestamp: Math.floor(lockTime / 1000),
              closeTimestamp: Math.floor(closeTime / 1000),
              bullAmount: round.up_bet_amount,
              bearAmount: round.down_bet_amount,
              totalAmount: round.total_bet_amount,
              status: status,
              result: round.result,
              closePrice: round.close_price
            };
          } else {
            // 資料庫還沒有，從鏈上獲取
            const roundInfo = await contract.rounds(BigInt(latestEpoch));
            const now = Math.floor(Date.now() / 1000);
            const lockTimestamp = Number(roundInfo.lockTimestamp);
            const closeTimestamp = Number(roundInfo.closeTimestamp);

            let status = 'ENDED';
            if (now < lockTimestamp) status = 'LIVE';
            else if (now < closeTimestamp) status = 'LOCKED';

            roundData = {
              epoch: latestEpoch,
              lockTimestamp: lockTimestamp,
              closeTimestamp: closeTimestamp,
              bullAmount: ethers.formatEther(roundInfo.bullAmount),
              bearAmount: ethers.formatEther(roundInfo.bearAmount),
              totalAmount: ethers.formatEther(roundInfo.totalAmount),
              status: status,
              result: null,
              closePrice: null
            };
          }

          ws.send(JSON.stringify({ type: 'current_round', data: roundData }));
          
          // 同時發送最新的回測數據
          try {
            const latestBacktest = await redisClient.get('latest_backtest');
            if (latestBacktest) {
              const backtestData = JSON.parse(latestBacktest);
              ws.send(JSON.stringify({ type: 'backtest_update', data: backtestData }));
            }
          } catch (error) {
            console.error('❌ 獲取回測數據錯誤:', error.message);
          }
          
          // 發送最新的實時預測
          try {
            const latestPredictions = await redisClient.get('latest_predictions');
            if (latestPredictions) {
              const predictionsData = JSON.parse(latestPredictions);
              ws.send(JSON.stringify({ type: 'live_predictions', data: predictionsData }));
            }
          } catch (error) {
            console.error('❌ 獲取預測數據錯誤:', error.message);
          }
        } catch (error) {
          console.error('❌ 獲取當前局次錯誤:', error.message);
        }
        break;
      }

      case 'get_realtime_bets': {
        const bets = await fetchBetsWithAnalysis(client, 'realbet', data.epoch);
        ws.send(JSON.stringify({ type: 'realtime_bets', data: bets }));
        break;
      }

      case 'get_historical_bets': {
        const bets = await fetchBetsWithAnalysis(client, 'hisbet', data.epoch);
        ws.send(JSON.stringify({ type: 'historical_bets', data: bets }));
        break;
      }

      case 'get_historical_round': {
        const roundRes = await client.query('SELECT * FROM round WHERE epoch = $1', [data.epoch]);
        if (roundRes && roundRes.rows.length > 0) {
          const round = roundRes.rows[0];
          const roundData = {
            epoch: round.epoch,
            bullAmount: round.up_bet_amount,
            bearAmount: round.down_bet_amount,
            totalAmount: round.total_bet_amount,
            result: round.result,
            closePrice: round.close_price
          };
          ws.send(JSON.stringify({ type: 'historical_round', data: roundData }));
        }
        break;
      }

      case 'force_backtest_refresh': {
        console.log('🔄 強制刷新回測數據');
        try {
          const latestBacktest = await redisClient.get('latest_backtest');
          if (latestBacktest) {
            const backtestData = JSON.parse(latestBacktest);
            console.log('📊 發送回測數據:', {
              strategies: Object.keys(backtestData.strategies || {}),
              hasHistory: backtestData.strategies?.momentum?.history ? backtestData.strategies.momentum.history.length : 0
            });
            ws.send(JSON.stringify({ type: 'backtest_update', data: backtestData }));
          } else {
            console.warn('⚠️ Redis 中沒有 latest_backtest 數據');
            ws.send(JSON.stringify({ 
              type: 'error', 
              message: 'Redis 中沒有回測數據，請稍後再試' 
            }));
          }
        } catch (error) {
          console.error('❗ 強制刷新回測數據錯誤:', error.message);
        }
        break;
      }

      default:
        console.warn(`⚠️ 未知的消息類型: ${data.type}`);
    }
  } catch (error) {
    console.error('❌ 處理客戶端訊息錯誤:', error.message);
    try {
      ws.send(JSON.stringify({ type: 'error', message: '處理請求時發生錯誤' }));
    } catch (sendError) {
      console.error('❌ 發送錯誤消息失敗:', sendError.message);
    }
  } finally {
    if (client) client.release();
  }
}

// ========================================
// WebSocket 廣播
// ========================================

function broadcast(data) {
  const payload = JSON.stringify(data);
  let successCount = 0;
  let failCount = 0;

  wss.clients.forEach((client) => {
    if (client.readyState === 1) { // WebSocket.OPEN
      try {
        client.send(payload);
        successCount++;
      } catch (error) {
        console.error('❌ 廣播失敗:', error.message);
        failCount++;
      }
    }
  });

  if (failCount > 0) {
    console.warn(`⚠️ 廣播完成: 成功 ${successCount}, 失敗 ${failCount}`);
  }
}

// ========================================
// WebSocket 連接管理
// ========================================

wss.on("connection", (ws) => {
  console.log('✅ 客戶端連接');

  ws.on("message", (message) => {
    handleClientMessage(ws, message).catch(error => {
      console.error('❌ 處理消息時發生未捕獲錯誤:', error);
    });
  });

  ws.on("close", () => {
    console.log('🔌 客戶端斷開');
  });

  ws.on("error", (error) => {
    console.error('❌ WebSocket 錯誤:', error.message);
  });
});

// ========================================
// 定期檢查最新局次
// ========================================

async function pollLatestRound() {
  try {
    // 從鏈上獲取最新局次
    const currentEpoch = await contract.currentEpoch();
    const latestEpoch = Number(currentEpoch);

    // 如果有新局次，主動廣播
    if (latestBroadcastEpoch === 0 || latestEpoch > latestBroadcastEpoch) {
      console.log(`🔍 偵測到新局次: ${latestEpoch}（上次: ${latestBroadcastEpoch}）`);

      // 從鏈上獲取該局次資料
      const roundInfo = await contract.rounds(BigInt(latestEpoch));
      const now = Math.floor(Date.now() / 1000);
      const lockTimestamp = Number(roundInfo.lockTimestamp);
      const closeTimestamp = Number(roundInfo.closeTimestamp);

      let status = 'ENDED';
      if (now < lockTimestamp) status = 'LIVE';
      else if (now < closeTimestamp) status = 'LOCKED';

      const roundData = {
        epoch: latestEpoch,
        lockTimestamp: lockTimestamp,
        closeTimestamp: closeTimestamp,
        bullAmount: ethers.formatEther(roundInfo.bullAmount),
        bearAmount: ethers.formatEther(roundInfo.bearAmount),
        totalAmount: ethers.formatEther(roundInfo.totalAmount),
        status: status,
        result: null,
        closePrice: null
      };

      // 更新最新局次並廣播
      latestBroadcastEpoch = latestEpoch;
      broadcast({ type: 'round_update', data: roundData });
      
      // 發布到 Redis channel 供 backtest 服務訂閱
      try {
        await redisClient.publish('round_update_channel', JSON.stringify(roundData));
      } catch (error) {
        console.error('❌ 發布到 Redis round_update_channel 失敗:', error.message);
      }
      
      console.log(`📡 主動廣播新局次 ${latestEpoch} (${status})`);
    }
  } catch (error) {
    console.error('❌ 輪詢最新局次錯誤:', error.message);
  }
}

// ========================================
// 主程序啟動
// ========================================

async function main() {
  try {
    console.log('🚀 啟動 BSC 預測市場監控系統...');

    // 1. 初始化資料庫
    await initDatabase();

    // 2. 連接 Redis
    console.log('🔌 連接 Redis...');
    await redisSubscriber.connect();
    await redisClient.connect();
    console.log('✅ Redis 客戶端連接成功');

    // 3. 啟動 HTTP 伺服器
    server.listen(PORT, HOST, () => {
      console.log(`📡 伺服器監聽: http://${HOST}:${PORT}`);
    });

    // 4. 啟動定期輪詢最新局次（每 30 秒）
    setInterval(pollLatestRound, 30000);
    // 立即執行一次
    pollLatestRound();

    console.log('✅ 系統啟動完成');

  } catch (error) {
    console.error('❌ 啟動失敗:', error);
    process.exit(1);
  }
}

// 優雅關閉
process.on('SIGTERM', async () => {
  console.log('📴 收到 SIGTERM，優雅關閉...');
  try {
    await redisSubscriber.quit();
    await redisClient.quit();
    await dbPool.end();
    server.close(() => {
      console.log('✅ 伺服器已關閉');
      process.exit(0);
    });
  } catch (error) {
    console.error('❌ 關閉時發生錯誤:', error);
    process.exit(1);
  }
});

process.on('SIGINT', async () => {
  console.log('📴 收到 SIGINT，優雅關閉...');
  try {
    await redisSubscriber.quit();
    await redisClient.quit();
    await dbPool.end();
    server.close(() => {
      console.log('✅ 伺服器已關閉');
      process.exit(0);
    });
  } catch (error) {
    console.error('❌ 關閉時發生錯誤:', error);
    process.exit(1);
  }
});

// 未捕獲異常處理
process.on('uncaughtException', (error) => {
  console.error('❌ 未捕獲異常:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  console.error('❌ 未處理的 Promise 拒絕:', reason);
  process.exit(1);
});

// 啟動
main();