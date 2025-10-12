// trader.js - 自動下單（預設乾跑）
// 功能：
// - 訂閱 Redis 的 live_predictions 與 round_update_channel
// - 查鏈上 rounds(epoch) 與 bufferSeconds 計算最晚可下單時間 T_stop
// - 在 final 表態（或你設定條件）且符合信心門檻時，於安全時間窗內觸發下單
// - 預設 DRY RUN（TRADE_DRY_RUN=true）只記錄與推送 trade_log，不送鏈上交易
// - 風控：一局一單、最小信心、可設方向過濾、可設金額、gas bump

require('dotenv').config();

const Redis = require('ioredis');
const { ethers } = require('ethers');
const { Pool } = require('pg');

// 讀取環境變數
const {
  REDIS_URL,
  RPC_URL,
  CONTRACT_ADDR,
  PRIVATE_KEY,
  // 交易控制
  TRADE_ENABLED = 'false',   // 'true' 才會實單
  TRADE_DRY_RUN = 'true',    // 預設乾跑
  TRADE_AMOUNT_BNB = '0.001',
  TRADE_MIN_CONFIDENCE = 'high', // 'low'|'medium'|'high'
  TRADE_ALLOWED_STRATEGY = 'auto', // 'auto'|'momentum'|'follow_best'|'reverse_low'
  TRADE_SIDE_FILTER = '',    // 'UP'|'DOWN' 或空字串不過濾
  TRADE_DELTA_MS,            // 若未設，使用 FINAL_ADVANCE_MS 或預設 4000
  FINAL_ADVANCE_MS,
  GAS_BUMP = '1.2',
  // 預武裝（arming）參數
  TRADE_ARM_ENABLED = 'true',
  ARM_SLOPE_MIN = '0.05',
  ARM_VOLUME_MIN = '1.5',
  ARM_UPDIFF_MIN = '0.10',
  ARM_MAX_AGE_MS = '30000'
} = process.env;

// 連線
const redis = new Redis(REDIS_URL || 'redis://127.0.0.1:6379');
const subscriber = new Redis(REDIS_URL || 'redis://127.0.0.1:6379');
const provider = RPC_URL ? new ethers.JsonRpcProvider(RPC_URL) : null;

// 合約 ABI（只用到必要函數）
const CONTRACT_ABI = [
  { inputs: [], name: 'bufferSeconds', outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }], stateMutability: 'view', type: 'function' },
  { inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }], name: 'rounds', outputs: [
      { internalType: 'uint256', name: 'epoch', type: 'uint256' },
      { internalType: 'uint256', name: 'startTimestamp', type: 'uint256' },
      { internalType: 'uint256', name: 'lockTimestamp', type: 'uint256' },
      { internalType: 'uint256', name: 'closeTimestamp', type: 'uint256' },
      { internalType: 'int256', name: 'lockPrice', type: 'int256' },
      { internalType: 'int256', name: 'closePrice', type: 'int256' },
      { internalType: 'uint256', name: 'bullAmount', type: 'uint256' },
      { internalType: 'uint256', name: 'bearAmount', type: 'uint256' },
      { internalType: 'uint256', name: 'totalAmount', type: 'uint256' },
    ], stateMutability: 'view', type: 'function' },
  { inputs: [{ internalType: 'uint256', name: 'epoch', type: 'uint256' }, { internalType: 'address', name: 'user', type: 'address' }], name: 'ledger', outputs: [
      { internalType: 'uint8', name: 'position', type: 'uint8' },
      { internalType: 'uint256', name: 'amount', type: 'uint256' },
      { internalType: 'bool', name: 'claimed', type: 'bool' }
    ], stateMutability: 'view', type: 'function' },
  { inputs: [{ internalType: 'uint256', name: 'epoch', type: 'uint256' }], name: 'betBull', outputs: [], stateMutability: 'payable', type: 'function' },
  { inputs: [{ internalType: 'uint256', name: 'epoch', type: 'uint256' }], name: 'betBear', outputs: [], stateMutability: 'payable', type: 'function' },
];

const contract = (provider && CONTRACT_ADDR) ? new ethers.Contract(CONTRACT_ADDR, CONTRACT_ABI, provider) : null;
const wallet = (provider && PRIVATE_KEY && PRIVATE_KEY.startsWith('0x')) ? new ethers.Wallet(PRIVATE_KEY, provider) : null;
const sender = wallet ? wallet : null;
const writable = (sender && contract) ? contract.connect(sender) : null;

// 可選：資料庫連線（用於記錄績效與時間統計）
const pgPool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL, max: 5 }) : null;

const minConfRank = { low: 0, medium: 1, high: 2 };
// 清洗信心與方向參數
const rawMinConf = String(TRADE_MIN_CONFIDENCE || 'high').toLowerCase();
const normMinConf = rawMinConf.includes('high') ? 'high' : rawMinConf.includes('medium') ? 'medium' : rawMinConf.includes('low') ? 'low' : 'high';
const minConfidenceRank = minConfRank[normMinConf];
const rawSide = String(TRADE_SIDE_FILTER || '').toUpperCase();
const sideFilter = rawSide === 'UP' || rawSide === 'DOWN' ? rawSide : '';
const gasBump = Math.max(1, Number(GAS_BUMP || '1.2'));

let defaultDeltaMs = Number(TRADE_DELTA_MS || FINAL_ADVANCE_MS);
if (!Number.isFinite(defaultDeltaMs)) defaultDeltaMs = 4000;

const ARM_ENABLED = String(TRADE_ARM_ENABLED || 'true') === 'true';
const ARM_CFG = {
  slopeMin: Number(ARM_SLOPE_MIN || '0.05'),
  volumeMin: Number(ARM_VOLUME_MIN || '1.5'),
  upDiffMin: Number(ARM_UPDIFF_MIN || '0.10'),
  maxAgeMs: Number(ARM_MAX_AGE_MS || '30000')
};

console.log('[TRADER] 啟動', {
  TRADE_ENABLED,
  TRADE_DRY_RUN,
  TRADE_AMOUNT_BNB,
  TRADE_MIN_CONFIDENCE: normMinConf,
  TRADE_ALLOWED_STRATEGY,
  TRADE_SIDE_FILTER: sideFilter || '(none)',
  defaultDeltaMs,
  ARM_ENABLED,
  ARM_CFG
});

// 狀態
const state = {
  epochMeta: new Map(), // epoch -> { lockMs, bufferSec, tStop }
  placed: new Set(),    // 已下單的 epoch，避免重複
  armed: new Map(),     // epoch -> { prediction, ts, nonce, amountBNB }
};

async function ensureTables() {
  if (!pgPool) return;
  const sql = `
  CREATE TABLE IF NOT EXISTS trade_log (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT NOW(),
    epoch BIGINT NOT NULL,
    phase TEXT NOT NULL,           -- arm | final_dryrun | final_sent | final_receipt
    strategy TEXT NOT NULL,
    prediction TEXT NOT NULL,      -- UP | DOWN
    confidence TEXT,
    amount_bnb NUMERIC(20,8),
    delta_ms INT,
    tstop_ms BIGINT,
    version INT,
    nonce BIGINT,
    tx_hash TEXT,
    send_ms INT,
    mined_ms INT,
    total_ms INT,
    success BOOLEAN,
    error TEXT
  );`;
  try {
    await pgPool.query(sql);
  } catch (e) {
    console.error('[TRADER] 建表失敗:', e.message);
  }
}

async function logTrade(row) {
  if (!pgPool) return;
  const sql = `INSERT INTO trade_log
    (ts, epoch, phase, strategy, prediction, confidence, amount_bnb, delta_ms, tstop_ms, version, nonce, tx_hash, send_ms, mined_ms, total_ms, success, error)
    VALUES(NOW(), $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`;
  const args = [
    row.epoch,
    row.phase,
    row.strategy || 'momentum',
    row.prediction,
    row.confidence || null,
    row.amountBNB != null ? Number(row.amountBNB) : null,
    row.deltaMs != null ? Number(row.deltaMs) : null,
    row.tStop != null ? Number(row.tStop) : null,
    row.version != null ? Number(row.version) : null,
    row.nonce != null ? Number(row.nonce) : null,
    row.txHash || null,
    row.send_ms != null ? Number(row.send_ms) : null,
    row.mined_ms != null ? Number(row.mined_ms) : null,
    row.total_ms != null ? Number(row.total_ms) : null,
    typeof row.success === 'boolean' ? row.success : null,
    row.error || null
  ];
  try {
    await pgPool.query(sql, args);
  } catch (e) {
    console.error('[TRADER] 寫入 trade_log 失敗:', e.message);
  }
}

async function getEpochMeta(epoch) {
  if (!contract) return null;
  if (state.epochMeta.has(epoch)) return state.epochMeta.get(epoch);
  try {
    const r = await contract.rounds(BigInt(epoch));
    const bufferSec = Number(await contract.bufferSeconds());
    const lockMs = Number(r.lockTimestamp) * 1000;
    const tStop = lockMs - bufferSec * 1000;
    const meta = { lockMs, bufferSec, tStop };
    state.epochMeta.set(epoch, meta);
    return meta;
  } catch (e) {
    console.error('[TRADER] 讀取 round 失敗:', e.message);
    return null;
  }
}

async function alreadyBet(epoch, address) {
  if (!contract || !address) return false;
  try {
    const info = await contract.ledger(BigInt(epoch), address);
    const amt = info?.amount ? BigInt(info.amount) : 0n;
    return amt > 0n;
  } catch (e) {
    console.error('[TRADER] 讀取 ledger 失敗:', e.message);
    return false;
  }
}

function passConfidence(conf) {
  const r = minConfRank[(conf || '').toLowerCase()] ?? -1;
  return r >= minConfidenceRank;
}

async function getLatestBacktest() {
  try {
    const data = await redis.get('latest_backtest');
    return data ? JSON.parse(data) : null;
  } catch (e) {
    console.error('[TRADER] 獲取回測數據失敗:', e.message);
    return null;
  }
}

function selectBestStrategy(backtestData) {
  // 預設策略 - 如果沒有回測數據
  if (!backtestData || !backtestData.strategies) {
    console.log('[TRADER] 無回測數據，使用預設策略: follow_best');
    return { strategy: 'follow_best', reverse: false };
  }
  
  const strategies = backtestData.strategies;
  let bestStrategy = 'follow_best';
  let bestWinRate = 0;
  let shouldReverse = false;
  
  // 找出勝率最高的策略
  for (const [key, strategy] of Object.entries(strategies)) {
    if (strategy.winRate > bestWinRate && strategy.total >= 10) { // 至少需要10局樣本
      bestStrategy = key;
      bestWinRate = strategy.winRate;
    }
  }
  
  // 如果最佳策略勝率低於45%，考慮反向邏輯
  if (bestWinRate < 45) {
    console.log(`[TRADER] 最佳策略勝率過低 (${bestWinRate}%), 嘗試反向邏輯`);
    // 找出勝率最低的策略，反向操作
    let worstStrategy = null;
    let worstWinRate = 100;
    for (const [key, strategy] of Object.entries(strategies)) {
      if (strategy.winRate < worstWinRate && strategy.total >= 10) {
        worstStrategy = key;
        worstWinRate = strategy.winRate;
      }
    }
    // 如果有明顯的差策略（勝率<35%），反向使用它
    if (worstWinRate < 35) {
      bestStrategy = worstStrategy;
      shouldReverse = true;
      console.log(`[TRADER] 選用反向策略: ${bestStrategy} (原勝率: ${worstWinRate}%, 將反向操作)`);
    }
  }
  
  console.log(`[TRADER] 自動選擇策略: ${bestStrategy} (勝率: ${strategies[bestStrategy]?.winRate || 'N/A'}%, 反向: ${shouldReverse})`);
  return { strategy: bestStrategy, reverse: shouldReverse };
}

function isStrongPreFinal(strat) {
  if (!strat || !strat.features) return false;
  const f = strat.features;
  const slope = Number(f.slope ?? 0);
  const volR = Number(f.volumeRatio ?? 0);
  const upDiff = Number(f.upRatioDiff ?? 0);
  return (Math.abs(slope) >= ARM_CFG.slopeMin) && ((volR >= ARM_CFG.volumeMin) || (Math.abs(upDiff) >= ARM_CFG.upDiffMin));
}

async function maybeArm(epoch, prediction, strat) {
  try {
    if (!ARM_ENABLED) return;
    const meta = await getEpochMeta(epoch);
    if (!meta) return;
    const now = Date.now();
    const tSend = meta.tStop - defaultDeltaMs;
    if (now >= tSend - 500) return; // 已接近送單點，arm 沒太大意義
    if (state.armed.has(epoch)) return; // 已經武裝過

    // 取得 pending nonce，準備金額
    if (!sender) return;
    const nonce = await provider.getTransactionCount(sender.address, 'pending');
    const amountBNB = Number(TRADE_AMOUNT_BNB || '0.001');

    state.armed.set(epoch, { prediction, ts: now, nonce, amountBNB });
    const log = { type: 'trade_log', ts: new Date().toISOString(), epoch, prediction, armed: true, nonce, amountBNB, note: 'pre-final strong signal' };
    console.log('[TRADER][ARM]', log);
    try { await redis.publish('trade_log', JSON.stringify(log)); } catch {}
    await logTrade({ epoch, phase: 'arm', strategy: TRADE_ALLOWED_STRATEGY, prediction, confidence: 'pre', amountBNB, nonce });
  } catch (e) {
    console.error('[TRADER] 武裝失敗:', e.message);
  }
}

async function maybeTrade(signal) {
  try {
    if (!signal || !signal.strategies) return;
    const epoch = Number(signal.epoch);
    
    // 自動選擇最佳策略
    let strat, strategyName, shouldReverse = false;
    if (TRADE_ALLOWED_STRATEGY === 'auto') {
      // 從 Redis 獲取最新回測結果
      const backtestData = await getLatestBacktest();
      const result = selectBestStrategy(backtestData);
      strategyName = result.strategy;
      shouldReverse = result.reverse;
      strat = signal.strategies[strategyName];
    } else {
      strategyName = TRADE_ALLOWED_STRATEGY;
      strat = signal.strategies[TRADE_ALLOWED_STRATEGY];
    }
    
    if (!strat) return;

    let prediction = String(strat.prediction || '').toUpperCase();
    const confidence = String(strat.confidence || '').toLowerCase();
    
    // 如果需要反向操作，翻轉預測方向
    if (shouldReverse && prediction) {
      prediction = prediction === 'UP' ? 'DOWN' : 'UP';
      console.log(`[TRADER] 反向操作: 原預測 ${strat.prediction} -> 新預測 ${prediction}`);
    }

    // 非 final：若強信號則預武裝（保留 nonce，快速進入送單階段）
    if (signal.final !== true) {
      if (isStrongPreFinal(strat) && passConfidence(confidence) && (!sideFilter || prediction === sideFilter)) {
        await maybeArm(epoch, prediction, strat);
      }
      return;
    }

    if (!passConfidence(confidence)) return;
    if (sideFilter && prediction !== sideFilter) return;

    const meta = await getEpochMeta(epoch);
    if (!meta) return;

    const now = Date.now();
    const deltaMs = defaultDeltaMs; // 可改為動態 δ
    const tSend = meta.tStop - deltaMs;

    if (now < tSend - 1000) {
      // 太早，等待最後一秒（保守：提前 1s 檢查）
      const wait = Math.max(0, tSend - now - 500);
      console.log(`[TRADER] 尚未到送單點，等待 ${Math.floor(wait)}ms (epoch=${epoch})`);
      setTimeout(() => maybeTrade(signal), wait);
      return;
    }

    if (now >= meta.tStop - 100) {
      console.log(`[TRADER] 已過最晚可下注時間，放棄 (epoch=${epoch})`);
      return;
    }

    if (state.placed.has(epoch)) {
      console.log(`[TRADER] 本局已處理，下單跳過 (epoch=${epoch})`);
      return;
    }

    const address = sender?.address;
    if (address && await alreadyBet(epoch, address)) {
      console.log(`[TRADER] 本局已下注過，跳過 (epoch=${epoch})`);
      state.placed.add(epoch);
      return;
    }

    // 如果有武裝且方向一致，採用武裝參數（nonce），同時更新 gasPrice
    let amountBNB = Number(TRADE_AMOUNT_BNB || '0.001');
    const armed = state.armed.get(epoch);
    let useNonce = undefined;
    if (armed && armed.prediction === prediction) {
      // 過期判斷
      if (Date.now() - armed.ts <= ARM_CFG.maxAgeMs) {
        amountBNB = armed.amountBNB || amountBNB;
        useNonce = armed.nonce;
      } else {
        state.armed.delete(epoch);
      }
    }
    const valueWei = ethers.parseEther(String(amountBNB));

    const payload = {
      type: 'trade_log',
      ts: new Date().toISOString(),
      epoch,
      prediction,
      confidence,
      amountBNB,
      deltaMs,
      tStop: meta.tStop,
      version: signal.version,
      dryRun: (TRADE_DRY_RUN === 'true'),
      enabled: (TRADE_ENABLED === 'true')
    };

    if (TRADE_DRY_RUN === 'true' || TRADE_ENABLED !== 'true' || !writable) {
      console.log('[TRADER][DRY-RUN] would send:', payload);
      try { await redis.publish('trade_log', JSON.stringify(payload)); } catch {}
      await logTrade({ epoch, phase: 'final_dryrun', strategy: strategyName, prediction, confidence, amountBNB, deltaMs, tStop: meta.tStop, version: signal.version });
      state.placed.add(epoch);
      return;
    }

    // 真實下單
    const func = prediction === 'UP' ? 'betBull' : 'betBear';
    const fee = await provider.getFeeData();
    const gasPrice = fee.gasPrice ? BigInt(Math.floor(Number(fee.gasPrice) * gasBump)) : undefined;

    console.log(`[TRADER] Sending tx ${func} epoch=${epoch} amount=${amountBNB}BNB gasBump=${gasBump}`);
    const t0 = Date.now();
    const tx = await writable[func](BigInt(epoch), { value: valueWei, ...(gasPrice ? { gasPrice } : {}), ...(useNonce !== undefined ? { nonce: useNonce } : {}) });
    const t1 = Date.now();
    await logTrade({ epoch, phase: 'final_sent', strategy: strategyName, prediction, confidence, amountBNB, deltaMs, tStop: meta.tStop, version: signal.version, nonce: useNonce, txHash: tx.hash, send_ms: t1 - t0 });
    const receipt = await tx.wait(1);
    const t2 = Date.now();

    const ok = receipt.status === 1;
    const logPayload = {
      ...payload,
      dryRun: false,
      txHash: tx.hash,
      send_ms: t1 - t0,
      mined_ms: t2 - t1,
      total_ms: t2 - t0,
      success: ok
    };
    console.log('[TRADER] tx done:', logPayload);
    try { await redis.publish('trade_log', JSON.stringify(logPayload)); } catch {}
    await logTrade({ epoch, phase: 'final_receipt', strategy: strategyName, prediction, confidence, amountBNB, deltaMs, tStop: meta.tStop, version: signal.version, nonce: useNonce, txHash: tx.hash, send_ms: logPayload.send_ms, mined_ms: logPayload.mined_ms, total_ms: logPayload.total_ms, success: ok });
    state.placed.add(epoch);
  } catch (e) {
    console.error('[TRADER] 下單流程錯誤:', e.message);
  }
}

async function main() {
  console.log('[TRADER] 連線 Redis...');
  await subscriber.subscribe('live_predictions');
  await subscriber.subscribe('round_update_channel');
  console.log('[TRADER] 已訂閱 live_predictions, round_update_channel');
  await ensureTables();

  subscriber.on('message', async (channel, message) => {
    try {
      const data = JSON.parse(message);
      if (channel === 'round_update_channel') {
        // 嘗試相容不同格式
        const e = (data?.data?.epoch ?? data?.epoch);
        if (e != null) {
          // 重置防重（新局開始）
          state.placed.delete(Number(e));
        }
      } else if (channel === 'live_predictions') {
        try {
          // 為除錯選擇策略
          let debugStrategyName = TRADE_ALLOWED_STRATEGY;
          let debugReverse = false;
          if (TRADE_ALLOWED_STRATEGY === 'auto') {
            const backtestData = await getLatestBacktest();
            const result = selectBestStrategy(backtestData);
            debugStrategyName = result.strategy;
            debugReverse = result.reverse;
          }
          const strat = data?.strategies?.[debugStrategyName];
          if (strat) {
            let pred = String(strat.prediction||'').toUpperCase();
            if (debugReverse && pred) {
              pred = pred === 'UP' ? 'DOWN' : 'UP';
            }
            const dbg = {
              epoch: data.epoch,
              strategy: debugStrategyName + (debugReverse ? '_REVERSE' : ''),
              final: !!data.final,
              pred: pred,
              originalPred: debugReverse ? strat.prediction : undefined,
              conf: String(strat.confidence||'').toLowerCase(),
              v: data.version
            };
            console.log('[TRADER] 收到預測', dbg);
          }
        } catch {}
        await maybeTrade(data);
      }
    } catch (e) {
      console.error('[TRADER] 訊息處理錯誤:', e.message);
    }
  });
}

main().catch(err => {
  console.error('[TRADER] 啟動失敗:', err);
  process.exit(1);
});
