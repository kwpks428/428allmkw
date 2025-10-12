// tx_benchmark.js
// 測試：在鎖定前 delta 秒送出下注交易，量測送出/上鏈延遲並發布到 Redis
// 重要：私鑰只從環境變數讀取，不會印出

const dotenv = require('dotenv');
dotenv.config();

const { ethers } = require('ethers');
const Redis = require('ioredis');
const { Pool } = require('pg');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

// 環境變數
const RPC_URL = process.env.RPC_URL;
const CONTRACT_ADDR = process.env.CONTRACT_ADDR;
const PRIVATE_KEY = process.env.PRIVATE_KEY; // 不可印出
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const DATABASE_URL = process.env.DATABASE_URL; // 可選

if (!RPC_URL || !CONTRACT_ADDR || !PRIVATE_KEY) {
  console.error('缺少必要環境變數：RPC_URL, CONTRACT_ADDR, PRIVATE_KEY');
  process.exit(1);
}

// 載入 ABI（優先使用 ABI_PATH，其次嘗試 repo 根目錄 abi.json，最後嘗試 ../server/abi.json）
function loadAbi() {
  const candidates = [
    process.env.ABI_PATH,
    path.resolve(__dirname, '../abi.json'),
    path.resolve(__dirname, '../server/abi.json'),
    path.resolve(__dirname, 'abi.json'),
  ].filter(Boolean);
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const abi = JSON.parse(fs.readFileSync(p, 'utf8'));
        return abi;
      }
    } catch (e) {}
  }
  console.error('找不到 abi.json，請設定 ABI_PATH 或將 abi.json 放在專案根目錄或 server/ 下');
  process.exit(1);
}

const ABI = loadAbi();

// 連線元件
const provider = new ethers.JsonRpcProvider(RPC_URL);
const wallet = new ethers.Wallet(PRIVATE_KEY, provider);
const contract = new ethers.Contract(CONTRACT_ADDR, ABI, wallet);
const redis = new Redis(REDIS_URL);

// 可選：資料庫（如未設定則略過）
let pgPool = null;
if (DATABASE_URL) {
  pgPool = new Pool({ connectionString: DATABASE_URL, max: 5 });
}

function parseArgs() {
  // 簡易參數解析
  const args = process.argv.slice(2);
  const out = {
    mode: 'single', // 'single' | 'scan'
    side: 'UP',     // 'UP' | 'DOWN'
    amount: '0.01', // BNB 字串
    delta: 5,       // 單點測試時的提前秒數
    deltaStart: 10, // 掃描模式起點
    deltaEnd: 2,    // 掃描模式終點
    attempts: 3,    // 每個 delta 測試次數
    gasBump: 1.20,  // gasPrice 乘數
  };
  for (const a of args) {
    const [k, v] = a.split('=');
    const key = k.replace(/^--/, '');
    if (key in out) {
      if (['delta','deltaStart','deltaEnd','attempts'].includes(key)) out[key] = Number(v);
      else out[key] = v;
    }
    if (key === 'mode') out.mode = v;
    if (key === 'side') out.side = v?.toUpperCase() === 'DOWN' ? 'DOWN' : 'UP';
  }
  return out;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function nowMs() { return Date.now(); }

function newRunId() { return crypto.randomBytes(6).toString('hex'); }

async function ensureTable() {
  if (!pgPool) return;
  const sql = `
  CREATE TABLE IF NOT EXISTS tx_benchmark_results (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    epoch BIGINT,
    side TEXT,
    delta_seconds INT,
    send_ms INT,
    mined_ms INT,
    total_ms INT,
    gas_price NUMERIC,
    gas_used NUMERIC,
    success BOOLEAN,
    error TEXT,
    tx_hash TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );`;
  try { await pgPool.query(sql); } catch (e) { console.error('建表失敗:', e.message); }
}

async function publishResult(payload) {
  try { await redis.publish('tx_benchmark', JSON.stringify(payload)); } catch {}
  if (pgPool) {
    try {
      await pgPool.query(
        `INSERT INTO tx_benchmark_results
         (run_id, epoch, side, delta_seconds, send_ms, mined_ms, total_ms, gas_price, gas_used, success, error, tx_hash)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        [
          payload.runId,
          payload.epoch,
          payload.side,
          payload.delta,
          payload.send_ms,
          payload.mined_ms,
          payload.total_ms,
          payload.gas_price,
          payload.gas_used,
          payload.success,
          payload.error || null,
          payload.txHash || null,
        ]
      );
    } catch (e) {
      console.error('寫入資料庫失敗:', e.message);
    }
  }
}

async function getEpochAndStopTime() {
  const epochBN = await contract.currentEpoch();
  const epoch = Number(epochBN);
  const r = await contract.rounds(epochBN);
  const bufferSeconds = Number(await contract.bufferSeconds());
  const lockTsMs = Number(r.lockTimestamp) * 1000;
  const tStop = lockTsMs - bufferSeconds * 1000; // 最晚可下注時間
  return { epoch, tStop };
}

async function getMinBetBN() {
  try {
    const min = await contract.minBetAmount();
    return min; // wei (BigInt)
  } catch {
    return null;
  }
}

async function sendOne(epoch, side, amountBNB, gasBump) {
  const runId = newRunId();
  const fee = await provider.getFeeData();
  const gasPrice = fee.gasPrice ? BigInt(Math.floor(Number(fee.gasPrice) * gasBump)) : undefined;

  const valueWei = ethers.parseEther(String(amountBNB));
  const func = side === 'DOWN' ? 'betBear' : 'betBull';

  const t0 = nowMs();
  let tx, t1, receipt, t2, success = false, err = null, gasUsed = null;
  try {
    tx = await contract[func](BigInt(epoch), { value: valueWei, ...(gasPrice ? { gasPrice } : {}) });
    t1 = nowMs();
    receipt = await tx.wait(1);
    t2 = nowMs();
    gasUsed = receipt.gasUsed ? Number(receipt.gasUsed) : null;
    success = receipt.status === 1;
  } catch (e) {
    t1 = t1 || nowMs();
    t2 = nowMs();
    err = e?.shortMessage || e?.reason || e?.message || 'unknown error';
  }

  const send_ms = t1 - t0;
  const mined_ms = t2 - t1;
  const total_ms = t2 - t0;

  const payload = {
    type: 'tx_benchmark',
    runId,
    epoch,
    side,
    delta: null, // 呼叫端填
    send_ms,
    mined_ms,
    total_ms,
    gas_price: gasPrice ? Number(gasPrice) : (fee.gasPrice ? Number(fee.gasPrice) : null),
    gas_used: gasUsed,
    success,
    error: success ? null : err,
    txHash: tx?.hash || null,
    address: wallet.address,
    ts: new Date().toISOString(),
  };
  return payload;
}

async function waitUntil(tsTarget) {
  const now = nowMs();
  const delay = tsTarget - now;
  if (delay > 5) await sleep(delay - 5);
  // 最後 5ms 直接送（Node 計時精度有限，已足夠）
}

async function runSingle(side, amount, delta, gasBump) {
  const { epoch, tStop } = await getEpochAndStopTime();
  const minBetWei = await getMinBetBN();
  if (minBetWei) {
    const minBNB = Number(ethers.formatEther(minBetWei));
    if (Number(amount) < minBNB) {
      console.warn(`指定金額 ${amount} BNB 小於 minBetAmount ${minBNB} BNB，可能會失敗`);
    }
  }

  const tTarget = tStop - delta * 1000;
  const now = nowMs();
  if (tTarget <= now) {
    console.warn(`來不及，距 T_stop 僅 ${((tStop - now)/1000).toFixed(2)} 秒（delta=${delta}s）`);
  } else {
    await waitUntil(tTarget);
  }

  const result = await sendOne(epoch, side, amount, gasBump);
  result.delta = delta;
  await publishResult(result);

  const tag = result.success ? '✅' : '❌';
  console.log(`${tag} epoch=${epoch} side=${side} delta=${delta}s -> send=${result.send_ms}ms mined=${result.mined_ms}ms total=${result.total_ms}ms tx=${result.txHash || '-'} err=${result.error || '-'} `);
  return result;
}

async function runScan(side, amount, deltaStart, deltaEnd, attempts, gasBump) {
  const deltas = [];
  for (let d = deltaStart; d >= deltaEnd; d--) deltas.push(d);
  const stats = {};

  for (const d of deltas) {
    stats[d] = { total: 0, ok: 0, samples: [] };
    for (let i = 0; i < attempts; i++) {
      const res = await runSingle(side, amount, d, gasBump);
      stats[d].total++;
      if (res.success) stats[d].ok++;
      stats[d].samples.push(res.total_ms);
    }
  }

  // 簡單彙總輸出
  console.log('\n=== 掃描結果彙總 ===');
  for (const d of deltas) {
    const s = stats[d];
    const rate = s.total ? (s.ok / s.total * 100).toFixed(1) : '0.0';
    const p50 = percentile(s.samples, 50);
    const p90 = percentile(s.samples, 90);
    const p99 = percentile(s.samples, 99);
    console.log(`delta=${d}s -> 成功率=${rate}% samples=${s.total} P50=${p50}ms P90=${p90}ms P99=${p99}ms`);
  }
}

function percentile(arr, p) {
  if (!arr || arr.length === 0) return null;
  const a = [...arr].sort((x,y) => x - y);
  const idx = Math.max(0, Math.min(a.length - 1, Math.floor((p/100) * a.length)));
  return a[idx];
}

(async () => {
  if (pgPool) await ensureTable();
  const args = parseArgs();
  console.log('🧪 交易延遲測試 啟動', { mode: args.mode, side: args.side, amount: args.amount });

  if (args.mode === 'scan') {
    await runScan(args.side, args.amount, args.deltaStart, args.deltaEnd, args.attempts, Number(args.gasBump || 1.2));
  } else {
    await runSingle(args.side, args.amount, Number(args.delta || 5), Number(args.gasBump || 1.2));
  }

  await redis.quit();
  if (pgPool) await pgPool.end();
})();
