/**
 * BSC 預測市場歷史數據處理器 - 正確邏輯版 + 詳細日誌
 */

const dotenv = require("dotenv");
dotenv.config();

const { ethers } = require("ethers");
const colors = require("colors");
const pg = require("pg");
const fs = require("fs");
const { Pool } = pg;
const Redis = require("ioredis");

// 配置
if (!process.env.DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!process.env.RPC_URL) throw new Error("Missing RPC_URL");
if (!process.env.CONTRACT_ADDR) throw new Error("Missing CONTRACT_ADDR");
if (!process.env.REDIS_URL) throw new Error("Missing REDIS_URL");

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100000;
const CACHE_MAX = parseInt(process.env.CACHE_MAX) || 5000;
const RETRY_MAX = parseInt(process.env.RETRY_MAX) || 3;
const SIDE_DEPTH = 3;

// 確保使用 .env 的 REDIS_URL
const redisPublisher = new Redis(process.env.REDIS_URL, {
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    if (times > 3) return null;
    return Math.min(times * 1000, 3000);
  }
});
redisPublisher.on('error', (err) => console.error('Redis 錯誤:', err.message));

// LRU 快取
class LRUCache {
  constructor(limit = CACHE_MAX) {
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
const epochCache = new LRUCache();

// 動態區塊範圍計算器
class BlockRangeCalculator {
  /**
   * 計算指定局次附近的「每局區塊數」最大值
   * @param {number} referenceEpoch - 參考局次
   * @param {number} lookBehind - 往前看幾局（預設10局）
   * @returns {Promise<number>} 每局最大區塊數
   */
  async getBlocksPerEpoch(referenceEpoch, lookBehind = 10) {
    const result = await pool.query(`
      WITH epoch_blocks AS (
        SELECT epoch, MAX(block_number) as last_block
        FROM hisbet
        WHERE epoch BETWEEN $1 AND $2
        GROUP BY epoch
        HAVING COUNT(*) > 5
      )
      SELECT e1.last_block - e2.last_block as diff
      FROM epoch_blocks e1
      JOIN epoch_blocks e2 ON e2.epoch = e1.epoch - 1
      WHERE e1.last_block IS NOT NULL AND e2.last_block IS NOT NULL
    `, [referenceEpoch - lookBehind, referenceEpoch]);

    if (result.rows.length === 0) {
      console.log(`[BlockRange] epoch ${referenceEpoch} 附近無數據，使用預設值 410`);
      return 410; // 根據歷史統計，平均 408，取 410 作為安全值
    }

    const diffs = result.rows.map(r => Number(r.diff));
    const maxBlocks = Math.max(...diffs);
    console.log(`[BlockRange] epoch ${referenceEpoch} 附近 ${result.rows.length} 局數據，每局最大區塊數: ${maxBlocks}`);
    return maxBlocks;
  }

  /**
   * 獲取指定 epoch 的區塊範圍
   * @param {number} epoch - 要查詢的局次
   * @returns {Promise<{startBlock: number, endBlock: number}>}
   */
  async getBlockRangeForEpoch(epoch) {
    const SEARCH_RANGE = 5; // 搜尋前後各5局，應對跳過/失敗的情況

    // 策略1: 查詢後面幾局（往回處理歷史時）
    const nextBlocks = await pool.query(`
      SELECT epoch, MIN(block_number) as min_block
      FROM hisbet
      WHERE epoch BETWEEN $1 AND $2
        AND block_number IS NOT NULL
      GROUP BY epoch
      HAVING COUNT(*) > 5
      ORDER BY epoch ASC
      LIMIT 1
    `, [epoch + 1, epoch + SEARCH_RANGE]);

    if (nextBlocks.rows[0]?.min_block) {
      const foundEpoch = Number(nextBlocks.rows[0].epoch);
      const endBlock = Number(nextBlocks.rows[0].min_block);
      const blocksPerEpoch = await this.getBlocksPerEpoch(foundEpoch, 10);

      // 計算跨越的局數
      const epochGap = foundEpoch - epoch;
      const startBlock = endBlock - (blocksPerEpoch * epochGap) - 50;

      console.log(`[BlockRange] epoch ${epoch} 使用後面第 ${epochGap} 局 (epoch ${foundEpoch}) 推算: ${startBlock} - ${endBlock + 50}`);
      return { startBlock, endBlock: endBlock + 50 };
    }

    // 策略2: 查詢前面幾局（往前處理時）
    const prevBlocks = await pool.query(`
      SELECT epoch, MAX(block_number) as max_block
      FROM hisbet
      WHERE epoch BETWEEN $1 AND $2
        AND block_number IS NOT NULL
      GROUP BY epoch
      HAVING COUNT(*) > 5
      ORDER BY epoch DESC
      LIMIT 1
    `, [epoch - SEARCH_RANGE, epoch - 1]);

    if (prevBlocks.rows[0]?.max_block) {
      const foundEpoch = Number(prevBlocks.rows[0].epoch);
      const prevLastBlock = Number(prevBlocks.rows[0].max_block);
      const blocksPerEpoch = await this.getBlocksPerEpoch(foundEpoch, 10);

      // 計算跨越的局數
      const epochGap = epoch - foundEpoch;
      const startBlock = prevLastBlock - 50;
      const endBlock = prevLastBlock + (blocksPerEpoch * epochGap) + 50;

      console.log(`[BlockRange] epoch ${epoch} 使用前面第 ${epochGap} 局 (epoch ${foundEpoch}) 推算: ${startBlock} - ${endBlock}`);
      return { startBlock, endBlock };
    }

    // 策略3: 前後各5局都沒數據 = 錯誤
    throw new Error(`❌ Epoch ${epoch} 前後各 ${SEARCH_RANGE} 局都沒有數據，無法推算區塊範圍！`);
  }
}

const blockRangeCalc = new BlockRangeCalculator();

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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 區塊鏈連接
const provider = new ethers.JsonRpcProvider(
  process.env.RPC_URL,
  { chainId: 56, name: 'binance' },
  { staticNetwork: true, polling: false }
);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 60000,
});

const contractAddr = process.env.CONTRACT_ADDR;
const contractAbi = JSON.parse(fs.readFileSync("./abi.json", "utf8"));
const contract = new ethers.Contract(contractAddr, contractAbi, provider);

// 重試機制
async function retryFn(fn, retries = RETRY_MAX) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i === retries - 1) throw e;
      await sleep(1000 * (i + 1));
    }
  }
}

// 區塊鏈查詢 - 優先從資料庫查詢
async function getBlockTimestamp(blockNumber) {
  const cached = blockTimestampCache.get(blockNumber);
  if (cached) return cached;

  // 先從 hisbet 表查詢已知的區塊時間戳
  try {
    const res = await pool.query(
      'SELECT bet_time FROM hisbet WHERE block_number = $1 LIMIT 1',
      [blockNumber]
    );
    if (res.rows.length > 0) {
      const timestamp = Math.floor(new Date(res.rows[0].bet_time).getTime() / 1000);
      blockTimestampCache.set(blockNumber, timestamp);
      return timestamp;
    }
  } catch (e) {
    // 查詢失敗則繼續用 RPC
  }

  // 資料庫沒有才用 RPC
  const block = await retryFn(() => provider.getBlock(blockNumber));
  blockTimestampCache.set(blockNumber, block.timestamp);
  return block.timestamp;
}

async function findBlockByTime(targetTime, lowBlock, highBlock) {
  while (lowBlock <= highBlock) {
    const mid = Math.floor((lowBlock + highBlock) / 2);
    const ts = await getBlockTimestamp(mid);
    if (ts === targetTime) return mid;
    if (ts < targetTime) lowBlock = mid + 1;
    else highBlock = mid - 1;
  }
  return lowBlock;
}

async function getRoundData(epoch) {
  const cached = epochCache.get(epoch);
  if (cached) return cached;
  const round = await retryFn(() => contract.rounds(BigInt(epoch)));
  const data = {
    epoch: Number(round.epoch),
    startTimestamp: Number(round.startTimestamp),
    lockTimestamp: Number(round.lockTimestamp),
    closeTimestamp: Number(round.closeTimestamp),
    lockPrice: round.lockPrice.toString(),
    closePrice: round.closePrice.toString(),
    totalAmount: round.totalAmount.toString(),
    bullAmount: round.bullAmount.toString(),
    bearAmount: round.bearAmount.toString(),
  };
  const lp = parseFloat(ethers.formatUnits(data.lockPrice, 8));
  const cp = parseFloat(ethers.formatUnits(data.closePrice, 8));
  if (lp > 0 && cp > 0) epochCache.set(epoch, data);
  return data;
}

async function getLatestEpoch() {
  return Number(await contract.currentEpoch());
}

// 資料庫操作
async function epochAlreadyDone(epoch) {
  const res = await pool.query(`SELECT 1 FROM finepoch WHERE epoch = $1`, [epoch]);
  return res.rowCount > 0;
}

async function logFailedEpoch(epoch, error, stage) {
  try {
    await pool.query(`
      INSERT INTO failed_epochs (epoch, error_message, stage, failed_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (epoch) DO UPDATE SET
        error_message = EXCLUDED.error_message,
        stage = EXCLUDED.stage,
        failed_at = NOW(),
        retry_count = failed_epochs.retry_count + 1
    `, [epoch, error.message.substring(0, 500), stage]);
  } catch (e) {}
}

async function getFailCount(epoch) {
  try {
    const result = await pool.query(
      'SELECT retry_count FROM failed_epochs WHERE epoch = $1',
      [epoch]
    );
    return result.rows[0]?.retry_count || 0;
  } catch (e) {
    return 0;
  }
}

// 驗證函數
async function validateRoundDataFromChain(rd, epoch) {
  const errors = [];
  if (!rd.startTimestamp || rd.startTimestamp <= 0) errors.push('缺少開始時間');
  if (!rd.lockTimestamp || rd.lockTimestamp <= 0) errors.push('缺少鎖定時間');
  if (!rd.closeTimestamp || rd.closeTimestamp <= 0) errors.push('缺少結束時間');
  if (rd.startTimestamp >= rd.lockTimestamp) errors.push('時間邏輯錯誤');
  if (rd.lockTimestamp >= rd.closeTimestamp) errors.push('時間邏輯錯誤');
  
  const lp = parseFloat(ethers.formatUnits(rd.lockPrice, 8));
  const cp = parseFloat(ethers.formatUnits(rd.closePrice, 8));
  if (lp <= 0 || lp < 50 || lp > 5000) errors.push(`lockPrice: ${lp}`);
  if (cp <= 0 || cp < 50 || cp > 5000) errors.push(`closePrice: ${cp}`);
  if (lp > 0 && cp > 0 && Math.abs(cp - lp) / lp > 0.20) errors.push('價格變動>20%');
  
  const t = parseFloat(ethers.formatEther(rd.totalAmount));
  const b = parseFloat(ethers.formatEther(rd.bullAmount));
  const a = parseFloat(ethers.formatEther(rd.bearAmount));
  if (t < 0 || b < 0 || a < 0) errors.push('金額為負');
  if (t === 0 && b === 0 && a === 0) errors.push('金額全為0');
  if (Math.abs(t - (b + a)) > 0.001) errors.push('總額不匹配');
  
  if (errors.length > 0) throw new Error(`Epoch ${epoch} 驗證失敗: ${errors.join('; ')}`);
  return true;
}

async function validateBetEvents(events, rd, epoch) {
  const errors = [];
  if (events.betbull.length === 0) errors.push('缺少UP');
  if (events.betbear.length === 0) errors.push('缺少DOWN');
  
  for (let i = 0; i < events.betbull.length; i++) {
    const e = events.betbull[i];
    try {
      const addr = normalizeAddress(e.args.sender);
      if (addr === '0x0000000000000000000000000000000000000000') errors.push(`UP[${i}]零地址`);
    } catch (err) {
      errors.push(`UP[${i}]地址錯誤`);
    }
    if (!e.args.amount || e.args.amount <= 0n) errors.push(`UP[${i}]amount無效`);
  }
  
  for (let i = 0; i < events.betbear.length; i++) {
    const e = events.betbear[i];
    try {
      const addr = normalizeAddress(e.args.sender);
      if (addr === '0x0000000000000000000000000000000000000000') errors.push(`DOWN[${i}]零地址`);
    } catch (err) {
      errors.push(`DOWN[${i}]地址錯誤`);
    }
    if (!e.args.amount || e.args.amount <= 0n) errors.push(`DOWN[${i}]amount無效`);
  }
  
  if (errors.length > 0) throw new Error(`Epoch ${epoch} 下注驗證失敗: ${errors.join('; ')}`);
  return true;
}

async function validateClaimEvents(events, procEpoch) {
  const errors = [];
  if (events.claim.length === 0) errors.push('缺少Claim');
  
  for (let i = 0; i < events.claim.length; i++) {
    const e = events.claim[i];
    const be = Number(e.args.epoch);
    try {
      const addr = normalizeAddress(e.args.sender);
      if (addr === '0x0000000000000000000000000000000000000000') errors.push(`Claim[${i}]零地址`);
    } catch (err) {
      errors.push(`Claim[${i}]地址錯誤`);
    }
    if (be <= 0) errors.push(`Claim[${i}]bet_epoch無效`);
    if (procEpoch <= be) errors.push(`Claim[${i}]epoch<=bet_epoch`);
    if (!e.args.amount || e.args.amount <= 0n) errors.push(`Claim[${i}]amount無效`);
  }
  
  if (errors.length > 0) throw new Error(`Epoch ${procEpoch} Claim驗證失敗: ${errors.join('; ')}`);
  return true;
}

function verifyRoundBetsStrict(round, bets, epoch) {
  const errors = [];
  const txs = new Set();
  for (const b of bets) {
    if (!b.tx_hash) {
      errors.push('缺tx_hash');
      continue;
    }
    if (txs.has(b.tx_hash)) errors.push('重複tx');
    txs.add(b.tx_hash);
  }
  
  let tot = 0, up = 0, dn = 0, uc = 0, dc = 0;
  for (const b of bets) {
    tot += b.amount;
    if (b.direction === 'UP') { up += b.amount; uc++; }
    else if (b.direction === 'DOWN') { dn += b.amount; dc++; }
  }
  
  if (uc === 0) errors.push('UP為0');
  if (dc === 0) errors.push('DOWN為0');
  
  const et = parseFloat(round.total_bet_amount);
  const eu = parseFloat(round.up_bet_amount);
  const ed = parseFloat(round.down_bet_amount);
  if (Math.abs(tot - et) > 0.001) errors.push('總額不符');
  if (Math.abs(up - eu) > 0.001) errors.push('UP不符');
  if (Math.abs(dn - ed) > 0.001) errors.push('DOWN不符');
  
  if (errors.length > 0) throw new Error(`Epoch ${epoch} 金額驗證失敗: ${errors.join('; ')}`);
  return true;
}

async function verifyDatabaseWrite(epoch, round, bets, client) {
  const errors = [];
  const rc = await client.query('SELECT 1 FROM round WHERE epoch = $1', [epoch]);
  if (rc.rows.length === 0) errors.push('round失敗');
  
  const hc = await client.query('SELECT COUNT(*) as c FROM hisbet WHERE epoch = $1', [epoch]);
  if (parseInt(hc.rows[0].c) !== bets.length) errors.push('hisbet筆數不符');
  
  const fc = await client.query('SELECT 1 FROM finepoch WHERE epoch = $1', [epoch]);
  if (fc.rows.length === 0) errors.push('finepoch失敗');
  
  if (errors.length > 0) throw new Error(`資料庫驗證失敗: ${errors.join('; ')}`);
  return true;
}

// 事件處理 - 智能版：基於歷史數據推算區塊範圍
async function fetchEventsForEpoch(epoch) {
  console.log(`    [fetchEvents] 獲取 Round 數據...`);
  const rd = await getRoundData(epoch);

  console.log(`    [fetchEvents] 計算區塊範圍...`);
  // 使用智能計算器獲取區塊範圍（完全基於資料庫數據，零 RPC 請求）
  const { startBlock, endBlock } = await blockRangeCalc.getBlockRangeForEpoch(epoch);
  console.log(`    [fetchEvents] 區塊範圍: ${startBlock} - ${endBlock}`);

  const f = contract.filters;

  const events = {
    startround: [],
    lockround: [],
    endround: [],
    betbull: [],
    betbear: [],
    claim: []
  };

  // 批次查詢事件
  try {
    const [bullEvents, bearEvents, claimEvents] = await Promise.all([
      retryFn(() => contract.queryFilter(f.BetBull(null, BigInt(epoch)), startBlock, endBlock)),
      retryFn(() => contract.queryFilter(f.BetBear(null, BigInt(epoch)), startBlock, endBlock)),
      retryFn(() => contract.queryFilter(f.Claim(), startBlock, endBlock))
    ]);

    events.betbull = bullEvents;
    events.betbear = bearEvents;
    events.claim = claimEvents;

    await sleep(100); // 減少請求頻率
  } catch (e) {
    console.error(`查詢事件失敗: ${e.message}`);
  }

  return { events, roundData: rd };
}

async function parseRoundData(epoch, rd = null) {
  if (!rd) rd = await getRoundData(epoch);
  const lp = parseFloat(ethers.formatUnits(rd.lockPrice, 8));
  const cp = parseFloat(ethers.formatUnits(rd.closePrice, 8));
  const result = cp > lp ? "UP" : "DOWN";
  const bull = parseFloat(ethers.formatEther(rd.bullAmount));
  const bear = parseFloat(ethers.formatEther(rd.bearAmount));
  const tot = bull + bear;
  const after = tot * 0.97;

  return {
    epoch: rd.epoch,
    start_time: toTaipeiTimeString(rd.startTimestamp),
    lock_time: toTaipeiTimeString(rd.lockTimestamp),
    close_time: toTaipeiTimeString(rd.closeTimestamp),
    lock_price: ethers.formatUnits(rd.lockPrice, 8),
    close_price: ethers.formatUnits(rd.closePrice, 8),
    result: result,
    total_bet_amount: tot.toString(),
    up_bet_amount: bull.toString(),
    down_bet_amount: bear.toString(),
    up_payout: (bull > 0 ? after / bull : 0).toString(),
    down_payout: (bear > 0 ? after / bear : 0).toString()
  };
}

async function parseBets(events) {
  const bets = [];
  const bts = new Map();
  const proc = async (e, dir) => {
    let ts = bts.get(e.blockNumber);
    if (!ts) {
      ts = await getBlockTimestamp(e.blockNumber);
      bts.set(e.blockNumber, ts);
    }
    bets.push({
      epoch: Number(e.args.epoch),
      bet_time: toTaipeiTimeString(ts),
      wallet_address: normalizeAddress(e.args.sender),
      direction: dir,
      amount: parseFloat(ethers.formatEther(e.args.amount)),
      block_number: e.blockNumber,
      tx_hash: e.transactionHash.toLowerCase(),
    });
  };
  await Promise.all([
    ...events.betbull.map(e => proc(e, "UP")),
    ...events.betbear.map(e => proc(e, "DOWN"))
  ]);
  return bets;
}

function parseClaims(events, pe) {
  return events.claim.map(e => ({
    epoch: pe,
    block_number: e.blockNumber,
    wallet_address: normalizeAddress(e.args.sender),
    bet_epoch: Number(e.args.epoch),
    amount: parseFloat(ethers.formatEther(e.args.amount))
  }));
}

async function detectMultiClaims(claims) {
  const ws = new Map();
  for (const c of claims) {
    const ex = ws.get(c.wallet_address) || { claimedEpochs: new Set(), totalAmount: 0 };
    ex.claimedEpochs.add(c.bet_epoch);
    ex.totalAmount += c.amount;
    ws.set(c.wallet_address, ex);
  }
  const mce = [];
  for (const [w, s] of ws.entries()) {
    if (s.claimedEpochs.size >= 5 || s.totalAmount >= 1) {
      mce.push({
        epoch: claims[0].epoch,
        wallet_address: w,
        num_claimed_epochs: s.claimedEpochs.size,
        total_amount: s.totalAmount
      });
    }
  }
  return mce;
}

// 資料庫寫入（帶詳細日誌）
async function writeDataTransaction(round, bets, claims, multiClaims, client, lp = '') {
  await client.query("BEGIN");
  
  await client.query(`
    INSERT INTO round(epoch, start_time, lock_time, close_time, lock_price, close_price, result,
       total_bet_amount, up_bet_amount, down_bet_amount, up_payout, down_payout)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    ON CONFLICT (start_time, epoch) DO UPDATE SET
      lock_price=EXCLUDED.lock_price, close_price=EXCLUDED.close_price, result=EXCLUDED.result,
      total_bet_amount=EXCLUDED.total_bet_amount, up_bet_amount=EXCLUDED.up_bet_amount,
      down_bet_amount=EXCLUDED.down_bet_amount, up_payout=EXCLUDED.up_payout, down_payout=EXCLUDED.down_payout
    `, [round.epoch, round.start_time, round.lock_time, round.close_time, round.lock_price, round.close_price,
        round.result, round.total_bet_amount||0, round.up_bet_amount||0, round.down_bet_amount||0,
        round.up_payout||0, round.down_payout||0]);

  if (bets.length > 0) {
    const v = [], p = [];
    let i = 1;
    for (const b of bets) {
      v.push(`($${i},$${i+1},$${i+2},$${i+3},$${i+4},$${i+5},$${i+6},$${i+7})`);
      p.push(b.epoch, b.bet_time, b.wallet_address, b.direction, b.amount,
            b.block_number, b.tx_hash, b.direction === round.result ? "WIN" : "LOSS");
      i += 8;
    }
    await client.query(`
      INSERT INTO hisbet(epoch, bet_time, wallet_address, bet_direction, bet_amount, block_number, tx_hash, result)
      VALUES ${v.join(',')} ON CONFLICT (bet_time, tx_hash) DO NOTHING
    `, p);
  }

  if (claims.length > 0) {
    const v = [], p = [];
    let i = 1;
    for (const c of claims) {
      v.push(`($${i},$${i+1},$${i+2},$${i+3},$${i+4})`);
      p.push(c.epoch, c.block_number, c.wallet_address, c.bet_epoch, c.amount);
      i += 5;
    }
    await client.query(`
      INSERT INTO claim(epoch, block_number, wallet_address, bet_epoch, amount)
      VALUES ${v.join(',')} ON CONFLICT (block_number, wallet_address, bet_epoch) DO NOTHING
    `, p);
  }

  let multiClaimCount = 0;
  if (multiClaims.length > 0) {
    const v = [], p = [];
    let i = 1;
    for (const mc of multiClaims) {
      v.push(`($${i},$${i+1},$${i+2},$${i+3})`);
      p.push(mc.epoch, mc.wallet_address, mc.num_claimed_epochs, mc.total_amount);
      i += 4;
    }
    await client.query(`
      INSERT INTO multi_claim(epoch, wallet_address, num_claimed_epochs, total_amount)
      VALUES ${v.join(',')} ON CONFLICT (epoch, wallet_address) DO NOTHING
    `, p);
    multiClaimCount = multiClaims.length;
  }

  const now = Math.floor(Date.now() / 1000);
  const ct = new Date(round.close_time).getTime() / 1000;
  let realbetDeleted = 0;
  if (now - ct > 600) {
    const deleteResult = await client.query('DELETE FROM realbet WHERE epoch = $1', [round.epoch]);
    realbetDeleted = deleteResult.rowCount;
  }

  await client.query(`INSERT INTO finepoch(epoch) VALUES($1) ON CONFLICT DO NOTHING`, [round.epoch]);
  await client.query("COMMIT");

  // 詳細日誌
  console.log(`${lp}  multi_claim: ${multiClaimCount} 筆`);
  console.log(`${lp}  realbet 刪除: ${realbetDeleted} 筆`);

  try {
    await redisPublisher.publish('round_update_channel', JSON.stringify(round));
  } catch (e) {}
}

// 核心同步邏輯（帶詳細日誌）
async function syncEpoch(epoch, lp = '') {
  if (await epochAlreadyDone(epoch)) {
    console.log(`${lp}Epoch ${epoch} 已存在，跳過`);
    return { success: true, skipped: true };
  }

  const lk = `processing:epoch:${epoch}`;
  const la = await redisPublisher.set(lk, 'true', 'EX', 300, 'NX'); // 5分鐘鎖，避免雙線程衝突
  if (!la) {
    console.log(`${lp}Epoch ${epoch} 已被鎖定，跳過`);
    return { success: false, reason: 'locked' };
  }

  const client = await pool.connect();
  try {
    console.log(`${lp}📍 開始處理 Epoch ${epoch}`);

    console.log(`${lp}  → 獲取事件數據...`);
    const { events, roundData } = await fetchEventsForEpoch(epoch);
    
    // 日誌：事件統計
    const upCount = events.betbull.length;
    const downCount = events.betbear.length;
    const claimCount = events.claim.length;
    console.log(`${lp}  UP: ${upCount} 筆, DOWN: ${downCount} 筆, 領獎: ${claimCount} 筆`);
    
    await validateRoundDataFromChain(roundData, epoch);
    await validateBetEvents(events, roundData, epoch);
    await validateClaimEvents(events, epoch);

    const round = await parseRoundData(epoch, roundData);
    const bets = await parseBets(events);
    const claims = parseClaims(events, epoch);
    const mc = await detectMultiClaims(claims);

    verifyRoundBetsStrict(round, bets, epoch);
    await writeDataTransaction(round, bets, claims, mc, client, lp);
    await verifyDatabaseWrite(epoch, round, bets, client);

    console.log(`${lp}✅ Epoch ${epoch} 完成`);
    return { success: true };

  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`${lp}❌ Epoch ${epoch}: ${error.message}`);
    await logFailedEpoch(epoch, error, 'sync');
    return { success: false, error: error.message };
  } finally {
    client.release();
    await redisPublisher.del(lk);
  }
}

// 三線系統狀態
let latestEpochFromRedis = null;
let latestLockTimestamp = null;
let upLinePaused = false;
let downLinePaused = false;
let gapLinePaused = false;

/**
 * 獲取數據母體邊界
 */
async function getDataBoundaries() {
  const result = await pool.query(`
    SELECT MIN(epoch) as min_epoch, MAX(epoch) as max_epoch, COUNT(DISTINCT epoch) as total_epochs
    FROM hisbet
  `);
  return result.rows[0];
}

/**
 * 訂閱 Redis round_update_channel
 */
async function setupRedisSubscription() {
  const redisSubscriber = new Redis(process.env.REDIS_URL, {
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
      if (times > 3) return null;
      return Math.min(times * 1000, 3000);
    }
  });

  redisSubscriber.subscribe('round_update_channel', (err) => {
    if (err) {
      console.error('❌ Redis 訂閱失敗:', err);
    } else {
      console.log('✅ 已訂閱 round_update_channel');
    }
  });

  redisSubscriber.on('message', async (channel, message) => {
    if (channel === 'round_update_channel') {
      try {
        const data = JSON.parse(message);
        let epoch, lockTimestamp;
        
        // 支援兩種格式：
        // 1. { type: 'round_update', data: { epoch: ..., lockTimestamp: ... } }
        // 2. { epoch: ..., lockTimestamp: ... } 或 { epoch: ..., status: ... }
        if (data.type === 'round_update' && data.data) {
          epoch = Number(data.data.epoch);
          lockTimestamp = Number(data.data.lockTimestamp);
        } else if (data.epoch) {
          epoch = Number(data.epoch);
          lockTimestamp = data.lockTimestamp ? Number(data.lockTimestamp) : null;
        } else {
          console.log(`⚠️ 未知訊息格式:`, data);
          return;
        }

        console.log(`📥 收到局次更新: Epoch ${epoch}${lockTimestamp ? `, lockTimestamp: ${lockTimestamp}` : ''}`);

        // 更新全局狀態
        latestEpochFromRedis = epoch;
        if (lockTimestamp) latestLockTimestamp = lockTimestamp;
        
      } catch (err) {
        console.error('❌ 處理 round_update 訊息失敗:', err.message);
      }
    }
  });

  redisSubscriber.on('error', (err) => {
    console.error('❌ Redis 訂閱錯誤:', err.message);
  });

  return redisSubscriber;
}

/**
 * 上行線：從 DB 最大往前補到最新 N-2
 */
async function upLine() {
  const up = colors.blue('[上行線] ');
  console.log(`${up}啟動`);

  while (true) {
    try {
      if (upLinePaused) {
        await sleep(5000);
        continue;
      }

      // 直接從智能合約獲取最新局次
      let latestEpoch;
      try {
        latestEpoch = await getLatestEpoch();
        console.log(`${up}從智能合約獲取最新局次: ${latestEpoch}`);
      } catch (error) {
        console.error(`${up}獲取最新局次失敗: ${error.message}`);
        await sleep(10000);
        continue;
      }

      const targetEpoch = latestEpoch - 2;
      const boundaries = await getDataBoundaries();
      const dbMax = boundaries.max_epoch ? Number(boundaries.max_epoch) : 0;

      console.log(`${up}最新局次: ${latestEpoch}, 目標補到: ${targetEpoch}, DB最大: ${dbMax}`);

      if (dbMax >= targetEpoch) {
        console.log(`${up}已補齊到目標，休息等待下一局...`);

        // 休息等待下一局（預設5分鐘局間）
        console.log(`${up}休息 60 秒，等待下一局開始`);
        await sleep(60000); // 等待1分鐘
        continue;
      }

      // 從 dbMax + 1 補到 targetEpoch
      for (let epoch = dbMax + 1; epoch <= targetEpoch; epoch++) {
        if (await epochAlreadyDone(epoch)) {
          console.log(`${up}Epoch ${epoch} 已存在`);
          continue;
        }

        // 檢查失敗次數，3次後跳過
        const failCount = await getFailCount(epoch);
        if (failCount >= RETRY_MAX) {
          console.log(`${up}Epoch ${epoch} 已失敗 ${failCount} 次，跳過`);
          continue;
        }

        console.log(`${up}處理 Epoch ${epoch}${failCount > 0 ? ` (重試 ${failCount + 1}/${RETRY_MAX})` : ''}`);
        await syncEpoch(epoch, up);
        await sleep(100);
      }

      console.log(`${up}✅ 補齊完成`);

    } catch (e) {
      console.error(`${up}❌ 異常:`, e.message);
      await sleep(10000);
    }
  }
}

/**
 * 下行線：從 DB 最小往回補歷史
 */
async function downLine() {
  const down = colors.cyan('[下行線] ');
  await sleep(30000); // 延遲啟動，讓上行線先運行
  console.log(`${down}啟動`);

  while (true) {
    try {
      if (downLinePaused) {
        await sleep(5000);
        continue;
      }

      const boundaries = await getDataBoundaries();
      const dbMin = boundaries.min_epoch ? Number(boundaries.min_epoch) : null;

      if (!dbMin) {
        console.log(`${down}資料庫無數據，等待上行線建立母體`);
        await sleep(60000);
        continue;
      }

      const targetEpoch = dbMin - 1;

      if (targetEpoch < 1) {
        console.log(`${down}已補到 epoch 1，休息`);
        await sleep(300000); // 休息5分鐘
        continue;
      }

      if (await epochAlreadyDone(targetEpoch)) {
        console.log(`${down}Epoch ${targetEpoch} 已存在`);
        continue;
      }

      // 檢查失敗次數，3次後跳過
      const failCount = await getFailCount(targetEpoch);
      if (failCount >= RETRY_MAX) {
        console.log(`${down}Epoch ${targetEpoch} 已失敗 ${failCount} 次，跳過`);
        continue;
      }

      console.log(`${down}處理 Epoch ${targetEpoch}${failCount > 0 ? ` (重試 ${failCount + 1}/${RETRY_MAX})` : ''}`);
      await syncEpoch(targetEpoch, down);
      await sleep(2000); // 慢慢補，不急

    } catch (e) {
      console.error(`${down}❌ 異常:`, e.message);
      await sleep(10000);
    }
  }
}

/**
 * 支線（間隙填補線）：每 30 分鐘掃描 DB 範圍內的缺失局次
 */
async function gapLine() {
  const gap = colors.green('[支線] ');
  await sleep(30 * 60 * 1000); // 延遲30分鐘啟動
  console.log(`${gap}啟動`);

  while (true) {
    try {
      if (gapLinePaused) {
        await sleep(5000);
        continue;
      }

      const boundaries = await getDataBoundaries();
      if (!boundaries.min_epoch || !boundaries.max_epoch) {
        console.log(`${gap}資料庫無數據，跳過掃描`);
        await sleep(30 * 60 * 1000);
        continue;
      }

      const dbMin = Number(boundaries.min_epoch);
      const dbMax = Number(boundaries.max_epoch);
      const totalEpochs = Number(boundaries.total_epochs);
      const expectedEpochs = dbMax - dbMin + 1;

      console.log(`${gap}掃描範圍: ${dbMin} - ${dbMax}, 已有: ${totalEpochs}, 應有: ${expectedEpochs}`);

      if (totalEpochs >= expectedEpochs) {
        console.log(`${gap}✅ 無缺失局次`);
      } else {
        const missing = expectedEpochs - totalEpochs;
        console.log(`${gap}⚠️ 發現 ${missing} 個缺失局次，開始補齊...`);

        // 找出缺失的局次
        const result = await pool.query(`
          SELECT epoch + 1 AS missing_epoch
          FROM hisbet
          WHERE epoch + 1 NOT IN (SELECT epoch FROM hisbet WHERE epoch BETWEEN $1 AND $2)
            AND epoch BETWEEN $1 AND $2 - 1
          ORDER BY epoch
          LIMIT 100
        `, [dbMin, dbMax]);

        for (const row of result.rows) {
          const missingEpoch = Number(row.missing_epoch);
          console.log(`${gap}補齊缺失局次: ${missingEpoch}`);
          try {
            await syncEpoch(missingEpoch, gap);
          } catch (err) {
            console.error(`${gap}❌ 補齊 ${missingEpoch} 失敗: ${err.message}`);
          }
          await sleep(500);
        }

        console.log(`${gap}✅ 間隙補齊完成`);
      }

    } catch (e) {
      console.error(`${gap}❌ 異常:`, e.message);
    }

    await sleep(30 * 60 * 1000); // 每30分鐘運行一次
  }
}

// 啟動
(async () => {
  console.log("啟動歷史數據同步 - 三線架構");
  console.log("上行線: 從 DB 最大往前補到最新 N-2，休息等待下一局");
  console.log("下行線: 從 DB 最小往回補歷史");
  console.log("支線: 每 30 分鐘掃描並補齊缺失局次");

  const client = await pool.connect();
  await client.query("SELECT 1");
  client.release();
  console.log("數據庫連接成功");

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS failed_epochs (
        epoch BIGINT PRIMARY KEY,
        error_message TEXT NOT NULL,
        stage VARCHAR(50) NOT NULL,
        failed_at TIMESTAMPTZ NOT NULL,
        retry_count INT DEFAULT 0
      )
    `);
  } catch (e) {}

  // 訂閱 Redis 獲取最新局次
  await setupRedisSubscription();

  // 啟動三線
  upLine();
  downLine();
  gapLine();
})();