require('dotenv').config();
const { genkit } = require('genkit');
const { googleAI } = require('@genkit-ai/google-genai');
const { z } = require('zod');

// Initialize Genkit
const ai = genkit({
  plugins: [googleAI()],
  logLevel: 'info',
  enableTracingAndMetrics: true,
});
const { Pool } = require('pg');
const { createClient } = require('ioredis');
const { ethers } = require('ethers');
const fs = require('fs');

// Configuration from environment variables
if (!process.env.DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!process.env.RPC_URL) throw new Error("Missing RPC_URL");
if (!process.env.CONTRACT_ADDR) throw new Error("Missing CONTRACT_ADDR");
if (!process.env.REDIS_URL) throw new Error("Missing REDIS_URL");

const RETRY_MAX = parseInt(process.env.RETRY_MAX) || 3;
const CACHE_MAX = parseInt(process.env.CACHE_MAX) || 5000;
const RPC_CALL_DELAY_MS = parseInt(process.env.RPC_CALL_DELAY_MS) || 200; // New configurable RPC delay

// Initialize PostgreSQL Pool
const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 60000,
});

// Initialize Redis Client for publishing (used by actions and orchestrator)
const redisPublisher = createClient({
  url: process.env.REDIS_URL,
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    if (times > 3) return null;
    return Math.min(times * 1000, 3000);
  }
});
redisPublisher.on('error', (err) => console.error('Redis Publisher Error:', err.message));

// Initialize Ethers Provider and Contract
const provider = new ethers.JsonRpcProvider(
  process.env.RPC_URL,
  { chainId: 56, name: 'binance' },
  { staticNetwork: true, polling: false }
);
const contractAddr = process.env.CONTRACT_ADDR;
const contractAbi = JSON.parse(fs.readFileSync("./abi.json", "utf8"));
const contract = new ethers.Contract(contractAddr, contractAbi, provider);

// --- Utility Classes and Functions (ported from hisbet.js) ---

// LRU Cache
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

// Block Range Calculator
class BlockRangeCalculator {
  async getBlocksPerEpoch(referenceEpoch, lookBehind = 10) {
    const result = await pgPool.query(`
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
      return 410;
    }
    const diffs = result.rows.map(r => Number(r.diff));
    const maxBlocks = Math.max(...diffs);
    console.log(`[BlockRange] epoch ${referenceEpoch} 附近 ${result.rows.length} 局數據，每局最大區塊數: ${maxBlocks}`);
    return maxBlocks;
  }

  async getBlockRangeForEpoch(epoch) {
    const SEARCH_RANGE = 5;

    const nextBlocks = await pgPool.query(`
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
      const epochGap = foundEpoch - epoch;
      const startBlock = endBlock - (blocksPerEpoch * epochGap) - 50;
      console.log(`[BlockRange] epoch ${epoch} 使用後面第 ${epochGap} 局 (epoch ${foundEpoch}) 推算: ${startBlock} - ${endBlock + 50}`);
      return { startBlock, endBlock: endBlock + 50 };
    }

    const prevBlocks = await pgPool.query(`
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
      const epochGap = epoch - foundEpoch;
      const startBlock = prevLastBlock - 50;
      const endBlock = prevLastBlock + (blocksPerEpoch * epochGap) + 50;
      console.log(`[BlockRange] epoch ${epoch} 使用前面第 ${epochGap} 局 (epoch ${foundEpoch}) 推算: ${startBlock} - ${endBlock}`);
      return { startBlock, endBlock };
    }

    throw new Error(`❌ Epoch ${epoch} 前後各 ${SEARCH_RANGE} 局都沒有數據，無法推算區塊範圍！`);
  }
}
const blockRangeCalc = new BlockRangeCalculator();

// Utility functions
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

async function retryFn(fn, retries = RETRY_MAX) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i === retries - 1) throw e;
      // Add configurable delay between retries to manage RPC load
      await sleep(RPC_CALL_DELAY_MS * (i + 1)); 
    }
  }
  // If all retries fail, the last error is re-thrown by the loop
}

async function getBlockTimestamp(blockNumber) {
  const cached = blockTimestampCache.get(blockNumber);
  if (cached) return cached;
  try {
    const res = await pgPool.query(
      'SELECT bet_time FROM hisbet WHERE block_number = $1 LIMIT 1',
      [blockNumber]
    );
    if (res.rows.length > 0) {
      const timestamp = Math.floor(new Date(res.rows[0].bet_time).getTime() / 1000);
      blockTimestampCache.set(blockNumber, timestamp);
      return timestamp;
    }
  } catch (e) {
    // Query failed, continue with RPC
  }
  const block = await retryFn(() => provider.getBlock(blockNumber));
  blockTimestampCache.set(blockNumber, block.timestamp);
  return block.timestamp;
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

async function getDataBoundaries() {
  const result = await pgPool.query(`
    SELECT MIN(epoch) as min_epoch, MAX(epoch) as max_epoch, COUNT(DISTINCT epoch) as total_epochs
    FROM hisbet
  `);
  return result.rows[0];
}

// --- Genkit Actions ---

const acquireEpochLockAction = ai.defineTool(
  {
    name: 'acquireEpochLock',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.object({ success: z.boolean(), reason: z.string() }),
  },
  async ({ epoch }) => {
    const lk = `processing:epoch:${epoch}`;
    const la = await redisPublisher.set(lk, 'true', 'EX', 300, 'NX'); // 5 minutes lock
    if (!la) {
      console.log(`Epoch ${epoch} already locked, skipping.`);
      return { success: false, reason: 'locked' };
    }
    return { success: true };
  }
);

const releaseEpochLockAction = ai.defineTool(
  {
    name: 'releaseEpochLock',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ epoch }) => {
    const lk = `processing:epoch:${epoch}`;
    await redisPublisher.del(lk);
    return { success: true };
  }
);

const epochAlreadyDoneAction = ai.defineTool(
  {
    name: 'epochAlreadyDone',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.object({ done: z.boolean() }),
  },
  async ({ epoch }) => {
    const res = await pgPool.query(`SELECT 1 FROM finepoch WHERE epoch = $1`, [epoch]);
    return { done: res.rowCount > 0 };
  }
);

const getFailCountAction = ai.defineTool(
  {
    name: 'getFailCount',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.object({ count: z.number() }),
  },
  async ({ epoch }) => {
    try {
      const result = await pgPool.query(
        'SELECT retry_count FROM failed_epochs WHERE epoch = $1',
        [epoch]
      );
      return { count: result.rows[0]?.retry_count || 0 };
    } catch (e) {
      return { count: 0 };
    }
  }
);

const logFailedEpochAction = ai.defineTool(
  {
    name: 'logFailedEpoch',
    inputSchema: z.object({
        epoch: z.number(),
        errorMessage: z.string(),
        stage: z.string(),
      }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ epoch, errorMessage, stage }) => {
    try {
      await pgPool.query(`
        INSERT INTO failed_epochs (epoch, error_message, stage, failed_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (epoch) DO UPDATE SET
          error_message = EXCLUDED.error_message,
          stage = EXCLUDED.stage,
          failed_at = NOW(),
          retry_count = failed_epochs.retry_count + 1
      `, [epoch, errorMessage.substring(0, 500), stage]);
      return { success: true };
    } catch (e) {
      console.error(`Failed to log failed epoch ${epoch}:`, e.message);
      return { success: false };
    }
  }
);

const fetchRoundDataAction = ai.defineTool(
  {
    name: 'fetchRoundData',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.any(),
  },
  async ({ epoch }) => {
    return await getRoundData(epoch);
  }
);

const calculateBlockRangeAction = ai.defineTool(
  {
    name: 'calculateBlockRange',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.object({ startBlock: z.number(), endBlock: z.number() }),
  },
  async ({ epoch }) => {
    return await blockRangeCalc.getBlockRangeForEpoch(epoch);
  }
);

const fetchContractEventsAction = ai.defineTool(
  {
    name: 'fetchContractEvents',
    inputSchema: z.object({
        epoch: z.number(),
        startBlock: z.number(),
        endBlock: z.number(),
      }),
    outputSchema: z.any(),
  },
  async ({ epoch, startBlock, endBlock }) => {
    const f = contract.filters;
    const events = {
      betbull: [],
      betbear: [],
      claim: []
    };
    try {
      const [bullEvents, bearEvents, claimEvents] = await Promise.all([
        retryFn(() => contract.queryFilter(f.BetBull(null, BigInt(epoch)), startBlock, endBlock)),
        retryFn(() => contract.queryFilter(f.BetBear(null, BigInt(epoch)), startBlock, endBlock)),
        retryFn(() => contract.queryFilter(f.Claim(), startBlock, endBlock))
      ]);
      events.betbull = bullEvents;
      events.betbear = bearEvents;
      events.claim = claimEvents;
      await sleep(RPC_CALL_DELAY_MS); // Add configurable delay after fetching events
    } catch (e) {
      console.error(`Failed to query events for epoch ${epoch}: ${e.message}`);
      throw e; // Re-throw to be caught by the flow's error handling
    }
    return events;
  }
);

const validateRoundDataAction = ai.defineTool(
  {
    name: 'validateRoundData',
    inputSchema: z.object({ roundData: z.any(), epoch: z.number() }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ roundData, epoch }) => {
    const errors = [];
    if (!roundData.startTimestamp || roundData.startTimestamp <= 0) errors.push('Missing start time');
    if (!roundData.lockTimestamp || roundData.lockTimestamp <= 0) errors.push('Missing lock time');
    if (!roundData.closeTimestamp || roundData.closeTimestamp <= 0) errors.push('Missing close time');
    if (roundData.startTimestamp >= roundData.lockTimestamp) errors.push('Time logic error');
    if (roundData.lockTimestamp >= roundData.closeTimestamp) errors.push('Time logic error');
    
    const lp = parseFloat(ethers.formatUnits(roundData.lockPrice, 8));
    const cp = parseFloat(ethers.formatUnits(roundData.closePrice, 8));
    if (lp <= 0 || lp < 50 || lp > 5000) errors.push(`lockPrice: ${lp}`);
    if (cp <= 0 || cp < 50 || cp > 5000) errors.push(`closePrice: ${cp}`);
    if (lp > 0 && cp > 0 && Math.abs(cp - lp) / lp > 0.20) errors.push('Price change > 20%');
    
    const t = parseFloat(ethers.formatEther(roundData.totalAmount));
    const b = parseFloat(ethers.formatEther(roundData.bullAmount));
    const a = parseFloat(ethers.formatEther(roundData.bearAmount));
    if (t < 0 || b < 0 || a < 0) errors.push('Negative amount');
    if (t === 0 && b === 0 && a === 0) errors.push('All amounts are zero');
    if (Math.abs(t - (b + a)) > 0.001) errors.push('Total amount mismatch');
    
    if (errors.length > 0) throw new Error(`Epoch ${epoch} validation failed: ${errors.join('; ')}`);
    return { success: true };
  }
);

const validateBetEventsAction = ai.defineTool(
  {
    name: 'validateBetEvents',
    inputSchema: z.object({ events: z.any(), roundData: z.any(), epoch: z.number() }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ events, roundData, epoch }) => {
    const errors = [];
    if (events.betbull.length === 0) errors.push('Missing UP bets');
    if (events.betbear.length === 0) errors.push('Missing DOWN bets');
    
    for (let i = 0; i < events.betbull.length; i++) {
      const e = events.betbull[i];
      try {
        const addr = normalizeAddress(e.args.sender);
        if (addr === '0x0000000000000000000000000000000000000000') errors.push(`UP[${i}] zero address`);
      } catch (err) {
        errors.push(`UP[${i}] invalid address`);
      }
      if (!e.args.amount || e.args.amount <= 0n) errors.push(`UP[${i}] invalid amount`);
    }
    
    for (let i = 0; i < events.betbear.length; i++) {
      const e = events.betbear[i];
      try {
        const addr = normalizeAddress(e.args.sender);
        if (addr === '0x0000000000000000000000000000000000000000') errors.push(`DOWN[${i}] zero address`);
      } catch (err) {
        errors.push(`DOWN[${i}] invalid address`);
      }
      if (!e.args.amount || e.args.amount <= 0n) errors.push(`DOWN[${i}] invalid amount`);
    }
    
    if (errors.length > 0) throw new Error(`Epoch ${epoch} bet validation failed: ${errors.join('; ')}`);
    return { success: true };
  }
);

const validateClaimEventsAction = ai.defineTool(
  {
    name: 'validateClaimEvents',
    inputSchema: z.object({ events: z.any(), epoch: z.number() }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ events, epoch }) => {
    const errors = [];
    if (events.claim.length === 0) errors.push('Missing Claim events');
    
    for (let i = 0; i < events.claim.length; i++) {
      const e = events.claim[i];
      const be = Number(e.args.epoch);
      try {
        const addr = normalizeAddress(e.args.sender);
        if (addr === '0x0000000000000000000000000000000000000000') errors.push(`Claim[${i}] zero address`);
      } catch (err) {
        errors.push(`Claim[${i}] invalid address`);
      }
      if (be <= 0) errors.push(`Claim[${i}] bet_epoch invalid`);
      if (epoch <= be) errors.push(`Claim[${i}] epoch <= bet_epoch`);
      if (!e.args.amount || e.args.amount <= 0n) errors.push(`Claim[${i}] amount invalid`);
    }
    
    if (errors.length > 0) throw new Error(`Epoch ${epoch} Claim validation failed: ${errors.join('; ')}`);
    return { success: true };
  }
);

const parseRoundDataAction = ai.defineTool(
  {
    name: 'parseRoundData',
    inputSchema: z.object({ epoch: z.number(), roundData: z.any() }),
    outputSchema: z.any(),
  },
  async ({ epoch, roundData }) => {
    const lp = parseFloat(ethers.formatUnits(roundData.lockPrice, 8));
    const cp = parseFloat(ethers.formatUnits(roundData.closePrice, 8));
    const result = cp > lp ? "UP" : "DOWN";
    const bull = parseFloat(ethers.formatEther(roundData.bullAmount));
    const bear = parseFloat(ethers.formatEther(roundData.bearAmount));
    const tot = bull + bear;
    const after = tot * 0.97;

    return {
      epoch: roundData.epoch,
      start_time: toTaipeiTimeString(roundData.startTimestamp),
      lock_time: toTaipeiTimeString(roundData.lockTimestamp),
      close_time: toTaipeiTimeString(roundData.closeTimestamp),
      lock_price: ethers.formatUnits(roundData.lockPrice, 8),
      close_price: ethers.formatUnits(roundData.closePrice, 8),
      result: result,
      total_bet_amount: tot.toString(),
      up_bet_amount: bull.toString(),
      down_bet_amount: bear.toString(),
      up_payout: (bull > 0 ? after / bull : 0).toString(),
      down_payout: (bear > 0 ? after / bear : 0).toString()
    };
  }
);

const parseBetEventsAction = ai.defineTool(
  {
    name: 'parseBetEvents',
    inputSchema: z.object({ events: z.any() }),
    outputSchema: z.array(z.any()),
  },
  async ({ events }) => {
    const bets = [];
    const bts = new Map(); // blockTimestamp cache for this batch
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
);

const parseClaimEventsAction = ai.defineTool(
  {
    name: 'parseClaimEvents',
    inputSchema: z.object({ events: z.any(), epoch: z.number() }),
    outputSchema: z.array(z.any()),
  },
  async ({ events, epoch }) => {
    return events.claim.map(e => ({
      epoch: epoch,
      block_number: e.blockNumber,
      wallet_address: normalizeAddress(e.args.sender),
      bet_epoch: Number(e.args.epoch),
      amount: parseFloat(ethers.formatEther(e.args.amount))
    }));
  }
);

const detectMultiClaimsAction = ai.defineTool(
  {
    name: 'detectMultiClaims',
    inputSchema: z.array(z.any()),
    outputSchema: z.array(z.any()),
  },
  async (claims) => {
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
          epoch: claims[0].epoch, // Assuming all claims are for the same processing epoch
          wallet_address: w,
          num_claimed_epochs: s.claimedEpochs.size,
          total_amount: s.totalAmount
        });
      }
    }
    return mce;
  }
);

const verifyRoundBetsStrictAction = ai.defineTool(
  {
    name: 'verifyRoundBetsStrict',
    inputSchema: z.object({
        round: z.any(),
        bets: z.array(z.any()),
        epoch: z.number(),
      }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ round, bets, epoch }) => {
    const errors = [];
    const txs = new Set();
    for (const b of bets) {
      if (!b.tx_hash) {
        errors.push('Missing tx_hash');
        continue;
      }
      if (txs.has(b.tx_hash)) errors.push('Duplicate tx');
      txs.add(b.tx_hash);
    }
    
    let tot = 0, up = 0, dn = 0, uc = 0, dc = 0;
    for (const b of bets) {
      tot += b.amount;
      if (b.direction === 'UP') { up += b.amount; uc++; }
      else if (b.direction === 'DOWN') { dn += b.amount; dc++; }
    }
    
    if (uc === 0) errors.push('UP count is zero');
    if (dc === 0) errors.push('DOWN count is zero');
    
    const et = parseFloat(round.total_bet_amount);
    const eu = parseFloat(round.up_bet_amount);
    const ed = parseFloat(round.down_bet_amount);
    if (Math.abs(tot - et) > 0.001) errors.push('Total amount mismatch');
    if (Math.abs(up - eu) > 0.001) errors.push('UP amount mismatch');
    if (Math.abs(dn - ed) > 0.001) errors.push('DOWN amount mismatch');
    
    if (errors.length > 0) throw new Error(`Epoch ${epoch} amount verification failed: ${errors.join('; ')}`);
    return { success: true };
  }
);

const writeDataTransactionAction = ai.defineTool(
  {
    name: 'writeDataTransaction',
    inputSchema: z.object({
        round: z.any(),
        bets: z.array(z.any()),
        claims: z.array(z.any()),
        multiClaims: z.array(z.any()),
      }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ round, bets, claims, multiClaims }) => {
    const client = await pgPool.connect();
    try {
      await client.query("BEGIN");
      
      await client.query(`
        INSERT INTO round(epoch, start_time, lock_time, close_time, lock_price, close_price, result,
           total_bet_amount, up_bet_amount, down_bet_amount, up_payout, down_payout)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (start_time, epoch) DO UPDATE SET
          lock_price=EXCLUDED.lock_price, close_price=EXCLUDED.close_price, result=EXCLUDED.result,
          total_bet_amount=EXCLUDED.total_bet_amount,
          up_bet_amount=EXCLUDED.up_bet_amount,
          down_bet_amount=EXCLUDED.down_bet_amount,
          up_payout=EXCLUDED.up_payout,
          down_payout=EXCLUDED.down_payout
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
      if (now - ct > 600) {
        await client.query('DELETE FROM realbet WHERE epoch = $1', [round.epoch]);
      }

      await client.query(`INSERT INTO finepoch(epoch) VALUES($1) ON CONFLICT DO NOTHING`, [round.epoch]);
      await client.query("COMMIT");

      await redisPublisher.publish('round_update_channel', JSON.stringify(round));
      return { success: true };
    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`Data transaction failed: ${error.message}`);
      throw error; // Re-throw to be caught by the flow's error handling
    } finally {
      client.release();
    }
  }
);

const verifyDatabaseWriteAction = ai.defineTool(
  {
    name: 'verifyDatabaseWrite',
    inputSchema: z.object({
        epoch: z.number(),
        round: z.any(),
        bets: z.array(z.any()),
      }),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async ({ epoch, round, bets }) => {
    const errors = [];
    const client = await pgPool.connect();
    try {
      const rc = await client.query('SELECT 1 FROM round WHERE epoch = $1', [epoch]);
      if (rc.rows.length === 0) errors.push('Round not found');
      
      const hc = await client.query('SELECT COUNT(*) as c FROM hisbet WHERE epoch = $1', [epoch]);
      if (parseInt(hc.rows[0].c) !== bets.length) errors.push('hisbet count mismatch');
      
      const fc = await client.query('SELECT 1 FROM finepoch WHERE epoch = $1', [epoch]);
      if (fc.rows.length === 0) errors.push('finepoch not found');
      
      if (errors.length > 0) throw new Error(`Database verification failed: ${errors.join('; ')}`);
      return { success: true };
    } finally {
      client.release();
    }
  }
);

// --- Genkit Flow: gk_sync_epoch_flow (Core Sync Logic) ---
const gk_sync_epoch_flow = ai.defineFlow(
  {
    name: 'gk_sync_epoch_flow',
    inputSchema: z.object({ epoch: z.number() }),
    outputSchema: z.object({ success: z.boolean(), epoch: z.number() }),
  },
  async (input) => {
    const { epoch } = input;
    console.log(`Starting sync for Epoch ${epoch}`);

    const { done } = await epochAlreadyDoneAction({ epoch });
    if (done) {
      console.log(`Epoch ${epoch} already processed, skipping.`);
      return { success: true, epoch };
    }

    const { success: lockAcquired, reason } = await acquireEpochLockAction({ epoch });
    if (!lockAcquired) {
      return { success: false, epoch, reason };
    }

    try {
      const roundData = await fetchRoundDataAction({ epoch });
      const { startBlock, endBlock } = await calculateBlockRangeAction({ epoch });
      const events = await fetchContractEventsAction({ epoch, startBlock, endBlock });

      await validateRoundDataAction({ roundData, epoch });
      await validateBetEventsAction({ events, roundData, epoch });
      await validateClaimEventsAction({ events, epoch });

      const parsedRound = await parseRoundDataAction({ epoch, roundData });
      const parsedBets = await parseBetEventsAction({ events });
      const parsedClaims = await parseClaimEventsAction({ events, epoch });
      const multiClaims = await detectMultiClaimsAction(parsedClaims);

      await verifyRoundBetsStrictAction({ round: parsedRound, bets: parsedBets, epoch });
      await writeDataTransactionAction({ round: parsedRound, bets: parsedBets, claims: parsedClaims, multiClaims });
      await verifyDatabaseWriteAction({ epoch, round: parsedRound, bets: parsedBets });

      console.log(`Successfully synced Epoch ${epoch}`);
      return { success: true, epoch };
    } catch (error) {
      console.error(`Error syncing Epoch ${epoch}: ${error.message}`);
      await logFailedEpochAction({ epoch, errorMessage: error.message, stage: 'sync' });
      return { success: false, epoch, error: error.message };
    } finally {
      await releaseEpochLockAction({ epoch });
    }
  }
);

// --- Genkit Flows for Orchestration (replacing hisbet.js's three lines) ---

const gk_up_line_flow = ai.defineFlow(
  {
    name: 'gk_up_line_flow',
    inputSchema: z.object({}),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async () => {
    console.log('[UpLineFlow] Starting...');

    while (true) {
      try {
        // TODO: Implement upLinePaused logic if needed

        const latestEpoch = await getLatestEpoch();
        const targetEpoch = latestEpoch - 2;
        const boundaries = await getDataBoundaries();
        const dbMax = boundaries.max_epoch ? Number(boundaries.max_epoch) : 0;

        console.log(`[UpLineFlow] Latest Epoch: ${latestEpoch}, Target: ${targetEpoch}, DB Max: ${dbMax}`);

        if (dbMax >= targetEpoch) {
          console.log('[UpLineFlow] DB is caught up to target, waiting for next epoch...');
          await sleep(60000); // Wait 1 minute
          continue;
        }

        for (let epoch = (dbMax || 0) + 1; epoch <= targetEpoch; epoch++) {
          const { count: failCount } = await getFailCountAction({ epoch });
          if (failCount >= RETRY_MAX) {
            console.log(`[UpLineFlow] Epoch ${epoch} failed ${failCount} times, skipping.`);
            continue;
          }

          console.log(`[UpLineFlow] Processing Epoch ${epoch}${failCount > 0 ? ` (retry ${failCount + 1}/${RETRY_MAX})` : ''}`);
          const result = await gk_sync_epoch_flow({ epoch });
          if (!result.success) {
            console.error(`[UpLineFlow] Failed to sync epoch ${epoch}: ${result.error || result.reason}`);
            await logFailedEpochAction({ epoch, errorMessage: result.error || result.reason, stage: 'upLine' });
          }
          await sleep(RPC_CALL_DELAY_MS); // Small delay to avoid overwhelming
        }
        console.log('[UpLineFlow] Catch-up complete.');

      } catch (e) {
        console.error('[UpLineFlow] Error:', e.message);
        await sleep(10000);
      }
    }
  }
);

const gk_down_line_flow = ai.defineFlow(
  {
    name: 'gk_down_line_flow',
    inputSchema: z.object({}),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async () => {
    console.log('[DownLineFlow] Starting...');
    await sleep(30000); // Delay start, let upLine run first

    while (true) {
      try {
        // TODO: Implement downLinePaused logic if needed

        const { min_epoch: dbMin } = await getDataBoundaries();

        if (!dbMin) {
          console.log('[DownLineFlow] No data in DB, waiting for upLine to establish base.');
          await sleep(60000);
          continue;
        }

        const targetEpoch = dbMin - 1;

        if (targetEpoch < 1) {
          console.log('[DownLineFlow] Reached epoch 1, resting.');
          await sleep(300000); // Rest 5 minutes
          continue;
        }

        const { done } = await epochAlreadyDoneAction({ epoch: targetEpoch });
        if (done) {
          console.log(`[DownLineFlow] Epoch ${targetEpoch} already processed.`);
          continue;
        }

        const { count: failCount } = await getFailCountAction({ epoch: targetEpoch });
        if (failCount >= RETRY_MAX) {
          console.log(`[DownLineFlow] Epoch ${targetEpoch} failed ${failCount} times, skipping.`);
          continue;
        }

        console.log(`[DownLineFlow] Processing Epoch ${targetEpoch}${failCount > 0 ? ` (retry ${failCount + 1}/${RETRY_MAX})` : ''}`);
        const result = await gk_sync_epoch_flow({ epoch: targetEpoch });
        if (!result.success) {
          console.error(`[DownLineFlow] Failed to sync epoch ${targetEpoch}: ${result.error || result.reason}`);
          await logFailedEpochAction({ epoch: targetEpoch, errorMessage: result.error || result.reason, stage: 'downLine' });
        }
        await sleep(RPC_CALL_DELAY_MS * 2); // Slower backfill

      } catch (e) {
        console.error('[DownLineFlow] Error:', e.message);
        await sleep(10000);
      }
    }
  }
);

const gk_gap_line_flow = ai.defineFlow(
  {
    name: 'gk_gap_line_flow',
    inputSchema: z.object({}),
    outputSchema: z.object({ success: z.boolean() }),
  },
  async () => {
    console.log('[GapLineFlow] Starting...');
    await sleep(30 * 60 * 1000); // Delay 30 minutes to start

    while (true) {
      try {
        // TODO: Implement gapLinePaused logic if needed

        const { min_epoch: dbMin, max_epoch: dbMax, total_epochs: totalEpochs } = await getDataBoundaries();

        if (!dbMin || !dbMax) {
          console.log('[GapLineFlow] No data in DB, skipping scan.');
          await sleep(30 * 60 * 1000);
          continue;
        }

        const expectedEpochs = dbMax - dbMin + 1;

        console.log(`[GapLineFlow] Scan range: ${dbMin} - ${dbMax}, Existing: ${totalEpochs}, Expected: ${expectedEpochs}`);

        if (totalEpochs >= expectedEpochs) {
          console.log('[GapLineFlow] No missing epochs.');
        } else {
          const missing = expectedEpochs - totalEpochs;
          console.log(`[GapLineFlow] Found ${missing} missing epochs, starting to fill...`);

          const result = await pgPool.query(`
            SELECT epoch + 1 AS missing_epoch
            FROM hisbet
            WHERE epoch + 1 NOT IN (SELECT epoch FROM hisbet WHERE epoch BETWEEN $1 AND $2)
              AND epoch BETWEEN $1 AND $2 - 1
            ORDER BY epoch
            LIMIT 100
          `, [dbMin, dbMax]);
          const missingEpochs = result.rows.map(row => Number(row.missing_epoch));

          for (const missingEpoch of missingEpochs) {
            const { count: failCount } = await getFailCountAction({ epoch: missingEpoch });
            if (failCount >= RETRY_MAX) {
              console.log(`[GapLineFlow] Epoch ${missingEpoch} failed ${failCount} times, skipping.`);
              continue;
            }

            console.log(`[GapLineFlow] Filling missing epoch: ${missingEpoch}${failCount > 0 ? ` (retry ${failCount + 1}/${RETRY_MAX})` : ''}`);
            try {
              const syncResult = await gk_sync_epoch_flow({ epoch: missingEpoch });
              if (!syncResult.success) {
                console.error(`[GapLineFlow] Failed to sync missing epoch ${missingEpoch}: ${syncResult.error || syncResult.reason}`);
                await logFailedEpochAction({ epoch: missingEpoch, errorMessage: syncResult.error || syncResult.reason, stage: 'gapLine' });
              }
            } catch (err) {
              console.error(`[GapLineFlow] Error processing missing epoch ${missingEpoch}: ${err.message}`);
              await logFailedEpochAction({ epoch: missingEpoch, errorMessage: err.message, stage: 'gapLine' });
            }
            await sleep(RPC_CALL_DELAY_MS); // Small delay
          }
          console.log('[GapLineFlow] Gap filling complete.');
        }

      } catch (e) {
        console.error('[GapLineFlow] Error:', e.message);
      }

      await sleep(30 * 60 * 1000); // Run every 30 minutes
    }
  }
);

// --- Main Orchestration Logic (replacing hisbet.js's main block) ---

let redisSubscriber;

async function setupRedisSubscription() {
  redisSubscriber = createClient({
    url: process.env.REDIS_URL,
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

        // In the original hisbet.js, this updated latestEpochFromRedis and latestLockTimestamp
        // These global states would need to be managed differently in a Genkit context,
        // perhaps by passing them as input to flows or using a shared state mechanism.
        // For now, we just log the update.
        
      } catch (err) {
        console.error('❌ 處理 round_update 訊息失敗:', err.message);
      }
    }
  });

  redisSubscriber.on('error', (err) => {
    console.error('❌ Redis 訂閱錯誤:', err.message);
  });


}

// Main entry point for the orchestrator
async function startHisbetOrchestrator() {
  console.log("🚀 啟動歷史數據同步協調器 (Genkit 版)");

  try {
    // Connect Redis Publisher


    // Test DB connection
    const client = await pgPool.connect();
    await client.query("SELECT 1");
    client.release();
    console.log("✅ 資料庫連接成功");

    // Ensure failed_epochs table exists
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS failed_epochs (
        epoch BIGINT PRIMARY KEY,
        error_message TEXT NOT NULL,
        stage VARCHAR(50) NOT NULL,
        failed_at TIMESTAMPTZ NOT NULL,
        retry_count INT DEFAULT 0
      )
    `);
    console.log("✅ failed_epochs 表格檢查完成");

    // Setup Redis subscription
    await setupRedisSubscription();

    // Start the three Genkit flows concurrently
    console.log("Starting Genkit flows: UpLine, DownLine, GapLine...");
    
    // These flows contain infinite loops (while(true)), so they will block.
    // In a real-world Genkit application, these would likely be triggered by a scheduler
    // or a Genkit server, not directly called in a blocking manner like this.
    // For now, we'll just start them and let them run.
    Promise.all([
      gk_up_line_flow({}),
      gk_down_line_flow({}),
      gk_gap_line_flow({}),
    ]).catch(err => {
      console.error("❌ One of the Genkit flows failed:", err);
      process.exit(1);
    });

    console.log("Genkit flows initiated. Orchestrator will remain active.");

    // Graceful shutdown
    process.on('SIGTERM', async () => {
      console.log('📴 Received SIGTERM, shutting down gracefully...');
      if (redisSubscriber) await redisSubscriber.quit();
      if (redisPublisher) await redisPublisher.quit();
      await pgPool.end();
      process.exit(0);
    });

    process.on('SIGINT', async () => {
      console.log('📴 Received SIGINT, shutting down gracefully...');
      if (redisSubscriber) await redisSubscriber.quit();
      if (redisPublisher) await redisPublisher.quit();
      await pgPool.end();
      process.exit(0);
    });

  } catch (err) {
    console.error(`❌ Orchestrator startup failed: ${err.message}`);
    process.exit(1);
  }
}

// Export flows for Genkit CLI and start orchestrator if run directly
module.exports = {
  gk_sync_epoch_flow,
  gk_up_line_flow,
  gk_down_line_flow,
  gk_gap_line_flow,
  startHisbetOrchestrator,
};

// If this script is run directly (e.g., `node gk_hisbet.js`)
if (require.main === module) {
  startHisbetOrchestrator();
}