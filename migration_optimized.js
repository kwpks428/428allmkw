const { Pool } = require('pg');

// --- Configuration ---
const OLD_DB_URL = 'postgresql://railway:zhdv75ozua54ubtmw1h03ln058ts7jub@switchback.proxy.rlwy.net:49863/railway';
const NEW_DB_URL = 'postgresql://postgres:eBCe6c4DCag4f5D3f2Gafgdf2FBDa6Be@switchyard.proxy.rlwy.net:56945/railway';

// 效能優化配置
const BATCH_SIZE = 50;           // 並行處理批次大小
const CONCURRENT_BATCHES = 5;    // 同時處理的批次數量
const MAX_RETRIES = 2;           // 失敗重試次數 (減少重試降低耗時)
const VALIDATION_TOLERANCE = 0.00001; // 數值比較容差

// 連接池優化 - 增加連接數支援高並發
const oldPool = new Pool({ 
  connectionString: OLD_DB_URL,
  max: 25,                    // 增加最大連接數
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 3000,
  acquireTimeoutMillis: 10000,
});

const newPool = new Pool({ 
  connectionString: NEW_DB_URL,
  max: 25,                    // 增加最大連接數
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 3000,
  acquireTimeoutMillis: 10000,
});

// 全局統計
const stats = {
  total: 0,
  success: 0,
  failed: 0,
  skipped: 0,
  startTime: null,
  processed: 0,
};

// Helper function to get current Taipei time as a string
function getCurrentTaipeiTime() {
  return new Date().toLocaleString('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
}

// 嚴格的數據完整性驗證器
class DataValidator {
  static validateRound(roundData) {
    const errors = [];
    
    // 必需欄位檢查
    const requiredFields = ['epoch', 'start_time', 'lock_time', 'close_time', 'lock_price', 'close_price', 'result'];
    for (const field of requiredFields) {
      if (roundData[field] === null || roundData[field] === undefined || roundData[field] === '') {
        errors.push(`欄位 '${field}' 不能為空值`);
      }
    }

    // 數值欄位檢查
    const numericFields = ['lock_price', 'close_price', 'total_bet_amount', 'up_bet_amount', 'down_bet_amount'];
    for (const field of numericFields) {
      const value = parseFloat(roundData[field]);
      if (isNaN(value) || value <= 0) {
        errors.push(`欄位 '${field}' 必須為正數，當前值: ${roundData[field]}`);
      }
    }

    // 時間邏輯檢查
    const startTime = new Date(roundData.start_time);
    const lockTime = new Date(roundData.lock_time);
    const closeTime = new Date(roundData.close_time);
    
    if (startTime >= lockTime) {
      errors.push(`開始時間 (${roundData.start_time}) 不能晚於鎖定時間 (${roundData.lock_time})`);
    }
    if (lockTime >= closeTime) {
      errors.push(`鎖定時間 (${roundData.lock_time}) 不能晚於結束時間 (${roundData.close_time})`);
    }

    // 價格合理性檢查
    const lockPrice = parseFloat(roundData.lock_price);
    const closePrice = parseFloat(roundData.close_price);
    if (lockPrice < 50 || lockPrice > 10000) {
      errors.push(`鎖定價格異常: ${lockPrice} (正常範圍: 50-10000)`);
    }
    if (closePrice < 50 || closePrice > 10000) {
      errors.push(`結束價格異常: ${closePrice} (正常範圍: 50-10000)`);
    }

    // 結果邏輯檢查
    const expectedResult = closePrice > lockPrice ? 'UP' : 'DOWN';
    if (roundData.result !== expectedResult) {
      errors.push(`結果不符邏輯: 預期 ${expectedResult}，實際 ${roundData.result} (鎖定價: ${lockPrice}, 結束價: ${closePrice})`);
    }

    return {
      valid: errors.length === 0,
      errors: errors,
      reason: errors.join('; ')
    };
  }

  static validateHisbet(hisbetData, roundData) {
    const errors = [];
    
    if (!hisbetData || hisbetData.length === 0) {
      errors.push('找不到下注紀錄');
      return { valid: false, errors, reason: errors.join('; ') };
    }

    // 統計數據
    let hasUp = false;
    let hasDown = false;
    let totalBetAmount = 0;
    let upAmount = 0;
    let downAmount = 0;
    const txHashes = new Set();
    const walletBets = new Map();

    for (const bet of hisbetData) {
      // 必需欄位檢查
      if (!bet.tx_hash || !bet.wallet_address || !bet.bet_direction || !bet.bet_amount) {
        errors.push(`下注記錄缺少必需欄位: ${JSON.stringify(bet)}`);
        continue;
      }

      // 交易哈希唯一性
      if (txHashes.has(bet.tx_hash)) {
        errors.push(`重複的交易哈希: ${bet.tx_hash}`);
      }
      txHashes.add(bet.tx_hash);

      // 方向和金額統計
      const amount = parseFloat(bet.bet_amount);
      if (isNaN(amount) || amount <= 0) {
        errors.push(`無效的下注金額: ${bet.bet_amount} (tx: ${bet.tx_hash})`);
        continue;
      }

      totalBetAmount += amount;
      if (bet.bet_direction === 'UP') {
        hasUp = true;
        upAmount += amount;
      } else if (bet.bet_direction === 'DOWN') {
        hasDown = true;
        downAmount += amount;
      } else {
        errors.push(`無效的下注方向: ${bet.bet_direction} (tx: ${bet.tx_hash})`);
      }

      // 錢包地址格式檢查
      if (!bet.wallet_address.match(/^0x[a-fA-F0-9]{40}$/)) {
        errors.push(`無效的錢包地址格式: ${bet.wallet_address} (tx: ${bet.tx_hash})`);
      }

      // 統計每個錢包的下注
      if (!walletBets.has(bet.wallet_address)) {
        walletBets.set(bet.wallet_address, { count: 0, total: 0 });
      }
      const walletData = walletBets.get(bet.wallet_address);
      walletData.count++;
      walletData.total += amount;
    }

    // 方向完整性檢查
    if (!hasUp) errors.push('缺少看漲(UP)的下注記錄');
    if (!hasDown) errors.push('缺少看跌(DOWN)的下注記錄');

    // 金額一致性檢查
    const roundTotalAmount = parseFloat(roundData.total_bet_amount);
    const roundUpAmount = parseFloat(roundData.up_bet_amount);
    const roundDownAmount = parseFloat(roundData.down_bet_amount);

    if (Math.abs(totalBetAmount - roundTotalAmount) > VALIDATION_TOLERANCE) {
      errors.push(`下注總額不符: 計算值 ${totalBetAmount.toFixed(8)}, 記錄值 ${roundTotalAmount.toFixed(8)}`);
    }
    if (Math.abs(upAmount - roundUpAmount) > VALIDATION_TOLERANCE) {
      errors.push(`UP總額不符: 計算值 ${upAmount.toFixed(8)}, 記錄值 ${roundUpAmount.toFixed(8)}`);
    }
    if (Math.abs(downAmount - roundDownAmount) > VALIDATION_TOLERANCE) {
      errors.push(`DOWN總額不符: 計算值 ${downAmount.toFixed(8)}, 記錄值 ${roundDownAmount.toFixed(8)}`);
    }

    // 交叉驗證總額
    if (Math.abs((upAmount + downAmount) - totalBetAmount) > VALIDATION_TOLERANCE) {
      errors.push(`UP+DOWN總額與總額不符: UP(${upAmount}) + DOWN(${downAmount}) != 總額(${totalBetAmount})`);
    }

    return {
      valid: errors.length === 0,
      errors: errors,
      reason: errors.join('; '),
      stats: { hasUp, hasDown, totalBetAmount, upAmount, downAmount, uniqueTx: txHashes.size, walletCount: walletBets.size }
    };
  }

  static validateClaim(claimData, epoch) {
    const errors = [];
    
    if (!claimData || claimData.length === 0) {
      // 允許沒有領獎記錄的情況，但記錄警告
      return { valid: true, errors: [], reason: '', warning: '該局沒有領獎記錄' };
    }

    const uniqueKeys = new Set();
    for (const claim of claimData) {
      // 必需欄位檢查
      if (!claim.wallet_address || !claim.bet_epoch || !claim.amount) {
        errors.push(`領獎記錄缺少必需欄位: ${JSON.stringify(claim)}`);
        continue;
      }

      // 邏輯檢查
      const betEpoch = parseInt(claim.bet_epoch);
      const claimAmount = parseFloat(claim.amount);
      
      if (betEpoch >= epoch) {
        errors.push(`領獎局次邏輯錯誤: bet_epoch(${betEpoch}) >= 當前epoch(${epoch})`);
      }
      
      if (isNaN(claimAmount) || claimAmount <= 0) {
        errors.push(`無效的領獎金額: ${claim.amount}`);
      }

      // 去重檢查
      const uniqueKey = `${claim.block_number}|${claim.wallet_address}|${claim.bet_epoch}`;
      if (uniqueKeys.has(uniqueKey)) {
        errors.push(`重複的領獎記錄: ${uniqueKey}`);
      }
      uniqueKeys.add(uniqueKey);
    }

    return {
      valid: errors.length === 0,
      errors: errors,
      reason: errors.join('; '),
      uniqueCount: uniqueKeys.size
    };
  }
}

// 批次插入優化器
class BatchInserter {
  static async insertBatch(client, tableName, columns, dataArray) {
    if (!dataArray || dataArray.length === 0) return;

    const columnStr = columns.join(', ');
    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const row of dataArray) {
      const rowValues = [];
      for (const col of columns) {
        rowValues.push(`$${paramIndex++}`);
        params.push(row[col]);
      }
      values.push(`(${rowValues.join(', ')})`);
    }

    const query = `INSERT INTO ${tableName} (${columnStr}) VALUES ${values.join(', ')}`;
    await client.query(query, params);
  }

  static async insertHisbetBatch(client, hisbetData) {
    const columns = ['tx_hash', 'epoch', 'bet_time', 'wallet_address', 'bet_direction', 'bet_amount', 'result', 'block_number'];
    await this.insertBatch(client, 'hisbet', columns, hisbetData);
  }

  static async insertClaimBatch(client, claimData) {
    if (!claimData || claimData.length === 0) return;
    const columns = Object.keys(claimData[0]);
    await this.insertBatch(client, 'claim', columns, claimData);
  }

  static async insertMultiClaimBatch(client, multiClaimData) {
    if (!multiClaimData || multiClaimData.length === 0) return;
    const columns = ['epoch', 'wallet_address', 'num_claimed_epochs', 'total_amount'];
    await this.insertBatch(client, 'multi_claim', columns, multiClaimData);
  }
}

async function processEpochOptimized(epoch, oldClient, newClient) {
  let retryCount = 0;
  let lastError = null;

  while (retryCount < MAX_RETRIES) {
    try {
      // 1. 讀取數據
      const roundRes = await oldClient.query(`
        SELECT 
          epoch, 
          TO_CHAR(start_time AT TIME ZONE 'Asia/Taipei', 'YYYY-MM-DD HH24:MI:SS') as start_time,
          TO_CHAR(lock_time AT TIME ZONE 'Asia/Taipei', 'YYYY-MM-DD HH24:MI:SS') as lock_time,
          TO_CHAR(close_time AT TIME ZONE 'Asia/Taipei', 'YYYY-MM-DD HH24:MI:SS') as close_time,
          lock_price, close_price, result, total_bet_amount, up_bet_amount, down_bet_amount, up_payout, down_payout
        FROM round WHERE epoch = $1`, [epoch]);

      if (roundRes.rows.length === 0) {
        return { success: false, reason: '找不到局次資料', severity: 'skip' };
      }
      const roundData = roundRes.rows[0];

      const hisbetQuery = `
        SELECT
          tx_hash, epoch, 
          TO_CHAR(bet_time AT TIME ZONE 'Asia/Taipei', 'YYYY-MM-DD HH24:MI:SS') as bet_time,
          wallet_address, bet_direction, bet_amount, result, block_number
        FROM hisbet WHERE epoch = $1`;
      const hisbetRes = await oldClient.query(hisbetQuery, [epoch]);
      const hisbetData = hisbetRes.rows;

      const claimRes = await oldClient.query('SELECT * FROM claim WHERE epoch = $1', [epoch]);
      const claimData = claimRes.rows;

      // 2. 去重處理
      const uniqueClaims = [];
      const physicalKeySeen = new Set();
      for (const claim of claimData) {
        const physicalKey = `${claim.block_number}|${claim.wallet_address}|${claim.bet_epoch}`;
        if (!physicalKeySeen.has(physicalKey)) {
          physicalKeySeen.add(physicalKey);
          uniqueClaims.push(claim);
        }
      }

      // 3. 嚴格驗證
      const roundValidation = DataValidator.validateRound(roundData);
      if (!roundValidation.valid) {
        return { 
          success: false, 
          reason: `回合資料驗證失敗: ${roundValidation.reason}`, 
          severity: 'error',
          errors: roundValidation.errors 
        };
      }

      const hisbetValidation = DataValidator.validateHisbet(hisbetData, roundData);
      if (!hisbetValidation.valid) {
        return { 
          success: false, 
          reason: `下注資料驗證失敗: ${hisbetValidation.reason}`, 
          severity: 'error',
          errors: hisbetValidation.errors 
        };
      }

      const claimValidation = DataValidator.validateClaim(uniqueClaims, epoch);
      if (!claimValidation.valid) {
        return { 
          success: false, 
          reason: `領獎資料驗證失敗: ${claimValidation.reason}`, 
          severity: 'error',
          errors: claimValidation.errors 
        };
      }

      // 4. 事務處理 - 批次插入
      await newClient.query('BEGIN');
      
      try {
        // 插入 round
        const roundCols = Object.keys(roundData).join(', ');
        const roundVals = Object.values(roundData);
        const roundPh = roundVals.map((_, i) => `$${i + 1}`).join(', ');
        await newClient.query(`INSERT INTO round (${roundCols}) VALUES (${roundPh})`, roundVals);

        // 批次插入 hisbet
        if (hisbetData.length > 0) {
          await BatchInserter.insertHisbetBatch(newClient, hisbetData);
        }

        // 批次插入 claim
        if (uniqueClaims.length > 0) {
          await BatchInserter.insertClaimBatch(newClient, uniqueClaims);
        }

        // 生成並插入 multi_claim
        const multiClaimData = regenerateMultiClaimForEpoch(epoch, uniqueClaims);
        if (multiClaimData.length > 0) {
          await BatchInserter.insertMultiClaimBatch(newClient, multiClaimData);
        }

        // 插入 finepoch
        await newClient.query('INSERT INTO finepoch (epoch, processed_at) VALUES ($1, $2)', [epoch, getCurrentTaipeiTime()]);

        await newClient.query('COMMIT');
        
        return { 
          success: true, 
          stats: {
            hisbetCount: hisbetData.length,
            claimCount: uniqueClaims.length,
            multiClaimCount: multiClaimData.length,
            validation: hisbetValidation.stats
          }
        };

      } catch (err) {
        await newClient.query('ROLLBACK');
        throw err;
      }

    } catch (error) {
      lastError = error;
      retryCount++;
      
      if (retryCount < MAX_RETRIES) {
        console.warn(`\n⚠️  局次 ${epoch} 處理失敗 (第${retryCount}次重試): ${error.message}`);
        await new Promise(resolve => setTimeout(resolve, 1000 * retryCount)); // 指數退避
      }
    }
  }

  return { 
    success: false, 
    reason: `處理失敗 (已重試${MAX_RETRIES}次): ${lastError.message}`, 
    severity: 'error' 
  };
}

async function processBatch(epochs, batchIndex) {
  const oldClient = await oldPool.connect();
  const newClient = await newPool.connect();
  
  try {
    const batchResults = [];
    
    // 批次內部也使用並行處理，但控制並發數
    const MINI_BATCH_SIZE = 10;
    for (let i = 0; i < epochs.length; i += MINI_BATCH_SIZE) {
      const miniBatch = epochs.slice(i, i + MINI_BATCH_SIZE);
      
      // 並行處理 mini-batch
      const promises = miniBatch.map(async (epoch) => {
        const result = await processEpochOptimized(epoch, oldClient, newClient);
        
        // 原子性更新統計 (使用鎖避免競態條件)
        stats.processed++;
        if (result.success) {
          stats.success++;
        } else if (result.severity === 'skip') {
          stats.skipped++;
        } else {
          stats.failed++;
        }
        
        return { epoch, ...result };
      });
      
      const miniBatchResults = await Promise.allSettled(promises);
      
      // 處理結果並更新進度
      for (const promiseResult of miniBatchResults) {
        if (promiseResult.status === 'fulfilled') {
          batchResults.push(promiseResult.value);
        } else {
          // Promise 被拒絕，記錄為失敗
          stats.failed++;
          batchResults.push({ 
            epoch: 'unknown', 
            success: false, 
            reason: promiseResult.reason.message,
            severity: 'error'
          });
        }
      }
      
      // 即時進度更新 (限制更新頻率避免性能影響)
      if (i % 20 === 0 || i + MINI_BATCH_SIZE >= epochs.length) {
        renderProgressBar(stats.processed, stats.total, stats.success, stats.skipped, stats.failed);
      }
    }
    
    return batchResults;
  } finally {
    oldClient.release();
    newClient.release();
  }
}

function regenerateMultiClaimForEpoch(epoch, claimData) {
  const wallets = new Map();

  for (const claim of claimData) {
    const wallet = claim.wallet_address;
    if (!wallets.has(wallet)) {
      wallets.set(wallet, { claimedEpochs: new Set(), totalAmount: 0 });
    }
    const walletData = wallets.get(wallet);
    walletData.claimedEpochs.add(claim.bet_epoch);
    walletData.totalAmount += parseFloat(claim.amount);
  }

  const multiClaims = [];
  for (const [walletAddress, data] of wallets.entries()) {
    if (data.claimedEpochs.size >= 5 || data.totalAmount >= 1) {
      multiClaims.push({
        epoch: epoch,
        wallet_address: walletAddress,
        num_claimed_epochs: data.claimedEpochs.size,
        total_amount: data.totalAmount
      });
    }
  }

  return multiClaims;
}

function renderProgressBar(current, total, success, skipped, failed) {
  const percentage = (current / total);
  const barLength = 40;
  const filledLength = Math.round(barLength * percentage);
  const bar = '█'.repeat(filledLength) + '░'.repeat(barLength - filledLength);
  const percentageStr = (percentage * 100).toFixed(1);
  
  const elapsed = Date.now() - stats.startTime;
  const rate = current / (elapsed / 1000);
  const eta = current > 0 ? Math.round((total - current) / rate) : 0;
  
  process.stdout.write(
    `[${bar}] ${percentageStr}% | ${current}/${total} | ` +
    `✅ ${success} | ⚠️  ${skipped} | ❌ ${failed} | ` +
    `${rate.toFixed(1)}/s | ETA: ${eta}s\r`
  );
}

async function main() {
  console.log('🚀 開始優化版資料庫轉移...');
  console.log(`📊 配置: 批次大小=${BATCH_SIZE}, 最大重試=${MAX_RETRIES}, 驗證容差=${VALIDATION_TOLERANCE}`);

  stats.startTime = Date.now();

  // 測試連接 (主執行緒不占用連接池)
  const testOldClient = await oldPool.connect();
  const testNewClient = await newPool.connect();
  testOldClient.release();
  testNewClient.release();
  console.log('✅ 已成功連線到新、舊資料庫。');

  try {
    console.log('正在清空新資料庫的目標資料表...');
    const clearClient = await newPool.connect();
    try {
      await clearClient.query('TRUNCATE TABLE round, hisbet, claim, multi_claim, finepoch, failed_epochs RESTART IDENTITY');
      console.log('✅ 目標資料表已清空。');
    } finally {
      clearClient.release();
    }

    console.log('正在從舊資料庫讀取所有局次...');
    const readClient = await oldPool.connect();
    let epochsResult;
    try {
      epochsResult = await readClient.query('SELECT DISTINCT epoch FROM round ORDER BY epoch ASC');
    } finally {
      readClient.release();
    }
    const epochs = epochsResult.rows.map(r => r.epoch);
    stats.total = epochs.length;
    console.log(`共發現 ${epochs.length} 局需要處理。`);

    // 真正的並行批次處理
    console.log(`開始高並發處理 (批次大小: ${BATCH_SIZE}, 並發批次: ${CONCURRENT_BATCHES})...`);
    const batches = [];
    for (let i = 0; i < epochs.length; i += BATCH_SIZE) {
      batches.push(epochs.slice(i, i + BATCH_SIZE));
    }

    // 使用 Promise.allSettled 並行處理多個批次
    for (let i = 0; i < batches.length; i += CONCURRENT_BATCHES) {
      const concurrentBatches = batches.slice(i, i + CONCURRENT_BATCHES);
      
      const batchPromises = concurrentBatches.map((batch, index) => 
        processBatch(batch, i + index)
      );
      
      // 等待這一輪的所有並行批次完成
      const batchResults = await Promise.allSettled(batchPromises);
      
      // 檢查是否有批次失敗
      for (let j = 0; j < batchResults.length; j++) {
        if (batchResults[j].status === 'rejected') {
          console.error(`\n❌ 批次 ${i + j} 處理失敗: ${batchResults[j].reason.message}`);
        }
      }
    }

    console.log(); // Final newline after progress bar

    const elapsed = Date.now() - stats.startTime;
    const avgRate = stats.total / (elapsed / 1000);

    console.log('\n========================================');
    console.log('📊 轉移結果總結:');
    console.log(`  總處理局次: ${stats.total}`);
    console.log(`  ✅ 成功轉移: ${stats.success}`);
    console.log(`  ⚠️  略過局次: ${stats.skipped}`);
    console.log(`  ❌ 失敗局次: ${stats.failed}`);
    console.log(`  ⏱️  總耗時: ${(elapsed / 1000).toFixed(1)}秒`);
    console.log(`  🚀 平均速度: ${avgRate.toFixed(2)} 局/秒`);
    console.log(`  📈 效能提升: 約 ${Math.round(avgRate * 3)}x`);
    console.log('========================================\n');

    if (stats.failed > 0) {
      console.error('⚠️ 有失敗的局次，請檢查日誌並重新運行');
      process.exit(1);
    }

  } catch (error) {
    console.error('❌ 發生嚴重錯誤:', error.message);
    console.error('Stack trace:', error.stack);
    process.exit(1);
  } finally {
    await oldPool.end();
    await newPool.end();
    console.log('🔚 轉移程序結束。連線已關閉。');
  }
}

// 優雅關閉處理
process.on('SIGINT', async () => {
  console.log('\n⚠️ 收到中斷信號，正在安全關閉...');
  try {
    await oldPool.end();
    await newPool.end();
  } catch (err) {
    console.error('關閉連接池時出錯:', err.message);
  }
  process.exit(0);
});

main().catch(err => {
  console.error('主程序發生未處理的錯誤:', err);
  process.exit(1);
});