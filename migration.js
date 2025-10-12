const { Pool } = require('pg');

// --- Configuration ---
const OLD_DB_URL = 'postgresql://railway:zhdv75ozua54ubtmw1h03ln058ts7jub@switchback.proxy.rlwy.net:49863/railway';
const NEW_DB_URL = 'postgresql://postgres:eBCe6c4DCag4f5D3f2Gafgdf2FBDa6Be@switchyard.proxy.rlwy.net:56945/railway';

const oldPool = new Pool({ connectionString: OLD_DB_URL });
const newPool = new Pool({ connectionString: NEW_DB_URL });

async function main() {
  console.log('🚀 開始資料庫轉移...');

  const oldClient = await oldPool.connect();
  const newClient = await newPool.connect();
  console.log('✅ 已成功連線到新、舊資料庫。');

  try {
    console.log('正在清空新資料庫的目標資料表...');
    await newClient.query('TRUNCATE TABLE round, hisbet, claim, multi_claim, finepoch RESTART IDENTITY');
    console.log('✅ 目標資料表已清空。');

    console.log('正在從舊資料庫讀取所有局次...');
    const epochsResult = await oldClient.query('SELECT DISTINCT epoch FROM round ORDER BY epoch ASC');
    const epochs = epochsResult.rows.map(r => r.epoch);
    console.log(`共發現 ${epochs.length} 局需要處理。`);

    let successCount = 0;
    let skippedCount = 0;

    for (let i = 0; i < epochs.length; i++) {
      const epoch = epochs[i];
      try {
        const { success, reason } = await processEpoch(epoch, oldClient, newClient);
        if (success) {
          successCount++;
        } else {
          skippedCount++;
          // console.warn(`- 局次 ${epoch}: 已略過。原因: ${reason}`);
        }

        renderProgressBar(i + 1, epochs.length, successCount, skippedCount);

      } catch (epochError) {
        skippedCount++;
        console.error(`
- 局次 ${epoch}: 發生嚴重錯誤而失敗: ${epochError.message}`);
      }
    }

    console.log(); // Final newline after progress bar

    console.log('\n----------------------------------------');
    console.log('📊 轉移結果總結:');
    console.log(`  總處理局次: ${epochs.length}`);
    console.log(`  ✅ 成功轉移: ${successCount}`);
    console.log(`  ❌ 略過/失敗: ${skippedCount}`);
    console.log('----------------------------------------\n');

  } catch (error) {
    console.error('❌ 發生嚴重錯誤:', error.message);
  } finally {
    await oldClient.release();
    await newClient.release();
    await oldPool.end();
    await newPool.end();
    console.log('🔚 轉移程序結束。連線已關閉。');
  }
}

async function processEpoch(epoch, oldClient, newClient) {
  const roundRes = await oldClient.query('SELECT * FROM round WHERE epoch = $1', [epoch]);
  if (roundRes.rows.length === 0) {
    return { success: false, reason: '找不到局次資料。' };
  }
  const roundData = roundRes.rows[0];

  const hisbetRes = await oldClient.query('SELECT * FROM hisbet WHERE epoch = $1', [epoch]);
  const hisbetData = hisbetRes.rows;

  const claimRes = await oldClient.query('SELECT * FROM claim WHERE epoch = $1', [epoch]);
  const claimData = claimRes.rows;

  // De-duplicate claim data based on the PHYSICAL PRIMARY KEY to prevent DB errors.
  const uniqueClaims = [];
  const physicalKeySeen = new Set();
  for (const claim of claimData) {
    const physicalKey = `${claim.block_number}|${claim.wallet_address}|${claim.bet_epoch}`;
    if (!physicalKeySeen.has(physicalKey)) {
      physicalKeySeen.add(physicalKey);
      uniqueClaims.push(claim);
    }
  }

  const roundValidation = validateRound(roundData);
  if (!roundValidation.valid) {
    return { success: false, reason: `回合資料無效: ${roundValidation.reason}` };
  }

  const hisbetValidation = validateHisbet(hisbetData, roundData);
  if (!hisbetValidation.valid) {
    return { success: false, reason: `下注資料無效: ${hisbetValidation.reason}` };
  }

  const claimValidation = validateClaim(uniqueClaims);
  if (!claimValidation.valid) {
    return { success: false, reason: `領獎資料無效: ${claimValidation.reason}` };
  }

  const formattedRoundData = {
    ...roundData,
    start_time: toTaipeiTimeString(roundData.start_time),
    lock_time: toTaipeiTimeString(roundData.lock_time),
    close_time: toTaipeiTimeString(roundData.close_time),
  };

  const formattedHisbetData = hisbetData.map(bet => ({
    ...bet,
    bet_time: toTaipeiTimeString(bet.bet_time),
  }));

  try {
    await newClient.query('BEGIN');

    const roundCols = Object.keys(formattedRoundData).join(', ');
    const roundVals = Object.values(formattedRoundData);
    const roundPh = roundVals.map((_, i) => `$${i + 1}`).join(', ');
    await newClient.query(`INSERT INTO round (${roundCols}) VALUES (${roundPh})`, roundVals);

    if (formattedHisbetData.length > 0) {
      for (const bet of formattedHisbetData) {
        const betCols = Object.keys(bet).join(', ');
        const betVals = Object.values(bet);
        const betPh = betVals.map((_, i) => `$${i + 1}`).join(', ');
        await newClient.query(`INSERT INTO hisbet (${betCols}) VALUES (${betPh})`, betVals);
      }
    }

    if (uniqueClaims.length > 0) {
      for (const claim of uniqueClaims) {
        const claimCols = Object.keys(claim).join(', ');
        const claimVals = Object.values(claim);
        const claimPh = claimVals.map((_, i) => `$${i + 1}`).join(', ');
        await newClient.query(`INSERT INTO claim (${claimCols}) VALUES (${claimPh})`, claimVals);
      }
    }

    const multiClaimData = regenerateMultiClaimForEpoch(epoch, uniqueClaims);
    if (multiClaimData.length > 0) {
      for (const mc of multiClaimData) {
        const mcCols = Object.keys(mc).join(', ');
        const mcVals = Object.values(mc);
        const mcPh = mcVals.map((_, i) => `$${i + 1}`).join(', ');
        await newClient.query(`INSERT INTO multi_claim (${mcCols}) VALUES (${mcPh})`, mcVals);
      }
    }

    await newClient.query('INSERT INTO finepoch (epoch, processed_at) VALUES ($1, $2)', [epoch, toTaipeiTimeString(new Date())]);

    await newClient.query('COMMIT');
    return { success: true };

  } catch (err) {
    await newClient.query('ROLLBACK');
    throw new Error(`局次 ${epoch} 的事務處理失敗: ${err.message}`);
  }
}

function validateRound(roundData) {
  const fieldsToCheck = ['lock_price', 'close_price', 'total_bet_amount', 'up_bet_amount', 'down_bet_amount'];
  for (const field of fieldsToCheck) {
    if (roundData[field] === null || +roundData[field] === 0) {
      return { valid: false, reason: `欄位 '${field}' 為空值或零。` };
    }
  }
  return { valid: true };
}

function validateHisbet(hisbetData, roundData) {
  if (hisbetData.length === 0) {
    return { valid: false, reason: '找不到下注紀錄。' };
  }

  let hasUp = false;
  let hasDown = false;
  let totalBetAmount = 0;

  for (const bet of hisbetData) {
    if (bet.bet_direction === 'UP') hasUp = true;
    if (bet.bet_direction === 'DOWN') hasDown = true;
    totalBetAmount += parseFloat(bet.bet_amount);
  }

  if (!hasUp || !hasDown) {
    return { valid: false, reason: '缺少看漲或看跌的下注紀錄。' };
  }

  if (Math.abs(totalBetAmount - parseFloat(roundData.total_bet_amount)) > 0.00001) {
    return { valid: false, reason: `下注總額 (${totalBetAmount}) 與該局總額 (${roundData.total_bet_amount}) 不符。` };
  }

  return { valid: true };
}

function validateClaim(claimData) {
  if (claimData.length === 0) {
    return { valid: false, reason: '找不到該局的領獎紀錄。' };
  }
  return { valid: true };
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

function toTaipeiTimeString(ts) {
  if (ts === null || ts === undefined) return null;

  const date = new Date(ts);
  if (isNaN(date.getTime())) return null;

  return date.toLocaleString('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
}

function renderProgressBar(current, total, success, skipped) {
  const percentage = (current / total);
  const barLength = 30;
  const filledLength = Math.round(barLength * percentage);
  const bar = '='.repeat(filledLength) + '-'.repeat(barLength - filledLength);
  const percentageStr = (percentage * 100).toFixed(2);

  process.stdout.write(`[${bar}] ${percentageStr}% | ${current}/${total} | ✅ 成功: ${success} | ❌ 略過: ${skipped}\r`);
}

main().catch(err => console.error('主程序發生未處理的錯誤:', err));
