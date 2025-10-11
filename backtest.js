/**
 * 回測服務 - 計算各種策略的歷史勝率
 */

const dotenv = require("dotenv");
dotenv.config();

const { Pool } = require("pg");
const Redis = require("ioredis");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

const redis = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');

/**
 * 策略1：跟隨最高勝率錢包
 * 從 wallet_analysis 找最高勝率，再hisbet/realbet找下註方向
 */
async function backtestFollowBest(startEpoch, rounds = 48) {
  const results = {
    strategy: 'follow_best',
    total: 0,
    wins: 0,
    losses: 0,
    winRate: 0,
    history: []
  };

  for (let i = 0; i < rounds; i++) {
    const epoch = startEpoch - i;

    // 步駟1: 從 wallet_analysis 找出該局勝率最高的錢包
    const walletQuery = await pool.query(`
      SELECT wallet_address, short_12_win_rate, mid_48_win_rate
      FROM wallet_analysis
      WHERE epoch = $1 AND short_12_win_rate IS NOT NULL
      ORDER BY short_12_win_rate DESC, mid_48_win_rate DESC
      LIMIT 1
    `, [epoch]);

    if (walletQuery.rows.length === 0) {
      results.history.push({
        epoch,
        prediction: null,
        result: null,
        isWin: null
      });
      continue;
    }

    const wallet = walletQuery.rows[0].wallet_address;

    // 步駟2: 查該錢包在該局的下註方向和結果（先查 hisbet，再查 realbet）
    let betQuery = await pool.query(`
      SELECT bet_direction, result FROM hisbet 
      WHERE epoch = $1 AND wallet_address = $2 
      ORDER BY bet_time DESC LIMIT 1
    `, [epoch, wallet]);

    let actualResult = null;
    if (betQuery.rows.length === 0) {
      // hisbet 沒有，查 realbet（realbet 沒有 result 欄位，因為還沒結束）
      betQuery = await pool.query(`
        SELECT bet_direction FROM realbet 
        WHERE epoch = $1 AND wallet_address = $2 
        ORDER BY bet_time DESC LIMIT 1
      `, [epoch, wallet]);
      actualResult = null; // realbet 中的局次還沒結果
    } else {
      // 從 hisbet 的 result 推算該局的真實結果
      const bet = betQuery.rows[0];
      if (bet.result === 'WIN') {
        actualResult = bet.bet_direction; // 他壓對了，結果就是他壓的方向
      } else if (bet.result === 'LOSS') {
        actualResult = bet.bet_direction === 'UP' ? 'DOWN' : 'UP'; // 他壓錯了，結果是反向
      }
    }

    if (betQuery.rows.length === 0) {
      results.history.push({
        epoch,
        prediction: null,
        result: actualResult,
        isWin: null
      });
      continue;
    }

    const prediction = betQuery.rows[0].bet_direction;
    const isWin = actualResult ? (prediction === actualResult) : null;

    if (actualResult && isWin !== null) {
      results.total++;
      if (isWin) results.wins++;
      else results.losses++;
    }

    results.history.push({
      epoch,
      prediction,
      result: actualResult,
      isWin
    });
  }

  results.winRate = results.total > 0 ? (results.wins / results.total * 100).toFixed(2) : 0;
  return results;
}

/**
 * 策略2：反向最低勝率錢包
 */
async function backtestReverseLow(startEpoch, rounds = 48) {
  const results = {
    strategy: 'reverse_low',
    total: 0,
    wins: 0,
    losses: 0,
    winRate: 0,
    history: []
  };

  for (let i = 0; i < rounds; i++) {
    const epoch = startEpoch - i;

    // 步駟1: 從 wallet_analysis 找出該局勝率最低的錢包
    const walletQuery = await pool.query(`
      SELECT wallet_address, short_12_win_rate, mid_48_win_rate
      FROM wallet_analysis
      WHERE epoch = $1 AND short_12_win_rate IS NOT NULL
      ORDER BY short_12_win_rate ASC, mid_48_win_rate ASC
      LIMIT 1
    `, [epoch]);

    if (walletQuery.rows.length === 0) {
      results.history.push({
        epoch,
        prediction: null,
        result: null,
        isWin: null
      });
      continue;
    }

    const wallet = walletQuery.rows[0].wallet_address;

    // 步駟2: 查該錢包在該局的下註方向和結果
    let betQuery = await pool.query(`
      SELECT bet_direction, result FROM hisbet 
      WHERE epoch = $1 AND wallet_address = $2 
      ORDER BY bet_time DESC LIMIT 1
    `, [epoch, wallet]);

    let actualResult = null;
    if (betQuery.rows.length === 0) {
      betQuery = await pool.query(`
        SELECT bet_direction FROM realbet 
        WHERE epoch = $1 AND wallet_address = $2 
        ORDER BY bet_time DESC LIMIT 1
      `, [epoch, wallet]);
      actualResult = null;
    } else {
      const bet = betQuery.rows[0];
      if (bet.result === 'WIN') {
        actualResult = bet.bet_direction;
      } else if (bet.result === 'LOSS') {
        actualResult = bet.bet_direction === 'UP' ? 'DOWN' : 'UP';
      }
    }

    if (betQuery.rows.length === 0) {
      results.history.push({
        epoch,
        prediction: null,
        result: actualResult,
        isWin: null
      });
      continue;
    }

    const originalDirection = betQuery.rows[0].bet_direction;
    const reversePrediction = originalDirection === 'UP' ? 'DOWN' : 'UP';
    const isWin = actualResult ? (reversePrediction === actualResult) : null;

    if (actualResult && isWin !== null) {
      results.total++;
      if (isWin) results.wins++;
      else results.losses++;
    }

    results.history.push({
      epoch,
      prediction: reversePrediction,
      result: actualResult,
      isWin
    });
  }

  results.winRate = results.total > 0 ? (results.wins / results.total * 100).toFixed(2) : 0;
  return results;
}

/**
 * 策略3：時間序列動量策略
 * 分析連續趨勢、資金流動、價格動量等時間序列特徵
 */
async function backtestMomentumStrategy(startEpoch, rounds = 48) {
  const results = {
    strategy: 'momentum',
    total: 0,
    wins: 0,
    losses: 0,
    winRate: 0,
    history: []
  };

  for (let i = 0; i < rounds; i++) {
    const epoch = startEpoch - i;

    try {
      // 獲取該局的基本信息
      const currentRound = await pool.query(`
        SELECT result, up_bet_amount, down_bet_amount, total_bet_amount, lock_price, close_price
        FROM round WHERE epoch = $1
      `, [epoch]);

      if (currentRound.rows.length === 0) {
        results.history.push({
          epoch,
          prediction: null,
          result: null,
          isWin: null
        });
        continue;
      }
      const round = currentRound.rows[0];

      // 分析最近5局的趨勢數據
      const trendAnalysis = await pool.query(`
        SELECT 
          epoch,
          result,
          up_bet_amount,
          down_bet_amount,
          total_bet_amount,
          (up_bet_amount::float / NULLIF(total_bet_amount::float, 0)) as up_ratio,
          CASE WHEN close_price > 0 AND lock_price > 0 
               THEN (close_price::float - lock_price::float) / lock_price::float 
               ELSE 0 END as price_change
        FROM round 
        WHERE epoch BETWEEN $1 AND $2 AND result IS NOT NULL
        ORDER BY epoch DESC
      `, [epoch - 5, epoch - 1]);

      if (trendAnalysis.rows.length < 3) {
        results.history.push({
          epoch,
          prediction: null,
          result: round.result,
          isWin: null
        });
        continue;
      }

      // 計算動量指標
      const prediction = calculateMomentumPrediction(trendAnalysis.rows, round);
      const actualResult = round.result;
      const isWin = (prediction === actualResult);

      results.total++;
      if (isWin) {
        results.wins++;
      } else {
        results.losses++;
      }

      // 記錄歷史
      results.history.push({
        epoch,
        prediction,
        result: actualResult,
        isWin
      });

    } catch (error) {
      console.error(`動量策略計算錯誤 epoch ${epoch}:`, error.message);
      results.history.push({
        epoch,
        prediction: null,
        result: null,
        isWin: null
      });
      continue;
    }
  }

  results.winRate = results.total > 0 ? (results.wins / results.total * 100).toFixed(2) : 0;
  return results;
}

/**
 * 時間軸調整版本 - 跟隨最高勝率策略
 * 直接產生 50 局數據：N, N-1, N-2, ..., N-49
 */
async function backtestFollowBestWithTimeline(currentEpoch) {
  // 直接產生 50 局（從 currentEpoch 開始往前推）
  const results = await backtestFollowBest(currentEpoch, 50);
  return results;
}

/**
 * 時間軸調整版本 - 反向最低勝率策略  
 */
async function backtestReverseLowWithTimeline(currentEpoch) {
  const results = await backtestReverseLow(currentEpoch, 50);
  return results;
}

/**
 * 時間軸調整版本 - 動量策略
 */
async function backtestMomentumStrategyWithTimeline(currentEpoch) {
  const results = await backtestMomentumStrategy(currentEpoch, 50);
  return results;
}


/**
 * 計算動量預測
 * 基於多個時間序列特徵進行綜合判斷
 */
function calculateMomentumPrediction(historicalData, currentRound) {
  let upScore = 0;
  let downScore = 0;

  // 特徵1: 連續趨勢分析
  const recentResults = historicalData.map(r => r.result).slice(0, 3);
  const upStreak = recentResults.filter(r => r === 'UP').length;
  const downStreak = recentResults.filter(r => r === 'DOWN').length;
  
  // 趨勢延續 vs 趨勢反轉判斷
  if (upStreak >= 2) {
    // 連續UP後，根據強度決定延續還是反轉
    if (upStreak >= 3) downScore += 2; // 強反轉信號
    else upScore += 1; // 弱延續信號
  }
  if (downStreak >= 2) {
    if (downStreak >= 3) upScore += 2;
    else downScore += 1;
  }

  // 特徵2: 資金流動分析
  const currentUpRatio = parseFloat(currentRound.up_bet_amount) / parseFloat(currentRound.total_bet_amount);
  const historicalUpRatios = historicalData.map(r => parseFloat(r.up_ratio)).filter(r => !isNaN(r));
  
  if (historicalUpRatios.length > 0) {
    const avgHistoricalUpRatio = historicalUpRatios.reduce((a, b) => a + b) / historicalUpRatios.length;
    
    // 當前資金流向與歷史平均的偏差
    const ratioDiff = currentUpRatio - avgHistoricalUpRatio;
    if (Math.abs(ratioDiff) > 0.1) { // 顯著偏差
      if (ratioDiff > 0) upScore += 2;
      else downScore += 2;
    }
  }

  // 特徵3: 下注量動量
  const currentVolume = parseFloat(currentRound.total_bet_amount);
  const historicalVolumes = historicalData.map(r => parseFloat(r.total_bet_amount)).filter(v => v > 0);
  
  if (historicalVolumes.length > 0) {
    const avgHistoricalVolume = historicalVolumes.reduce((a, b) => a + b) / historicalVolumes.length;
    const volumeRatio = currentVolume / avgHistoricalVolume;
    
    // 異常高成交量通常預示方向變化
    if (volumeRatio > 1.5) {
      // 高成交量配合資金流向
      if (currentUpRatio > 0.6) upScore += 1;
      else if (currentUpRatio < 0.4) downScore += 1;
    }
  }

  // 特徵4: 價格動量模式
  const recentPriceChanges = historicalData.map(r => parseFloat(r.price_change)).filter(p => !isNaN(p));
  if (recentPriceChanges.length >= 2) {
    const avgPriceChange = recentPriceChanges.reduce((a, b) => a + b) / recentPriceChanges.length;
    const priceVolatility = calculateVolatility(recentPriceChanges);
    
    // 低波動後的突破
    if (priceVolatility < 0.01) { // 低波動期
      const lastPriceChange = recentPriceChanges[0];
      if (Math.abs(lastPriceChange) > 0.02) { // 突破信號
        if (lastPriceChange > 0) upScore += 2;
        else downScore += 2;
      }
    }
  }

  // 綜合決策 - 總是給出明確預測
  const scoreDiff = upScore - downScore;
  
  // 如果得分相同，使用資金流向作為決定性因素
  if (scoreDiff === 0) {
    const currentUpRatio = parseFloat(currentRound.up_bet_amount) / parseFloat(currentRound.total_bet_amount);
    return currentUpRatio > 0.5 ? 'UP' : 'DOWN';
  }
  
  return scoreDiff > 0 ? 'UP' : 'DOWN';
}

/**
 * 計算波動率
 */
function calculateVolatility(values) {
  if (values.length < 2) return 0;
  
  const mean = values.reduce((a, b) => a + b) / values.length;
  const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
  return Math.sqrt(variance);
}

/**
 * 生成對下一局的實時預測
 */
async function generateRealTimePredictions(currentEpoch) {
  console.log(`🔮 生成局次 ${currentEpoch} 的實時預測`);
  
  const predictions = {
    epoch: currentEpoch,
    timestamp: new Date().toISOString(),
    strategies: {}
  };

  try {
    // 獲取當前局的基本信息（用於動量策略）
    const currentRound = await pool.query(`
      SELECT up_bet_amount, down_bet_amount, total_bet_amount, lock_price, close_price
      FROM round WHERE epoch = $1
    `, [currentEpoch]);

    if (currentRound.rows.length > 0) {
      const round = currentRound.rows[0];
      
      // 分析最近5局的趨勢數據（用於動量策略）
      const trendAnalysis = await pool.query(`
        SELECT 
          epoch, result, up_bet_amount, down_bet_amount, total_bet_amount,
          (up_bet_amount::float / NULLIF(total_bet_amount::float, 0)) as up_ratio,
          CASE WHEN close_price > 0 AND lock_price > 0 
               THEN (close_price::float - lock_price::float) / lock_price::float 
               ELSE 0 END as price_change
        FROM round 
        WHERE epoch BETWEEN $1 AND $2 AND result IS NOT NULL
        ORDER BY epoch DESC
      `, [currentEpoch - 5, currentEpoch - 1]);

      if (trendAnalysis.rows.length >= 3) {
        const momentumPrediction = calculateMomentumPrediction(trendAnalysis.rows, round);
        const upRatio = parseFloat(round.up_bet_amount) / Math.max(1, parseFloat(round.total_bet_amount));
        const ratios = trendAnalysis.rows.map(r => parseFloat(r.up_ratio)).filter(v => !isNaN(v));
        const avgHistUp = ratios.length ? (ratios.reduce((a,b)=>a+b,0)/ratios.length) : 0.5;
        const vols = trendAnalysis.rows.map(r => parseFloat(r.total_bet_amount)).filter(v => v>0);
        const avgVol = vols.length ? (vols.reduce((a,b)=>a+b,0)/vols.length) : 0;
        const volumeRatio = avgVol>0 ? (parseFloat(round.total_bet_amount)/avgVol) : 0;
        const upDiff = upRatio - avgHistUp;
        const reasons = [];
        let score = 0;
        if (Math.abs(upDiff) > 0.1) { reasons.push(`資金偏離 ${upDiff>0?'+':''}${(upDiff*100).toFixed(1)}%`); score += 2; }
        if (volumeRatio >= 1.5) { reasons.push(`成交量放大 x${volumeRatio.toFixed(2)}`); score += 2; }
        else if (volumeRatio >= 1.2) { reasons.push(`成交量偏高 x${volumeRatio.toFixed(2)}`); score += 1; }
        const confidence = score>=3 ? 'high' : 'medium';
        predictions.strategies.momentum = {
          name: '時間序列動量',
          prediction: momentumPrediction,
          confidence,
          score,
          reasons,
          features: {
            upRatio: Number(upRatio.toFixed(4)),
            upRatioDiff: Number(upDiff.toFixed(4)),
            volumeRatio: Number(volumeRatio.toFixed(2))
          }
        };
      }
    }

    // 跟隨最高勝率策略預測
    const followBestQuery = await pool.query(`
      SELECT h.bet_direction, wa.short_12_win_rate
      FROM hisbet h
      JOIN wallet_analysis wa ON wa.wallet_address = h.wallet_address AND wa.epoch = h.epoch
      WHERE h.epoch = $1 AND wa.short_12_win_rate IS NOT NULL
      ORDER BY wa.short_12_win_rate DESC, wa.mid_48_win_rate DESC
      LIMIT 1
    `, [currentEpoch]);

    if (followBestQuery.rows.length > 0) {
      predictions.strategies.follow_best = {
        name: '跟隨最高勝率',
        prediction: followBestQuery.rows[0].bet_direction,
        confidence: 'high'
      };
    }

    // 反向最低勝率策略預測  
    const reverseLowQuery = await pool.query(`
      SELECT h.bet_direction
      FROM hisbet h
      JOIN wallet_analysis wa ON wa.wallet_address = h.wallet_address AND wa.epoch = h.epoch
      WHERE h.epoch = $1 AND wa.short_12_win_rate IS NOT NULL
      ORDER BY wa.short_12_win_rate ASC, wa.mid_48_win_rate ASC
      LIMIT 1
    `, [currentEpoch]);

    if (reverseLowQuery.rows.length > 0) {
      // 反向預測
      const originalDirection = reverseLowQuery.rows[0].bet_direction;
      const reversePrediction = originalDirection === 'UP' ? 'DOWN' : 'UP';
      predictions.strategies.reverse_low = {
        name: '反向最低勝率',
        prediction: reversePrediction,
        confidence: 'medium'
      };
    }

  } catch (error) {
    console.error('❌ 生成實時預測失敗:', error.message);
  }

  console.log(`🔮 實時預測結果: follow_best=${predictions.strategies.follow_best?.prediction || 'N/A'}, reverse_low=${predictions.strategies.reverse_low?.prediction || 'N/A'}, momentum=${predictions.strategies.momentum?.prediction || 'N/A'}`);
  
  // 發布預測到 Redis
  await redis.publish('live_predictions', JSON.stringify(predictions));
  await redis.set('latest_predictions', JSON.stringify(predictions), 'EX', 1800);

  return predictions;
}

/**
 * 動量策略 - 局內動態預測聚合器
 */
const DYNAMIC_CONFIG = {
  publishIntervalMs: 3000,
  minUpRatioDelta: 0.03, // 3%
  volumeBuckets: [1.2, 1.5], // thresholds for volume ratio buckets
  finalAdvanceMs: (() => {
    const v = process.env.FINAL_ADVANCE_MS;
    const n = v ? Number(v) : NaN;
    // 統一在鎖倉前5秒發布最終預測
    return Number.isFinite(n) && n >= 0 ? n : 5000;
  })()
};

let liveAgg = {
  epoch: null,
  up: 0,
  down: 0,
  total: 0,
  lastPublishTs: 0,
  lastUpRatio: null,
  lastVolumeBucket: null,
  version: 0,
  histRows: null,
  avgHistUpRatio: null,
  avgHistVolume: null,
  finalTimer: null,
  series: [] // { t: ms, upRatio: number, total: number }
};

function resetLiveAgg(epoch) {
  if (liveAgg.finalTimer) {
    clearTimeout(liveAgg.finalTimer);
    liveAgg.finalTimer = null;
  }
  liveAgg = {
    epoch,
    up: 0,
    down: 0,
    total: 0,
    lastPublishTs: 0,
    lastUpRatio: null,
    lastVolumeBucket: null,
    version: 0,
    histRows: null,
    avgHistUpRatio: null,
    avgHistVolume: null,
    finalTimer: null,
    series: []
  };
}

async function initEpochContext(epoch) {
  if (liveAgg.epoch !== epoch) resetLiveAgg(epoch);

  // 回補當前局現有下注總額（避免漏掉早期訊息）
  try {
    const sums = await pool.query(`
      SELECT bet_direction, SUM(bet_amount)::float AS sum_amt
      FROM realbet
      WHERE epoch = $1
      GROUP BY bet_direction
    `, [epoch]);
    let up = 0, down = 0;
    for (const r of sums.rows) {
      if (r.bet_direction === 'UP') up = parseFloat(r.sum_amt);
      if (r.bet_direction === 'DOWN') down = parseFloat(r.sum_amt);
    }
    liveAgg.up = up;
    liveAgg.down = down;
    liveAgg.total = up + down;
  } catch (e) {
    console.error('❌ 初始化當局下注總額失敗:', e.message);
  }

  // 取得最近5局的歷史特徵（只用已結束）
  try {
    const trend = await pool.query(`
      SELECT 
        epoch,
        result,
        up_bet_amount,
        down_bet_amount,
        total_bet_amount,
        (up_bet_amount::float / NULLIF(total_bet_amount::float, 0)) as up_ratio,
        CASE WHEN close_price > 0 AND lock_price > 0 
             THEN (close_price::float - lock_price::float) / lock_price::float 
             ELSE 0 END as price_change
      FROM round 
      WHERE epoch BETWEEN $1 AND $2 AND result IS NOT NULL
      ORDER BY epoch DESC
    `, [epoch - 5, epoch - 1]);
    liveAgg.histRows = trend.rows;
    if (trend.rows.length > 0) {
      const ratios = trend.rows.map(r => parseFloat(r.up_ratio)).filter(v => !isNaN(v));
      liveAgg.avgHistUpRatio = ratios.length ? (ratios.reduce((a, b) => a + b, 0) / ratios.length) : 0.5;
      const vols = trend.rows.map(r => parseFloat(r.total_bet_amount)).filter(v => v > 0);
      liveAgg.avgHistVolume = vols.length ? (vols.reduce((a, b) => a + b, 0) / vols.length) : 0;
    } else {
      liveAgg.avgHistUpRatio = 0.5;
      liveAgg.avgHistVolume = 0;
    }
  } catch (e) {
    console.error('❌ 初始化歷史特徵失敗:', e.message);
  }
}

function getVolumeBucket(volumeRatio) {
  if (volumeRatio >= DYNAMIC_CONFIG.volumeBuckets[1]) return 'high';
  if (volumeRatio >= DYNAMIC_CONFIG.volumeBuckets[0]) return 'mid';
  return 'base';
}

function computeSlope(nowMs, windowMs = 8000) {
  // 以最近 windowMs 的 upRatio 變化估算斜率（每秒）
  const cutoff = nowMs - windowMs;
  const pts = (liveAgg.series || []).filter(p => p.t >= cutoff);
  if (pts.length < 2) return 0;
  const first = pts[0];
  const last = pts[pts.length - 1];
  const dt = Math.max(1, (last.t - first.t) / 1000);
  return (last.upRatio - first.upRatio) / dt; // 每秒的 upRatio 變化量
}

async function maybePublishLivePrediction({ force = false, final = false } = {}) {
  if (!liveAgg.epoch) return;
  if (liveAgg.total <= 0) return; // 尚無有效下注

  const now = Date.now();
  if (!force && now - liveAgg.lastPublishTs < DYNAMIC_CONFIG.publishIntervalMs) return;

  const upRatio = liveAgg.total > 0 ? (liveAgg.up / liveAgg.total) : 0.5;
  // 記錄序列用於斜率
  liveAgg.series.push({ t: now, upRatio, total: liveAgg.total });
  if (liveAgg.series.length > 50) liveAgg.series.shift();
  const volumeRatio = (liveAgg.avgHistVolume && liveAgg.avgHistVolume > 0) ? (liveAgg.total / liveAgg.avgHistVolume) : 0;
  const volumeBucket = getVolumeBucket(volumeRatio);

  const crossedMid = (liveAgg.lastUpRatio != null) && ((liveAgg.lastUpRatio < 0.5 && upRatio >= 0.5) || (liveAgg.lastUpRatio > 0.5 && upRatio <= 0.5));
  const movedEnough = (liveAgg.lastUpRatio == null) || (Math.abs(upRatio - liveAgg.lastUpRatio) >= DYNAMIC_CONFIG.minUpRatioDelta);
  const volumeChanged = (liveAgg.lastVolumeBucket == null) || (volumeBucket !== liveAgg.lastVolumeBucket);

  if (!force && !final && !movedEnough && !crossedMid && !volumeChanged) return;

  // 使用聚合值構造當局 round 物件
  const roundLike = {
    up_bet_amount: String(liveAgg.up),
    total_bet_amount: String(liveAgg.total),
    lock_price: '0',
    close_price: '0'
  };

  let prediction = 'UP';
  try {
    const hist = liveAgg.histRows || [];
    if (hist.length >= 3) {
      prediction = calculateMomentumPrediction(hist, roundLike);
    } else {
      prediction = upRatio >= 0.5 ? 'UP' : 'DOWN';
    }
  } catch (e) {
    console.error('❌ 動量計算失敗（局內）:', e.message);
    prediction = upRatio >= 0.5 ? 'UP' : 'DOWN';
  }

  // 進階信心與理由
  const upDiff = upRatio - (liveAgg.avgHistUpRatio ?? 0.5);
  const slope = computeSlope(now, 8000);
  const reasons = [];
  let score = 0;
  if (Math.abs(upDiff) > 0.1) { reasons.push(`資金偏離 ${upDiff>0?'+':''}${(upDiff*100).toFixed(1)}%`); score += 2; }
  if (volumeRatio >= 1.5) { reasons.push(`成交量放大 x${volumeRatio.toFixed(2)}`); score += 2; }
  else if (volumeRatio >= 1.2) { reasons.push(`成交量偏高 x${volumeRatio.toFixed(2)}`); score += 1; }
  if (Math.abs(slope) > 0.04) { reasons.push(`斜率 ${(slope>0?'+':'')}${slope.toFixed(3)}/s`); score += 1; }

  let confidence = 'medium';
  if (score >= 3) confidence = 'high';
  if (liveAgg.total < (liveAgg.avgHistVolume * 0.2)) { reasons.push('樣本偏少'); if (confidence==='high') confidence='medium'; }
  if (final && confidence==='low') confidence = 'medium';

  const predictions = {
    epoch: liveAgg.epoch,
    timestamp: new Date().toISOString(),
    version: ++liveAgg.version,
    final: !!final,
    strategies: {
      momentum: {
        name: '時間序列動量',
        prediction,
        confidence,
        score,
        reasons,
        features: {
          upRatio: Number(upRatio.toFixed(4)),
          upRatioDiff: Number(upDiff.toFixed(4)),
          volumeRatio: Number(volumeRatio.toFixed(2)),
          slope: Number(slope.toFixed(4))
        }
      }
    }
  };

  await redis.publish('live_predictions', JSON.stringify(predictions));
  await redis.set('latest_predictions', JSON.stringify(predictions), 'EX', 1800);

  liveAgg.lastPublishTs = now;
  liveAgg.lastUpRatio = upRatio;
  liveAgg.lastVolumeBucket = volumeBucket;
  console.log(`🔮 已發布局內預測 v${liveAgg.version}（${final ? 'final' : 'live'}）: ${prediction}, upRatio=${upRatio.toFixed(3)}, volR=${volumeRatio.toFixed(2)}`);
}

async function scheduleFinalPrediction(epoch) {
  try {
    const res = await pool.query('SELECT lock_time FROM round WHERE epoch = $1', [epoch]);
    if (!res.rows.length || !res.rows[0].lock_time) return;
    const lockTimeStr = res.rows[0].lock_time;
    const lockMs = new Date(lockTimeStr).getTime();
    if (isNaN(lockMs)) return;
    const now = Date.now();
    const delay = Math.max(0, lockMs - now - DYNAMIC_CONFIG.finalAdvanceMs);
    if (delay < 500) {
      // 太近，稍後強制一次
      setTimeout(() => { maybePublishLivePrediction({ force: true, final: true }).catch(() => {}); }, 500);
      return;
    }
    if (liveAgg.finalTimer) clearTimeout(liveAgg.finalTimer);
    liveAgg.finalTimer = setTimeout(() => {
      maybePublishLivePrediction({ force: true, final: true }).catch(() => {});
    }, delay);
    console.log(`🕒 已排程 final 預測：${(delay/1000).toFixed(1)} 秒後`);
  } catch (e) {
    console.error('❌ 排程 final 預測失敗:', e.message);
  }
}

/**
 * 執行所有策略的回測
 * 時間軸：[當前局] [前1局] [前2-49局統計歷史]
 */
async function runAllBacktests(currentEpoch) {
  console.log(`🔍 回測當前局次: ${currentEpoch}`);

  // 獲取 50 局數據：當前 + 49 局歷史
  const [followBest, reverseLow, momentum] = await Promise.all([
    backtestFollowBestWithTimeline(currentEpoch),
    backtestReverseLowWithTimeline(currentEpoch),
    backtestMomentumStrategyWithTimeline(currentEpoch)
  ]);

  const summary = {
    timestamp: new Date().toISOString(),
    currentEpoch,
    strategies: {
      follow_best: {
        name: '跟隨最高勝率',
        winRate: parseFloat(followBest.winRate),
        wins: followBest.wins,
        total: followBest.total,
        history: followBest.history // 新增：每局詳細記錄
      },
      reverse_low: {
        name: '反向最低勝率',
        winRate: parseFloat(reverseLow.winRate),
        wins: reverseLow.wins,
        total: reverseLow.total,
        history: reverseLow.history
      },
      momentum: {
        name: '時間序列動量',
        winRate: parseFloat(momentum.winRate),
        wins: momentum.wins,
        total: momentum.total,
        history: momentum.history
      }
    }
  };

  console.log(`📊 回測結果: follow_best=${summary.strategies.follow_best.winRate}%, reverse_low=${summary.strategies.reverse_low.winRate}%, momentum=${summary.strategies.momentum.winRate}%, history長度=${summary.strategies.momentum.history.length}`);

  // 發布到 Redis
  await redis.publish('backtest_results', JSON.stringify(summary));
  await redis.set('latest_backtest', JSON.stringify(summary), 'EX', 3600);

  // 同時生成對當前局的實時預測
  await generateRealTimePredictions(currentEpoch);

  return summary;
}

// 啟動服務
(async () => {
  console.log('🚀 回測服務啟動');

  // 訂閱新局次更新
  const subscriber = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');

  subscriber.subscribe('round_update_channel', (err) => {
    if (err) {
      console.error('❌ 訂閱失敗:', err);
    } else {
      console.log('✅ 已訂閱 round_update_channel');
    }
  });
  subscriber.subscribe('instant_bet_channel', (err) => {
    if (err) {
      console.error('❌ 訂閱 instant_bet_channel 失敗:', err);
    } else {
      console.log('✅ 已訂閱 instant_bet_channel');
    }
  });

  let latestProcessedEpoch = null;

  subscriber.on('message', async (channel, message) => {
    if (channel === 'round_update_channel') {
      try {
        const data = JSON.parse(message);
        const epoch = Number(data.epoch);

        // 初始化本局上下文與 final 排程
        await initEpochContext(epoch);
        scheduleFinalPrediction(epoch).catch(() => {});

        // 只處理比當前更新的局次
        if (!latestProcessedEpoch || epoch > latestProcessedEpoch) {
          console.log(`📥 收到新局次: ${epoch}`);
          await runAllBacktests(epoch);
          latestProcessedEpoch = epoch;
        }
      } catch (err) {
        console.error('❌ 處理訊息失敗:', err.message);
      }
    } else if (channel === 'instant_bet_channel') {
      try {
        const payload = JSON.parse(message);
        if (payload.type === 'instant_bet' && payload.data) {
          const bet = payload.data;
          const epoch = Number(bet.epoch);
          if (!liveAgg.epoch || epoch > liveAgg.epoch) {
            await initEpochContext(epoch);
          }
          if (epoch !== liveAgg.epoch) {
            // 舊局次，忽略
            return;
          }
          const amt = parseFloat(bet.bet_amount || bet.amount || '0');
          if (!isNaN(amt) && amt > 0) {
            if (bet.bet_direction === 'UP') liveAgg.up += amt; else if (bet.bet_direction === 'DOWN') liveAgg.down += amt;
            liveAgg.total = liveAgg.up + liveAgg.down;
            // 嘗試依門檻發佈
            await maybePublishLivePrediction();
          }
        }
      } catch (err) {
        console.error('❌ 處理 instant_bet 訊息失敗:', err.message);
      }
    }
  });

  // 啟動時執行一次
  const latestEpoch = await pool.query(`SELECT MAX(epoch) as max FROM hisbet`);
  if (latestEpoch.rows[0]?.max) {
    const epoch = Number(latestEpoch.rows[0].max);
    await runAllBacktests(epoch);
    latestProcessedEpoch = epoch;
  }
})();
