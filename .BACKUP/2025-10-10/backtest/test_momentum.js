const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

async function testMomentumStrategy() {
  try {
    console.log('🧪 測試 momentum 策略...');
    const epoch = 419130;
    
    // 獲取該局的基本信息
    const currentRound = await pool.query(`
      SELECT result, up_bet_amount, down_bet_amount, total_bet_amount, lock_price, close_price
      FROM round WHERE epoch = $1
    `, [epoch]);
    
    if (currentRound.rows.length === 0) {
      console.log('❌ 找不到局次資料');
      return;
    }
    
    const round = currentRound.rows[0];
    console.log('✅ 當前局次資料:', round);
    
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
    
    console.log('✅ 趨勢分析資料筆數:', trendAnalysis.rows.length);
    if (trendAnalysis.rows.length > 0) {
      console.log('前3筆趨勢資料:', trendAnalysis.rows.slice(0, 3));
      
      // 測試動量計算邏輯
      const prediction = calculateMomentumPrediction(trendAnalysis.rows, round);
      console.log('🎯 動量預測結果:', prediction);
      console.log('📊 實際結果:', round.result);
      console.log('✅ 預測正確:', prediction === round.result);
    }
    
  } catch (error) {
    console.error('❌ 錯誤:', error.message);
    console.error(error.stack);
  } finally {
    await pool.end();
  }
}

function calculateMomentumPrediction(historicalData, currentRound) {
  let upScore = 0;
  let downScore = 0;
  
  console.log('🔍 開始動量分析...');

  // 特徵1: 連續趋势分析
  const recentResults = historicalData.map(r => r.result).slice(0, 3);
  console.log('近期結果:', recentResults);
  
  const upStreak = recentResults.filter(r => r === 'UP').length;
  const downStreak = recentResults.filter(r => r === 'DOWN').length;
  
  console.log(`UP連勝: ${upStreak}, DOWN連勝: ${downStreak}`);
  
  // 趨勢延續 vs 趨勢反轉判斷
  if (upStreak >= 2) {
    if (upStreak >= 3) {
      downScore += 2; // 強反轉信號
      console.log('強反轉信號 (DOWN +2)');
    } else {
      upScore += 1; // 弱延續信號
      console.log('弱延續信號 (UP +1)');
    }
  }
  if (downStreak >= 2) {
    if (downStreak >= 3) {
      upScore += 2;
      console.log('強反轉信號 (UP +2)');
    } else {
      downScore += 1;
      console.log('弱延續信號 (DOWN +1)');
    }
  }

  // 特徵2: 資金流動分析
  const currentUpRatio = parseFloat(currentRound.up_bet_amount) / parseFloat(currentRound.total_bet_amount);
  const historicalUpRatios = historicalData.map(r => parseFloat(r.up_ratio)).filter(r => !isNaN(r));
  
  console.log(`當前UP比例: ${currentUpRatio.toFixed(3)}`);
  
  if (historicalUpRatios.length > 0) {
    const avgHistoricalUpRatio = historicalUpRatios.reduce((a, b) => a + b) / historicalUpRatios.length;
    console.log(`歷史平均UP比例: ${avgHistoricalUpRatio.toFixed(3)}`);
    
    const ratioDiff = currentUpRatio - avgHistoricalUpRatio;
    console.log(`比例差異: ${ratioDiff.toFixed(3)}`);
    
    if (Math.abs(ratioDiff) > 0.1) {
      if (ratioDiff > 0) {
        upScore += 2;
        console.log('資金流向UP (UP +2)');
      } else {
        downScore += 2;
        console.log('資金流向DOWN (DOWN +2)');
      }
    }
  }

  console.log(`🎯 最終得分 - UP: ${upScore}, DOWN: ${downScore}`);
  
  // 綜合決策 - 總是給出明確預測
  const scoreDiff = upScore - downScore;
  
  // 如果得分相同，使用資金流向作為決定性因素
  if (scoreDiff === 0) {
    const prediction = currentUpRatio > 0.5 ? 'UP' : 'DOWN';
    console.log(`⚖️  得分相同，使用資金流向決定: ${prediction}`);
    return prediction;
  }
  
  const prediction = scoreDiff > 0 ? 'UP' : 'DOWN';
  console.log(`✅ 預測: ${prediction}`);
  return prediction;
}

function calculateVolatility(values) {
  if (values.length < 2) return 0;
  
  const mean = values.reduce((a, b) => a + b) / values.length;
  const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
  return Math.sqrt(variance);
}

testMomentumStrategy();