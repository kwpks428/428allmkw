const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

// 完全複製 backtest.js 中的動量策略邏輯
async function backtestMomentumStrategy(startEpoch, rounds = 48) {
  const results = {
    strategy: 'momentum',
    total: 0,
    wins: 0,
    losses: 0,
    winRate: 0,
    details: [] // 添加詳細記錄
  };

  console.log(`🔍 開始驗證動量策略，起始局次: ${startEpoch}, 回測 ${rounds} 局`);
  console.log('='.repeat(80));

  for (let i = 0; i < rounds; i++) {
    const epoch = startEpoch - i;
    console.log(`\n📍 第 ${i+1}/${rounds} 局，局次: ${epoch}`);

    try {
      // 獲取該局的基本信息
      const currentRound = await pool.query(`
        SELECT result, up_bet_amount, down_bet_amount, total_bet_amount, lock_price, close_price
        FROM round WHERE epoch = $1
      `, [epoch]);

      if (currentRound.rows.length === 0) {
        console.log(`❌ 跳過：找不到局次 ${epoch} 的資料`);
        continue;
      }
      
      const round = currentRound.rows[0];
      console.log(`💰 實際結果: ${round.result}`);
      console.log(`📊 下注金額 - UP: ${round.up_bet_amount}, DOWN: ${round.down_bet_amount}`);

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
        console.log(`❌ 跳過：歷史數據不足 (${trendAnalysis.rows.length} < 3)`);
        continue; 
      }

      console.log(`📈 歷史趨勢數據 (${trendAnalysis.rows.length} 局):`);
      trendAnalysis.rows.forEach((r, idx) => {
        console.log(`   ${idx+1}. 局次 ${r.epoch}: ${r.result}, UP比例: ${(r.up_ratio * 100).toFixed(1)}%`);
      });

      // 計算動量指標
      const prediction = calculateMomentumPrediction(trendAnalysis.rows, round, epoch);
      
      results.total++;
      const actualResult = round.result;
      const isCorrect = prediction === actualResult;
      
      if (isCorrect) {
        results.wins++;
        console.log(`✅ 預測正確！`);
      } else {
        results.losses++;
        console.log(`❌ 預測錯誤！`);
      }

      // 記錄詳細信息
      results.details.push({
        epoch,
        prediction,
        actual: actualResult,
        correct: isCorrect,
        upRatio: parseFloat(round.up_bet_amount) / parseFloat(round.total_bet_amount)
      });

    } catch (error) {
      console.error(`💥 局次 ${epoch} 計算錯誤:`, error.message);
      continue;
    }
  }

  results.winRate = results.total > 0 ? (results.wins / results.total * 100).toFixed(2) : 0;
  
  console.log('\n' + '='.repeat(80));
  console.log(`📊 最終統計:`);
  console.log(`   總局次: ${results.total}`);
  console.log(`   勝場: ${results.wins}`);  
  console.log(`   敗場: ${results.losses}`);
  console.log(`   勝率: ${results.winRate}%`);
  console.log('='.repeat(80));

  return results;
}

function calculateMomentumPrediction(historicalData, currentRound, epoch) {
  let upScore = 0;
  let downScore = 0;
  
  console.log(`🧠 動量分析 (局次 ${epoch}):`);

  // 特徵1: 連續趨勢分析
  const recentResults = historicalData.map(r => r.result).slice(0, 3);
  console.log(`   📈 近期結果: [${recentResults.join(', ')}]`);
  
  const upStreak = recentResults.filter(r => r === 'UP').length;
  const downStreak = recentResults.filter(r => r === 'DOWN').length;
  
  console.log(`   🔄 UP連勝: ${upStreak}, DOWN連勝: ${downStreak}`);
  
  // 趨勢延續 vs 趨勢反轉判斷
  if (upStreak >= 2) {
    if (upStreak >= 3) {
      downScore += 2;
      console.log(`   ⬇️  強反轉信號 (DOWN +2)`);
    } else {
      upScore += 1;
      console.log(`   ⬆️  弱延續信號 (UP +1)`);
    }
  }
  if (downStreak >= 2) {
    if (downStreak >= 3) {
      upScore += 2;
      console.log(`   ⬆️  強反轉信號 (UP +2)`);
    } else {
      downScore += 1;
      console.log(`   ⬇️  弱延續信號 (DOWN +1)`);
    }
  }

  // 特徵2: 資金流動分析
  const currentUpRatio = parseFloat(currentRound.up_bet_amount) / parseFloat(currentRound.total_bet_amount);
  const historicalUpRatios = historicalData.map(r => parseFloat(r.up_ratio)).filter(r => !isNaN(r));
  
  console.log(`   💰 當前UP比例: ${(currentUpRatio * 100).toFixed(1)}%`);
  
  if (historicalUpRatios.length > 0) {
    const avgHistoricalUpRatio = historicalUpRatios.reduce((a, b) => a + b) / historicalUpRatios.length;
    console.log(`   📊 歷史平均UP比例: ${(avgHistoricalUpRatio * 100).toFixed(1)}%`);
    
    const ratioDiff = currentUpRatio - avgHistoricalUpRatio;
    console.log(`   📏 比例差異: ${(ratioDiff * 100).toFixed(1)}%`);
    
    if (Math.abs(ratioDiff) > 0.1) {
      if (ratioDiff > 0) {
        upScore += 2;
        console.log(`   💹 資金流向UP (UP +2)`);
      } else {
        downScore += 2;
        console.log(`   💸 資金流向DOWN (DOWN +2)`);
      }
    }
  }

  console.log(`   🎯 得分統計 - UP: ${upScore}, DOWN: ${downScore}`);
  
  // 綜合決策
  const scoreDiff = upScore - downScore;
  
  let prediction;
  if (scoreDiff === 0) {
    prediction = currentUpRatio > 0.5 ? 'UP' : 'DOWN';
    console.log(`   ⚖️  得分相同，使用資金流向決定: ${prediction}`);
  } else {
    prediction = scoreDiff > 0 ? 'UP' : 'DOWN';
    console.log(`   🎲 最終預測: ${prediction}`);
  }
  
  return prediction;
}

async function main() {
  try {
    console.log('🔬 動量策略完整驗證');
    console.log('====================');
    
    // 使用與 backtest.js 相同的參數
    const currentEpoch = 419135;
    const startEpoch = currentEpoch - 2; // 419133
    
    const results = await backtestMomentumStrategy(startEpoch, 48);
    
    console.log('\n📋 前10局詳細結果:');
    results.details.slice(0, 10).forEach((detail, idx) => {
      const status = detail.correct ? '✅' : '❌';
      console.log(`${status} 局次 ${detail.epoch}: 預測 ${detail.prediction}, 實際 ${detail.actual}`);
    });
    
  } catch (error) {
    console.error('💥 驗證失敗:', error);
  } finally {
    await pool.end();
  }
}

main();