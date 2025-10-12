-- 創建預測快照表
CREATE TABLE IF NOT EXISTS prediction_snapshots (
    epoch BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- 跟隨最高勝率策略
    follow_best_prediction VARCHAR(10),
    follow_best_wallet VARCHAR(42),
    follow_best_win_rate_12 NUMERIC(5, 2),
    follow_best_win_rate_48 NUMERIC(5, 2),
    
    -- 反向最低勝率策略
    reverse_low_prediction VARCHAR(10),
    reverse_low_wallet VARCHAR(42),
    reverse_low_win_rate_12 NUMERIC(5, 2),
    reverse_low_win_rate_48 NUMERIC(5, 2),
    
    -- 動量策略
    momentum_prediction VARCHAR(10),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 創建索引
CREATE INDEX IF NOT EXISTS idx_prediction_snapshots_epoch ON prediction_snapshots(epoch DESC);
CREATE INDEX IF NOT EXISTS idx_prediction_snapshots_created ON prediction_snapshots(created_at DESC);

-- 添加約束：預測值只能是 UP 或 DOWN
ALTER TABLE prediction_snapshots 
ADD CONSTRAINT check_follow_best_prediction 
CHECK (follow_best_prediction IN ('UP', 'DOWN') OR follow_best_prediction IS NULL);

ALTER TABLE prediction_snapshots 
ADD CONSTRAINT check_reverse_low_prediction 
CHECK (reverse_low_prediction IN ('UP', 'DOWN') OR reverse_low_prediction IS NULL);

ALTER TABLE prediction_snapshots 
ADD CONSTRAINT check_momentum_prediction 
CHECK (momentum_prediction IN ('UP', 'DOWN') OR momentum_prediction IS NULL);

-- 錢包地址小寫約束
ALTER TABLE prediction_snapshots
ADD CONSTRAINT check_follow_best_wallet_lowercase
CHECK (follow_best_wallet = LOWER(follow_best_wallet) OR follow_best_wallet IS NULL);

ALTER TABLE prediction_snapshots
ADD CONSTRAINT check_reverse_low_wallet_lowercase
CHECK (reverse_low_wallet = LOWER(reverse_low_wallet) OR reverse_low_wallet IS NULL);
