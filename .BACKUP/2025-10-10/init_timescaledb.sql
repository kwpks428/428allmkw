-- 啟用 TimescaleDB 擴展
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ========================================
-- 核心表：局次資訊
-- ========================================
CREATE TABLE IF NOT EXISTS round (
    epoch BIGINT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  lock_time TIMESTAMPTZ NOT NULL,
  close_time TIMESTAMPTZ NOT NULL,
    lock_price NUMERIC(20, 8),
    close_price NUMERIC(20, 8),
  result TEXT,
    total_bet_amount NUMERIC(20, 8) NOT NULL,
    up_bet_amount NUMERIC(20, 8) NOT NULL,
    down_bet_amount NUMERIC(20, 8) NOT NULL,
    up_payout NUMERIC(20, 10),
    down_payout NUMERIC(20, 10),
    PRIMARY KEY (start_time, epoch)
);

-- 轉換為 Hypertable（時間序列優化）
SELECT create_hypertable('round', 'start_time', if_not_exists => TRUE);

-- 索引
CREATE INDEX IF NOT EXISTS idx_round_epoch ON round (epoch);

-- ========================================
-- 歷史下注表（永久保存）
-- ========================================
CREATE TABLE IF NOT EXISTS hisbet (
  tx_hash TEXT NOT NULL,
    epoch BIGINT NOT NULL,
  bet_time TIMESTAMPTZ NOT NULL,
  wallet_address TEXT NOT NULL,
  bet_direction TEXT NOT NULL,
    bet_amount NUMERIC(20, 8) NOT NULL,
  result TEXT,
    block_number BIGINT NOT NULL,
    PRIMARY KEY (bet_time, tx_hash)
);

-- 轉換為 Hypertable
SELECT create_hypertable('hisbet', 'bet_time', if_not_exists => TRUE);

-- 索引
CREATE INDEX IF NOT EXISTS idx_hisbet_wallet_epoch ON hisbet (wallet_address, epoch);
CREATE INDEX IF NOT EXISTS idx_hisbet_epoch_result ON hisbet (epoch, result);
CREATE INDEX IF NOT EXISTS idx_hisbet_epoch ON hisbet (epoch);

-- 地址必須小寫約束
ALTER TABLE hisbet 
  ADD CONSTRAINT check_hisbet_wallet_lowercase 
  CHECK (wallet_address = LOWER(wallet_address));

-- ========================================
-- 即時下注表（暫存，會被 hisbet.js 清理）
-- ========================================
CREATE TABLE IF NOT EXISTS realbet (
  tx_hash TEXT NOT NULL,
    epoch BIGINT NOT NULL,
  bet_time TIMESTAMPTZ NOT NULL,
  wallet_address TEXT NOT NULL,
  bet_direction TEXT NOT NULL,
    bet_amount NUMERIC(20, 8) NOT NULL,
    block_number BIGINT NOT NULL,
    PRIMARY KEY (bet_time, tx_hash)
);

-- 轉換為 Hypertable
SELECT create_hypertable('realbet', 'bet_time', if_not_exists => TRUE);

-- 索引
CREATE INDEX IF NOT EXISTS idx_realbet_epoch ON realbet (epoch);
CREATE UNIQUE INDEX IF NOT EXISTS idx_realbet_tx_hash_unique ON realbet (bet_time, tx_hash);

-- 地址必須小寫約束
ALTER TABLE realbet 
  ADD CONSTRAINT check_realbet_wallet_lowercase 
  CHECK (wallet_address = LOWER(wallet_address));

-- 自動清理策略（可選，7 天後自動刪除）
-- SELECT add_retention_policy('realbet', INTERVAL '7 days', if_not_exists => TRUE);

-- ========================================
-- 領獎記錄表
-- ========================================
CREATE TABLE IF NOT EXISTS claim (
    epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
  wallet_address TEXT NOT NULL,
    bet_epoch BIGINT NOT NULL,
    amount NUMERIC(20, 8) NOT NULL,
    PRIMARY KEY (block_number, wallet_address, bet_epoch)
);

-- 注意：claim 表以 block_number 為時間維度
SELECT create_hypertable('claim', 'block_number', if_not_exists => TRUE, chunk_time_interval => 1000000);

-- 索引
CREATE INDEX IF NOT EXISTS idx_claim_bet_epoch ON claim (bet_epoch);
CREATE INDEX IF NOT EXISTS idx_claim_user_bet_epoch ON claim (wallet_address, bet_epoch);

-- 地址必須小寫約束
ALTER TABLE claim 
  ADD CONSTRAINT check_claim_wallet_lowercase 
  CHECK (wallet_address = LOWER(wallet_address));

-- ========================================
-- 大額領獎表（識別大戶）
-- ========================================
CREATE TABLE IF NOT EXISTS multi_claim (
    epoch BIGINT NOT NULL,
  wallet_address TEXT NOT NULL,
    num_claimed_epochs INT NOT NULL,
    total_amount NUMERIC(20, 8) NOT NULL,
    PRIMARY KEY (epoch, wallet_address)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_multi_claim_addr_total ON multi_claim (wallet_address, total_amount DESC);
CREATE INDEX IF NOT EXISTS idx_multi_claim_total ON multi_claim (total_amount DESC) WHERE total_amount > 100;

-- 地址必須小寫約束
ALTER TABLE multi_claim 
  ADD CONSTRAINT check_multi_claim_wallet_lowercase 
  CHECK (wallet_address = LOWER(wallet_address));

-- ========================================
-- 已處理局次標記表
-- ========================================
CREATE TABLE IF NOT EXISTS finepoch (
    epoch BIGINT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_finepoch_processed ON finepoch (processed_at);

-- ========================================
-- 失敗局次記錄表（用於錯誤追蹤）
-- ========================================
CREATE TABLE IF NOT EXISTS failed_epochs (
    epoch BIGINT PRIMARY KEY,
    error_message TEXT NOT NULL,
  stage TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    retry_count INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_failed_epochs_retry ON failed_epochs (retry_count);
CREATE INDEX IF NOT EXISTS idx_failed_epochs_time ON failed_epochs (failed_at);

-- ========================================
-- 完成訊息
-- ========================================
DO $$ 
BEGIN 
    RAISE NOTICE '✅ TimescaleDB 表格初始化完成';
    RAISE NOTICE '- round: 局次資訊';
    RAISE NOTICE '- hisbet: 歷史下注（永久）';
    RAISE NOTICE '- realbet: 即時下注（暫存）';
    RAISE NOTICE '- claim: 領獎記錄';
    RAISE NOTICE '- multi_claim: 大額領獎';
    RAISE NOTICE '- finepoch: 已處理標記';
    RAISE NOTICE '- failed_epochs: 失敗記錄';
END $$;
