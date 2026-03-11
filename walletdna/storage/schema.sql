-- ─────────────────────────────────────────────────────────────────────────────
-- WalletDNA Database Schema
-- TimescaleDB (PostgreSQL 15+)
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ─── Enums ───────────────────────────────────────────────────────────────────

CREATE TYPE chain_type AS ENUM ('ethereum', 'tron', 'dogecoin');
CREATE TYPE wallet_class AS ENUM ('BOT', 'LIKELY_BOT', 'LIKELY_HUMAN', 'HUMAN', 'UNKNOWN');
CREATE TYPE tx_direction AS ENUM ('in', 'out', 'self');

-- ─── Wallets ─────────────────────────────────────────────────────────────────

CREATE TABLE wallets (
    id              BIGSERIAL PRIMARY KEY,
    address         VARCHAR(100)    NOT NULL,
    chain           chain_type      NOT NULL,
    label           VARCHAR(255),                   -- e.g. "BDAG Presale Collector #3"
    is_target       BOOLEAN         DEFAULT FALSE,  -- TRUE = BDAG destination wallet
    is_sender       BOOLEAN         DEFAULT FALSE,  -- TRUE = your own wallet
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ,
    tx_count        INTEGER         DEFAULT 0,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE(address, chain)
);

CREATE INDEX idx_wallets_address ON wallets(address);
CREATE INDEX idx_wallets_chain ON wallets(chain);
CREATE INDEX idx_wallets_is_target ON wallets(is_target);

-- ─── Raw Transactions ────────────────────────────────────────────────────────
-- Hypertable — partitioned by time for efficient range queries

CREATE TABLE transactions (
    id              BIGSERIAL,
    wallet_id       BIGINT          NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    tx_hash         VARCHAR(100)    NOT NULL,
    chain           chain_type      NOT NULL,
    block_number    BIGINT,
    block_time      TIMESTAMPTZ     NOT NULL,
    from_address    VARCHAR(100)    NOT NULL,
    to_address      VARCHAR(100)    NOT NULL,
    direction       tx_direction    NOT NULL,
    value_native    NUMERIC(36,18)  NOT NULL DEFAULT 0,  -- in chain's native unit
    value_usd       NUMERIC(20,4),                       -- USD at time of tx
    fee_native      NUMERIC(36,18),
    fee_usd         NUMERIC(20,4),
    gas_price       NUMERIC(36,18),                      -- ETH only
    gas_used        BIGINT,                              -- ETH only
    gas_limit       BIGINT,                              -- ETH only
    energy_used     BIGINT,                              -- TRX only
    bandwidth_used  BIGINT,                              -- TRX only
    is_contract_call BOOLEAN        DEFAULT FALSE,
    contract_method VARCHAR(20),                         -- 4-byte selector
    token_symbol    VARCHAR(20),                         -- USDT, etc.
    confirmation_blocks INTEGER,
    raw_data        JSONB,                               -- full chain response
    ingested_at     TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (id, block_time)
);

-- Convert to hypertable — partition by month
SELECT create_hypertable('transactions', 'block_time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

CREATE INDEX idx_tx_wallet_id     ON transactions(wallet_id, block_time DESC);
CREATE INDEX idx_tx_hash          ON transactions(tx_hash);
CREATE INDEX idx_tx_from          ON transactions(from_address, block_time DESC);
CREATE INDEX idx_tx_to            ON transactions(to_address, block_time DESC);
CREATE INDEX idx_tx_chain_time    ON transactions(chain, block_time DESC);

-- ─── DNA Profiles ─────────────────────────────────────────────────────────────

CREATE TABLE dna_profiles (
    id                  BIGSERIAL PRIMARY KEY,
    wallet_id           BIGINT          NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    generated_at        TIMESTAMPTZ     DEFAULT NOW(),

    -- Raw feature values
    gas_mean_gwei       NUMERIC(20,6),
    gas_std_gwei        NUMERIC(20,6),
    gas_percentile_50   NUMERIC(20,6),
    gas_percentile_95   NUMERIC(20,6),

    active_hour_start   SMALLINT,           -- UTC hour 0-23
    active_hour_end     SMALLINT,
    timing_entropy      NUMERIC(6,4),       -- 0=predictable, 1=random
    median_interval_sec INTEGER,            -- median seconds between txs
    sleep_gap_hours     NUMERIC(8,2),       -- longest dormancy gap

    value_herfindahl    NUMERIC(6,4),       -- 0=diverse, 1=single dominant
    value_fragmentation VARCHAR(20),        -- HIGH/MED/LOW
    round_number_ratio  NUMERIC(6,4),       -- % of txs with round USD values

    contract_dex_ratio  NUMERIC(6,4),
    contract_bridge_ratio NUMERIC(6,4),
    contract_eoa_ratio  NUMERIC(6,4),
    top_contract_type   VARCHAR(50),

    mempool_avg_wait_blocks NUMERIC(8,2),
    mempool_instant_ratio   NUMERIC(6,4),   -- confirmed in <=2 blocks

    burst_score         NUMERIC(6,4),       -- 0=steady, 1=extreme bursts
    dormancy_score      NUMERIC(6,4),

    -- Classification
    wallet_class        wallet_class        DEFAULT 'UNKNOWN',
    bot_confidence      NUMERIC(6,4),       -- 0.0 - 1.0
    bot_signals         TEXT[],             -- which signals triggered

    -- DNA string (human-readable)
    dna_string          TEXT,               -- G:MED-STABLE | T:... | ...
    dna_vector          NUMERIC(20,6)[],    -- numeric vector for similarity

    -- Metadata
    tx_count_analysed   INTEGER,
    analysis_window_days INTEGER,
    confidence_score    NUMERIC(6,4),       -- overall profile confidence
    notes               TEXT,

    UNIQUE(wallet_id, generated_at)
);

CREATE INDEX idx_dna_wallet_id    ON dna_profiles(wallet_id);
CREATE INDEX idx_dna_generated_at ON dna_profiles(generated_at DESC);
CREATE INDEX idx_dna_class        ON dna_profiles(wallet_class);

-- ─── Similarity Results ───────────────────────────────────────────────────────

CREATE TABLE similarity_results (
    id              BIGSERIAL PRIMARY KEY,
    wallet_a_id     BIGINT      NOT NULL REFERENCES wallets(id),
    wallet_b_id     BIGINT      NOT NULL REFERENCES wallets(id),
    similarity      NUMERIC(6,4) NOT NULL,   -- 0.0 - 1.0
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(wallet_a_id, wallet_b_id)
);

CREATE INDEX idx_sim_wallet_a     ON similarity_results(wallet_a_id, similarity DESC);
CREATE INDEX idx_sim_wallet_b     ON similarity_results(wallet_b_id, similarity DESC);
CREATE INDEX idx_sim_score        ON similarity_results(similarity DESC);

-- ─── Clusters ────────────────────────────────────────────────────────────────

CREATE TABLE clusters (
    id              BIGSERIAL PRIMARY KEY,
    cluster_label   VARCHAR(100),
    wallet_ids      BIGINT[]        NOT NULL,
    avg_similarity  NUMERIC(6,4),
    dominant_class  wallet_class,
    notes           TEXT,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- ─── Ingestion Log ────────────────────────────────────────────────────────────

CREATE TABLE ingestion_log (
    id              BIGSERIAL PRIMARY KEY,
    wallet_id       BIGINT          REFERENCES wallets(id),
    chain           chain_type,
    status          VARCHAR(20)     NOT NULL,   -- success | error | rate_limited
    tx_count        INTEGER         DEFAULT 0,
    error_message   TEXT,
    duration_ms     INTEGER,
    started_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- ─── Seed: Your Known Wallets ─────────────────────────────────────────────────

INSERT INTO wallets (address, chain, label, is_sender) VALUES
    ('0xD038A997444Db594BBE62AAad8B4735584D8db2d', 'ethereum', 'Purchased Wallet (Primary)', TRUE),
    ('0x3B18DD8653EddC873FcFE4601353b5DCAe4Ac85D', 'ethereum', 'Large DoubleUp Wallet',      TRUE),
    ('0xb4Bf4E2168b8cbEdE6B7ea5eb2334C988d47D0e1', 'ethereum', 'Medium Size Wallet',         TRUE),
    ('0xD86a53FEDFACCBA2e080C0Ea1DD831E0FCEacd90', 'ethereum', 'Tiny Wasted Wallet',         TRUE)
ON CONFLICT (address, chain) DO NOTHING;

-- ─── Useful Views ─────────────────────────────────────────────────────────────

CREATE VIEW v_latest_dna AS
    SELECT DISTINCT ON (wallet_id)
        w.address,
        w.chain,
        w.label,
        w.is_target,
        d.dna_string,
        d.wallet_class,
        d.bot_confidence,
        d.confidence_score,
        d.tx_count_analysed,
        d.generated_at
    FROM dna_profiles d
    JOIN wallets w ON w.id = d.wallet_id
    ORDER BY wallet_id, generated_at DESC;

CREATE VIEW v_cluster_summary AS
    SELECT
        c.id,
        c.cluster_label,
        array_length(c.wallet_ids, 1) AS wallet_count,
        c.avg_similarity,
        c.dominant_class,
        c.created_at
    FROM clusters c
    ORDER BY c.avg_similarity DESC;
