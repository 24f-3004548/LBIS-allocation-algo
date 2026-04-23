-- ============================================================
-- Stock Scoring & Allocation Model — Final Schema
-- ============================================================

-- POSITIONS
-- The central anchor. Each row is an independent buy unit.
-- A stock (ISIN) can have multiple units simultaneously.
-- State resets to active + ladder/count/partial_sell reset
-- when unit re-enters after full exit (BFB/Stoploss/Old Buy).

CREATE TABLE positions (
    unit_id                SERIAL          PRIMARY KEY,
    isin                   VARCHAR(12)     NOT NULL,
    name                   VARCHAR(255)    NOT NULL,
    status                 VARCHAR(10)     NOT NULL DEFAULT 'active'
                                           CHECK (status IN ('active', 'free', 'sold')),
    num_shares             INTEGER         NOT NULL DEFAULT 0,
    total_investment       NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    -- Allocation outputs (computed at score time, updated on rebalance)
    allocation_green_count NUMERIC(18, 4),
    allocation_score       NUMERIC(18, 4),
    allocation_max_return  NUMERIC(18, 4),
    -- Sell ladder (resets to 0 on re-entry)
    -- Values: 0, 25, 50, 75, 100 — represents % of original holding sold via L1-L4
    sell_ladder_pct        SMALLINT        NOT NULL DEFAULT 0
                                           CHECK (sell_ladder_pct IN (0, 25, 50, 75, 100)),
    -- Bottom fishing count (resets to 0 on re-entry, max 10)
    bottom_fish_count      SMALLINT        NOT NULL DEFAULT 0
                                           CHECK (bottom_fish_count BETWEEN 0 AND 10),
    -- Partial sell flag (resets to false on re-entry)
    partial_sell_done      BOOLEAN         NOT NULL DEFAULT FALSE,
    last_update            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ============================================================

-- STOCK_DIRECTIVES
-- Intake table fed from the financial system.
-- isin + name carried here so a BUY directive for a new unit
-- is self-contained and used to seed the positions row.
-- For BUY: positions row is created first, then directive inserted.
-- For all other directives: unit_id is known upfront.

CREATE TABLE stock_directives (
    directive_id           SERIAL          PRIMARY KEY,
    unit_id                INTEGER         NOT NULL
                                           REFERENCES positions (unit_id) ON DELETE RESTRICT,
    isin                   VARCHAR(12)     NOT NULL,
    name                   VARCHAR(255)    NOT NULL,
    date                   DATE            NOT NULL DEFAULT CURRENT_DATE,
    directive              VARCHAR(20)     NOT NULL
                                           CHECK (directive IN (
                                               'BUY',
                                               'BUY-IN-BUY',
                                               'PARTIAL SELL',
                                               'SELL L1',
                                               'SELL L2',
                                               'SELL L3',
                                               'SELL L4',
                                               'BOTTOM FISHING BUY',
                                               'STOPLOSS BUY',
                                               'OLD BUY',
                                               'ADJ BUY',
                                               'ADJ SELL'
                                           )),
    current_price          NUMERIC(18, 4)  NOT NULL,
    processed_at           TIMESTAMPTZ     DEFAULT NULL
);

-- ============================================================

-- SCORING
-- One active row per unit. Created on BUY/BUY-IN-BUY.
-- Deleted when unit is fully sold. Recreated fresh if unit re-enters.
-- Ranks are relative to all active units at time of scoring/rebalance
-- and must be recalculated across all units on every rebalance day.

CREATE TABLE scoring (
    score_id               SERIAL          PRIMARY KEY,
    unit_id                INTEGER         NOT NULL UNIQUE
                                           REFERENCES positions (unit_id) ON DELETE CASCADE,
    -- Raw scoring values
    green_count            INTEGER         NOT NULL DEFAULT 0,
    score                  NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    max_return             NUMERIC(10, 4),
                                           -- (3y target price - current price) / current price
    -- Individual criterion ranks (1 = best)
    score_rank             INTEGER,
    green_count_rank       INTEGER,
    return_rank            INTEGER,
    -- Composite rank = score_rank + return_rank, sorted ascending
    composite_rank         INTEGER
);

-- ============================================================

-- LEDGER
-- Immutable delta log. One row per transaction.
-- delta_shares and delta_investment are signed:
--   positive = buy / increase
--   negative = sell / decrease
-- Full position history reconstructable by summing deltas per unit_id.

CREATE TABLE ledger (
    ledger_id              SERIAL          PRIMARY KEY,
    unit_id                INTEGER         NOT NULL
                                           REFERENCES positions (unit_id) ON DELETE RESTRICT,
    directive_id           INTEGER         NOT NULL
                                           REFERENCES stock_directives (directive_id) ON DELETE RESTRICT,
    delta_shares           INTEGER         NOT NULL,
    delta_investment       NUMERIC(18, 4)  NOT NULL,
    date                   DATE            NOT NULL DEFAULT CURRENT_DATE
);

-- ============================================================

-- PORTFOLIO_STATE
-- Single-row portfolio snapshot updated after every rebalance.
-- buffer_available is computed automatically and always in sync.

CREATE TABLE portfolio_state (
    state_id               SERIAL          PRIMARY KEY,
    total_capital          NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    total_invested         NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    buffer_available       NUMERIC(18, 4)  GENERATED ALWAYS AS
                           (total_capital - total_invested) STORED,
    last_rebalance_date    TIMESTAMPTZ
);

-- ============================================================

-- ENGINE_ERRORS
-- Error log for failed directives (buy/sell).
-- Each row = one failed directive attempt.
-- Retries: attempt + 1 on each insert; max 10 attempts.
-- Resolved = false until error is manually cleared.
-- No cascade-delete from positions/directives — errors persist for audit.

CREATE TABLE IF NOT EXISTS engine_errors (
    error_id        SERIAL          PRIMARY KEY,
    directive_id    INTEGER         REFERENCES stock_directives (directive_id) ON DELETE SET NULL,
    unit_id         INTEGER         REFERENCES positions (unit_id) ON DELETE SET NULL,
    directive_type  VARCHAR(20),
    error_message   TEXT            NOT NULL,
    traceback       TEXT,
    attempt         SMALLINT        NOT NULL DEFAULT 1,
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ============================================================

-- STOCKS VIEW
-- Live aggregate of all units grouped by ISIN.
-- Always in sync with positions — no duplication risk.
-- current_total_value uses the most recent directive price per unit.
-- Use this anywhere you need a stock-level (not unit-level) view.

CREATE VIEW stocks AS
WITH latest_price AS (
    SELECT DISTINCT ON (unit_id)
        unit_id,
        current_price
    FROM stock_directives
    ORDER BY unit_id, date DESC, directive_id DESC
)
SELECT
    p.isin,
    MAX(p.name)                                             AS name,
    COUNT(*)                                                AS total_units,
    COUNT(*) FILTER (WHERE p.status = 'active')             AS active_units,
    COUNT(*) FILTER (WHERE p.status = 'free')               AS free_units,
    COUNT(*) FILTER (WHERE p.status = 'sold')               AS sold_units,
    SUM(p.num_shares)                                       AS total_shares,
    SUM(p.total_investment)                                 AS total_investment,
    SUM(p.num_shares * lp.current_price)                    AS current_total_value
FROM positions p
LEFT JOIN latest_price lp ON lp.unit_id = p.unit_id
GROUP BY p.isin;

-- ============================================================
-- Indexes
-- ============================================================

CREATE INDEX idx_positions_isin        ON positions (isin);
CREATE INDEX idx_positions_status      ON positions (status);
CREATE INDEX idx_directives_unit       ON stock_directives (unit_id);
CREATE INDEX idx_directives_isin       ON stock_directives (isin);
CREATE INDEX idx_directives_date       ON stock_directives (date);
CREATE INDEX idx_directives_directive  ON stock_directives (directive);
CREATE INDEX idx_ledger_unit           ON ledger (unit_id);
CREATE INDEX idx_ledger_directive      ON ledger (directive_id);
CREATE INDEX idx_ledger_date           ON ledger (date);
CREATE INDEX idx_scoring_composite     ON scoring (composite_rank);
CREATE INDEX idx_engine_errors_directive ON engine_errors (directive_id);
CREATE INDEX idx_engine_errors_unit      ON engine_errors (unit_id);
CREATE INDEX idx_engine_errors_resolved  ON engine_errors (resolved);