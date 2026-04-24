-- ============================================================
-- Stock Scoring & Allocation Model — Final Schema
-- ============================================================

CREATE TABLE positions (
    unit_id                SERIAL          PRIMARY KEY,
    isin                   VARCHAR(12)     NOT NULL,
    name                   VARCHAR(255)    NOT NULL,
    status                 VARCHAR(10)     NOT NULL DEFAULT 'active'
                                           CHECK (status IN ('active', 'free', 'sold')),
    num_shares             INTEGER         NOT NULL DEFAULT 0,
    total_investment       NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    allocation_green_count NUMERIC(18, 4),
    allocation_score       NUMERIC(18, 4),
    allocation_max_return  NUMERIC(18, 4),
    sell_ladder_pct        SMALLINT        NOT NULL DEFAULT 0
                                           CHECK (sell_ladder_pct IN (0, 25, 50, 75, 100)),
    bottom_fish_count      SMALLINT        NOT NULL DEFAULT 0
                                           CHECK (bottom_fish_count BETWEEN 0 AND 10),
    partial_sell_done      BOOLEAN         NOT NULL DEFAULT FALSE,
    last_update            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE stock_directives (
    directive_id           SERIAL          PRIMARY KEY,
    unit_id                INTEGER         NOT NULL
                                           REFERENCES positions (unit_id) ON DELETE RESTRICT,
    isin                   VARCHAR(12)     NOT NULL,
    name                   VARCHAR(255)    NOT NULL,
    date                   DATE            NOT NULL DEFAULT CURRENT_DATE,
    directive              VARCHAR(20)     NOT NULL
                                           CHECK (directive IN (
                                               'BUY', 'BUY-IN-BUY', 'PARTIAL SELL',
                                               'SELL L1', 'SELL L2', 'SELL L3', 'SELL L4',
                                               'BOTTOM FISHING BUY', 'STOPLOSS BUY', 'OLD BUY',
                                               'ADJ BUY', 'ADJ SELL'
                                           )),
    current_price          NUMERIC(18, 4)  NOT NULL,
    processed_at           TIMESTAMPTZ     DEFAULT NULL
);

CREATE TABLE scoring (
    score_id               SERIAL          PRIMARY KEY,
    unit_id                INTEGER         NOT NULL UNIQUE
                                           REFERENCES positions (unit_id) ON DELETE CASCADE,
    green_count            INTEGER         NOT NULL DEFAULT 0,
    score                  NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    max_return             NUMERIC(10, 4),
    score_rank             INTEGER,
    green_count_rank       INTEGER,
    return_rank            INTEGER,
    composite_rank         INTEGER
);

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

CREATE TABLE portfolio_state (
    state_id               SERIAL          PRIMARY KEY,
    total_capital          NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    total_invested         NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    buffer_available       NUMERIC(18, 4)  GENERATED ALWAYS AS
                           (total_capital - total_invested) STORED,
    last_rebalance_date    TIMESTAMPTZ
);

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